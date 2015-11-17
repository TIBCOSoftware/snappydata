package org.apache.spark.sql.columntable

import java.nio.ByteBuffer
import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.SpecificMutableRow
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.collection.{UUIDRegionKey, Utils}
import org.apache.spark.sql.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.columnar.{ConnectionType, ColumnAccessor, CachedBatch, ColumnType, ColumnarRelationProvider, ExternalStoreUtils, JDBCAppendableRelation}
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, JdbcUtils}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.row.{GemFireXDDialect, JDBCMutableRelation}
import org.apache.spark.sql.rowtable.RowFormatScanRDD
import org.apache.spark.sql.sources.{Filter, JdbcExtendedDialect, JdbcExtendedUtils, RowInsertableRelation}
import org.apache.spark.sql.store.ExternalStore
import org.apache.spark.sql.store.impl.JDBCSourceAsColumnarStore
import org.apache.spark.sql.store.util.StoreUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SQLContext, SaveMode, SnappyContext}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{Logging, Partition, SparkContext}

/**
 * Created by rishim on 29/10/15.
 * This class acts as a DataSource provider for column format tables provided Snappy. It uses GemFireXD as actual datastore to physically locate the tables.
 * Column tables can be used for storing data in columnar compressed format.
 * A example usage is given below.
 *
 * val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createExternalTable(tableName, "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    This provider scans underlying tables in parallel and is aware of the data partition.
    It does not introduces a shuffle if simple table query is fired.
    One can insert a single or multiple rows into this table as well as do a bulk insert by a Spark DataFrame.
    Bulk insert example is shown above.

 */
class ColumnFormatRelation(
    override val url: String,
    override val table: String,
    override val provider: String,
    override val mode: SaveMode,
    userSchema: StructType,
    schemaExtensions: String,
    ddlExtensionForShadowTable: String,
    parts: Array[Partition],
    _poolProps: Map[String, String],
    override val connProperties: Properties,
    override val hikariCP: Boolean,
    override val origOptions: Map[String, String],
    override val externalStore: ExternalStore,
    blockMap: Map[InternalDistributedMember, BlockManagerId],
    @transient override val sqlContext: SQLContext
    )
    (private var uuidList: ArrayBuffer[RDD[UUIDRegionKey]] = new ArrayBuffer[RDD[UUIDRegionKey]]()
        )
    extends JDBCAppendableRelation(url, table, provider, mode, userSchema, parts, _poolProps, connProperties, hikariCP, origOptions, externalStore, sqlContext)()
    with RowInsertableRelation {

  lazy val connectionType = ExternalStoreUtils.getConnectionType(url)

  lazy val connFunctor = ColumnFormatRelation.getConnector(table, driver, _poolProps,
    connProperties, hikariCP)

  val rowInsertStr = ColumnFormatRelation.getInsertString(table, userSchema)

  // TODO: Suranjan currently doesn't apply any filters.
  // will see that later.
  override def buildScan(requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {
    val requestedColumns = if (requiredColumns.isEmpty) {
      val narrowField =
        schema.fields.minBy { a =>
          ColumnType(a.dataType).defaultSize
        }

      Array(narrowField.name)
    } else {
      requiredColumns
    }

    val cachedColumnBuffers: RDD[CachedBatch] = readLock {
      externalStore.getCachedBatchRDD(table, requestedColumns.map(column => columnPrefix + column), uuidList,
        sqlContext.sparkContext)
    }

    val outputTypes = requestedColumns.map { a => schema(a) }
    //val converter = outputTypes.map(CatalystTypeConverters.createToScalaConverter)
    val converter = CatalystTypeConverters.createToScalaConverter(StructType(outputTypes))

    val colRDD = cachedColumnBuffers.mapPartitions { cachedBatchIterator =>
      // Find the ordinals and data types of the requested columns.  If none are requested, use the
      // narrowest (the field with minimum default element size).
      val (requestedColumnIndices, requestedColumnDataTypes) = requestedColumns.map { a =>
        schema.getFieldIndex(a).get -> schema(a).dataType
      }.unzip
      val nextRow = new SpecificMutableRow(requestedColumnDataTypes)
      def cachedBatchesToRows(cacheBatches: Iterator[CachedBatch]): Iterator[Row] = {
        val rows = cacheBatches.flatMap { cachedBatch =>
          // Build column accessors
          val columnAccessors = requestedColumnIndices.zipWithIndex.map { case(schemaIndex, bufferIndex) =>
            ColumnAccessor(
              schema.fields(schemaIndex).dataType,
              ByteBuffer.wrap(cachedBatch.buffers(bufferIndex)))
          }
          // Extract rows via column accessors
          new Iterator[InternalRow] {
            private[this] val rowLen = nextRow.numFields

            override def next(): InternalRow = {
              var i = 0
              while (i < rowLen) {
                columnAccessors(i).extractTo(nextRow, i)
                i += 1
              }
              if (requiredColumns.isEmpty) InternalRow.empty else nextRow
            }

            override def hasNext: Boolean = columnAccessors(0).hasNext
          }
        }
        rows.map(converter(_).asInstanceOf[Row])
      }
      cachedBatchesToRows(cachedBatchIterator)
    }

    colRDD.union(connectionType match {
      case ConnectionType.Embedded =>
        new RowFormatScanRDD(
          sqlContext.sparkContext,
          connFunctor,
          JDBCMutableRelation.pruneSchema(schemaFields, requiredColumns),
          table,
          requiredColumns,
          filters,
          parts,
          blockMap,
          connProperties
        ).map(converter(_).asInstanceOf[Row])

      case _ => super.buildScan(requiredColumns, filters)
    })
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    insert(data, if (overwrite) SaveMode.Overwrite else SaveMode.Append)
  }

  def insert(data: DataFrame, mode: SaveMode): Unit = {
    if (mode == SaveMode.Overwrite) {
      sqlContext.asInstanceOf[SnappyContext].truncateExternalTable(table)
    }
    JdbcUtils.saveTable(data, url, table, connProperties)
  }

  /**
   * Insert a sequence of rows into the table represented by this relation.
   *
   * @param rows the rows to be inserted
   *
   * @return number of rows inserted
   */
  override def insert(rows: Seq[Row]): Int = {
    val numRows = rows.length
    if (numRows == 0) {
      throw new IllegalArgumentException(
        "JDBCAppendableRelation.insert: no rows provided")
    }
    val connection = ConnectionPool.getPoolConnection(table,
      _poolProps, connProperties, hikariCP)
    try {
      val stmt = connection.prepareStatement(rowInsertStr)
      if (numRows > 1) {
        for (row <- rows) {
          ColumnFormatRelation.setStatementParameters(stmt, userSchema.fields,
            row, dialect)
          stmt.addBatch()
        }
      } else {
        ColumnFormatRelation.setStatementParameters(stmt, userSchema.fields,
          rows.head, dialect)
      }
      val result = stmt.executeUpdate()
      stmt.close()
      result
    } finally {
      connection.close()
    }
  }

  // truncate both actual and shadow table
  override def truncate() = writeLock {
    val dialect = JdbcDialects.get(externalStore.url)
    externalStore.tryExecute(table + shadowTableNamePrefix, {
      case conn =>
        JdbcExtendedUtils.truncateTable(conn, table + shadowTableNamePrefix, dialect)
    })
    externalStore.tryExecute(table, {
      case conn =>
        JdbcExtendedUtils.truncateTable(conn, table, dialect)
    })
    uuidList.clear()
  }

  override def dropTable(tableName: String, ifExists: Boolean): Unit = {

    val dialect = JdbcDialects.get(externalStore.url)
    externalStore.tryExecute(tableName + shadowTableNamePrefix, {
      case conn =>
        JdbcExtendedUtils.dropTable(conn, tableName + shadowTableNamePrefix, dialect, sqlContext,
          ifExists)
    })

    externalStore.tryExecute(tableName, {
      case conn =>
        JdbcExtendedUtils.dropTable(conn, tableName, dialect, sqlContext,
          ifExists)
    })
  }

  override def createTable(mode: SaveMode): Unit = {
    var conn: Connection = null
    val dialect = JdbcDialects.get(url)
    try {
      conn = JdbcUtils.createConnection(url, connProperties)
      val tableExists = JdbcExtendedUtils.tableExists(conn, table,
        dialect, sqlContext)
      if (mode == SaveMode.Ignore && tableExists) {
        return
      }

      if (mode == SaveMode.ErrorIfExists && tableExists) {
        sys.error(s"Table $table already exists.")
      }
    }
    createActualTable(table, externalStore)
  }

  private def createExternalTableForCachedBatches(tableName: String,
      externalStore: ExternalStore): Unit = {
    require(tableName != null && tableName.length > 0,
      "createExternalTableForCachedBatches: expected non-empty table name")

    val (primarykey, partitionStrategy) = dialect match {
      // The driver if not a loner should be an accessor only
      case d: JdbcExtendedDialect =>
        (s"constraint ${tableName}_bucketCheck check (bucketId != -1), " +
            "primary key (uuid, bucketId) ", d.getPartitionByClause("bucketId"))
      case _ => ("primary key (uuid)", "") //TODO. How to get primary key contraint from each DB
    }
    val colocationClause = s"COLOCATE WITH (${table})"

    createTable(externalStore, s"create table $tableName (uuid varchar(36) " +
        "not null, bucketId integer, stats blob, " +
        userSchema.fields.map(structField => columnPrefix + structField.name + " blob").mkString(" ", ",", " ") +
        s", $primarykey) $partitionStrategy $colocationClause $ddlExtensionForShadowTable", tableName, dropIfExists = false)
  }


  def createActualTable(tableName: String, externalStore: ExternalStore) = {
    // Create the table if the table didn't exist.
    var conn: Connection = null
    try {
      conn = JdbcUtils.createConnection(url, connProperties)
      val tableExists = JdbcExtendedUtils.tableExists(conn, table,
        dialect, sqlContext)
      if (!tableExists) {
        var partitionByClause = ""
        if (!schemaExtensions.contains(" PARTITION ")) {
          // create partition by extension with all the columns
          partitionByClause = dialect match {
            case d: JdbcExtendedDialect =>
              d.getPartitionByClause(s"${schemaFields.keys.mkString(", ")}")
          }
        }

        val sql = s"CREATE TABLE ${tableName} $schemaExtensions $partitionByClause"
        logInfo("Applying DDL : " + sql)
        JdbcExtendedUtils.executeUpdate(sql, conn)
        dialect match {
          case d: JdbcExtendedDialect => d.initializeTable(tableName, conn)
        }
        createExternalTableForCachedBatches(tableName + shadowTableNamePrefix, externalStore)
      }
    }
    catch {
      case sqle: java.sql.SQLException =>
        if (sqle.getMessage.contains("No suitable driver found")) {
          throw new AnalysisException(s"${sqle.getMessage}\n" +
              "Ensure that the 'driver' option is set appropriately and " +
              "the driver jars available (--jars option in spark-submit).")
        } else {
          throw sqle
        }
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
  }
}


object ColumnFormatRelation extends Logging with StoreCallback {
  // register the call backs with the JDBCSource so that
  // bucket region can insert into the column table

  def registerStoreCallbacks(table: String, userSchema: StructType, externalStore: ExternalStore) = {
    // if already registered don't register
    StoreCallbacksImpl.registerExternalStoreAndSchema(table.toUpperCase, userSchema, externalStore)
  }

  def getInsertString(table: String, userSchema: StructType) = {
    val sb = new mutable.StringBuilder("INSERT INTO ")
    sb.append(table).append(" VALUES (")
    (1 until userSchema.length).foreach(sb.append("?,"))
    sb.append("?)").toString()
  }

  def setStatementParameters(stmt: PreparedStatement,
      columns: Array[StructField], row: Row, dialect: JdbcDialect): Unit = {
    var col = 0
    val len = columns.length
    while (col < len) {
      val dataType = columns(col).dataType
      if (!row.isNullAt(col)) {
        dataType match {
          case IntegerType => stmt.setInt(col + 1, row.getInt(col))
          case LongType => stmt.setLong(col + 1, row.getLong(col))
          case DoubleType => stmt.setDouble(col + 1, row.getDouble(col))
          case FloatType => stmt.setFloat(col + 1, row.getFloat(col))
          case ShortType => stmt.setInt(col + 1, row.getShort(col))
          case ByteType => stmt.setInt(col + 1, row.getByte(col))
          case BooleanType => stmt.setBoolean(col + 1, row.getBoolean(col))
          case StringType => stmt.setString(col + 1, row.getString(col))
          case BinaryType =>
            stmt.setBytes(col + 1, row(col).asInstanceOf[Array[Byte]])
          case TimestampType => row(col) match {
            case ts: java.sql.Timestamp => stmt.setTimestamp(col + 1, ts)
            case s: String => stmt.setString(col + 1, s)
            case o => stmt.setObject(col + 1, o)
          }
          case DateType => row(col) match {
            case d: java.sql.Date => stmt.setDate(col + 1, d)
            case s: String => stmt.setString(col + 1, s)
            case o => stmt.setObject(col + 1, o)
          }
          case d: DecimalType =>
            row(col) match {
              case d: Decimal => stmt.setBigDecimal(col + 1, d.toJavaBigDecimal)
              case bd: java.math.BigDecimal => stmt.setBigDecimal(col + 1, bd)
              case s: String => stmt.setString(col + 1, s)
              case o => stmt.setObject(col + 1, o)
            }
          case _ => stmt.setObject(col + 1, row(col))
        }
      } else {
        stmt.setNull(col + 1, getJDBCType(dialect, dataType))
      }
      col += 1
    }
  }

  def getJDBCType(dialect: JdbcDialect, dataType: DataType) = {
    dialect.getJDBCType(dataType).map(_.jdbcNullType).getOrElse(
      dataType match {
        case IntegerType => java.sql.Types.INTEGER
        case LongType => java.sql.Types.BIGINT
        case DoubleType => java.sql.Types.DOUBLE
        case FloatType => java.sql.Types.REAL
        case ShortType => java.sql.Types.INTEGER
        case ByteType => java.sql.Types.INTEGER
        // need to keep below mapping to BIT instead of BOOLEAN for MySQL
        case BooleanType => java.sql.Types.BIT
        case StringType => java.sql.Types.CLOB
        case BinaryType => java.sql.Types.BLOB
        case TimestampType => java.sql.Types.TIMESTAMP
        case DateType => java.sql.Types.DATE
        case d: DecimalType => java.sql.Types.DECIMAL
        case NullType => java.sql.Types.NULL
        case _ => throw new IllegalArgumentException(
          s"Can't translate to JDBC value for type $dataType")
      })
  }

  def getConnector(id: String, driver: String, poolProps: Map[String, String],
      connProps: Properties, hikariCP: Boolean): () => Connection = {
    () => {
      try {
        if (driver != null) DriverRegistry.register(driver)
      } catch {
        case cnfe: ClassNotFoundException =>

          logWarning(s"Couldn't find driver class $driver", cnfe)
      }
      ConnectionPool.getPoolConnection(id, poolProps, connProps, hikariCP)
    }
  }

  /**
   * Prune all but the specified columns from the specified Catalyst schema.
   *
   * @param fieldMap - The Catalyst column name to metadata of the master table
   * @param columns - The list of desired columns
   *
   * @return A Catalyst schema corresponding to columns in the given order.
   */
  def pruneSchema(fieldMap: Map[String, StructField],
      columns: Array[String]): StructType = {
    new StructType(columns.map { col =>
      fieldMap.getOrElse(col, fieldMap.getOrElse(Utils.normalizeId(col),
        throw new AnalysisException("JDBCAppendableRelation: Cannot resolve " +
            s"""column name "$col" among (${fieldMap.keys.mkString(", ")})""")
      ))
    })
  }

}

final class DefaultSource extends ColumnarRelationProvider {

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schema: StructType) = {
    val parameters = new CaseInsensitiveMutableHashMap(options)
    val parametersForShadowTable = new CaseInsensitiveMutableHashMap(options)
    StoreUtils.removeInternalProps(parametersForShadowTable)

    val table = StoreUtils.removeInternalProps(parameters)

    val sc = sqlContext.sparkContext
    val ddlExtension = StoreUtils.ddlExtensionString(parameters)
    //val preservepartitions = parameters.remove("preservepartitions")
    val (url, driver, poolProps, connProps, hikariCP) =
      ExternalStoreUtils.validateAndGetAllProps(sc, parameters.toMap)

    val ddlExtensionForShadowTable = StoreUtils.ddlExtensionString(parametersForShadowTable)

    val dialect = JdbcDialects.get(url)
    val blockMap =
      dialect match {
        case GemFireXDDialect => StoreUtils.initStore(sc, url, connProps)
        case _ => Map.empty[InternalDistributedMember, BlockManagerId]
      }
    val schemaString = JdbcExtendedUtils.schemaString(schema, dialect)
    val schemaExtension = s"$schemaString $ddlExtension"

    val externalStore = getExternalSource(sc, url, driver, poolProps, connProps, hikariCP)
    ColumnFormatRelation.registerStoreCallbacks(table, schema, externalStore)

    new ColumnFormatRelation(url,
      SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName, mode, schema, schemaExtension, ddlExtensionForShadowTable, Seq.empty.toArray,
      poolProps, connProps, hikariCP, options, externalStore, blockMap, sqlContext)()
  }

  override def getExternalSource(sc: SparkContext, url: String,
      driver: String,
      poolProps: Map[String, String],
      connProps: Properties,
      hikariCP: Boolean): ExternalStore = {

    val dialect = JdbcDialects.get(url)
    val blockMap =
      dialect match {
        case GemFireXDDialect => StoreUtils.initStore(sc, url, connProps)
        case _ => Map.empty[InternalDistributedMember, BlockManagerId]
      }
    new JDBCSourceAsColumnarStore(url, driver, poolProps, connProps, hikariCP, blockMap)
  }
}
