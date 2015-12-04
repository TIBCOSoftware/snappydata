package org.apache.spark.sql.columntable

import java.sql.Connection
import java.util.Properties

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.collection.{UUIDRegionKey, Utils}
import org.apache.spark.sql.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.columnar._
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.row.GemFireXDDialect
import org.apache.spark.sql.rowtable.RowFormatScanRDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.impl.JDBCSourceAsColumnarStore
import org.apache.spark.sql.store.{ExternalStore, StoreUtils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, SaveMode, _}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{Logging, Partition}

import scala.collection.mutable.ArrayBuffer
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
    extends JDBCAppendableRelation(url, table, provider, mode, userSchema, parts, _poolProps, connProperties,
      hikariCP, origOptions, externalStore, sqlContext)()
    with RowInsertableRelation {

  lazy val connectionType = ExternalStoreUtils.getConnectionType(url)
  lazy val connFunctor = ExternalStoreUtils.getConnector(table, driver, dialect, _poolProps,
    connProperties, hikariCP)
  val rowInsertStr = ExternalStoreUtils.getInsertStringWithColumnName(table, userSchema)

  // TODO: Suranjan currently doesn't apply any filters.
  // will see that later.
  override def buildScan(requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {
    val colRDD = super.scanTable(table+shadowTableNamePrefix, requiredColumns, filters)

    // TODO: Suranjan scanning over column rdd before row will make sure that we don't have duplicates
    // we may miss some result though
    // TODO: can we optimize the union by providing partitioner
    colRDD.union(connectionType match {
      case ConnectionType.Embedded =>
        new RowFormatScanRDD(
          sqlContext.sparkContext,
          connFunctor,
          ExternalStoreUtils.pruneSchema(schemaFields, requiredColumns),
          table,
          requiredColumns,
          filters,
          parts,
          blockMap,
          connProperties
        ).asInstanceOf[RDD[Row]]

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
  //TODO: Suranjan same code in ROWFormatRelation/JDBCMutableRelation
  override def insert(rows: Seq[Row]): Int = {
    val numRows = rows.length
    if (numRows == 0) {
      throw new IllegalArgumentException(
        "JDBCAppendableRelation.insert: no rows provided")
    }
    val connection = ConnectionPool.getPoolConnection(table, None, dialect,
      _poolProps, connProperties, hikariCP)
    try {
      val stmt = connection.prepareStatement(rowInsertStr)
      if (numRows > 1) {
        for (row <- rows) {
          ExternalStoreUtils.setStatementParameters(stmt, userSchema.fields,
            row, dialect)
          stmt.addBatch()
        }
      } else {
        ExternalStoreUtils.setStatementParameters(stmt, userSchema.fields,
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

  /**
   * Destroy and cleanup this relation. It may include, but not limited to,
   * dropping the external table that this relation represents.
   */
  override def destroy(ifExists: Boolean): Unit = {
    // clean up the connection pool on executors first
    Utils.mapExecutors(sqlContext,
      ColumnFormatRelation.removePool(table)).count()
    // then on the driver
    ColumnFormatRelation.removePool(table)
    // drop the external table using a non-pool connection
    val conn = JdbcUtils.createConnection(url, connProperties)
    try {
      JdbcExtendedUtils.dropTable(conn, table + shadowTableNamePrefix, dialect, sqlContext,
        ifExists)
      JdbcExtendedUtils.dropTable(conn, table, dialect, sqlContext, ifExists)
    } finally {
      conn.close()
    }
  }

  override def createTable(mode: SaveMode): Unit = {
    var conn: Connection = null
    val dialect = JdbcDialects.get(url)
    try {
      conn = JdbcUtils.createConnection(url, connProperties)
      val tableExists = JdbcExtendedUtils.tableExists(table, conn,
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

  override def createExternalTableForCachedBatches(tableName: String,
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


  //TODO: Suranjan make sure that this table doesn't evict to disk by setting some property, may be MemLRU?
  def createActualTable(tableName: String, externalStore: ExternalStore) = {

    // Create the table if the table didn't exist.
    var conn: Connection = null
    try {
      conn = JdbcUtils.createConnection(url, connProperties)
      val tableExists = JdbcExtendedUtils.tableExists(tableName, conn,
        dialect, sqlContext)
      if (!tableExists) {
        val sql = s"CREATE TABLE ${tableName} $schemaExtensions "
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

  /**
   * Execute a DML SQL and return the number of rows affected.
   */
  override def executeUpdate(sql: String): Int = {
    val connection = ConnectionPool.getPoolConnection(table, None, dialect,
      _poolProps, connProperties, hikariCP)
    try {
      val stmt = connection.prepareStatement(sql)
      //stmt.setSt
      val result = stmt.executeUpdate()
      stmt.close()
      result
    } finally {
      connection.close()
    }
  }
}


object ColumnFormatRelation extends Logging with StoreCallback {
  // register the call backs with the JDBCSource so that
  // bucket region can insert into the column table

  def registerStoreCallbacks(sqlContext: SQLContext,table: String, userSchema: StructType, externalStore: ExternalStore) = {
    StoreCallbacksImpl.registerExternalStoreAndSchema(sqlContext, table.toUpperCase, userSchema,
      externalStore, sqlContext.conf.columnBatchSize, sqlContext.conf.useCompression)
  }

  private def removePool(table: String): () => Iterator[Unit] = () => {
    ConnectionPool.removePoolReference(table)
    Iterator.empty
  }
}

final class DefaultSource extends ColumnarRelationProvider {

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schema: StructType) = {
    val parameters = new CaseInsensitiveMutableHashMap(options)
    val parametersForShadowTable = new CaseInsensitiveMutableHashMap(options)
    StoreUtils.removeInternalProps(parametersForShadowTable)

    val table = ExternalStoreUtils.removeInternalProps(parameters)
    val sc = sqlContext.sparkContext

    val primaryKeyClause = StoreUtils.getPrimaryKeyClause(parameters)

    val ddlExtension = StoreUtils.ddlExtensionString(parameters, false, false)
    val (url, driver, poolProps, connProps, hikariCP) =
      ExternalStoreUtils.validateAndGetAllProps(sc, parameters)

    val ddlExtensionForShadowTable = StoreUtils.ddlExtensionString(parametersForShadowTable, false, true)

    val dialect = JdbcDialects.get(url)
    val blockMap =
      dialect match {
        case GemFireXDDialect => StoreUtils.initStore(sqlContext, url, connProps, poolProps, hikariCP, table, Some(schema))
        case _ => Map.empty[InternalDistributedMember, BlockManagerId]
      }
    val schemaString = JdbcExtendedUtils.schemaString(schema, dialect)
    val schemaExtension = if (schemaString.length > 0) {
      val temp = schemaString.substring(0, schemaString.length - 1).
          concat(s", ${StoreUtils.SHADOW_COLUMN}, $primaryKeyClause )")
      s"$temp $ddlExtension"
    } else {
      s"$schemaString $ddlExtension"
    }

    val externalStore = getExternalSource(sqlContext, url, driver, poolProps, connProps, hikariCP, table, schema)
    ColumnFormatRelation.registerStoreCallbacks(sqlContext, table, schema, externalStore)

    new ColumnFormatRelation(url,
      SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName, mode, schema, schemaExtension, ddlExtensionForShadowTable, Array[Partition](),
      poolProps, connProps, hikariCP, options, externalStore, blockMap, sqlContext)()
  }

  def getExternalSource(sqlContext: SQLContext, url: String,
      driver: String,
      poolProps: Map[String, String],
      connProps: Properties,
      hikariCP: Boolean,
      table:String,
      schema:StructType): ExternalStore = {

    val dialect = JdbcDialects.get(url)
    val blockMap =
      dialect match {
        case GemFireXDDialect => StoreUtils.initStore(sqlContext, url, connProps, poolProps, hikariCP, table, Some(schema))
        case _ => Map.empty[InternalDistributedMember, BlockManagerId]
      }
    new JDBCSourceAsColumnarStore(url, driver, poolProps, connProps, hikariCP, blockMap)
  }
}
