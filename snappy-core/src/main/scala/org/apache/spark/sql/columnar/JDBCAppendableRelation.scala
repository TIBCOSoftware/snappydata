package org.apache.spark.sql.columnar

import java.nio.ByteBuffer
import java.sql.Connection
import java.util.Properties
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.collection.{UUIDRegionKey, Utils}
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.datasources.ResolvedDataSource
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, JDBCPartitioningInfo, JDBCRelation, JdbcUtils}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.row.GemFireXDBaseDialect
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.{ExternalStore, JDBCSourceAsStore}
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * A LogicalPlan implementation for an external column table whose contents
 * are retrieved using a JDBC URL or DataSource.
 */

class JDBCAppendableRelation(
    val url: String,
    val table: String,
    val provider: String,
    val mode: SaveMode,
    userSchema: StructType,
    schemaExtensions: String,
    parts: Array[Partition],
    _poolProps: Map[String, String],
    val connProperties: Properties,
    val hikariCP: Boolean,
    val origOptions: Map[String, String],
    val externalStore: ExternalStore,
    @transient override val sqlContext: SQLContext)(
    private var uuidList: ArrayBuffer[RDD[UUIDRegionKey]]
    = new ArrayBuffer[RDD[UUIDRegionKey]]()
    )
    extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation
    with DestroyRelation
    with Logging
    with Serializable {

  self =>

  final val columnPrefix = "Col_"
  final val shadowTableNamePrefix = "_shadow_"
  final val dialect = JdbcDialects.get(url)
  val driver = DriverRegistry.getDriverClassName(url)

  final val schemaFields = Map(schema.fields.flatMap { f =>
    val name = if (f.metadata.contains("name")) f.metadata.getString("name") else f.name
    Iterator((name, f))
  }: _*)

  createTable(mode)
  private val bufferLock = new ReentrantReadWriteLock()

  /** Acquires a read lock on the cache for the duration of `f`. */
  private[sql] def readLock[A](f: => A): A = {
    val lock = bufferLock.readLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  /** Acquires a write lock on the cache for the duration of `f`. */
  private[sql] def writeLock[A](f: => A): A = {
    val lock = bufferLock.writeLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  override def schema: StructType = userSchema

  // TODO: Suranjan currently doesn't apply any filters.
  // will see that later.
  override def buildScan(requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {

    def cachedColumnBuffers: RDD[CachedBatch] = readLock {
      externalStore.getCachedBatchRDD(table+shadowTableNamePrefix, requiredColumns.map(column => columnPrefix + column), uuidList,
        sqlContext.sparkContext)
    }

    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    cachedColumnBuffers.mapPartitions { cachedBatchIterator =>
      // Find the ordinals and data types of the requested columns.  If none are requested, use the
      // narrowest (the field with minimum default element size).
      val (requestedColumnIndices, requestedColumnDataTypes) = if (requiredColumns.isEmpty) {
        val (narrowestOrdinal, narrowestDataType) =
          schema.fields.zipWithIndex.map { case (a, ordinal) =>
            ordinal -> a.dataType
          } minBy { case (_, dataType) =>
            ColumnType(dataType).defaultSize
          }
        Seq(narrowestOrdinal) -> Seq(narrowestDataType)
      } else {
        requiredColumns.map { a =>
          schema.getFieldIndex(a).get -> schema(a).dataType
        }.unzip
      }
      val nextRow = new SpecificMutableRow(requestedColumnDataTypes)
      def cachedBatchesToRows(cacheBatches: Iterator[CachedBatch]): Iterator[Row] = {
        val rows = cacheBatches.flatMap { cachedBatch =>
          // Build column accessors
          val columnAccessors = requestedColumnIndices.map { batchColumnIndex =>
            ColumnAccessor(
              schema.fields(batchColumnIndex).dataType,
              ByteBuffer.wrap(cachedBatch.buffers(batchColumnIndex)))
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
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    insert(data, if (overwrite) SaveMode.Overwrite else SaveMode.Append)
  }

  def insert(data: DataFrame, mode: SaveMode): Unit = {
    if (mode == SaveMode.Overwrite) {
      sqlContext.asInstanceOf[SnappyContext].truncateExternalTable(table)
    }
    insert(data)
  }

  def insert(data: DataFrame): Unit = {
    JdbcUtils.saveTable(data, url, table, connProperties)
  }

  //TODO: Suranjan remove this
  def insertColumn(df: DataFrame, overwrite: Boolean = true): Unit = {
    assert(df.schema.equals(schema))

    // We need to truncate the table
    if (overwrite) sqlContext.asInstanceOf[SnappyContext].truncateExternalTable(table+shadowTableNamePrefix)

    val useCompression = sqlContext.conf.useCompression
    val columnBatchSize = sqlContext.conf.columnBatchSize

    val output = df.logicalPlan.output
    val cached = df.mapPartitions { rowIterator =>
      def uuidBatchAggregate(accumulated: ArrayBuffer[UUIDRegionKey],
          batch: CachedBatch): ArrayBuffer[UUIDRegionKey] = {
        val uuid = externalStore.storeCachedBatch(batch, table+shadowTableNamePrefix)
        accumulated += uuid
      }

      def columnBuilders = output.map { attribute =>
        val columnType = ColumnType(attribute.dataType)
        val initialBufferSize = columnType.defaultSize * columnBatchSize
        ColumnBuilder(attribute.dataType, initialBufferSize,
          attribute.name, useCompression)
      }.toArray

      val holder = new CachedBatchHolder(columnBuilders, 0, columnBatchSize, schema,
        new ArrayBuffer[UUIDRegionKey](1), uuidBatchAggregate)

      val batches = holder.asInstanceOf[CachedBatchHolder[ArrayBuffer[Serializable]]]
      val converter = CatalystTypeConverters.createToCatalystConverter(schema)
      rowIterator.map(converter(_).asInstanceOf[InternalRow])
          .foreach(batches.appendRow((), _))
      batches.forceEndOfBatch().iterator
    }
    // trigger an Action to materialize 'cached' batch
    cached.count
    appendUUIDBatch(cached.asInstanceOf[RDD[UUIDRegionKey]])
  }

  def appendUUIDBatch(batch: RDD[UUIDRegionKey]) = writeLock {
    uuidList += batch
  }

  // truncate both actual and shadow table
  def truncate() = writeLock {
    val dialect = JdbcDialects.get(externalStore.url)
    externalStore.tryExecute(table+shadowTableNamePrefix, {
      case conn =>
        JdbcExtendedUtils.truncateTable(conn, table+shadowTableNamePrefix, dialect)
    })
    externalStore.tryExecute(table, {
      case conn =>
        JdbcExtendedUtils.truncateTable(conn, table, dialect)
    })
    uuidList.clear()
  }

  def createTable(mode: SaveMode): Unit = {
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

  def createActualTable(tableName: String, externalStore: ExternalStore) = {
    // Create the table if the table didn't exist.
    var conn: Connection = null
    try {
      conn = JdbcUtils.createConnection(url, connProperties)
      val tableExists = JdbcExtendedUtils.tableExists(conn, table,
        dialect, sqlContext)
      if (!tableExists) {
        // TODO: If there is no partition column then create default partition table
        // In this case partition on all the columns
        var partitionByClause = ""
        if(!schemaExtensions.contains(" PARTITION ")) {
          // create partition by extension with all the columns
          partitionByClause = dialect match {
            // The driver if not a loner should be an accessor only
            case d: JdbcExtendedDialect =>
              d.getPartitionByClause(s"${schemaFields.keys.mkString(", ")}")
          }
        }

        val sql = s"CREATE TABLE ${tableName} $schemaExtensions $partitionByClause"
        println("Applying DDL" + sql)
        logInfo("Applying DDL : " + sql)
        JdbcExtendedUtils.executeUpdate(sql, conn)
        dialect match {
          case d: JdbcExtendedDialect => d.initializeTable(tableName, conn)
        }
        createExternalTableForCachedBatches(tableName+shadowTableNamePrefix, externalStore)
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
        s", $primarykey) $partitionStrategy $colocationClause", tableName, dropIfExists = false)
  }

  def createTable(externalStore: ExternalStore, tableStr: String,
      tableName: String, dropIfExists: Boolean) = {

    externalStore.tryExecute(tableName, {
      case conn =>
        if (dropIfExists) {
          JdbcExtendedUtils.dropTable(conn, tableName, dialect, sqlContext,
            ifExists = true)
        }
        val tableExists = JdbcExtendedUtils.tableExists(conn, tableName,
          dialect, sqlContext)
        if (!tableExists) {
          JdbcExtendedUtils.executeUpdate(tableStr, conn)
          dialect match {
            case d: JdbcExtendedDialect => d.initializeTable(tableName, conn)
            case _ => // do nothing

          }
        }
    })
  }

  /**
   * Destroy and cleanup this relation. It may include, but not limited to,
   * dropping the external table that this relation represents.
   */
  override def destroy(ifExists: Boolean): Unit = {
    dropTable(table, ifExists)
  }

  private def dropTable(tableName: String, ifExists: Boolean): Unit = {
    val dialect = JdbcDialects.get(externalStore.url)
    externalStore.tryExecute(tableName, {
      case conn =>
        JdbcExtendedUtils.dropTable(conn, tableName+shadowTableNamePrefix, dialect, sqlContext,
          ifExists)
        JdbcExtendedUtils.dropTable(conn, tableName, dialect, sqlContext,
          ifExists)
    })
  }

}

object JDBCAppendableRelation extends Logging {
  def apply(url: String,
      table: String,
      provider: String,
      mode: SaveMode,
      schema: StructType,
      parts: Array[Partition],
      poolProps: Map[String, String],
      connProps: Properties,
      hikariCP: Boolean,
      options: Map[String, String],
      sqlContext: SQLContext): JDBCAppendableRelation =
    new JDBCAppendableRelation(url,
      SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName, mode, schema, "", parts,
      poolProps, connProps, hikariCP, options, null, sqlContext)()


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


final class DefaultSource extends ColumnarRelationProvider

class ColumnarRelationProvider extends SchemaRelationProvider
with CreatableRelationProvider {

  def createRelation(sqlContext: SQLContext, mode: SaveMode, options: Map[String, String], schema: StructType) = {
    val parameters = new mutable.HashMap[String, String]
    parameters ++= options

    val partitionColumn = parameters.remove("partitioncolumn")
    val lowerBound = parameters.remove("lowerbound")
    val upperBound = parameters.remove("upperbound")
    val numPartitions = parameters.remove("numpartitions")


    val (url, driver, poolProps, connProps, hikariCP) =
      ExternalStoreUtils.validateAndGetAllProps(sqlContext.sparkContext, options)


    val dbtableProp = JdbcExtendedUtils.DBTABLE_PROPERTY
    val table = parameters.remove(dbtableProp)
        .getOrElse(sys.error(s"Option '$dbtableProp' not specified"))

    parameters.remove(JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY)
    parameters.remove(JdbcExtendedUtils.SCHEMA_PROPERTY)
    parameters.remove("serialization.format")


    val partitionInfo = if (partitionColumn.isEmpty) {
      null
    } else {
      if (lowerBound.isEmpty || upperBound.isEmpty || numPartitions.isEmpty) {
        throw new IllegalArgumentException("JDBCAppendableRelation: " +
            "incomplete partitioning specified")
      }
      JDBCPartitioningInfo(
        partitionColumn.get,
        lowerBound.get.toLong,
        upperBound.get.toLong,
        numPartitions.get.toInt)
    }
    val parts = JDBCRelation.columnPartition(partitionInfo)

    val externalStore = getExternalSource(sqlContext.sparkContext, url, driver, poolProps, connProps, hikariCP)

    new JDBCAppendableRelation(url,
      SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName, mode, schema, "", parts,
      poolProps, connProps, hikariCP, options, externalStore, sqlContext)()
  }

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String], schema: StructType) = {

    val allowExisting = options.get(JdbcExtendedUtils
        .ALLOW_EXISTING_PROPERTY).exists(_.toBoolean)
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists

    val rel = getRelation(sqlContext, options)
    rel.createRelation(sqlContext, mode, options, schema)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, options: Map[String, String], data: DataFrame): BaseRelation = {
    val rel = getRelation(sqlContext, options)
    val relation = rel.createRelation(sqlContext, mode, options, data.schema)
    relation.insert(data, mode == SaveMode.Overwrite)
    relation
  }

  def getRelation(sqlContext: SQLContext, options: Map[String, String]): ColumnarRelationProvider = {

    val (url, _, _, _, _) =
      ExternalStoreUtils.validateAndGetAllProps(sqlContext.sparkContext, options)

    val clazz = JdbcDialects.get(url) match {
      case d: GemFireXDBaseDialect => {
        ResolvedDataSource.lookupDataSource("org.apache.spark.sql.columntable.DefaultSource")
      }
      case _ => classOf[columnar.DefaultSource]
    }
    clazz.newInstance().asInstanceOf[ColumnarRelationProvider]
  }

  def getExternalSource(sc: SparkContext, url: String,
      driver: String,
      poolProps: Map[String, String],
      connProps: Properties,
      hikariCP: Boolean): ExternalStore = {
    new JDBCSourceAsStore(url, driver, poolProps, connProps, hikariCP)
  }
}