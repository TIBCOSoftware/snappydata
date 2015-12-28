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
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.row.{GemFireXDBaseDialect, JDBCMutableRelation}
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.{ExternalStore, JDBCSourceAsStore}
import org.apache.spark.sql.types.StructType
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
    numPartitions:Int,
    _poolProps: Map[String, String],
    val connProperties: Properties,
    val hikariCP: Boolean,
    val origOptions: Map[String, String],
    val externalStore: ExternalStore,
    @transient override val sqlContext: SQLContext)(
    private var uuidList: ArrayBuffer[RDD[UUIDRegionKey]] =
    new ArrayBuffer[RDD[UUIDRegionKey]]())
    extends BaseRelation
    with PrunedFilteredScan
    with InsertableRelation
    with DestroyRelation
    with Logging
    with Serializable {

  self =>

  final val shadowTableNamePrefix = "_shadow_"
  override val needConversion: Boolean = false

  final val columnPrefix = "Col_"

  val driver = DriverRegistry.getDriverClassName(url)

  final val dialect = JdbcDialects.get(url)

  val schemaFields = Map(userSchema.fields.flatMap { f =>
    val name = if (f.metadata.contains("name")) f.metadata.getString("name") else f.name
    Iterator((name, f))
  }: _*)

  final lazy val connector = ExternalStoreUtils.getConnector(table, driver,
    dialect, _poolProps, connProperties, hikariCP)

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
    scanTable(table, requiredColumns, filters)
  }

  def scanTable(tableName: String, requiredColumns: Array[String],
      filters: Array[Filter]) :RDD[Row] = {
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
      externalStore.getCachedBatchRDD(tableName,
        requestedColumns.map(column => columnPrefix + column), uuidList,
        sqlContext.sparkContext)
    }

    cachedColumnBuffers.mapPartitionsPreserve { cachedBatchIterator =>
      // Find the ordinals and data types of the requested columns.
      // If none are requested, use the narrowest (the field with
      // minimum default element size).
      val (requestedColumnIndices, requestedColumnDataTypes) = requestedColumns.map { a =>
        schema.getFieldIndex(a).get -> schema(a).dataType
      }.unzip
      val nextRow = new SpecificMutableRow(requestedColumnDataTypes)
      def cachedBatchesToRows(
          cacheBatches: Iterator[CachedBatch]): Iterator[InternalRow] = {
        val rows = cacheBatches.flatMap { cachedBatch =>
          // Build column accessors
          val columnAccessors = requestedColumnIndices.zipWithIndex.map {
            case (schemaIndex, bufferIndex) =>
              ColumnAccessor(schema.fields(schemaIndex).dataType,
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

            override def hasNext: Boolean = columnAccessors.head.hasNext
          }
        }
        rows
      }
      cachedBatchesToRows(cachedBatchIterator)
    }.asInstanceOf[RDD[Row]]
  }

  override def insert(df: DataFrame, overwrite: Boolean = true): Unit = {
    insert(df.rdd, df, overwrite)
  }

  protected def insert(rdd : RDD[Row], df: DataFrame, overwrite: Boolean) : Unit = {

    assert(df.schema.equals(schema))

    // We need to truncate the table
    if (overwrite) {
      truncate
    }

    val useCompression = sqlContext.conf.useCompression
    val columnBatchSize = sqlContext.conf.columnBatchSize

    val output = df.logicalPlan.output
    val cached = rdd.mapPartitionsPreserveWithIndex({case (split, rowIterator) =>
      def uuidBatchAggregate(accumulated: ArrayBuffer[UUIDRegionKey],
          batch: CachedBatch): ArrayBuffer[UUIDRegionKey] = {
        //TODO - currently using the length from the part Object but it needs to be handled more generically
        //in order to replace UUID
        val uuid = externalStore.storeCachedBatch(batch, table , numPartitions)
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
    },true)
    // trigger an Action to materialize 'cached' batch
    cached.count()
    appendUUIDBatch(cached.asInstanceOf[RDD[UUIDRegionKey]])
  }


  def appendUUIDBatch(batch: RDD[UUIDRegionKey]) = writeLock {
    uuidList += batch
  }

  // truncate both actual and shadow table
  def truncate() = writeLock {
    val dialect = JdbcDialects.get(externalStore.url)
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
      conn = ExternalStoreUtils.getConnection(url, connProperties,
        dialect, isLoner = Utils.isLoner(sqlContext.sparkContext))
      val tableExists = JdbcExtendedUtils.tableExists(table, conn,
        dialect, sqlContext)
      if (mode == SaveMode.Ignore && tableExists) {
        dialect match {
          case d: JdbcExtendedDialect => {
            d.initializeTable(table,
              sqlContext.conf.caseSensitiveAnalysis, conn)
          }
          case _ => // do nothing
        }
        return
      }

      if (mode == SaveMode.ErrorIfExists && tableExists) {
        sys.error(s"Table $table already exists.")
      }
    }
    createExternalTableForCachedBatches(table, externalStore)
  }

  protected def createExternalTableForCachedBatches(tableName: String,
      externalStore: ExternalStore): Unit = {
    require(tableName != null && tableName.length > 0,
      "createExternalTableForCachedBatches: expected non-empty table name")

    val (primarykey, partitionStrategy) = dialect match {
      // The driver if not a loner should be an accesor only
      case d: JdbcExtendedDialect =>
        (s"constraint ${tableName}_bucketCheck check (bucketId != -1), " +
            "primary key (uuid, bucketId)", d.getPartitionByClause("bucketId"))
      case _ => ("primary key (uuid)", "") // TODO. How to get primary key contraint from each DB
    }

    createTable(externalStore, s"create table $tableName (uuid varchar(36) " +
        "not null, bucketId integer not null, numRows integer not null, " +
        "stats blob, " + userSchema.fields.map(structField => columnPrefix +
        structField.name + " blob").mkString(" ", ",", " ") +
        s", $primarykey) $partitionStrategy",
      tableName, dropIfExists = false) // for test make it false
  }

  def createTable(externalStore: ExternalStore, tableStr: String,
      tableName: String, dropIfExists: Boolean) = {

    externalStore.tryExecute(tableName, {
      case conn =>
        if (dropIfExists) {
          JdbcExtendedUtils.dropTable(conn, tableName, dialect, sqlContext,
            ifExists = true)
        }
        val tableExists = JdbcExtendedUtils.tableExists(tableName, conn,
          dialect, sqlContext)
        if (!tableExists) {
          JdbcExtendedUtils.executeUpdate(tableStr, conn)
          dialect match {
            case d: JdbcExtendedDialect => d.initializeTable(tableName,
              sqlContext.conf.caseSensitiveAnalysis, conn)
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
    // clean up the connection pool on executors first
    Utils.mapExecutors(sqlContext,
      JDBCAppendableRelation.removePool(table)).count()
    // then on the driver
    JDBCAppendableRelation.removePool(table)
    // drop the external table using a non-pool connection
    val conn = ExternalStoreUtils.getConnection(url, connProperties,
      dialect, isLoner = Utils.isLoner(sqlContext.sparkContext))
    try {
      JdbcExtendedUtils.dropTable(conn, table, dialect, sqlContext, ifExists)
    } finally {
      conn.close()
    }
  }
}

object JDBCAppendableRelation extends Logging {
  def apply(url: String,
      table: String,
      provider: String,
      mode: SaveMode,
      schema: StructType,
      numPartitions:Integer ,
      poolProps: Map[String, String],
      connProps: Properties,
      hikariCP: Boolean,
      options: Map[String, String],
      sqlContext: SQLContext): JDBCAppendableRelation =
    new JDBCAppendableRelation(url,
      SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName, mode, schema, numPartitions,
      poolProps, connProps, hikariCP, options, null, sqlContext)()

  private def removePool(table: String): () => Iterator[Unit] = () => {
    ConnectionPool.removePoolReference(table)
    Iterator.empty
  }
}

final class DefaultSource extends ColumnarRelationProvider

class ColumnarRelationProvider
    extends SchemaRelationProvider
    with CreatableRelationProvider {

  def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schema: StructType) = {
    val parameters = new mutable.HashMap[String, String]
    parameters ++= options

    val table = ExternalStoreUtils.removeInternalProps(parameters)
    val sc = sqlContext.sparkContext

    val (url, driver, poolProps, connProps, hikariCP) =
      ExternalStoreUtils.validateAndGetAllProps(sc, parameters)

    val externalStore = getExternalSource(sqlContext, url, driver, poolProps,
      connProps, hikariCP)

    new JDBCAppendableRelation(url,
      SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName, mode, schema, getPartitions(parameters),
      poolProps, connProps, hikariCP, options, externalStore, sqlContext)()
  }

  protected def getPartitions(parameters: mutable.Map[String, String]): Int = {
    //TODO - After TPCH checkin it will change. it should come from columnFormatRelation
    val DEFAULT_BUCKETS_FOR_COLUMN = "113"
    parameters.remove("BUCKETS").getOrElse(DEFAULT_BUCKETS_FOR_COLUMN).toInt
  }

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String], schema: StructType) = {

    val allowExisting = options.get(JdbcExtendedUtils
        .ALLOW_EXISTING_PROPERTY).exists(_.toBoolean)
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists

    val rel = getRelation(sqlContext, options)
    rel.createRelation(sqlContext, mode, options, schema)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], data: DataFrame): BaseRelation = {
    val rel = getRelation(sqlContext, options)
    val relation = rel.createRelation(sqlContext, mode, options, data.schema)
    relation.insert(data, mode == SaveMode.Overwrite)
    relation
  }

  def getRelation(sqlContext: SQLContext,
      options: Map[String, String]): ColumnarRelationProvider = {

    val url = options.getOrElse("url",
      ExternalStoreUtils.defaultStoreURL(sqlContext.sparkContext))
    val clazz = JdbcDialects.get(url) match {
      case d: GemFireXDBaseDialect => ResolvedDataSource.
          lookupDataSource("org.apache.spark.sql.columntable.DefaultSource")
      case _ => classOf[columnar.DefaultSource]
    }
    clazz.newInstance().asInstanceOf[ColumnarRelationProvider]
  }

  def getExternalSource(sqlContext: SQLContext, url: String,
      driver: String,
      poolProps: Map[String, String],
      connProps: Properties,
      hikariCP: Boolean): ExternalStore = {
    new JDBCSourceAsStore(url, driver, poolProps, connProps, hikariCP)
  }
}
