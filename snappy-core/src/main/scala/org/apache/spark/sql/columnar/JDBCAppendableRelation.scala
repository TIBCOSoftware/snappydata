package org.apache.spark.sql.columnar

import java.nio.ByteBuffer
import java.sql.Connection
import java.util.{UUID, Properties}
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.collection.{ExecutorLocalPartition, UUIDRegionKey, Utils}
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcUtils, DriverRegistry, JDBCPartitioningInfo, JDBCRelation}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.ExternalStore
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class JDBCAppendableRelation(
    val url: String,
    val table: String,
    val provider: String,
    val mode: SaveMode,
    userSchema: StructType,
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
    with DeletableRelation
    with Logging
    with Serializable {

  self =>

  private final val columnPrefix="Col_"
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



//  val partitionFilters: Seq[Expression] = {
//    predicates.flatMap { p =>
//      val filter = buildFilter.lift(p)
//      val boundFilter =
//        filter.map(
//          BindReferences.bindReference(
//            _,
//            relation.partitionStatistics.schema,
//            allowFailures = true))
//
//      boundFilter.foreach(_ =>
//        filter.foreach(f => logInfo(s"Predicate $p generates partition filter: $f")))
//
//      // If the filter can't be resolved then we are missing required statistics.
//      boundFilter.filter(_.resolved)
//    }
//  }

  override def schema: StructType = userSchema

  lazy val enableAccumulators: Boolean =
    sqlContext.getConf("spark.sql.inMemoryTableScanStatistics.enable", "false").toBoolean

  // Accumulators used for testing purposes
  lazy val readPartitions: Accumulator[Int] = sqlContext.sparkContext.accumulator(0)
  lazy val readBatches: Accumulator[Int] = sqlContext.sparkContext.accumulator(0)

  private val inMemoryPartitionPruningEnabled = false //sqlContext.conf.inMemoryPartitionPruning


  // currently doesn't apply any filters.
  // will see that later.
  override def buildScan(requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {

    if (enableAccumulators) {
      readPartitions.setValue(0)
      readBatches.setValue(0)
    }

    def cachedColumnBuffers: RDD[CachedBatch] = readLock {
      externalStore.getCachedBatchRDD(table, requiredColumns.map(column=> columnPrefix + column), uuidList,
        sqlContext.sparkContext)
    }

    cachedColumnBuffers.mapPartitions { cachedBatchIterator =>
      //           val partitionFilter = newPredicate(
      //             partitionFilters.reduceOption(And).getOrElse(Literal(true)),
      //              relation.partitionStatistics.schema)

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

        if (rows.hasNext && enableAccumulators) {
          readPartitions += 1
        }

        val converter = CatalystTypeConverters.createToScalaConverter(schema)
        rows.map(converter(_).asInstanceOf[Row])
      }

      // Do partition batch pruning if enabled
      val cachedBatchesToScan =
      //        if (inMemoryPartitionPruningEnabled) {
      //          cachedBatchIterator.filter { cachedBatch =>
      //            if (!partitionFilter(cachedBatch.stats)) {
      //              def statsString: String = relation.partitionStatistics.schema.zipWithIndex.map {
      //                case (a, i) =>
      //                  val value = cachedBatch.stats.get(i, a.dataType)
      //                  s"${a.name}: $value"
      //              }.mkString(", ")
      //              logInfo(s"Skipping partition based on stats $statsString")
      //              false
      //            } else {
      //              if (enableAccumulators) {
      //                readBatches += 1
      //              }
      //              true
      //            }
      //          }
      //        } else {
        cachedBatchIterator
      //}

      cachedBatchesToRows(cachedBatchesToScan)
    }


  }

  override def insert(df: DataFrame, overwrite: Boolean = true): Unit = {
    assert(df.schema.equals(schema))

    // We need to truncate the table
    if(overwrite) sqlContext.asInstanceOf[SnappyContext].truncateExternalTable(table)

    val useCompression = sqlContext.conf.useCompression
    val columnBatchSize = sqlContext.conf.columnBatchSize

    //TODO: Suranjan See if we need to cache
    val tableIdent = sqlContext.catalog.asInstanceOf[SnappyStoreHiveCatalog].newQualifiedTableName(table)
    //    val plan = catalog.lookupRelation(tableIdent, None)
    //    val relation = cacheManager.lookupCachedData(plan).getOrElse {
    //      cacheManager.cacheQuery(DataFrame(self, plan),
    //        Some(tableIdent.table), storageLevel)
    //
    //      cacheManager.lookupCachedData(plan).getOrElse {
    //        sys.error(s"couldn't cache table $tableIdent")
    //      }
    //    }
    val output = df.logicalPlan.output
    val cached = df.mapPartitions { rowIterator =>
      def uuidBatchAggregate(accumulated: ArrayBuffer[UUIDRegionKey],
          batch: CachedBatch): ArrayBuffer[UUIDRegionKey] = {
        val uuid = externalStore.storeCachedBatch(batch, table)
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

  def truncate() = writeLock {
    externalStore.truncate(table)
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
    createExternalTableForCachedBatches(table, externalStore)
  }

  private def createExternalTableForCachedBatches(tableName: String,
      externalStore: ExternalStore): Unit = {
    require(tableName != null && tableName.length > 0,
      "registerAndInsertIntoExternalStore: expected non-empty table name")

    val (primarykey, partitionStrategy) = ExternalStoreUtils.getConnectionType(
      externalStore.url) match {
      case ConnectionType.Embedded =>
        (s"constraint ${tableName}_bucketCheck check (bucketId != -1), " +
            "primary key (uuid, bucketId)", "partition by column (bucketId)")
      // TODO: [sumedh] Neeraj, the partition clause should come from JdbcDialect or something
      // TODO: [Suranjan] we can get the colocation clause here for colocation as well.
      case _ => ("primary key (uuid)", "partition by primary key")
    }

    createTable(externalStore, s"create table $tableName (uuid varchar(36) " +
      "not null, bucketId integer, stats blob, "+
      userSchema.fields.map(structField => columnPrefix + structField.name + " blob").mkString(" ", "," ," ")   +
      s", $primarykey) $partitionStrategy", tableName, dropIfExists = false) //for test make it false
  }

  def createTable(externalStore: ExternalStore, tableStr: String,
      tableName: String, dropIfExists: Boolean) = {
    //convert to SnappyContext to see if it Loner
    val isLoner = sqlContext.asInstanceOf[SnappyContext].isLoner

    val rdd = new DummyRDD(sqlContext) {
      override def compute(split: Partition,
          taskContext: TaskContext): Iterator[InternalRow] = {
        DriverRegistry.register(externalStore.driver)
        JdbcDialects.get(externalStore.url) match {
          case d: JdbcExtendedDialect =>
            val extraProps = d.extraCreateTableProperties(isLoner).propertyNames
            while (extraProps.hasMoreElements) {
              val p = extraProps.nextElement()
              if (externalStore.connProps.get(p) != null) {
                sys.error(s"Master specific property $p " +
                    "shouldn't exist here in Executors")
              }
            }
        }

        val conn = ExternalStoreUtils.getConnection(externalStore.url,
          externalStore.connProps)
        conn.close()
        Iterator.empty
      }

      override protected def getPartitions: Array[Partition] = {
        //TODO : Suranjan Find a cleaner way of starting all executors.
        val partitions = new Array[Partition](100)
        for (p <- 0 until 100) {
          partitions(p) = new ExecutorLocalPartition(p, null)
        }
        partitions
      }
    }
    rdd.collect()

    val connProps = externalStore.connProps
    val dialect = JdbcDialects.get(externalStore.url)
    dialect match {
      case d: JdbcExtendedDialect =>
        connProps.putAll(d.extraCreateTableProperties(isLoner))
    }

    externalStore.tryExecute(tableName, {
      case conn =>
        if (dropIfExists) {
          JdbcExtendedUtils.dropTable(conn, tableName, dialect, sqlContext,
            ifExists = true)
        }
        val tableExists = JdbcExtendedUtils.tableExists(conn, table,
          dialect, sqlContext)
        if (!tableExists) {
          JdbcExtendedUtils.executeUpdate(tableStr, conn)
          dialect match {
            case d: JdbcExtendedDialect => d.initializeTable(tableName, conn)
          }
        }
    })
  }

  /**
   * Delete a set of row matching given criteria.
   * @param filterExpr SQL WHERE criteria to select rows that will be deleted
   * @return number of rows deleted
   */
  override def delete(filterExpr: String): Int = ???

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
        JdbcExtendedUtils.dropTable(conn, tableName, dialect, sqlContext,
          ifExists)
    })
  }

}

object JDBCAppendableRelation {
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
      getClass.getCanonicalName, mode, schema, parts,
      poolProps, connProps, hikariCP, options, null, sqlContext)()
}

final class DefaultSource extends SchemaRelationProvider
  with CreatableRelationProvider {

  def createRelation(sqlContext: SQLContext, mode: SaveMode, options: Map[String, String], schema: StructType) = {
    val parameters = new mutable.HashMap[String, String]
    parameters ++= options
    val url = parameters.remove("url")
        .getOrElse(sys.error("JDBC URL option 'url' not specified"))
    val dbtableProp = JdbcExtendedUtils.DBTABLE_PROPERTY
    val table = parameters.remove(dbtableProp)
        .getOrElse(sys.error(s"Option '$dbtableProp' not specified"))
    val driver = parameters.remove("driver")
    val poolImpl = parameters.remove("poolimpl")
    val poolProperties = parameters.remove("poolproperties")
    val partitionColumn = parameters.remove("partitioncolumn")
    val lowerBound = parameters.remove("lowerbound")
    val upperBound = parameters.remove("upperbound")
    val numPartitions = parameters.remove("numpartitions")
    val serializationFormat = parameters.remove("serialization.format")

    // remove ALLOW_EXISTING property, if remaining
    parameters.remove(JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY)

    DriverRegistry.register(driver.get)

    val hikariCP = poolImpl.map(Utils.normalizeId) match {
      case Some("hikari") => true
      case Some("tomcat") => false
      case Some(p) =>
        throw new IllegalArgumentException("JDBCAppendableRelation: " +
            s"unsupported pool implementation '$p' " +
            s"(supported values: tomcat, hikari)")
      case None => false
    }
    val poolProps = poolProperties.map(p => Map(p.split(",").map { s =>
      val eqIndex = s.indexOf('=')
      if (eqIndex >= 0) {
        (s.substring(0, eqIndex).trim, s.substring(eqIndex + 1).trim)
      } else {
        // assume a boolean property to be enabled
        (s.trim, "true")
      }
    }: _*)).getOrElse(Map.empty)

    val partitionInfo = if (partitionColumn.isEmpty) {
      null
    } else {
      if (lowerBound.isEmpty || upperBound.isEmpty || numPartitions.isEmpty) {
        throw new IllegalArgumentException("JDBCUpdatableRelation: " +
            "incomplete partitioning specified")
      }
      JDBCPartitioningInfo(
        partitionColumn.get,
        lowerBound.get.toLong,
        upperBound.get.toLong,
        numPartitions.get.toInt)
    }
    val parts = JDBCRelation.columnPartition(partitionInfo)
    // remaining parameters are passed as properties to getConnection
    val connProps = new Properties()
    parameters.foreach(kv => connProps.setProperty(kv._1, kv._2))

    // store user schema somewhere for
    val externalStore = getExternalTable(options - JdbcExtendedUtils.DBTABLE_PROPERTY - JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY - "serialization.format")

    // also provide the new schema to create the table in gemfireXD
    new JDBCAppendableRelation(url,
      SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName, mode, schema, parts,
      poolProps, connProps, hikariCP, options, externalStore, sqlContext)()
  }

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String], schema: StructType) = {
    options.getOrElse("url", sys.error("Option 'url' not specified"))

    val allowExisting = options.get(JdbcExtendedUtils
        .ALLOW_EXISTING_PROPERTY).exists(_.toBoolean)
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists

    createRelation(sqlContext, mode, options, schema)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, options: Map[String, String], data: DataFrame): BaseRelation = {
    val relation = createRelation(sqlContext, mode, options, data.schema)
    relation.insert(data, mode == SaveMode.Overwrite)
    relation
  }

  def getExternalTable(jdbcSource: Map[String, String]): ExternalStore = {
    val externalSource = jdbcSource.get("jdbcStore") match {
      case Some(x) => x
      case None => "org.apache.spark.sql.store.impl.JDBCSourceAsColumnarStore"
    }
    val constructor = Class.forName(externalSource).getConstructors()(0)
    return constructor.newInstance(jdbcSource).asInstanceOf[ExternalStore]
  }
}