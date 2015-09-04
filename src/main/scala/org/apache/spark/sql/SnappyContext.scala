package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => u}
import scala.reflect.runtime.universe.TypeTag

import io.snappydata.util.SqlUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.sql.LockUtils.ReadWriteLock
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Subquery}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, ScalaReflection}
import org.apache.spark.sql.collection.{UUIDRegionKey, Utils}
import org.apache.spark.sql.columnar._
import org.apache.spark.sql.execution.{TopKStub, _}
import org.apache.spark.sql.execution.row._
import org.apache.spark.sql.execution.streamsummary.StreamSummaryAggregation
import org.apache.spark.sql.hive.{QualifiedTableName, SnappyStoreHiveCatalog}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.impl.JDBCSourceAsStore
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.{Partition, Partitioner, SparkContext, TaskContext}

/**
 * An instance of the Spark SQL execution engine that delegates to supplied SQLContext
 * offering additional capabilities.
 *
 * Created by Soubhik on 5/13/15.
 */

protected[sql] final class SnappyContext(sc: SparkContext)
  extends SQLContext(sc) with Serializable {

  self =>

  // initialize GemFireXDDialect so that it gets registered
  GemFireXDDialect.init()

  @transient
  override protected[sql] val ddlParser = new SnappyDDLParser(sqlParser.parse)

  @transient
  val topKLocks = scala.collection.mutable.Map[String, ReadWriteLock]()

  override protected[sql] def dialectClassName = if (conf.dialect == "sql") {
    classOf[SnappyParserDialect].getCanonicalName
  } else {
    conf.dialect
  }

  @transient
  override protected[sql] lazy val catalog =
    new SnappyStoreHiveCatalog(this)

  @transient
  override protected[sql] val cacheManager = new SnappyCacheManager(this)

  def saveStream[T: ClassTag](stream: DStream[T],
    aqpTables: Seq[String],
    formatter: (RDD[T], StructType) => RDD[Row],
    schema: StructType,
    transform: DataFrame => DataFrame = null) {
    stream.foreachRDD((rdd: RDD[T], time: Time) => {

      val rddRows = formatter(rdd, schema)

      val rows = if (transform != null) {
        // avoid conversion to Catalyst rows and back for both calls below,
        // so not using DataFrame.rdd call directly in second step below
        val rDF = createDataFrame(rddRows, schema, needsConversion = false)
        transform(rDF).queryExecution.toRdd
      } else rddRows

      collectSamples(rows, aqpTables, time.milliseconds)
    })
  }

  def externalTable(tableName: String): DataFrame =
    DataFrame(this, catalog.lookupRelation(Seq(tableName)))

  protected[sql] def collectSamples(rows: RDD[Row], aqpTables: Seq[String],
    time: Long,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK) {
    val useCompression = conf.useCompression
    val columnBatchSize = conf.columnBatchSize
    val aqpTableNames = mutable.Set(aqpTables.map(
      catalog.newQualifiedTableName): _*)

    val sampleTables = catalog.tables.collect {
      case (name, sample: StratifiedSample) if aqpTableNames.contains(name) =>
        aqpTableNames.remove(name)
        (name, sample.options, sample.schema, sample.output,
          cacheManager.lookupCachedData(sample).getOrElse(sys.error(
            s"SnappyContext.saveStream: failed to lookup cached plan for " +
              s"sampling table $name")).cachedRepresentation)
    }

    val topKWrappers = catalog.topKStructures.filter {
      case (name, topkstruct) => aqpTableNames.remove(name)
    }

    if (aqpTableNames.nonEmpty) {
      throw new IllegalArgumentException("collectSamples: no sampling or " +
        s"topK structures for ${aqpTableNames.mkString(", ")}")
    }

    // TODO: this iterates rows multiple times
    val rdds = sampleTables.map {
      case (name, samplingOptions, schema, output, relation) =>
        (relation, rows.mapPartitions(rowIterator => {
          val sampler = StratifiedSampler(samplingOptions, Array.emptyIntArray,
            nameSuffix = "", columnBatchSize, schema, cached = true)
          val catalystConverters = schema.fields.map(f =>
            CatalystTypeConverters.createToCatalystConverter(f.dataType))
          // create a new holder for set of CachedBatches
          val batches = ExternalStoreRelation(useCompression,
            columnBatchSize, name, schema, relation, output)
          sampler.append(rowIterator, catalystConverters, (),
            batches.appendRow, batches.endRows)
          batches.forceEndOfBatch().iterator

        }))
    }
    // TODO: A different set of job is created for topK structure

    topKWrappers.foreach {
      case (name, (topKWrapper, topkRDD)) =>
        val clazz = SqlUtils.getInternalType(
          topKWrapper.schema(topKWrapper.key.name).dataType)
        val ct = ClassTag(clazz)
        SnappyContext.populateTopK(rows, topKWrapper, this,
          name, topkRDD, time)(ct)

    }

    // add to list in relation
    // TODO: avoid a separate job for each RDD and instead try to do it
    // TODO: using a single UnionRDD or something
    rdds.foreach {
      case (relation, rdd) =>
        val cached = rdd.persist(storageLevel)
        if (cached.count() > 0) {
          relation match {
            case externalStore: ExternalStoreRelation =>
              externalStore.appendUUIDBatch(cached.asInstanceOf[RDD[UUIDRegionKey]])
            case appendable: InMemoryAppendableRelation =>
              appendable.appendBatch(cached.asInstanceOf[RDD[CachedBatch]])
          }
        }
    }
  }

  def appendToCache(df: DataFrame, tableIdent: String,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK) {
    val useCompression = conf.useCompression
    val columnBatchSize = conf.columnBatchSize

    val tableName = catalog.newQualifiedTableName(tableIdent)
    val plan = catalog.lookupRelation(tableName, None)
    val relation = cacheManager.lookupCachedData(plan).getOrElse {
      cacheManager.cacheQuery(DataFrame(this, plan),
        Some(tableName.qualifiedName), storageLevel)

      cacheManager.lookupCachedData(plan).getOrElse {
        sys.error(s"couldn't cache table $tableName")
      }
    }

    val (schema, output) = (df.schema, df.logicalPlan.output)

    val cached = df.mapPartitions { rowIterator =>

      val batches = ExternalStoreRelation(useCompression, columnBatchSize,
        tableName, schema, relation.cachedRepresentation, output)

      val converter = CatalystTypeConverters.createToCatalystConverter(schema)
      rowIterator.map(converter(_).asInstanceOf[Row])
        .foreach(batches.appendRow((), _))
      batches.forceEndOfBatch().iterator
    }.persist(storageLevel)

    // trigger an Action to materialize 'cached' batch
    if (cached.count() > 0) {
      relation.cachedRepresentation match {
        case externalStore: ExternalStoreRelation =>
          externalStore.appendUUIDBatch(cached.asInstanceOf[RDD[UUIDRegionKey]])
        case appendable: InMemoryAppendableRelation =>
          appendable.appendBatch(cached.asInstanceOf[RDD[CachedBatch]])
      }
    }
  }

  def appendToCacheRDD(rdd: RDD[_], tableIdent: String, schema: StructType,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK) {
    val useCompression = conf.useCompression
    val columnBatchSize = conf.columnBatchSize

    val tableName = catalog.newQualifiedTableName(tableIdent)
    val plan = catalog.lookupRelation(tableName, None)
    val relation = cacheManager.lookupCachedData(plan).getOrElse {
      cacheManager.cacheQuery(DataFrame(this, plan),
        Some(tableName.qualifiedName), storageLevel)

      cacheManager.lookupCachedData(plan).getOrElse {
        sys.error(s"couldn't cache table $tableName")
      }
    }



    val cached = rdd.mapPartitions { rowIterator =>

      val batches = ExternalStoreRelation(useCompression, columnBatchSize,
        tableName, schema, relation.cachedRepresentation, schema.toAttributes)

      val converter = CatalystTypeConverters.createToCatalystConverter(schema)
      rowIterator.map(converter(_).asInstanceOf[Row])
          .foreach(batches.appendRow((), _))
      batches.forceEndOfBatch().iterator
    }.persist(storageLevel)

    // trigger an Action to materialize 'cached' batch
    if (cached.count() > 0) {
      relation.cachedRepresentation match {
        case externalStore: ExternalStoreRelation =>
          externalStore.appendUUIDBatch(cached.asInstanceOf[RDD[UUIDRegionKey]])
        case appendable: InMemoryAppendableRelation =>
          appendable.appendBatch(cached.asInstanceOf[RDD[CachedBatch]])
      }
    }
  }

  def truncateTable(tableName: String): Unit = {
    cacheManager.lookupCachedData(catalog.lookupRelation(
      tableName)).foreach(_.cachedRepresentation.
      asInstanceOf[InMemoryAppendableRelation].truncate())
  }

  def registerTable[A <: Product: u.TypeTag](tableName: String) = {
    if (u.typeOf[A] =:= u.typeOf[Nothing]) {
      sys.error("Type of case class object not mentioned. " +
        "Mention type information for e.g. registerSampleTableOn[<class>]")
    }

    SparkPlan.currentContext.set(self)
    val schema = ScalaReflection.schemaFor[A].dataType
      .asInstanceOf[StructType]

    val plan: LogicalRDD = LogicalRDD(schema.toAttributes,
      new DummyRDD(this))(this)

    catalog.registerTable(Seq(tableName), plan)
  }

  def registerSampleTable(tableName: String, schema: StructType,
    samplingOptions: Map[String, Any], streamTable: Option[String] = None,
    jdbcSource: Option[Map[String, String]] = None): SampleDataFrame = {
    catalog.registerSampleTable(tableName, schema, samplingOptions,
      None, streamTable.map(catalog.newQualifiedTableName), jdbcSource)
  }

  def registerSampleTableOn[A <: Product: u.TypeTag](tableName: String,
    samplingOptions: Map[String, Any], streamTable: Option[String] = None,
    jdbcSource: Option[Map[String, String]] = None): DataFrame = {
    if (u.typeOf[A] =:= u.typeOf[Nothing]) {
      sys.error("Type of case class object not mentioned. " +
        "Mention type information for e.g. registerSampleTableOn[<class>]")
    }
    SparkPlan.currentContext.set(self)
    val schemaExtract = ScalaReflection.schemaFor[A].dataType
      .asInstanceOf[StructType]
    registerSampleTable(tableName, schemaExtract, samplingOptions,
      streamTable, jdbcSource)
  }

  def registerAndInsertIntoExternalStore(df: DataFrame, tableName: String,
    schema: StructType, jdbcSource: Map[String, String]): Unit = {
    catalog.registerAndInsertIntoExternalStore(df, tableName, schema, jdbcSource)
  }

  def registerTopK(tableName: String, streamTableName: String,
    topkOptions: Map[String, Any], isStreamSummary: Boolean) = {
    val topKRDD = SnappyContext.createTopKRDD(tableName, this.sc, isStreamSummary)
    catalog.registerTopK(tableName, streamTableName,
      catalog.getStreamTableRelation(streamTableName).schema, topkOptions, topKRDD)
  }

  override def createExternalTable(
      tableName: String,
      provider: String,
      options: Map[String, String]): DataFrame = {
    createExternalTable(tableName, provider, None, options,
      allowExisting = false)
  }

  override def createExternalTable(
      tableName: String,
      provider: String,
      schema: StructType,
      options: Map[String, String]): DataFrame = {
    createExternalTable(tableName, provider, Some(schema), options,
      allowExisting = false)
  }

  /**
   * Create an external table with given options.
   */
  def createExternalTable(
      tableName: String,
      provider: String,
      userSpecifiedSchema: Option[StructType],
      options: Map[String, String],
      allowExisting: Boolean): DataFrame = {

    if (catalog.tableExists(tableName)) {
      if (allowExisting) {
        return table(tableName)
      } else {
        throw new AnalysisException(
          s"createExternalTable: Table $tableName already exists.")
      }
    }

    // add tableName in properties if not already present
    val dbtableProp = JdbcExtendedUtils.DBTABLE_PROPERTY
    val params = options.keysIterator.collectFirst {
      case key if key.equalsIgnoreCase(dbtableProp) => options
    }.getOrElse(options + (dbtableProp -> tableName))
    val resolved = ResolvedDataSource(
      this, userSpecifiedSchema, Array.empty[String], provider, params)

    val plan = LogicalRelation(resolved.relation)
    catalog.registerExternalTable(tableName, userSpecifiedSchema,
      provider, options, LogicalRelation(resolved.relation))
    DataFrame(this, plan)
  }

  /**
   * Drop an external table created by a call to createExternalTable.
   */
  def dropExternalTable(tableName: String, ifExists: Boolean): Unit = {
    val df = try {
      table(tableName)
    } catch {
      case ae: AnalysisException =>
        if (ifExists) return else throw ae
    }
    // additional cleanup for external tables, if required
    snappy.unwrapSubquery(df.logicalPlan) match {
      case LogicalRelation(br) =>
        cacheManager.tryUncacheQuery(df)
        catalog.unregisterExternalTable(tableName)
        br match {
          case d: DeletableRelation => d.destroy(ifExists)
        }
      case _ => throw new AnalysisException(
        s"dropExternalTable: Table $tableName not an external table")
    }
  }

  // insert/update/delete operations on an external table

  def insert(tableName: String, rows: Row*): Int = {
    catalog.lookupRelation(tableName) match {
      case LogicalRelation(r: RowInsertableRelation) => r.insert(rows)
      case _ => throw new AnalysisException(
        s"$tableName is not a row insertable table")
    }
  }

  def update(tableName: String, filterExpr: String, newColumnValues: Row,
    updateColumns: String*): Int = {
    catalog.lookupRelation(tableName) match {
      case LogicalRelation(u: UpdatableRelation) =>
        u.update(filterExpr, newColumnValues, updateColumns)
      case _ => throw new AnalysisException(
        s"$tableName is not an updatable table")
    }
  }

  def delete(tableName: String, filterExpr: String): Int = {
    catalog.lookupRelation(tableName) match {
      case LogicalRelation(d: DeletableRelation) => d.delete(filterExpr)
      case _ => throw new AnalysisException(
        s"$tableName is not a deletable table")
    }
  }

  // end of insert/update/delete operations

  @transient
  override protected[sql] lazy val analyzer: Analyzer =
    new Analyzer(catalog, functionRegistry, conf) {
      override val extendedResolutionRules =
        ExtractPythonUdfs ::
          sources.PreInsertCastAndRename ::
          WeightageRule ::
          Nil

      override val extendedCheckRules = Seq(
        sources.PreWriteCheck(catalog))
    }

  @transient override protected[sql] val planner = new SparkPlanner {
    val snappyContext = self

    override def strategies: Seq[Strategy] = Seq(
      SnappyStrategies, StreamStrategy, StoreStrategy) ++ super.strategies

    object SnappyStrategies extends Strategy {
      def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
        case s @ StratifiedSample(options, child, _) =>
          s.getExecution(planLater(child)) :: Nil
        case PhysicalOperation(projectList, filters,
          mem: columnar.InMemoryAppendableRelation) =>
          pruneFilterProject(
            projectList,
            filters,
            identity[Seq[Expression]], // All filters still need to be evaluated
            InMemoryAppendableColumnarTableScan(_, filters, mem)) :: Nil
        case _ => Nil
      }
    }

  }

  /**
   * Queries the topK structure between two points in time. If the specified
   * time lies between a topK interval the whole interval is considered
   *
   * @param topKName - The topK structure that is to be queried.
   * @param startTime start time as string of the format "yyyy-mm-dd hh:mm:ss".
   *                  If passed as null, oldest interval is considered as the start interval.
   * @param endTime  end time as string of the format "yyyy-mm-dd hh:mm:ss".
   *                 If passed as null, newest interval is considered as the last interval.
   * @param k Optional. Number of elements to be queried. This is to be passed only for stream summary
   * @return returns the top K elements with their respective frequencies between two time
   */
  def queryTopK[T: ClassTag](topKName: String,
    startTime: String = null, endTime: String = null,
    k: Int = -1): DataFrame = {
    val stime = if (startTime == null) 0L
    else CastLongTime.getMillis(java.sql.Timestamp.valueOf(startTime))

    val etime = if (endTime == null) Long.MaxValue
    else CastLongTime.getMillis(java.sql.Timestamp.valueOf(endTime))

    queryTopK[T](topKName, stime, etime, k)
  }

  def queryTopK[T: ClassTag](topKName: String,
    startTime: Long, endTime: Long): DataFrame =
    queryTopK[T](topKName, startTime, endTime, -1)

  def queryTopK[T: ClassTag](topKIdent: String,
    startTime: Long, endTime: Long, k: Int): DataFrame = {
    val topKTableName = catalog.newQualifiedTableName(topKIdent)
    topKLocks(topKTableName.toString()).executeInReadLock {
      val (topkWrapper, rdd) = catalog.topKStructures(topKTableName)
      //requery the catalog to obtain the TopKRDD

      val size = if (k > 0) k else topkWrapper.size

      val topKName = topKTableName.qualifiedName
      if (topkWrapper.stsummary) {
        queryTopkStreamSummary(topKName, startTime, endTime, topkWrapper, size, rdd)
      } else {
        queryTopkHokusai(topKName, startTime, endTime, topkWrapper, rdd, size)

      }
    }
  }

  import snappy.RDDExtensions

  def queryTopkStreamSummary[T: ClassTag](topKName: String,
    startTime: Long, endTime: Long,
    topkWrapper: TopKWrapper, k: Int, topkRDD: RDD[(Int, TopK)]): DataFrame = {
    val rdd = topkRDD.mapPartitionsPreserve[(T, Approximate)] { iter =>
      {
        iter.next()._2 match {
          case x: StreamSummaryAggregation[_] => {
            val arrayTopK = x.asInstanceOf[StreamSummaryAggregation[T]].getTopKBetweenTime(startTime,
              endTime, x.capacity)
            arrayTopK.map(_.toIterator).getOrElse(Iterator.empty)
          }
          case _ => Iterator.empty
        }
      }
    }
    val topKRDD = rdd.reduceByKey(_ + _).mapPreserve {
      case (key, approx) =>
        Row(key, approx.estimate, approx.lowerBound)
    }

    val aggColumn = "EstimatedValue"
    val errorBounds = "DeltaError"
    val topKSchema = StructType(Array(topkWrapper.key,
      StructField(aggColumn, LongType),
      StructField(errorBounds, LongType)))

    val df = createDataFrame(topKRDD, topKSchema)
    df.sort(df.col(aggColumn).desc).limit(k)
  }

  def queryTopkHokusai[T: ClassTag](topKName: String,
    startTime: Long, endTime: Long,
    topkWrapper: TopKWrapper, topkRDD: RDD[(Int, TopK)], k: Int): DataFrame = {

    // TODO: perhaps this can be done more efficiently via a shuffle but
    // using the straightforward approach for now

    // first collect keys from across the cluster
    val rdd = topkRDD.mapPartitionsPreserve[(T, Approximate)] { iter =>
      {
        iter.next()._2 match {
          case x: TopKHokusai[_] => {
            val arrayTopK = if (x.windowSize == Long.MaxValue)
              Some(x.asInstanceOf[TopKHokusai[T]].getTopKInCurrentInterval)
            else
              x.asInstanceOf[TopKHokusai[T]].getTopKBetweenTime(startTime, endTime)

            arrayTopK.map(_.toIterator).getOrElse(Iterator.empty)
          }
          case _ => Iterator.empty
        }
      }
    }
    val topKRDD = rdd.reduceByKey(_ + _).mapPreserve {
      case (key, approx) =>
        Row(key, approx.estimate, approx)
    }

    val aggColumn = "EstimatedValue"
    val errorBounds = "ErrorBoundsInfo"
    val topKSchema = StructType(Array(topkWrapper.key,
      StructField(aggColumn, LongType),
      StructField(errorBounds, ApproximateType)))

    val df = createDataFrame(topKRDD, topKSchema)
    df.sort(df.col(aggColumn).desc).limit(k)
  }

  private var storeConfig: Map[String, String] = _

  def setExternalStoreConfig(conf: Map[String, String]) = {
    this.storeConfig = conf
  }

  def getExternalStoreConfig: Map[String, String] = {
    storeConfig
  }

  def isLoner = sparkContext.schedulerBackend match {
    case lb: LocalBackend => true
    case _ => false
  }

}

object snappy extends Serializable {

  implicit def snappyOperationsOnDataFrame(df: DataFrame): SnappyOperations = {
    df.sqlContext match {
      case sc: SnappyContext => SnappyOperations(sc, df)
      case sc => throw new AnalysisException("Extended snappy operations " +
        s"require SnappyContext and not ${sc.getClass.getSimpleName}")
    }
  }

  def unwrapSubquery(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case Subquery(_, child) => unwrapSubquery(child)
      case _ => plan
    }
  }

  implicit def samplingOperationsOnDataFrame(df: DataFrame): SampleDataFrame = {
    df.sqlContext match {
      case sc: SnappyContext =>
        unwrapSubquery(df.logicalPlan) match {
          case ss: StratifiedSample =>
            new SampleDataFrame(sc, ss)
          case s => throw new AnalysisException("Stratified sampling " +
              "operations require stratifiedSample plan and not " +
              s"${s.getClass.getSimpleName}")
        }
      case sc => throw new AnalysisException("Extended snappy operations " +
          s"require SnappyContext and not ${sc.getClass.getSimpleName}")
    }
  }

  implicit def snappyOperationsOnDStream[T: ClassTag](
    ds: DStream[T]): SnappyDStreamOperations[T] =
    SnappyDStreamOperations(SnappyContext(ds.context.sparkContext), ds)

  implicit class SparkContextOperations(val s: SparkContext) {
    def getOrCreateStreamingContext(batchInterval: Int = 2): StreamingContext = {
      StreamingCtxtHolder(s, batchInterval)
    }
  }

  implicit class RDDExtensions[T: ClassTag](rdd: RDD[T]) extends Serializable {

    /**
     * Return a new RDD by applying a function to all elements of this RDD.
     */
    def mapPreserve[U: ClassTag](f: T => U): RDD[U] = rdd.withScope {
      val cleanF = rdd.sparkContext.clean(f)
      new MapPartitionsPreserveRDD[U, T](rdd,
        (context, pid, iter) => iter.map(cleanF))
    }

    /**
     * Return a new RDD by applying a function to each partition of given RDD.
     * This variant also preserves the preferred locations of parent RDD.
     *
     * `preservesPartitioning` indicates whether the input function preserves
     * the partitioner, which should be `false` unless this is a pair RDD and
     * the input function doesn't modify the keys.
     */
    def mapPartitionsPreserve[U: ClassTag](
      f: Iterator[T] => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = rdd.withScope {
      val cleanedF = rdd.sparkContext.clean(f)
      new MapPartitionsPreserveRDD(rdd,
        (context: TaskContext, index: Int, iter: Iterator[T]) => cleanedF(iter),
        preservesPartitioning)
    }
  }

}

object SnappyContext {

  private val atomicContext = new AtomicReference[SnappyContext]()

  def apply(sc: SparkContext,
    init: SnappyContext => SnappyContext = identity): SnappyContext = {
    val context = atomicContext.get
    if (context != null) {
      context
    } else {
      atomicContext.compareAndSet(null, init(new SnappyContext(sc)))
      atomicContext.get
    }
  }

  def createTopKRDD(name: String, context: SparkContext, isStreamSummary: Boolean): RDD[(Int, TopK)] = {
    val partCount = Utils.getAllExecutorsMemoryStatus(context).
      keySet.size * Runtime.getRuntime.availableProcessors()*2
    Utils.getFixedPartitionRDD[(Int, TopK)](context,
      (tc: TaskContext, part: Partition) => {
        scala.collection.Iterator(part.index -> TopKHokusai.createDummy)
      }, new Partitioner() {
        override def numPartitions: Int = partCount
        override def getPartition(key: Any) = scala.math.abs(key.hashCode()) % partCount
      }, partCount)
  }

  def getEpoch0AndIterator[T: ClassTag](name: String, topkWrapper: TopKWrapper,
    iterator: Iterator[Any]): (() => Long, Iterator[Any], Int) = {
    if (iterator.hasNext) {
      var tupleIterator = iterator
      val tsCol = if (topkWrapper.timeInterval > 0)
        topkWrapper.timeSeriesColumn
      else -1
      val epoch = () => {
        if (topkWrapper.epoch != -1L) {
          topkWrapper.epoch
        } else if (tsCol >= 0) {
          var epoch0 = -1L
          val iter = tupleIterator.asInstanceOf[Iterator[(T, Any)]]
          val tupleBuf = new mutable.ArrayBuffer[(T, Any)](4)

          // assume first row will have the least time
          // TODO: this assumption may not be correct and we may need to
          // do something more comprehensive
          do {
            val tuple = iter.next()
            epoch0 = tuple match {
              case (_, (_, epochh: Long)) => epochh
              case (_, epochh: Long) => epochh
            }

            tupleBuf += tuple.copy()
          } while (epoch0 <= 0)
          tupleIterator = tupleBuf.iterator ++ iter
          epoch0
        } else {
          System.currentTimeMillis()
        }

      }
      (epoch, tupleIterator, tsCol)
    } else {
      null
    }
  }

  def addDataForTopK[T: ClassTag](topKWrapper: TopKWrapper,
    tupleIterator: Iterator[Any], topK: TopK, tsCol: Int, time: Long): Unit = {

    val streamSummaryAggr: StreamSummaryAggregation[T] = if (topKWrapper.stsummary) {
      topK.asInstanceOf[StreamSummaryAggregation[T]]
    } else {
      null
    }
    val topKHokusai = if (!topKWrapper.stsummary) {
      topK.asInstanceOf[TopKHokusai[T]]
    } else {
      null
    }
    val topKKeyIndex = topKWrapper.schema.fieldIndex(topKWrapper.key.name)
    if (tsCol < 0) {
      if (topKWrapper.stsummary) {
        throw new IllegalStateException(
          "Timestamp column is required for stream summary")
      }
      topKWrapper.frequencyCol match {
        case None =>
           topKHokusai.addEpochData(tupleIterator.asInstanceOf[Iterator[T]].
               toSeq.foldLeft(
                   scala.collection.mutable.Map.empty[T, Long]){ 
             (m,x) => m +  ((x,m.getOrElse(x, 0l) + 1 )) 
             }, time)
        case Some(freqCol) =>
          val datamap = mutable.Map[T, Long]()
          tupleIterator.asInstanceOf[Iterator[(T, Long)]] foreach {
            case (key, freq) =>
              datamap.get(key) match {
                case Some(prevvalue) => datamap +=
                  (key -> (prevvalue + freq))
                case None => datamap +=
                  (key -> freq)
              }

          }
          topKHokusai.addEpochData(datamap, time)
      }
    } else {
      val dataBuffer = new mutable.ArrayBuffer[KeyFrequencyWithTimestamp[T]]
      val buffer = topKWrapper.frequencyCol match {
        case None =>
          tupleIterator.asInstanceOf[Iterator[(T, Long)]] foreach {
            case (key, timeVal) =>
              dataBuffer += new KeyFrequencyWithTimestamp[T](key, 1L, timeVal)
          }
          dataBuffer
        case Some(freqCol) =>
          tupleIterator.asInstanceOf[Iterator[(T, (Long, Long))]] foreach {
            case (key, (freq, timeVal)) =>
              dataBuffer += new KeyFrequencyWithTimestamp[T](key,
                freq, timeVal)
          }
          dataBuffer
      }
      if (topKWrapper.stsummary)
        streamSummaryAggr.addItems(buffer)
      else
        topKHokusai.addTimestampedData(buffer)

    }
  }

  def populateTopK[T: ClassTag](rows: RDD[Row], topkWrapper: TopKWrapper,
    context: SnappyContext, name: QualifiedTableName, topKRDD: RDD[(Int, TopK)],
    time: Long) {
    val partitioner = topKRDD.partitioner.get
    //val pairRDD = rows.map[(Int, Any)](topkWrapper.rowToTupleConverter(_, partitioner))
    val batches = mutable.ArrayBuffer.empty[(Int, mutable.ArrayBuffer[Any])]
    val pairRDD = rows.mapPartitions[(Int, mutable.ArrayBuffer[Any])](iter=> {
       val map =  iter.foldLeft(mutable.Map.empty[Int, mutable.ArrayBuffer[Any]])((m,x) => {
         val(partitionID, elem) = topkWrapper.rowToTupleConverter(x, partitioner)
         val list =  m.getOrElse(partitionID, mutable.ArrayBuffer[Any]()) += elem
         if(list.size > 1000) {
           batches += partitionID -> list
           m -= partitionID
         }else {
           m += (partitionID -> list)
         }
         m
       })
       map.toIterator ++ batches.iterator      
     }, true)
    
    val nameAsString = name.toString
    val newTopKRDD = topKRDD.cogroup(pairRDD).mapPartitions[(Int, TopK)](
      iterator => {
        val (key, (topkIterable, dataIterable)) = iterator.next()
        val tsCol = if (topkWrapper.timeInterval > 0)
          topkWrapper.timeSeriesColumn
        else -1
        
        topkIterable.head match {         
          case z: TopKStub =>
            val totalDataIterable = dataIterable.foldLeft[mutable.ArrayBuffer[Any]](mutable.ArrayBuffer.empty[Any])( (m,x) => {
            if(m.size > x.size) {
              m ++= x
            }else {
              x ++= m
            }
            })
            val (epoch0, iter, tsCol) = getEpoch0AndIterator[T](nameAsString, topkWrapper,
              totalDataIterable.iterator)
            val topK =  if (topkWrapper.stsummary) {
              StreamSummaryAggregation.create[T](topkWrapper.size,
                topkWrapper.timeInterval, epoch0, topkWrapper.maxinterval)
            } else {
              TopKHokusai.create[T](topkWrapper.cms, topkWrapper.size,
                tsCol, topkWrapper.timeInterval, epoch0)
            }
            SnappyContext.addDataForTopK[T](topkWrapper,
            iter, topK, tsCol, time)
            scala.collection.Iterator(key -> topK)
           
          case topK: TopK =>   
            dataIterable.foreach { x =>  SnappyContext.addDataForTopK[T](topkWrapper,
             x.iterator,  topK, tsCol, time) 
          }
            
          scala.collection.Iterator(key -> topK)
        }
        
      }, true)

    newTopKRDD.persist()
    //To allow execution of RDD
    newTopKRDD.count
    context.catalog.topKStructures.put(name, topkWrapper -> newTopKRDD)
    //Unpersist old rdd in a write lock

    context.topKLocks(name.toString()).executeInWriteLock {
      topKRDD.unpersist(false)
    }
  }
}

//end of SnappyContext

private[sql] case class SnappyOperations(context: SnappyContext,
  df: DataFrame) {

  /**
   * Creates stratified sampled data from given DataFrame
   * {{{
   *   peopleDf.stratifiedSample(Map("qcs" -> Array(1,2), "fraction" -> 0.01))
   * }}}
   */
  def stratifiedSample(options: Map[String, Any]): SampleDataFrame =
    new SampleDataFrame(context, StratifiedSample(options, df.logicalPlan)())

  def createTopK(ident: String, options: Map[String, Any]): Unit = {
    val name = context.catalog.newQualifiedTableName(ident)
    val schema = df.logicalPlan.schema

    // Create a very long timeInterval when the topK is being created
    // on a DataFrame.

    val topKWrapper = TopKWrapper(name, options, schema)

    val clazz = SqlUtils.getInternalType(
      topKWrapper.schema(topKWrapper.key.name).dataType)
    val ct = ClassTag(clazz)
    context.topKLocks += name.toString() -> new ReadWriteLock()
    val topKRDD = SnappyContext.createTopKRDD(name.toString, context.sparkContext, topKWrapper.stsummary)
    context.catalog.topKStructures.put(name, topKWrapper -> topKRDD)
    SnappyContext.populateTopK(df.rdd, topKWrapper, context,
      name, topKRDD, System.currentTimeMillis())(ct)

    /*df.foreachPartition((x: Iterator[Row]) => {
      context.addDataForTopK(name, topKWrapper, x)(ct)
    })*/

  }

  /**
   * Table must be registered using #registerSampleTable.
   */
  def insertIntoSampleTables(sampleTableName: String*) =
    context.collectSamples(df.rdd, sampleTableName, System.currentTimeMillis())

  /**
   * Append to an existing cache table.
   * Automatically uses #cacheQuery if not done already.
   */
  def appendToCache(tableName: String) = context.appendToCache(df, tableName)

  def registerAndInsertIntoExternalStore(tableName: String,
    jdbcSource: Map[String, String]): Unit = {
    context.registerAndInsertIntoExternalStore(df, tableName,
      df.schema, jdbcSource)
  }
}

private[sql] case class SnappyDStreamOperations[T: ClassTag](
  context: SnappyContext, ds: DStream[T]) {

  def saveStream(sampleTab: Seq[String],
    formatter: (RDD[T], StructType) => RDD[Row],
    schema: StructType,
    transform: DataFrame => DataFrame = null): Unit =
    context.saveStream(ds, sampleTab, formatter, schema, transform)

  def saveToExternalTable[A <: Product : TypeTag](externalTable : String, jdbcSource : Map[String, String]): Unit = {

    val schema : StructType = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    saveStreamToExternalTable(externalTable, schema, jdbcSource)
  }

  def saveToExternalTable(externalTable : String, schema: StructType, jdbcSource : Map[String, String]): Unit = {
    saveStreamToExternalTable(externalTable, schema, jdbcSource)
  }

  private def saveStreamToExternalTable(externalTable : String, schema: StructType, jdbcSource : Map[String, String]): Unit = {
    require(externalTable != null && externalTable.length > 0,
      "saveToExternalTable: expected non-empty table name")

    val qualifiedTable = context.catalog.newQualifiedTableName(externalTable)
    val externalStore = new JDBCSourceAsStore(jdbcSource)
    context.catalog.createExternalTableForCachedBatches(qualifiedTable.qualifiedName,
      externalStore)
    val attributeSeq = schema.toAttributes

    val dummyDF = {
      val plan: LogicalRDD = LogicalRDD(attributeSeq,
        new DummyRDD(context))(context)
      DataFrame(context, plan)
    }

    context.catalog.tables.put(qualifiedTable, dummyDF.logicalPlan)
    context.cacheManager.cacheQuery_ext(dummyDF, Some(qualifiedTable.qualifiedName),
      externalStore)

    ds.foreachRDD((rdd: RDD[T], time: Time) => {
      context.appendToCacheRDD(rdd, qualifiedTable.qualifiedName, schema)
    })
  }
}
