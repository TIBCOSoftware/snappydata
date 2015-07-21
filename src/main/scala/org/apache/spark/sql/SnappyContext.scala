package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => u}

import io.snappydata.util.SqlUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.columnar.{InMemoryAppendableColumnarTableScan, InMemoryAppendableRelation}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.row.UpdatableRelation
import org.apache.spark.sql.execution.streamsummary.StreamSummaryAggregation
import org.apache.spark.sql.sources.{CastLongTime, LogicalRelation, StreamStrategy, WeightageRule}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, Time}
import org.apache.spark.{Partitioner, SparkContext, TaskContext}

/**
 * An instance of the Spark SQL execution engine that delegates to supplied SQLContext
 * offering additional capabilities.
 *
 * Created by Soubhik on 5/13/15.
 */
protected[sql] final class SnappyContext(sc: SparkContext)
    extends SQLContext(sc) with Serializable {

  self =>

  @transient
  override protected[sql] val ddlParser = new SnappyDDLParser(sqlParser.parse)

  override protected[sql] def dialectClassName = if (conf.dialect == "sql") {
    classOf[SnappyParserDialect].getCanonicalName
  } else {
    conf.dialect
  }

  @transient
  override protected[sql] lazy val catalog =
    new SnappyStoreCatalog(this, conf)

  @transient
  override protected[sql] val cacheManager = new SnappyCacheManager(this)

  def saveStream[T: ClassTag](stream: DStream[T],
    sampleTab: Seq[String],
    formatter: (RDD[T], StructType) => RDD[Row],
    schema: StructType,
    transform: DataFrame => DataFrame = null) {
    stream.foreachRDD((rdd: RDD[T], time: Time) => {

      val row = formatter(rdd, schema)

      val rDF = createDataFrame(row, schema)

      val tDF = if (transform != null) {
        transform(rDF)
      } else rDF

      collectSamples(tDF, sampleTab)

    })
  }

  def collectSamples(tDF: DataFrame, sampleTab: Seq[String],
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK) {
    val useCompression = conf.useCompression
    val columnBatchSize = conf.columnBatchSize

    val sampleTables = (catalog.sampleTables.filter {
      case (name, df) => sampleTab.contains(name)
    } map {
      case (name, df) =>
        val sample = df.logicalPlan
        (name, sample.options, sample.schema, sample.output,
          cacheManager.lookupCachedData(df.logicalPlan).getOrElse(sys.error(
            s"SnappyContext.saveStream: failed to lookup cached plan for " +
              s"sampling table $name")).cachedRepresentation)
    }).toSeq

    // TODO: this iterates rows multiple times
    val rdds = sampleTables.map {
      case (name, samplingOptions, schema, output, relation) =>
        (relation, tDF.mapPartitions(rowIterator => {
          val sampler = StratifiedSampler(samplingOptions, Array.emptyIntArray,
            nameSuffix = "", columnBatchSize, schema, cached = true)
          // create a new holder for set of CachedBatches
          val batches = InMemoryAppendableRelation(useCompression,
            columnBatchSize, name, schema, output)
          sampler.append(rowIterator, null, (),
            batches.appendRow, batches.endRows)
          batches.forceEndOfBatch().iterator
        }))
    }
    // TODO: A different set of job is created for topK structure
    catalog.topKStructures.filter {
      case (name, topkstruct) => sampleTab.contains(name)
    } foreach {
      case (name, topKWrapper) =>
        val clazz = SqlUtils.getInternalType(
          topKWrapper.schema(topKWrapper.key.name).dataType)
        val ct = ClassTag(clazz)
        SnappyContext.populateTopK(tDF, topKWrapper, this, name)(ct)
    }

    // add to list in relation
    // TODO: avoid a separate job for each RDD and instead try to do it
    // TODO: using a single UnionRDD or something
    rdds.foreach {
      case (relation, rdd) =>
        val cached = rdd.persist(storageLevel)
        if (cached.count() > 0) {
          relation.asInstanceOf[InMemoryAppendableRelation].appendBatch(cached)
        }
    }
  }

  def appendToCache(df: DataFrame, tableName: String,
    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK) {
    val useCompression = conf.useCompression
    val columnBatchSize = conf.columnBatchSize

    val relation = cacheManager.lookupCachedData(catalog.lookupRelation(
      Seq(tableName))).getOrElse {
      val lookup = catalog.lookupRelation(Seq(tableName))
      cacheManager.cacheQuery(
        DataFrame(this, lookup),
        Some(tableName), storageLevel)

      cacheManager.lookupCachedData(lookup).getOrElse {
        sys.error(s"couldn't cache table $tableName")
      }
    }

    val (schema, output) = (df.schema, df.logicalPlan.output)

    val cached = df.mapPartitions { rowIterator =>

      val batches = InMemoryAppendableRelation(useCompression,
        columnBatchSize, tableName, schema, output)

      rowIterator.foreach(batches.appendRow((), _))
      batches.forceEndOfBatch().iterator

    }.persist(storageLevel)

    // trigger an Action to materialize 'cached' batch
    if (cached.count() > 0) {
      relation.cachedRepresentation.asInstanceOf[InMemoryAppendableRelation].
        appendBatch(cached)
    }
  }

  def truncateTable(tableName: String): Unit = {
    cacheManager.lookupCachedData(catalog.lookupRelation(
      Seq(tableName))).foreach(_.cachedRepresentation.
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
    samplingOptions: Map[String, Any]): SampleDataFrame = {
    catalog.registerSampleTable(schema, tableName, samplingOptions)
  }

  def registerSampleTableOn[A <: Product: u.TypeTag](tableName: String,
    samplingOptions: Map[String, Any]): DataFrame = {
    if (u.typeOf[A] =:= u.typeOf[Nothing]) {
      sys.error("Type of case class object not mentioned. " +
        "Mention type information for e.g. registerSampleTableOn[<class>]")
    }
    SparkPlan.currentContext.set(self)
    val schemaExtract = ScalaReflection.schemaFor[A].dataType
      .asInstanceOf[StructType]
    registerSampleTable(tableName, schemaExtract, samplingOptions)
  }

  def registerTopK(tableName: String, streamTableName: String,
    topkOptions: Map[String, Any]) = {
    catalog.registerTopK(tableName, streamTableName,
      catalog.getStreamTable(streamTableName).schema, topkOptions)
  }

  // insert/update/delete/drop operations on an external table

  private def getUpdatableRelation(tableName: String): UpdatableRelation = {
    catalog.lookupRelation(Seq(tableName)) match {
      case LogicalRelation(u: UpdatableRelation) => u
      case _ => throw new AnalysisException(
        s"$tableName is not an updatable table")
    }
  }

  def insert(tableName: String, row: Row): Int = {
    getUpdatableRelation(tableName).insert(row)
  }

  def update(tableName: String, updatedColumns: Row, filterExpr: String,
      setColumns: String*): Int = {
    getUpdatableRelation(tableName).update(updatedColumns, setColumns.toSeq,
      filterExpr)
  }

  def delete(tableName: String, filterExpr: String): Int = {
    getUpdatableRelation(tableName).delete(filterExpr)
  }

  def dropExternalTable(tableName: String): Unit = {
    val df = table(tableName)
    // additional cleanup for external tables, if required
    df.logicalPlan match {
      case LogicalRelation(br) =>
        cacheManager.tryUncacheQuery(df)
        catalog.unregisterTable(Seq(tableName))
        br match {
          case u: UpdatableRelation => u.destroy()
        }
      case _ => throw new AnalysisException(
        s"Table $tableName not an external table")
    }
  }

  // end of insert/update/delete/drop operations

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
      SnappyStrategies, StreamStrategy) ++ super.strategies

    object SnappyStrategies extends Strategy {
      def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
        case s @ StratifiedSample(options, child) =>
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

  def queryTopK[T: ClassTag](topKName: String,
      startTime: Long, endTime: Long, k: Int): DataFrame = {
    val topk: TopKWrapper = catalog.topKStructures(topKName)

    val size = if (k > 0) k else topk.size
    if (topk.stsummary) {
      queryTopkStreamSummary(topKName, startTime, endTime, topk, size)
    }
    else {
      queryTopkHokusai(topKName, startTime, endTime, topk, size)
    }
  }

  import snappy.RDDExtensions

  def queryTopkStreamSummary[T: ClassTag](topKName: String,
      startTime: Long, endTime: Long,
      topK: TopKWrapper, k: Int): DataFrame = {
    val topKRDD = TopKRDD.streamRDD[T](topKName, startTime, endTime, this)
        .reduceByKey(_ + _).mapPreserve { case (key, approx) =>
      Row(key, approx.estimate, approx.lowerBound)
    }

    val aggColumn = "EstimatedValue"
    val errorBounds = "DeltaError"
    val topKSchema = StructType(Array(topK.key,
      StructField(aggColumn, LongType),
      StructField(errorBounds, LongType)))

    val df = createDataFrame(topKRDD, topKSchema)
    df.sort(df.col(aggColumn).desc).limit(k)
  }

  def queryTopkHokusai[T: ClassTag](topKName: String,
    startTime: Long, endTime: Long,
    topK: TopKWrapper, k: Int): DataFrame = {

    val topKRDD = TopKRDD.resultRDD[T](topKName, startTime, endTime, this)
        .reduceByKey(_ + _).mapPreserve { case (key, approx) =>
      Row(key, approx.estimate, approx)
    }

    val aggColumn = "EstimatedValue"
    val errorBounds = "ErrorBoundsInfo"
    val topKSchema = StructType(Array(topK.key,
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
}

object snappy extends Serializable {

  implicit def snappyOperationsOnDataFrame(df: DataFrame): SnappyOperations = {
    df.sqlContext match {
      case sc: SnappyContext => SnappyOperations(sc, df)
      case sc => throw new AnalysisException("Extended snappy operations " +
        s"require SnappyContext and not ${sc.getClass.getSimpleName}")
    }
  }

  implicit def samplingOperationsOnDataFrame(df: DataFrame): SampleDataFrame = {
    df.sqlContext match {
      case sc: SnappyContext =>
        df.logicalPlan match {
          case ss: StratifiedSample => new SampleDataFrame(sc, ss)
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

  def addDataForTopK[T: ClassTag](name: String, topKWrapper: TopKWrapper,
    iterator: Iterator[(T, Any)]): Unit = if (iterator.hasNext) {
    var tupleIterator = iterator
    val tsCol = if (topKWrapper.timeInterval > 0)
      topKWrapper.timeSeriesColumn
    else -1
    val epoch = () => {
      if (topKWrapper.epoch != -1L) {
        topKWrapper.epoch
      } else if (tsCol >= 0) {
        var epoch0 = -1L
        val iter = tupleIterator.asInstanceOf[Iterator[(T, (Long, Long))]]
        val tupleBuf = new mutable.ArrayBuffer[(T, (Long, Long))](4)

        // assume first row has the least time
        // if this is not the case then epoch needs to be provided via property
        do {
          val tuple = iter.next()
          epoch0 = tuple match {
            case (_, (_, epochh)) => epochh
          }

          tupleBuf += tuple.copy()
        } while (epoch0 <= 0)
        tupleIterator = tupleBuf.iterator ++ iter
        epoch0
      } else {
        System.currentTimeMillis()
      }
    }
    var topkhokusai: TopKHokusai[T] = null
    var streamSummaryAggr: StreamSummaryAggregation[T] = null
    if (topKWrapper.stsummary) {
      streamSummaryAggr = StreamSummaryAggregation[T](name,
        topKWrapper.size, tsCol, topKWrapper.timeInterval,
        epoch, topKWrapper.maxinterval)
    } else {
      topkhokusai = TopKHokusai[T](name, topKWrapper.cms,
        topKWrapper.size, tsCol, topKWrapper.timeInterval, epoch)
    }

    //val topKKeyIndex = topKWrapper.schema.fieldIndex(topKWrapper.key.name)
    if (tsCol < 0) {
      if (topKWrapper.stsummary) {
        throw new IllegalStateException(
          "Timestamp column is required for stream summary")
      }
      topKWrapper.frequencyCol match {
        case None =>
          topkhokusai.addEpochData(
            tupleIterator.map(_._1).toSeq)
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
          topkhokusai.addEpochData(datamap)
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
        topkhokusai.addTimestampedData(buffer)
    }
  }

  def populateTopK[T: ClassTag](df: DataFrame, topkWrapper: TopKWrapper,
    context: SnappyContext, name: String) {
    val pairRDD = df.rdd.map[(Any, Any)](topkWrapper.rowToTupleConverter(_))
    val partCount = Utils.getAllExecutorsMemoryStatus(context.sparkContext).
      keySet.size
    pairRDD.partitionBy(new Partitioner() {
      override def numPartitions: Int = partCount
      override def getPartition(key: Any) = scala.math.abs(key.hashCode()) % partCount
    }).foreachPartition((iter: Iterator[(Any, Any)]) =>
      SnappyContext.addDataForTopK(name, topkWrapper, iter.asInstanceOf[Iterator[(T, Any)]]))
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

  def createTopK(name: String, options: Map[String, Any]): Unit = {
    val schema = df.logicalPlan.schema

    // Create a very long timeInterval when the topK is being created
    // on a DataFrame.
    val topKWrapper = TopKWrapper(name, options, schema)
    context.catalog.topKStructures.put(name, topKWrapper)
    val clazz = SqlUtils.getInternalType(
      topKWrapper.schema(topKWrapper.key.name).dataType)
    val ct = ClassTag(clazz)
    SnappyContext.populateTopK(df, topKWrapper, context, name)(ct)

    /*df.foreachPartition((x: Iterator[Row]) => {
      context.addDataForTopK(name, topKWrapper, x)(ct)
    })*/
  }

  /**
   * Table must be registered using #registerSampleTable.
   */
  def insertIntoSampleTables(sampleTableName: String*) =
    context.collectSamples(df, sampleTableName)

  /**
   * Append to an existing cache table.
   * Automatically uses #cacheQuery if not done already.
   */
  def appendToCache(tableName: String) = context.appendToCache(df, tableName)

  /**
   * Insert into existing sample table or create one if necessary.
   */
  def createAndInsertIntoSampleTables(in: (String, Map[String, String])*) {
    in.map {
      case (tableName, options) => context.catalog.getOrAddStreamTable(
        tableName, df.schema, options)
    }
    context.collectSamples(df, in.map(_._1))
  }
}

private[sql] case class SnappyDStreamOperations[T: ClassTag](
  context: SnappyContext, ds: DStream[T]) {

  def saveStream(sampleTab: Seq[String],
    formatter: (RDD[T], StructType) => RDD[Row],
    schema: StructType,
    transform: DataFrame => DataFrame = null): Unit =
    context.saveStream(ds, sampleTab, formatter, schema, transform)
}
