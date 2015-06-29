package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicReference

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => u}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, ScalaReflection}
import org.apache.spark.sql.columnar.{InMemoryAppendableColumnarTableScan, InMemoryAppendableRelation}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.sources.{StreamStrategy, WeightageRule}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{StreamingContext, Time}

/**
 * An instance of the Spark SQL execution engine that delegates to supplied SQLContext
 * offering additional capabilities.
 *
 * Created by Soubhik on 5/13/15.
 */
class SnappyContext(sc: SparkContext)
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

  // TODO: do we really need a CatalystConverter below and in appendToCache??
  def collectSamples(tDF: DataFrame, sampleTab: Seq[String]) {
    val useCompression = conf.useCompression
    val columnBatchSize = conf.columnBatchSize

    val sampleTables = (catalog.sampleTables.filter {
      case (name, df) => sampleTab.contains(name)
    } map { case (name, df) =>
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
          sampler.append(rowIterator,
            CatalystTypeConverters.createToCatalystConverter(schema),
            (), batches.appendRow, batches.endRows)
          batches.forceEndOfBatch().iterator
        }))
    }
    // TODO: A different set of job is created for topk structure
    val topkStructures = (catalog.topKStructures.filter {
      case (name, topkstruct) => sampleTab.contains(name)
    }) map { case (name, topkWrapper) =>

      val confidence = topkWrapper.confidence
      val eps = topkWrapper.eps
      val timeInterval = topkWrapper.timeInterval
      val topK = topkWrapper.size
      tDF.foreachPartition(rowIterator => {
        addHokusaiData(name, topkWrapper, rowIterator)
      })

    }

    // add to list in relation
    // TODO: avoid a separate job for each RDD and instead try to do it
    // TODO: using a single UnionRDD or something
    rdds.foreach { case (relation, rdd) =>
      val cached = rdd.persist(StorageLevel.MEMORY_AND_DISK)
      if (cached.count() > 0) {
        relation.asInstanceOf[InMemoryAppendableRelation].appendBatch(cached)
      }
    }

  }

  def addHokusaiData(name: String, topkWrapper: TopKHokusaiWrapper[_], rowIterator: Iterator[Row]): Unit = {
    val topkhokusai = TopKHokusai(name, topkWrapper.confidence,
      topkWrapper.eps, topkWrapper.size, topkWrapper.timeInterval)
    topkWrapper.frequencyCol match {
      case None => topkhokusai.addEpochData ((rowIterator map (r => {
        r.get (r.fieldIndex (topkWrapper.key.name) )
      }) ).toSeq)
      case Some(freqCol) =>
        var datamap = Map[Any, Long]()
        rowIterator foreach  {r => {
          val key = r.get (r.fieldIndex (topkWrapper.key.name))
          datamap.get(key) match {
            case None =>
              datamap += (key -> (r.get(r.fieldIndex(freqCol.name))).asInstanceOf[Number].longValue())
            case Some(prevvalue )=>
              datamap += (key -> (prevvalue + (r.get(r.fieldIndex(freqCol.name))).asInstanceOf[Number].longValue()))
          }
        }
        }
        topkhokusai.addEpochData (datamap)
    }
  }

  def appendToCache(df: DataFrame, tableName: String) {
    val useCompression = conf.useCompression
    val columnBatchSize = conf.columnBatchSize

    val relation = cacheManager.lookupCachedData(catalog.lookupRelation(
      Seq(tableName))).getOrElse {
      val lookup = catalog.lookupRelation(Seq(tableName))
      cacheManager.cacheQuery(
        DataFrame(this, lookup),
        Some(tableName), StorageLevel.MEMORY_AND_DISK)

      cacheManager.lookupCachedData(lookup).getOrElse {
        sys.error(s"couldn't cache table $tableName")
      }
    }

    val (schema, output) = (df.schema, df.logicalPlan.output)

    val cached = df.mapPartitions { rowIterator =>

      val batches = InMemoryAppendableRelation(useCompression,
        columnBatchSize, tableName, schema, output)

      val converter = CatalystTypeConverters.createToCatalystConverter(schema)

      rowIterator foreach { r =>
        batches.appendRow((), converter(r).asInstanceOf[Row])
      }

      batches.forceEndOfBatch().iterator

    }.persist(StorageLevel.MEMORY_AND_DISK)

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

  def registerTable[A <: Product : u.TypeTag](tableName: String) = {
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

  def registerSampleTableOn[A <: Product : u.TypeTag]
  (tableName: String, samplingOptions: Map[String, Any]): DataFrame = {
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
    catalog.registerTopK(tableName, streamTableName, catalog.getStreamTable(streamTableName).schema,
       topkOptions)
  }

  @transient
  override protected[sql] lazy val analyzer: Analyzer =
    new Analyzer(catalog, functionRegistry, conf) {
      override val extendedResolutionRules =
        ExtractPythonUdfs ::
            sources.PreInsertCastAndRename ::
            WeightageRule ::
            Nil

      override val extendedCheckRules = Seq(
        sources.PreWriteCheck(catalog)
      )
    }


  @transient override protected[sql] val planner = new SparkPlanner {
    val snappyContext = self

    override def strategies: Seq[Strategy] = Seq(
      SnappyStrategies, StreamStrategy) ++ super.strategies

    object SnappyStrategies extends Strategy {
      def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
        case s@StratifiedSample(options, child) =>
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

  def queryTopK(topkName : String,
                startTime : Long = Long.MinValue,
                endTime: Long = Long.MaxValue) : RDD[Row]  = {
    val k: TopKHokusaiWrapper[_] = catalog.topKStructures(topkName)
    if (k.timeInterval == Long.MaxValue &&
      (endTime != Long.MaxValue || startTime != Long.MinValue))
      throw new IllegalStateException ("start and end time cannot be specified while " +
        "querying a topk created over a dataframe ")

    (new TopkResultRDD(topkName, startTime, endTime)(this)) reduceByKey
      ( _ + _ ) sortBy( x => { x._2 }, false ) map (Row.fromTuple(_))

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

  implicit def snappyOperationsOnDStream[T: ClassTag](ds: DStream[T]):
  SnappyDStreamOperations[T] = SnappyDStreamOperations(SnappyContext(
    ds.context.sparkContext), ds)

  implicit class SparkContextOperations(val s: SparkContext) {
    def getOrCreateStreamingContext(batchInterval: Int = 2): StreamingContext = {
      StreamingCtxtHolder(s, batchInterval)
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
    }
    else {
      atomicContext.compareAndSet(null, init(new SnappyContext(sc)))
      atomicContext.get
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


  def createTopK(name: String, options: Map[String, Any]): Unit =  {
    val schema = df.logicalPlan.schema

    if (options exists { _._1.toLowerCase.equals("timeinterval") })
      throw new IllegalStateException("timeInterval cannot be specified " +
        "when creating a Topk over dataframe")


    // Create a very long timeinterval when the topk is being created on a dataframe.
    val topkWrapper= TopKHokusaiWrapper(name, options  +  ("timeinterval" -> Long.MaxValue.toString), schema)
    context.catalog.topKStructures.put(name, topkWrapper)

    df.foreachPartition((x:Iterator[Row]) => {
      context.addHokusaiData(name, topkWrapper, x)
    })
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
