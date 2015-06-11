package org.apache.spark.sql

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.OverrideCatalog
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, UnaryNode}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, ScalaReflection}
import org.apache.spark.sql.columnar.{InMemoryAppendableColumnarTableScan, InMemoryAppendableRelation}
import org.apache.spark.sql.execution.{CachedData, SparkPlan, StratifiedSampler}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => u}

/**
 * An instance of the Spark SQL execution engine that delegates to supplied SQLContext
 * offering additional capabilities.
 *
 * Created by Soubhik on 5/13/15.
 */
class SnappyContext private(sc: SparkContext)
  extends SQLContext(sc) with Serializable {

  self =>

  @transient
  override protected[sql] val ddlParser = new SnappyParser(sqlParser.parse)

  @transient
  override protected[sql] lazy val catalog =
    new SnappyStoreCatalog(this, conf) with OverrideCatalog

  @transient
  override protected[sql] val cacheManager = new SnappyCacheManager(this)

  def saveStream[T: ClassTag](stream: DStream[T],
                              sampleTab: Seq[String],
                              formatter: (RDD[T], StructType) => RDD[Row],
                              schema: StructType,
                              transform: DataFrame => DataFrame = null): Unit = {
    stream.foreachRDD((rdd: RDD[T], time: Time) => {

      val row = formatter(rdd, schema)

      val rDF = createDataFrame(row, schema)

      val tDF = if (transform != null) {
        transform(rDF)
      } else rDF

      collectSamples(tDF, sampleTab)

    })
  }

  def collectSamples(tDF: DataFrame, sampleTab: Seq[String]): Unit = {
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
          val sampler = StratifiedSampler(samplingOptions, nameSuffix = "",
            columnBatchSize, schema, cached = true)
          // create a new holder for set of CachedBatches
          val batches = InMemoryAppendableRelation(useCompression,
            columnBatchSize, name, schema, output)
          sampler.append(rowIterator,
            CatalystTypeConverters.createToCatalystConverter(schema),
            (), batches.appendRow, batches.endRows)
          batches.forceEndOfBatch().iterator
        }))
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

  def registerSampleTable(tableName: String, schema: StructType,
                          samplingOptions: Map[String, Any]): DataFrame = {
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

  def registerTopKTable(streamTableName: String, tableName: String,
                        topkOptions: Map[String, String]): DataFrame = {
    catalog.registerTopKTable(catalog.getStreamTable(streamTableName).schema,
      tableName, topkOptions)
  }

  @transient override protected[sql] val planner = new SparkPlanner {
    val snappyContext = self

    override def strategies: Seq[Strategy] = Seq(
      SnappyStrategies, StreamStrategy) ++ super.strategies

    object SnappyStrategies extends Strategy {
      def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
        case s@StratifiedSample(options, child) =>
          execution.StratifiedSample(options, planLater(child), s.output) :: Nil
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
}

private[sql] class SnappyParser(parseQuery: String => LogicalPlan)
  extends DDLParser(parseQuery) {

  override protected lazy val ddl: Parser[LogicalPlan] =
    createTable | describeTable | refreshTable |
      createStream | createSampled | strmctxt

  protected val STREAM = Keyword("STREAM")
  protected val SAMPLED = Keyword("SAMPLED")
  protected val STRM = Keyword("STREAMING")
  protected val CTXT = Keyword("CONTEXT")
  protected val START = Keyword("START")
  protected val STOP = Keyword("STOP")
  protected val INIT = Keyword("INIT")


  protected lazy val createStream: Parser[LogicalPlan] =
    (CREATE ~> (STREAM ~> (TABLE ~> ident)) ~
      tableCols.? ~ (OPTIONS ~> options)) ^^ {
      case streamname ~ cols ~ opts =>
        val userColumns = cols.flatMap(fields => Some(StructType(fields)))
        CreateStream(streamname, userColumns, new CaseInsensitiveMap(opts))
    }

  protected lazy val createSampled: Parser[LogicalPlan] =
    (CREATE ~> (SAMPLED ~> (TABLE ~> ident)) ~
      (OPTIONS ~> options)) ^^ {
      case samplename ~ opts =>
        CreateSampledTable(samplename, new CaseInsensitiveMap(opts))
    }

  protected lazy val strmctxt: Parser[LogicalPlan] =
    (STRM ~> CTXT ~> (
      INIT ^^^ 0 |
        START ^^^ 1 |
        STOP ^^^ 2) ~ numericLit.?) ^^ {
      case action ~ batchInterval =>
        if (batchInterval.isDefined)
          StreamingCtxtActions(action, Some(batchInterval.get.toInt))
        else
          StreamingCtxtActions(action, None)

    }
}

private[sql] case class CreateStream(streamName: String,
                                     userColumns: Option[StructType],
                                     options: Map[String, String])
  extends LogicalPlan with Command {

  override def output: Seq[Attribute] = Seq.empty

  /** Returns a Seq of the children of this node */
  override def children: Seq[LogicalPlan] = Seq.empty
}

private[sql] case class CreateSampledTable(streamName: String,
                                           options: Map[String, String])
  extends LogicalPlan with Command {

  override def output: Seq[Attribute] = Seq.empty

  /** Returns a Seq of the children of this node */
  override def children: Seq[LogicalPlan] = Seq.empty
}

private[sql] case class StreamingCtxtActions(action: Int,
                                             batchInterval: Option[Int])
  extends LogicalPlan with Command {

  override def output: Seq[Attribute] = Seq.empty

  /** Returns a Seq of the children of this node */
  override def children: Seq[LogicalPlan] = Seq.empty
}


object snappy extends Serializable {

  implicit def snappyOperationsOnDataFrame(df: DataFrame): SnappyOperations = {
    df.sqlContext match {
      case sc: SnappyContext => SnappyOperations(sc, df)
      case sc => throw new AnalysisException("Extended snappy operations " +
        s"require SnappyContext and not ${sc.getClass.getSimpleName}")
    }
  }

  implicit def snappyOperationsOnDStream[T: ClassTag](ds: DStream[T]):
  SnappyDStreamOperations[T] = SnappyDStreamOperations(SnappyContext(
    ds.context.sparkContext), ds)
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

case class StratifiedSample(options: Map[String, Any],
                            override val child: LogicalPlan) extends UnaryNode {

  final val WEIGHTAGE_COLUMN_NAME = "__STRATIFIED_SAMPLER_WEIGHTAGE"

  /**
   * StratifiedSample will add one additional column for the ratio of total
   * rows seen for a stratum to the number of samples picked.
   */
  override val output = child.output :+ AttributeReference(WEIGHTAGE_COLUMN_NAME,
    LongType, nullable = false)()
}

//end of SnappyContext

class SampleDataFrame(@transient override val sqlContext: SnappyContext,
                      @transient override val logicalPlan: StratifiedSample)
  extends DataFrame(sqlContext, logicalPlan) with Serializable {

  // TODO: concurrency of the catalog?

  def registerSampleTable(tableName: String): Unit =
    sqlContext.catalog.registerSampleTable(schema, tableName,
      logicalPlan.options, Some(this))

  override def registerTempTable(tableName: String): Unit =
    registerSampleTable(tableName)
}

private[sql] case class SnappyOperations(context: SnappyContext, df: DataFrame) {

  /**
   * Creates stratified sampled data from given DataFrame
   * {{{
   *   peopleDf.stratifiedSample(Map("qcs" -> Array(1,2), "fraction" -> 0.01))
   * }}}
   */
  def stratifiedSample(options: Map[String, Any]): SampleDataFrame =
    new SampleDataFrame(context, StratifiedSample(options, df.logicalPlan))

  def insertIntoSampleTables(sampleTableName: String*) =
    context.collectSamples(df, sampleTableName)

  def createAndInsertIntoSampleTables(in: (String, Map[String, String])*) = {
    in.map {
      case (tableName, options) => context.catalog.getOrAddStreamTable(
        tableName, df.schema, options)
    }
    context.collectSamples(df, in.map(_._1))
  }
}

private[sql] case class SnappyDStreamOperations[T: ClassTag]
(context: SnappyContext, ds: DStream[T]) {

  def saveStream(sampleTab: Seq[String],
                 formatter: (RDD[T], StructType) => RDD[Row],
                 schema: StructType,
                 transform: DataFrame => DataFrame = null): Unit =
    context.saveStream(ds, sampleTab, formatter, schema, transform)

}

private[sql] class SnappyCacheManager(sqlContext: SnappyContext)
  extends execution.CacheManager(sqlContext) {

  /**
   * Caches the data produced by the logical representation of the given
   * schema rdd. Unlike `RDD.cache()`, the default storage level is set to be
   * `MEMORY_AND_DISK` because recomputing the in-memory columnar representation
   * of the underlying table is expensive.
   */
  override private[sql] def cacheQuery
  (query: DataFrame, tableName: Option[String] = None,
   storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK): Unit = writeLock {

    query.logicalPlan match {
      case plan@StratifiedSample(options, childPlan) =>
        val alreadyCached = lookupCachedData(plan)
        if (alreadyCached.nonEmpty) {
          logWarning("SnappyCacheManager: asked to cache already cached data.")
        }
        else {
          cachedData += CachedData(plan,
            columnar.InMemoryAppendableRelation(
              sqlContext.conf.useCompression,
              sqlContext.conf.columnBatchSize,
              storageLevel,
              query.queryExecution.executedPlan,
              tableName))
        }
      case _ => super.cacheQuery(query, tableName, storageLevel)
    }
  }
}

// end of CacheManager
