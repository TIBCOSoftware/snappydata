package org.apache.spark.sql

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.{universe => u}

import org.apache.spark.sql.LockUtils.ReadWriteLock
import org.apache.spark.sql.approximate.TopKUtil
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.types.StructType
import org.apache.spark.{TaskContext, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical.{Subquery, LogicalPlan}
import org.apache.spark.sql.execution.{LogicalRDD, TopKWrapper, StratifiedSample}
import org.apache.spark.streaming.{Time, StreamingContext}
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by rishim on 7/12/15.
 */
// scalastyle:off
object snappy extends Serializable {
  // scalastyle:on

  implicit def snappyOperationsOnDataFrame(df: DataFrame): SnappyDataFrameOperations = {
    df.sqlContext match {
      case sc: SnappyContext => SnappyDataFrameOperations(sc, df)
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

private[sql] case class SnappyDataFrameOperations(context: SnappyContext,
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

    val clazz = Utils.getInternalType(
      topKWrapper.schema(topKWrapper.key.name).dataType)
    val ct = ClassTag(clazz)
    context.topKLocks += name.toString() -> new ReadWriteLock()
    val topKRDD = TopKUtil.createTopKRDD(name.toString,
      context.sparkContext, topKWrapper.stsummary)
    context.catalog.topKStructures.put(name, topKWrapper -> topKRDD)
    TopKUtil.populateTopK(df.rdd, topKWrapper, context,
      name, topKRDD, System.currentTimeMillis())(ct)

  }

  /**
   * Table must be registered using #registerSampleTable.
   */
  def insertIntoSampleTables(sampleTableName: String*): Unit =
    context.collectSamples(df.rdd, sampleTableName, System.currentTimeMillis())

  /**
   * Append to an existing cache table.
   * Automatically uses #cacheQuery if not done already.
   */
  def appendToCache(tableName: String): Unit = context.appendToCache(df, tableName)
}

private[sql] case class SnappyDStreamOperations[T: ClassTag](
    context: SnappyContext, ds: DStream[T]) {

  def saveStream(sampleTab: Seq[String],
      formatter: (RDD[T], StructType) => RDD[Row],
      schema: StructType,
      transform: RDD[Row] => RDD[Row] = null): Unit =
    context.saveStream(ds, sampleTab, formatter, schema, transform)

  def saveToExternalTable[A <: Product : TypeTag](externalTable: String,
      jdbcSource: Map[String, String]): Unit = {
    val schema: StructType = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    saveStreamToExternalTable(externalTable, schema, jdbcSource)
  }

  def saveToExternalTable(externalTable: String, schema: StructType,
      jdbcSource: Map[String, String]): Unit = {
    saveStreamToExternalTable(externalTable, schema, jdbcSource)
  }

  private def saveStreamToExternalTable(externalTable: String,
      schema: StructType, jdbcSource: Map[String, String]): Unit = {
    require(externalTable != null && externalTable.length > 0,
      "saveToExternalTable: expected non-empty table name")

    val tableIdent = context.catalog.newQualifiedTableName(externalTable)
    val externalStore = context.catalog.getExternalTable(jdbcSource)
    context.catalog.createExternalTableForCachedBatches(tableIdent.table,
      externalStore)
    val attributeSeq = schema.toAttributes

    val dummyDF = {
      val plan: LogicalRDD = LogicalRDD(attributeSeq,
        new DummyRDD(context))(context)
      DataFrame(context, plan)
    }

    context.catalog.tables.put(tableIdent, dummyDF.logicalPlan)
    context.cacheManager.cacheQuery_ext(dummyDF, Some(tableIdent.table),
      externalStore)

    ds.foreachRDD((rdd: RDD[T], time: Time) => {
      context.appendToCacheRDD(rdd, tableIdent.table, schema)
    })
  }
}
