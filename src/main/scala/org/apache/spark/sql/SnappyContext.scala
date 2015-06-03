/*
 * <snappydataCopyRight/>
 */
package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.OverrideCatalog
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.columnar.{InMemoryAppendableColumnarTableScan, InMemoryAppendableRelation}
import org.apache.spark.sql.execution.{CachedData, SparkPlan, StratifiedSampler}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag
import scala.reflect.runtime.{universe => u}

/**
 * An instance of the Spark SQL execution engine that delegates to supplied SQLContext
 * offering additional capabilities.
 *
 * Created by soubhikc on 5/13/15.
 */
class SnappyContext(sc: SparkContext) extends SQLContext(sc) with Serializable {
  self =>

  @transient
  override protected[sql] lazy val catalog = new SnappyStoreCatalog(this, conf) with OverrideCatalog

  @transient
  override protected[sql] val cacheManager = new SnappyCacheManager(this)

  object operations extends Serializable {

    implicit def snappyOperationsOnDF(df: DataFrame): SnappyOperations = {
      SnappyOperations(self, df)
    }

    implicit def snappyOperationsOnDStream[T: ClassTag](ds: DStream[T]): SnappyDStreamOperations[T] = {
      SnappyDStreamOperations(self, ds)
    }

  }

  override def cacheTable(tableName: String): Unit = {
    if (catalog.sampleTables.contains(tableName))
      throw new Exception("cacheTable is disabled for sampleTable: " + tableName)

    super.cacheTable(tableName)
  }

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
      (name, df.samplingOptions, df.schema, df.queryExecution.analyzed.output,
        cacheManager.lookupCachedData(df.logicalPlan).getOrElse(sys.error(
          s"SnappyContext.saveStream: failed to lookup cached plan for " +
            s"sampling table $name")).cachedRepresentation)
    }).toSeq

    // TODO: this iterates rows multiple times
    val rdds = sampleTables.map {
      case (name, samplingOptions, schema, output, relation) =>
        (relation, tDF.mapPartitions(rowIterator => {
          val sampler = StratifiedSampler(samplingOptions, schema,
            cached = true)
          // create a new holder for set of CachedBatches
          val batches = InMemoryAppendableRelation(useCompression,
            columnBatchSize, name, sampler.schema, output)
          sampler.append(rowIterator, (), batches.appendRow, batches.endRows)
          batches.forceEndOfBatch().iterator
        }))
    }

    //      val unionRDD = new UnionRDD(this.sparkContext, rdds.map(_._2))
    //      if (unionRDD.count() > 0) {
    // add to list in InMemoryAppendableRelation
    rdds.foreach { case (relation, rdd) =>
      val cached = rdd.persist(StorageLevel.MEMORY_AND_DISK)
      if (cached.count() > 0) {
        relation.asInstanceOf[InMemoryAppendableRelation].appendBatch(cached)
      }
    }
  }

  def registerSampleTable(streamTable: DataFrame, tableName: String,
                          samplingOptions: Map[String, String]): DataFrame = {
    catalog.registerSampleTable(streamTable.schema, tableName, samplingOptions)
  }

  def registerTopKTable(streamTable: DataFrame, tableName: String,
                        topkOptions: Map[String, String]): DataFrame = {
    catalog.registerTopKTable(streamTable.schema, tableName, topkOptions)
  }

  def registerSampleTable(tableName: String, schema: StructType,
                          samplingOptions: Map[String, String]): DataFrame = {
    catalog.registerSampleTable(schema, tableName, samplingOptions)
  }

  def registerSampleTableOn[A <: Product : u.TypeTag](tableName: String, samplingOptions: Map[String, String]): DataFrame = {
    if (u.typeOf[A] =:= u.typeOf[Nothing]) {
      throw new Exception("Type of case class object not mentioned. Mention type information for e.g. registerSampleTableOn[<class>] ")
    }
    SparkPlan.currentContext.set(self)
    val schemaExtract = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    registerSampleTableOn(tableName, schemaExtract, samplingOptions)
  }


  def registerSampleTableOn(tableName: String, schema: StructType, samplingOptions: Map[String, String]): DataFrame = {
    catalog.registerSampleTable(schema, tableName, samplingOptions)
  }

  def registerTopKTable(streamTableName: String, tableName: String, topkOptions: Map[String, String]): DataFrame = {
    catalog.registerTopKTable(catalog.getStreamTable(streamTableName).schema, tableName, topkOptions)
  }

  @transient
  override protected[sql] val planner = new SparkPlanner {
    val snappyContext = self

    override def strategies: Seq[Strategy] = Seq(
      AppendableInMemoryScans) ++ super.strategies

    object AppendableInMemoryScans extends Strategy {
      def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
        case PhysicalOperation(projectList, filters, mem: columnar.InMemoryAppendableRelation) =>
          pruneFilterProject(
            projectList,
            filters,
            identity[Seq[Expression]], // All filters still need to be evaluated.
            InMemoryAppendableColumnarTableScan(_, filters, mem)) :: Nil
        case _ => Nil
      }
    }
  }
}

//end of SnappyContext

private[sql] case class SnappyOperations(context: SnappyContext, df: DataFrame) {

  def insertIntoSampleTables(sampleTableName: String*) = context.collectSamples(df, sampleTableName)

  def createAndInsertIntoSampleTables(in: Tuple2[String, Map[String, String]]*) = {
    in.map {
      case (tableName, options) => context.catalog.getOrAddStreamTable(tableName, df.schema, options)
    }
    context.collectSamples(df, in.map(_._1))
  }
}

private[sql] case class SnappyDStreamOperations[T: ClassTag](context: SnappyContext, ds: DStream[T]) {

  def saveStream(sampleTab: Seq[String],
                 formatter: (RDD[T], StructType) => RDD[Row],
                 schema: StructType,
                 transform: DataFrame => DataFrame = null): Unit = context.saveStream(ds, sampleTab, formatter, schema, transform)

}

private[sql] class SnappyCacheManager(sqlContext: SnappyContext) extends execution.CacheManager(sqlContext) {
  /**
   * Caches the data produced by the logical representation of the given schema rdd.  Unlike
   * `RDD.cache()`, the default storage level is set to be `MEMORY_AND_DISK` because recomputing
   * the in-memory columnar representation of the underlying table is expensive.
   */
  override private[sql] def cacheQuery(
                                        query: DataFrame,
                                        tableName: Option[String] = None,
                                        storageLevel: StorageLevel = MEMORY_AND_DISK): Unit = writeLock {

    val snappyTable = {
      if (sqlContext.catalog.sampleTables.contains {
        tableName.getOrElse({
          ""
        })
      }) true
      else false
    }

    if (!snappyTable) {
      super.cacheQuery(query, tableName, storageLevel)
    }

    val planToCache = query.queryExecution.analyzed
    val alreadyCached = lookupCachedData(planToCache)
    if (alreadyCached.nonEmpty) {
      logWarning("Asked to cache already cached data.")
      return
    }

    cachedData +=
      CachedData(
        planToCache,
        columnar.InMemoryAppendableRelation(
          sqlContext.conf.useCompression,
          sqlContext.conf.columnBatchSize,
          storageLevel,
          query.queryExecution.executedPlan,
          tableName))
  }
}

// end of CacheManager
