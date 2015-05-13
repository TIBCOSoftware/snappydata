/*
 * <snappydataCopyRight/>
 */
package org.apache.spark.sql

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.OverrideCatalog
import org.apache.spark.sql.columnar.InMemoryAppendableRelation
import org.apache.spark.sql.execution.{CachedData, StratifiedSampler}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

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
    var batchId = 1
    stream.foreachRDD((rdd: RDD[T], time: Time) => {

      val row = formatter(rdd, schema)

      val rDF = createDataFrame(row, schema)

      val tDF = if (transform != null) {
        transform(rDF)
      } else rDF

      val rddId = rdd.id
      val useCompression = conf.useCompression
      val columnBatchSize = conf.columnBatchSize

      val sampleTables = (catalog.sampleTables.filter {
        case (name, df) => sampleTab.contains(name)
      } map { case (name, df) =>
        (name, df.samplingOptions, df.schema, df.queryExecution.analyzed.output,
          cacheManager.lookupCachedData(df.logicalPlan).getOrElse(sys.error(
            s"SnappyContext.saveStream: failed to lookup cached plan for " +
              s"sampling table ${name}")).cachedRepresentation)
      }).toArray

      // TODO: this iterates rows multiple times
      val rdds = sampleTables.map { case (name, samplingOptions, schema, output, relation) => {
        (relation, tDF.mapPartitions(rowIterator => {
          val sampler = StratifiedSampler(samplingOptions, schema)
          rowIterator.flatMap { row =>
            if (sampler.append(row)) Iterator(InMemoryAppendableRelation.appendRows(sampler.iterator,
              name, sampler.schema, output, useCompression, columnBatchSize))
            else Iterator.empty
          }
        }))
      }
      }

      //      val unionRDD = new UnionRDD(this.sparkContext, rdds.map(_._2))
      //      if (unionRDD.count() > 0) {
      // add to list in InMemoryAppendableRelation
      rdds.foreach { case (relation, rdd) => {
        val cached = rdd.persist(StorageLevel.MEMORY_AND_DISK)
        if (cached.count() > 0) {
          relation.asInstanceOf[InMemoryAppendableRelation].appendBatch(cached)
        }
      }
      }

    })
  }


  def registerSampleTable(streamTable: DataFrame, tableName: String, samplingOptions: Map[String, String]): DataFrame = {
    catalog.registerSampleTable(streamTable.schema, tableName, samplingOptions)
  }

  def registerTopKTable(streamTable: DataFrame, tableName: String, topkOptions: Map[String, String]): DataFrame = {
    catalog.registerTopKTable(streamTable.schema, tableName, topkOptions)
  }

  def registerSampleTable(tableName: String, schema: StructType, samplingOptions: Map[String, String]): DataFrame = {
    catalog.registerSampleTable(schema, tableName, samplingOptions)
  }

  def registerTopKTable(streamTableName: String, tableName: String, topkOptions: Map[String, String]): DataFrame = {
    catalog.registerTopKTable(catalog.getStreamTable(Seq(streamTableName)).schema, tableName, topkOptions)
  }

  @transient
  override protected[sql] val planner = new SparkPlanner {
    val snappyContext = self

    /*
    override def strategies: Seq[Strategy] = Seq(
      AppendableInMemoryScans) ++ super.strategies

    object AppendableInMemoryScans extends Strategy {
      def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
        case PhysicalOperation(projectList, filters, mem: columnar.InMemoryAppendableRelation) =>
          pruneFilterProject(
            projectList,
            filters,
            identity[Seq[Expression]], // All filters still need to be evaluated.
            InMemoryMutableColumnarTableScan(_, filters, mem)) :: Nil
        case _ => Nil
      }
    }
    */
  }
}

//end of SnappyContext


class SnappyCacheManager(sqlContext: SnappyContext) extends execution.CacheManager(sqlContext) {
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
    val alreadyCached = lookupCachedData(planToCache);
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

