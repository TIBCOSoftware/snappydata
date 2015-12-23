package org.apache.spark.sql.aqp

import org.apache.spark.rdd.RDD


import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.{StoreDataSourceStrategy, DDLParser}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.sources.StoreStrategy
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

import scala.reflect.runtime.{universe => u}
import org.apache.spark.sql.{execution => sparkexecution}
/**
 * Created by ashahid on 12/11/15.
 */
object AQPDefault extends AQPContext{
  protected[sql] def executePlan(context: SnappyContext, plan: LogicalPlan): QueryExecution =
    new sparkexecution.QueryExecution(context, plan)

  def registerSampleTable(context: SnappyContext, tableName: String, schema: StructType,
                          samplingOptions: Map[String, Any], streamTable: Option[String] = None,
                          jdbcSource: Option[Map[String, String]] = None): SampleDataFrame
  = throw new UnsupportedOperationException("missing aqp jar")

  def registerSampleTableOn[A <: Product](context: SnappyContext,
                                                      tableName: String,
                                                      samplingOptions: Map[String, Any], streamTable: Option[String] = None,
                                                      jdbcSource: Option[Map[String, String]] = None)
                                         (implicit ev: u.TypeTag[A]): DataFrame
  = throw new UnsupportedOperationException("missing aqp jar")

  def createTopK(context: SnappyContext, tableName: String, keyColumnName: String, schema: StructType,
                   topkOptions: Map[String, Any], isStreamSummary: Boolean): Unit=
    throw new UnsupportedOperationException("missing aqp jar")


  def queryTopK[T: ClassTag](context: SnappyContext, topKName: String,
                             startTime: String = null, endTime: String = null,
                             k: Int = -1): DataFrame = throw new UnsupportedOperationException("missing aqp jar")

  def queryTopK[T: ClassTag](context: SnappyContext, topK: String,
                             startTime: Long, endTime: Long, k: Int): DataFrame
  = throw new UnsupportedOperationException("missing aqp jar")




  protected[sql] def collectSamples(context: SnappyContext, rows: RDD[Row], aqpTables: Seq[String],
                                    time: Long,
                                    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK)
  = throw new UnsupportedOperationException("missing aqp jar")


 /* def saveStream[T: ClassTag](context: SnappyContext, stream: DStream[T],
                              aqpTables: Seq[String],
                              formatter: (RDD[T], StructType) => RDD[Row],
                              schema: StructType,
                              transform: RDD[Row] => RDD[Row] = null)
  = throw new UnsupportedOperationException("missing aqp jar")*/



  def createSampleDataFrameContract(sqlContext: SnappyContext, df: DataFrame, logicalPlan: LogicalPlan): SampleDataFrameContract
  = throw new UnsupportedOperationException("missing aqp jar")

  def convertToStratifiedSample(options: Map[String, Any], logicalPlan: LogicalPlan): LogicalPlan
  = throw new UnsupportedOperationException("missing aqp jar")

  def getPlanner(context: SnappyContext) : SparkPlanner = new DefaultPlanner(context)

  def getSnappyCacheManager: SnappyCacheManager = new SnappyCacheManager()

  def getSQLDialectClassName: String = classOf[SnappyParserDialect].getCanonicalName

  def getSampleTablePopulator : Option[(SQLContext) => Unit] = None

  def getSnappyCatalogue(context: SnappyContext) : SnappyStoreHiveCatalog
  = new SnappyStoreHiveCatalog(context)

  def getSnappyDDLParser (planGenerator: String => LogicalPlan): DDLParser = new SnappyDDLParser(planGenerator)

  def dropSampleTable(tableName: String, ifExists: Boolean = false) =
    throw new UnsupportedOperationException("missing aqp jar")
}

class DefaultPlanner(snappyContext: SnappyContext) extends execution.SparkPlanner(snappyContext)  with SnappyStrategies{
  val sampleSnappyCase : PartialFunction[LogicalPlan, Seq[SparkPlan]] = {case _ => Nil}
  val sampleStreamCase : PartialFunction[LogicalPlan, Seq[SparkPlan]] = {case _ => Nil}




  // TODO temporary flag till we determine every thing works fine with the optimizations
  val storeOptimization = snappyContext.sparkContext.getConf.get(
    "snappy.store.optimization", "true").toBoolean

  val storeOptimizedRules: Seq[Strategy] = if (storeOptimization)
    Seq(StoreDataSourceStrategy , LocalJoinStrategies)
  else Nil

  override def strategies: Seq[Strategy] =
    Seq(SnappyStrategies, StreamDDLStrategy(snappyContext.aqpContext.getSampleTablePopulator, sampleStreamCase),
      StoreStrategy, StreamQueryStrategy) ++
      storeOptimizedRules ++
      super.strategies



  /*override def strategies: Seq[Strategy] = Seq( SnappyStrategies,
   StreamStrategy(context.aqpContext.getSampleTablePopulator, sampleStreamCase ),
   StoreStrategy) ++ super.strategies*/
  /*
  object SnappyStrategies extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      val x: PartialFunction[LogicalPlan, Seq[SparkPlan]]  = {
        case PhysicalOperation (projectList, filters,
        mem: columnar.InMemoryAppendableRelation) =>
        pruneFilterProject (
        projectList,
        filters,
        identity[Seq[Expression]], // All filters still need to be evaluated
        InMemoryAppendableColumnarTableScan (_, filters, mem) ) :: Nil
      }

      x.orElse(sampleSnappyCase)(plan)

    }



  }*/
}
