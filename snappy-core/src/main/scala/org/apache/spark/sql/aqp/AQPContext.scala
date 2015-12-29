package org.apache.spark.sql.aqp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.execution.datasources.DDLParser
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.{universe => u}
import scala.reflect.ClassTag
import scala.reflect.runtime._


trait AQPContext {

   protected[sql] def executePlan(context: SnappyContext, plan: LogicalPlan): QueryExecution

   def registerSampleTable(context: SnappyContext, tableName: String, schema: StructType,
                           samplingOptions: Map[String, Any], streamTable: Option[String] = None,
                           jdbcSource: Option[Map[String, String]] = None): SampleDataFrame

   def registerSampleTableOn[A <:Product](context: SnappyContext, tableName: String,
                                                       samplingOptions: Map[String, Any], streamTable: Option[String] ,
                                                       jdbcSource: Option[Map[String, String]])( implicit ev: TypeTag[A]): DataFrame

  /* def registerTopK(context: SnappyContext, tableName: String, schema: StructType,
                    topkOptions: Map[String, Any], isStreamSummary: Boolean): Unit*/

  def queryTopK[T: ClassTag](context: SnappyContext, topKName: String,
                             startTime: String = null, endTime: String = null,
                             k: Int = -1): DataFrame

  def queryTopK[T: ClassTag](context: SnappyContext, topK: String,
                             startTime: Long, endTime: Long, k: Int): DataFrame




  def createTopK(context: SnappyContext, topKName: String, keyColumnName: String,
                 inputDataSchema: StructType,
                 topkOptions: Map[String, Any], isStreamSummary: Boolean): Unit

  protected[sql] def collectSamples(context: SnappyContext, rows: RDD[Row], aqpTables: Seq[String],
                                    time: Long,
                                    storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK)


  /*def saveStream[T: ClassTag](context: SnappyContext, stream: DStream[T],
                              aqpTables: Seq[String],
                              formatter: (RDD[T], StructType) => RDD[Row],
                              schema: StructType,
                              transform: RDD[Row] => RDD[Row] = null)*/



  def createSampleDataFrameContract(sqlContext: SnappyContext, df: DataFrame, logicalPlan: LogicalPlan): SampleDataFrameContract

  def convertToStratifiedSample(options: Map[String, Any], logicalPlan: LogicalPlan): LogicalPlan

  def getPlanner(context: SnappyContext) : SparkPlanner

  def getSnappyCacheManager: SnappyCacheManager

  def getSQLDialectClassName: String

  def getSampleTablePopulator : Option[(SQLContext) => Unit]

  def getSnappyCatalogue(context: SnappyContext) : SnappyStoreHiveCatalog

  def getSnappyDDLParser (planGenerator: String => LogicalPlan): DDLParser

  def dropSampleTable(tableName: String, ifExists: Boolean = false)

  def isTungstenEnabled: Boolean
}
