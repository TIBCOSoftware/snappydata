package org.apache.spark.sql

import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.analysis.SimpleCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{Logging, sql}

import scala.collection.mutable


/**
 * Catalog primarily tracking stream/topK tables and returning LogicalPlan to materialize
 * these entities.
 *
 * Created by soubhikc on 5/13/15.
 */
class SnappyStoreCatalog(context: SnappyContext, override val conf: CatalystConf) extends SimpleCatalog(conf) with Logging {

  protected val currentDatabase: String = "snappydata"

  val streamTables = new mutable.HashMap[Seq[String], (LogicalPlan, DStream[_])]()

  /**
   * This logicalPlan will be on SampleDataFrame#logicalPlan which will know how to
   * parallely iterate over the RDD and sample for this qcs.
   */
  val sampleTables = new mutable.HashMap[String, SampleDataFrame]()

  val topkTables = new mutable.HashMap[String, TopKDataFrame]()

  override def unregisterAllTables(): Unit = {}

  override def unregisterTable(tableIdentifier: Seq[String]): Unit = ???

  override def lookupRelation(tableIdentifier: Seq[String], alias: Option[String]): LogicalPlan = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    val databaseName = tableIdent.lift(tableIdent.size - 2).getOrElse(
      currentDatabase)
    val tblName = tableIdent.last

    sampleTables.getOrElse(tblName, {throw new AnalysisException(s"sample table $tblName not found" )}).logicalPlan
    //SnappystoreRelation(databaseName, tblName, alias)(context)
  }

  override def registerTable(tableIdentifier: Seq[String], plan: LogicalPlan): Unit = ???


  def registerSampleTable(schema: StructType, tableName: String, samplingOptions: Map[String, String]): DataFrame = {
    //val accessPlan = DummyRDD(schema.toAttributes)(context)

    //val relation = LogicalRelation(new sources.InMemoryAppendableRelation(schema)(context))

    val options = if(samplingOptions.get("name").isDefined) {
      samplingOptions
    }
    else {
      samplingOptions + ("name" -> tableName)
    }

    options.getOrElse("name", {throw new Exception(s"${tableName} name not inserted")})

    val rDD: LogicalRDD = LogicalRDD(schema.toAttributes, CachedRDD()(context))(context)
    val sampleTab = SampleDataFrame(context, rDD, options)
    sampleTables.put(tableName, sampleTab)
    context.cacheManager.cacheQuery(sampleTab, Some(tableName))
    sampleTab
  }

  def registerTopKTable(schema: StructType, tableName: String, aggOptions: Map[String, String]): DataFrame = {
    val accessPlan = DummyRDD(schema.toAttributes)(context)
    val topkTab = TopKDataFrame(context, accessPlan, aggOptions)
    topkTables.put(tableName, topkTab)
    topkTab
  }

  def getStreamTable(tableName: Seq[String]): LogicalPlan =
    streamTables.getOrElse(tableName, {
      throw new Exception("Stream table " + tableName + " not found")
    })._1

  /**
   * Returns tuples of (tableName, isTemporary) for all tables in the given database.
   * isTemporary is a Boolean value indicates if a table is a temporary or not.
   */
  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = ???

  override def refreshTable(databaseName: String, tableName: String): Unit = ???

  override def tableExists(tableIdentifier: Seq[String]): Boolean = ???
}

class SampleDataFrame(@transient sqlContext: SnappyContext,
                      logicalPlan: LogicalPlan,
                      val samplingOptions: Map[String, String])
  extends sql.DataFrame(sqlContext, logicalPlan) {

  def this(incoming: DataFrame) {
    this(incoming.sqlContext.asInstanceOf[SnappyContext], {
      incoming.logicalPlan
    }, {
      val df: SampleDataFrame = incoming.sqlContext.asInstanceOf[SnappyContext].catalog.sampleTables.find(p =>
        p._2.logicalPlan.sameResult(incoming.logicalPlan)
      ).get._2
      df.samplingOptions
    })
  }

  def stratifiedSample(): DataFrame = ???

}

object SampleDataFrame {

  implicit def dataFrameToSampleDataFrame(df: DataFrame): SampleDataFrame = {
    assert(df.sqlContext.isInstanceOf[SnappyContext])
    new SampleDataFrame(df)
  }

  def apply(sqlContext: SnappyContext, logicalPlan: LogicalPlan, samplingOptions: Map[String, String]): SampleDataFrame = {
    new SampleDataFrame(sqlContext, logicalPlan, samplingOptions)
  }
}


class TopKDataFrame(@transient override val sqlContext: SnappyContext,
                    logicalPlan: LogicalPlan,
                    val aggOptions: Map[String, String])
  extends sql.DataFrame(sqlContext, logicalPlan) {

  def this(incoming: DataFrame) {
    this(incoming.sqlContext.asInstanceOf[SnappyContext], {
      incoming.logicalPlan
    }, {
      val df: TopKDataFrame = incoming.sqlContext.asInstanceOf[SnappyContext].catalog.sampleTables.find(p =>
        p._2.logicalPlan.sameResult(incoming.logicalPlan)
      ).get._2
      df.aggOptions
    })
  }


  def createApproxTopKFreq(): DataFrame = ???

}

object TopKDataFrame {

  implicit def dataFrameToTopKDataFrame(df: DataFrame): TopKDataFrame = {
    assert(df.sqlContext.isInstanceOf[SnappyContext])
    new TopKDataFrame(df)
  }

  def apply(sqlContext: SnappyContext, logicalPlan: LogicalPlan, aggOptions: Map[String, String]): TopKDataFrame = {
    new TopKDataFrame(sqlContext, logicalPlan, aggOptions)
  }

}
