package org.apache.spark.sql

import scala.collection.mutable
import scala.language.implicitConversions

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.analysis.SimpleCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.{ LogicalRDD, StratifiedSample, TopKWrapper }
import org.apache.spark.sql.sources.{ LogicalRelation, StreamRelation }
import org.apache.spark.sql.types.StructType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.TopK

/**
 * Catalog primarily tracking stream/topK tables and returning LogicalPlan to materialize
 * these entities.
 *
 * Created by Soubhik on 5/13/15.
 */
class SnappyStoreCatalog(context: SnappyContext,
  override val conf: CatalystConf)
  extends SimpleCatalog(conf) with Logging {

  protected val currentDatabase: String = "snappydata"

  // Stores all stream tables and their logicalPlan.
  // logicalPlan holds the DStream, schema and the options
  val streamTables = new mutable.HashMap[String, LogicalPlan]()

  // This keeps the stream table to Sample Table mapping
  val streamToStructureMap = new mutable.HashMap[String, Seq[String]]()

  /**
   * This logicalPlan will be on SampleDataFrame#logicalPlan which will know
   * how to iterate over the RDD in parallel and sample for this qcs.
   */
  val sampleTables = new mutable.HashMap[String, SampleDataFrame]()

  val topKStructures = new mutable.HashMap[String, (TopKWrapper, RDD[(Int, TopK)])]()

  override def unregisterAllTables(): Unit = {
    sampleTables.clear()
    streamToStructureMap.clear()
  }

  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    //val databaseName = tableIdent.lift(tableIdent.size - 2).getOrElse(
    //  currentDatabase)
    val tblName = tableIdent.last
    if (sampleTables.contains(tblName)) {
      context.truncateTable(tblName)
      sampleTables -= tblName
    }
    streamToStructureMap.get(tblName) match {
      case Some(x) => if (x.size > 0)
        throw new IllegalStateException(s"Stream $tblName has structure(s) ${x.mkString(",")} associated with it")
      else
        streamToStructureMap -= tblName
      case None => // do nothing
    }
    if (streamTables.contains(tblName)) {
      streamTables -= tblName
    }
    val matchingStream = streamToStructureMap filter { p: (String, Seq[String]) =>
      p._2.exists(tblName.equals(_))
    } foreach { s =>
      val difflist = s._2.diff(List(tblName))
      streamToStructureMap.put(s._1, difflist)
    }
    super.unregisterTable(tableIdentifier)
  }

  override def lookupRelation(tableIdentifier: Seq[String],
    alias: Option[String]): LogicalPlan = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    //val databaseName = tableIdent.lift(tableIdent.size - 2).getOrElse(
    //  currentDatabase)
    val tblName = tableIdent.last

    sampleTables.get(tblName).map(_.logicalPlan).getOrElse {
      streamTables.get(tblName).getOrElse(
        tables.getOrElse(tblName,
          sys.error(s"Table Not Found: $tblName")))
    }
  }

  def registerSampleTable(schema: StructType, tableName: String,
    samplingOptions: Map[String, Any],
    df: Option[SampleDataFrame] = None): SampleDataFrame = {
    require(tableName != null && tableName.length > 0,
      "registerSampleTable: expected non-empty table name")

    // add or overwrite existing name attribute
    val opts = Utils.normalizeOptions(samplingOptions)
      .filterKeys(_ != "name") + ("name" -> tableName)

    // update the options in any provided StratifiedSample LogicalPlan
    df foreach (_.logicalPlan.options = opts)
    // create new StratifiedSample LogicalPlan if none was passed
    // (currently for streaming case)
    val sample = df.getOrElse {
      val plan: LogicalRDD = LogicalRDD(schema.toAttributes,
        new DummyRDD(context))(context)
      val newDF = new SampleDataFrame(context, StratifiedSample(opts, plan)())
      context.cacheManager.cacheQuery(newDF, Some(tableName))
      newDF
    }
    sampleTables.put(tableName, sample)
    sample
  }

  def registerTopK(tableName: String, streamName: String, schema: StructType,
    topkOptions: Map[String, Any], topKRDD: RDD[(Int, TopK)]) = {

    topKStructures.put(tableName, TopKWrapper(tableName, topkOptions, schema) -> topKRDD)

    streamToStructureMap.put(streamName,
      streamToStructureMap.getOrElse(streamName, Nil) :+ tableName)
    ()
  }

  def getStreamTable(tableName: String): LogicalPlan =
    streamTables.getOrElse(tableName, {
      throw new Exception(s"Stream table $tableName not found")
    })

  def getStreamTableRelation[T](tableName: String): StreamRelation[T] = {
    val plan: LogicalPlan = streamTables.getOrElse(tableName,
      throw new IllegalStateException("Plan for stream not found"))

    plan match {
      case LogicalRelation(streamrelation) => streamrelation match {
        case sr: StreamRelation[T] => sr
        case _ => throw new IllegalStateException("StreamRelation was expected")
      }
      case _ => throw new IllegalStateException("StreamRelation was expected")
    }
  }

  def getOrAddStreamTable(tableName: String, schema: StructType,
    samplingOptions: Map[String, Any]) =
    streamTables.getOrElse(tableName, registerSampleTable(schema, tableName,
      samplingOptions).logicalPlan)

  override def getTables(dbName: Option[String]): Seq[(String, Boolean)] = {
    throw new NotImplementedError()
  }

  override def refreshTable(dbName: String, tableName: String): Unit = {
    throw new NotImplementedError()
  }

  override def tableExists(tableIdentifier: Seq[String]): Boolean = {
    streamTables.isDefinedAt(tableIdentifier.last)
  }
}
