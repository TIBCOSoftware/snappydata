package org.apache.spark.sql

import scala.collection.mutable
import scala.language.implicitConversions

import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.analysis.SimpleCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.sources.{LogicalRelation, StreamRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Logging, sql}

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
  val streamToSampleTblMap = new mutable.HashMap[String, Seq[String]]()

  /**
   * This logicalPlan will be on SampleDataFrame#logicalPlan which will know
   * how to iterate over the RDD in parallel and sample for this qcs.
   */
  val sampleTables = new mutable.HashMap[String, SampleDataFrame]()

  val topKTables = new mutable.HashMap[String, TopKDataFrame]()

  override def unregisterAllTables(): Unit = {
    throw new NotImplementedError()
  }

  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    throw new NotImplementedError()
  }

  override def lookupRelation(tableIdentifier: Seq[String],
      alias: Option[String]): LogicalPlan = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    //val databaseName = tableIdent.lift(tableIdent.size - 2).getOrElse(
    //  currentDatabase)
    val tblName = tableIdent.last

    sampleTables.get(tblName).map(_.logicalPlan).getOrElse {
      tables.getOrElse(tblName,
        sys.error(s"Table Not Found: $tblName"))
    }
  }

  def registerSampleTable(schema: StructType, tableName: String,
      samplingOptions: Map[String, Any],
      df: Option[SampleDataFrame] = None): SampleDataFrame = {
    require(tableName != null && tableName.length > 0,
      "registerSampleTable: expected non-empty table name")

    // add or overwrite existing name attribute
    val nameOption = "name"
    val options = samplingOptions.filterKeys(!_.equalsIgnoreCase(nameOption)) +
        (nameOption -> tableName)

    // update the options in any provided StratifiedSample LogicalPlan
    df foreach (_.logicalPlan.options = options)
    // create new StratifiedSample LogicalPlan if none was passed
    // (currently for streaming case)
    val sample = df.getOrElse {
      val plan: LogicalRDD = LogicalRDD(schema.toAttributes,
        new DummyRDD(context))(context)
      val newDF = new SampleDataFrame(context,
        StratifiedSample(options, plan)())
      context.cacheManager.cacheQuery(newDF, Some(tableName))
      newDF
    }
    sampleTables.put(tableName, sample)
    sample
  }

  def registerTopKTable(schema: StructType, tableName: String,
      aggOptions: Map[String, Any]): DataFrame = {
    //val accessPlan = DummyRDD(schema.toAttributes)(context)
    //val topKTab = TopKDataFrame(context, accessPlan, aggOptions)
    //topKTables.put(tableName, topKTab)
    null
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

class TopKDataFrame(@transient override val sqlContext: SnappyContext,
    logicalPlan: LogicalPlan,
    val aggOptions: Map[String, Any])
    extends sql.DataFrame(sqlContext, logicalPlan) {

  def this(incoming: DataFrame) {
    this(incoming.sqlContext.asInstanceOf[SnappyContext], {
      incoming.logicalPlan
    }, {
      val df: TopKDataFrame = incoming.sqlContext.asInstanceOf[SnappyContext].
          catalog.sampleTables.find(p => p._2.logicalPlan.sameResult(
        incoming.logicalPlan)).get._2
      df.aggOptions
    })
  }

  def createApproxTopKFreq(): DataFrame = {
    throw new NotImplementedError()
  }
}

object TopKDataFrame {

  implicit def dataFrameToTopKDataFrame(df: DataFrame): TopKDataFrame = {
    assert(df.sqlContext.isInstanceOf[SnappyContext])
    new TopKDataFrame(df)
  }

  def apply(sqlContext: SnappyContext, logicalPlan: LogicalPlan,
      aggOptions: Map[String, Any]): TopKDataFrame = {
    new TopKDataFrame(sqlContext, logicalPlan, aggOptions)
  }
}
