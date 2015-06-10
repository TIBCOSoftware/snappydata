package org.apache.spark.sql

import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.analysis.SimpleCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.sources.{LogicalRelation, StreamRelation}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Logging, sql}

import scala.collection.mutable
import scala.language.implicitConversions

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
  // logicalPlan holds the dstream, schema and the options
  val streamTables = new mutable.HashMap[String, LogicalPlan]()

  // This keeps the stream table to Sample Table mapping
  val streamToSampleTblMap =  new mutable.HashMap[String, Seq[String]]()

  /**
   * This logicalPlan will be on SampleDataFrame#logicalPlan which will know how to
   * parallely iterate over the RDD and sample for this qcs.
   */
  val sampleTables = new mutable.HashMap[String, SampleDataFrame]()

  val topkTables = new mutable.HashMap[String, TopKDataFrame]()

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

    sampleTables.getOrElse(tblName, {
      throw new AnalysisException(
        s"sample table $tblName not found")
    }).logicalPlan
    //SnappystoreRelation(databaseName, tblName, alias)(context)
  }

  override def registerTable(tableIdentifier: Seq[String],
                             plan: LogicalPlan): Unit = {
    throw new NotImplementedError()
  }

  def registerSampleTable(schema: StructType, tableName: String,
                          samplingOptions: Map[String, Any],
                          df: Option[SampleDataFrame] = None): DataFrame = {
    val options = if (samplingOptions.contains("name")) {
      samplingOptions
    }
    else {
      samplingOptions + ("name" -> tableName)
    }

    options.getOrElse("name", {
      throw new Exception(s"$tableName name not inserted")
    })

    val sample = df.getOrElse {
      val plan: LogicalRDD = LogicalRDD(schema.toAttributes,
        new CachedRDD(tableName, schema)(context))(context)
      val newDF = new SampleDataFrame(context, StratifiedSample(options, plan))
      context.cacheManager.cacheQuery(newDF, Some(tableName))
      newDF
    }
    sampleTables.put(tableName, sample)
    sample
  }

  def registerTopKTable(schema: StructType, tableName: String,
                        aggOptions: Map[String, Any]): DataFrame = {
    val accessPlan = DummyRDD(schema.toAttributes)(context)
    val topkTab = TopKDataFrame(context, accessPlan, aggOptions)
    topkTables.put(tableName, topkTab)
    topkTab
  }

  def getStreamTable(tableName: String): LogicalPlan =
    streamTables.getOrElse(tableName, {
      throw new Exception(s"Stream table $tableName not found")
    })


  def getStreamTableRelation[T](tableName: String): StreamRelation[T] = {
    val plan : LogicalPlan = streamTables.getOrElse(tableName,
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
                          samplingOptions: Map[String, Any]): LogicalPlan =
    streamTables.getOrElse(tableName, registerSampleTable(schema, tableName,
      samplingOptions).logicalPlan)

  /**
   * Returns tuples of (tableName, isTemporary) for all tables in the given database.
   * isTemporary is a Boolean value indicates if a table is a temporary or not.
   */
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
