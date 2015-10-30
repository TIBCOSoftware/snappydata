package org.apache.spark.sql.streaming

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{LogicalRDD, SparkPlan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState, Time}

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.{universe => u}


/**
 * Created by ymahajan on 25/09/15.
 */
protected final class StreamingSnappyContext(val streamingContext: StreamingContext)
  extends SnappyContext(streamingContext.sparkContext) with Serializable {

  self =>

  override def sql(sqlText: String): DataFrame = {
    StreamPlan.currentContext.set(this) //SnappyStreamingContext
    super.sql(sqlText)
  }

  def registerCQ(queryStr: String): SchemaDStream = {
    SparkPlan.currentContext.set(this) //SQLContext
    StreamPlan.currentContext.set(this) //SnappyStreamingContext
    val plan = super.sql(queryStr).queryExecution.logical
    //TODO Yogesh, This needs to get registered with catalog
    //catalog.registerCQ(queryStr, plan)
    new SchemaDStream(this, plan)
  }

  def registerStreamAsTable(tName: String, stream: SchemaDStream): Unit = {
    catalog.registerTable(Seq(tName), stream.logicalPlan)
  }

  def getSchemaDStream(tableName: String): SchemaDStream = {
    new SchemaDStream(this, catalog.lookupRelation(Seq(tableName)))
  }

  def createSchemaDStream(dStream: DStream[Any], schema: StructType): SchemaDStream = {
    SparkPlan.currentContext.set(this) //SQLContext
    StreamPlan.currentContext.set(this) //SnappyStreamingContext
    val attributes = schema.toAttributes
    val logicalPlan = LogicalDStreamPlan(attributes, dStream)(this)
    new SchemaDStream(this, logicalPlan)
  }

  protected lazy val streamStrategies = new StreamStrategies
  experimental.extraStrategies = streamStrategies.strategies

}

object snappy extends Serializable {
  implicit def snappyOperationsOnDStream[T: ClassTag](ds: DStream[T]): SnappyStreamOperations[T] =
    SnappyStreamOperations(SnappyContext(ds.context.sparkContext), ds)
}

object StreamingSnappyContext {

  @volatile private[this] var globalContext: StreamingSnappyContext = _
  private[this] val contextLock = new AnyRef

  def apply(sc: StreamingContext,
            init: StreamingSnappyContext => StreamingSnappyContext = identity): StreamingSnappyContext = {
    val snc = globalContext
    if (snc != null) {
      snc
    } else contextLock.synchronized {
      val snc = globalContext
      if (snc != null) {
        snc
      } else {
        val snc = init(new StreamingSnappyContext(sc))
        StreamingCtxtHolder.apply(sc)
        globalContext = snc
        snc
      }
    }
  }
}

case class SnappyStreamOperations[T: ClassTag](context: SnappyContext, ds: DStream[T]) {

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

object StreamingCtxtHolder {

  @volatile private[this] var globalContext: StreamingContext = _
  private[this] val contextLock = new AnyRef

  def streamingContext = globalContext

  def apply(sparkCtxt: SparkContext,
            duration: Int): StreamingContext = {
    val context = globalContext
    if (context != null &&
      context.getState() != StreamingContextState.STOPPED) {
      context
    } else contextLock.synchronized {
      val context = globalContext
      if (context != null &&
        context.getState() != StreamingContextState.STOPPED) {
        context
      } else {
        val context = new StreamingContext(sparkCtxt, Seconds(duration))
        globalContext = context
        context
      }
    }
  }

  def apply(strCtxt: StreamingContext): StreamingContext = {
    contextLock.synchronized {
      val context = strCtxt
      globalContext = context
      context
    }
  }
}
