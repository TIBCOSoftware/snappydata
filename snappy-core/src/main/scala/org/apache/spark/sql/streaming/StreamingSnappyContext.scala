package org.apache.spark.sql.streaming


import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.execution.{LogicalRDD, RDDConversions, SparkPlan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState, Time}

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.{universe => u}


/**
 * Provides an ability to manipulate SQL like query on DStream
 *
 * Created by ymahajan on 25/09/15.
 */

final class StreamingSnappyContext private(@transient val streamingContext:
                                           StreamingContext)
  extends SnappyContext(streamingContext.sparkContext)
  with Serializable {

  self =>

  override def sql(sqlText: String): DataFrame = {
    StreamPlan.currentContext.set(self) //StreamingSnappyContext
    super.sql(sqlText)
  }

  /**
   * Registers and executes given SQL query and
   * returns [[SchemaDStream]] to consume the results
   * @param queryStr
   * @return
   */
  def registerCQ(queryStr: String): SchemaDStream = {
    SparkPlan.currentContext.set(self) //SQLContext
    StreamPlan.currentContext.set(self) //StreamingSnappyContext
    val plan = sql(queryStr).queryExecution.logical
    // TODO Yogesh, This needs to get registered with catalog
    //catalog.registerCQ(queryStr, plan)
    val dStream = new SchemaDStream(self, plan)
    //TODO Yogesh, Remove this hack
    /*if (streamingContext.getState() == StreamingContextState.ACTIVE) {
      val zeroTime = streamingContext.graph.zeroTime
      var currentTime = Time(System.currentTimeMillis())
      while (!(currentTime - zeroTime).isMultipleOf(dStream.slideDuration)) {
        currentTime = Time(System.currentTimeMillis())
      }
      // For dynamic CQ
      dStream.initializeAfterContextStart(currentTime)
      //streamingContext.graph.addOutputStream(dStream)
    }*/
    dStream
  }

  /**
   * Registers the given [[SchemaDStream]] as a temporary table in the catalog.
   * Temporary tables exist only during the lifetime of this instance of SQLContext.
   * @param tName
   * @param stream
   */
  def registerStreamAsTable(tName: String, stream: SchemaDStream): Unit = {
    catalog.registerTable(catalog.newQualifiedTableName(tName),
      stream.logicalPlan)
  }

  def getSchemaDStream(tableName: String): SchemaDStream = {
    new SchemaDStream(self, catalog.lookupRelation(tableName))
  }

  /**
   * Creates a [[SchemaDStream]] from [[DStream]] containing [[Row]]s using
   * the given schema. It is important to make sure that the structure of
   * every [[Row]] of the provided DStream matches the provided schema.
   */
  implicit def createSchemaDStream(dStream: DStream[InternalRow],
                          schema: StructType): SchemaDStream = {
    val attributes = schema.toAttributes
    SparkPlan.currentContext.set(self)
    StreamPlan.currentContext.set(self)
    val logicalPlan = LogicalDStreamPlan(attributes, dStream)(self)
    new SchemaDStream(self, logicalPlan)
  }

  /**
   * Creates a [[SchemaDStream]] from an DStream of Product (e.g. case classes).
   */
  implicit def createSchemaDStream[A <: Product : TypeTag]
  (stream: DStream[A]): SchemaDStream = {
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val attributeSeq = schema.toAttributes
    SparkPlan.currentContext.set(self)
    StreamPlan.currentContext.set(self)
    val rowStream = stream.transform(rdd => RDDConversions.productToRowRdd
    (rdd, schema.map(_.dataType)))
    new SchemaDStream(self, LogicalDStreamPlan(attributeSeq,
      rowStream)(self))
  }
}

object snappy extends Serializable {
  implicit def snappyOperationsOnDStream[T: ClassTag]
  (ds: DStream[T]): SnappyStreamOperations[T] =
    SnappyStreamOperations(StreamingSnappyContext(ds.context), ds)
}

object StreamingSnappyContext {

  @volatile private[this] var globalContext: StreamingSnappyContext = _

  private[this] val contextLock = new AnyRef

  def apply(sc: StreamingContext): StreamingSnappyContext = {
    val snc = globalContext
    if (snc != null) {
      snc
    } else contextLock.synchronized {
      val snc = globalContext
      if (snc != null) {
        snc
      } else {
        // global initialization of SnappyContext
        SnappyContext.getOrCreate(sc.sparkContext)
        val snc = new StreamingSnappyContext(sc)
        StreamingCtxtHolder.apply(sc)
        globalContext = snc
        snc
      }
    }
  }

  def stop(stopSparkContext: Boolean = false,
      stopGracefully: Boolean = true): Unit = {
    val snc = globalContext
    if (snc != null) {
      snc.streamingContext.stop(stopSparkContext, stopGracefully)
      StreamPlan.currentContext.remove()
      globalContext = null
    }
  }
}

case class SnappyStreamOperations[T: ClassTag](context: StreamingSnappyContext,
                                               ds: DStream[T]) {

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
                                        schema: StructType,
                                        jdbcSource: Map[String, String]): Unit = {
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

trait StreamPlan {
  def streamingSnappyCtx = StreamPlan.currentContext.get()

  def stream: DStream[InternalRow]
}

object StreamPlan {
  val currentContext = new ThreadLocal[StreamingSnappyContext]()
}

