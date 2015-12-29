package org.apache.spark.sql.streaming

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.{RDDConversions, SparkPlan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext, StreamingContextState}

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.{universe => u}

/**
  * Provides an ability to manipulate SQL like query on DStream
  *
  * Created by ymahajan on 25/09/15.
  */

class SnappyStreamingContext protected[spark](@transient val snappyContext: SnappyContext,
                                              val batchDur: Duration)
  extends StreamingContext(snappyContext.sparkContext, batchDur) with Serializable {

  self =>

  def sql(sqlText: String): DataFrame = {
    StreamPlan.currentContext.set(self)
    snappyContext.sql(sqlText)
  }

  /**
    * Registers and executes given SQL query and
    * returns [[SchemaDStream]] to consume the results
    * @param queryStr
    * @return
    */
  def registerCQ(queryStr: String): SchemaDStream = {
    SparkPlan.currentContext.set(snappyContext) // SQLContext
    StreamPlan.currentContext.set(self) // StreamingSnappyContext
    val plan = sql(queryStr).queryExecution.logical
    // TODO Yogesh, This needs to get registered with catalog
    // catalog.registerCQ(queryStr, plan)
    val dStream = new SchemaDStream(self, plan)
    // TODO Yogesh, Remove this hack
    /* if (streamingContext.getState() == StreamingContextState.ACTIVE) {
      val zeroTime = streamingContext.graph.zeroTime
      var currentTime = Time(System.currentTimeMillis())
      while (!(currentTime - zeroTime).isMultipleOf(dStream.slideDuration)) {
        currentTime = Time(System.currentTimeMillis())
      }
      // For dynamic CQ
      dStream.initializeAfterContextStart(currentTime)
      //streamingContext.graph.addOutputStream(dStream)
    } */
    dStream
  }



//  override def stop(stopSparkContext: Boolean, stopGracefully: Boolean): Unit = {
//    super.stop(stopSparkContext, stopGracefully)
//    StreamPlan.currentContext.remove()
//  }

    def getSchemaDStream(tableName: String): SchemaDStream = {
    new SchemaDStream(self, snappyContext.catalog.lookupRelation(tableName))
  }

  /**
    * Creates a [[SchemaDStream]] from [[DStream]] containing [[Row]]s using
    * the given schema. It is important to make sure that the structure of
    * every [[Row]] of the provided DStream matches the provided schema.
    */
  def createSchemaDStream(dStream: DStream[InternalRow],
                                   schema: StructType): SchemaDStream = {
    val attributes = schema.toAttributes
    SparkPlan.currentContext.set(self.snappyContext)
    StreamPlan.currentContext.set(self)
    val logicalPlan = LogicalDStreamPlan(attributes, dStream)(self)
    new SchemaDStream(self, logicalPlan)
  }

  /**
    * Creates a [[SchemaDStream]] from an DStream of Product (e.g. case classes).
    */
  def createSchemaDStream[A <: Product : TypeTag]
  (stream: DStream[A]): SchemaDStream = {
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val attributeSeq = schema.toAttributes
    SparkPlan.currentContext.set(self.snappyContext)
    StreamPlan.currentContext.set(self)
    val rowStream = stream.transform(rdd => RDDConversions.productToRowRdd
    (rdd, schema.map(_.dataType)))
    new SchemaDStream(self, LogicalDStreamPlan(attributeSeq,
      rowStream)(self))
  }
}

 object SnappyStreamingContext {

  @volatile private[this] var globalContext: SnappyStreamingContext = _

  private[this] val contextLock = new AnyRef

  def apply(sc: SnappyContext, batchDur: Duration): SnappyStreamingContext = {
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
        val snc = new SnappyStreamingContext(sc, batchDur)
        StreamingCtxtHolder.apply(snc)
        globalContext = snc
        snc
      }
    }
  }

  def start(): Unit = {
    val snc = globalContext
    // TODO Register sampling of all the streams
    // start the streaming context
    snc.start()
   }

  def stop(stopSparkContext: Boolean = false,
           stopGracefully: Boolean = true): Unit = {
    val snc = globalContext
    if (snc != null) {
      snc.stop(false, stopGracefully)
      snc.snappyContext.clearCache()
      SnappyContext.stop()
      StreamPlan.currentContext.remove()
      globalContext = null
    }
  }
}

object StreamingCtxtHolder {

  @volatile private[this] var globalContext: StreamingContext = _
  private[this] val contextLock = new AnyRef

  def streamingContext: StreamingContext = globalContext

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
  def streamingSnappy: SnappyStreamingContext = StreamPlan.currentContext.get()

  def stream: DStream[InternalRow]
}

object StreamPlan {
  val currentContext = new ThreadLocal[SnappyStreamingContext]()
}

