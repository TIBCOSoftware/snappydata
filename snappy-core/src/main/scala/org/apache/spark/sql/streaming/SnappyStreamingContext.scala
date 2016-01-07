package org.apache.spark.sql.streaming

import java.util.concurrent.atomic.AtomicReference

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.{universe => u}

import org.apache.spark.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{InternalRow, ScalaReflection}
import org.apache.spark.sql.execution.{RDDConversions}
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Milliseconds, Duration, StreamingContext}

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
    snappyContext.sql(sqlText)
  }

  /**
    * Registers and executes given SQL query and
    * returns [[SchemaDStream]] to consume the results
    * @param queryStr
    * @return
    */
  def registerCQ(queryStr: String): SchemaDStream = {
    val plan = sql(queryStr).queryExecution.logical
    val dStream = new SchemaDStream(self, plan)
    dStream
  }

  def getSchemaDStream(tableName: String): SchemaDStream = {
    new SchemaDStream(self, snappyContext.catalog.lookupRelation(tableName))
  }

  /**
    * Creates a [[SchemaDStream]] from an DStream of Product (e.g. case classes).
    */
  def createSchemaDStream[A <: Product : TypeTag]
  (stream: DStream[A]): SchemaDStream = {
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val attributeSeq = schema.toAttributes
    val rowStream = stream.transform(rdd => RDDConversions.productToRowRdd
    (rdd, schema.map(_.dataType)))
    new SchemaDStream(self, LogicalDStreamPlan(attributeSeq,
      rowStream)(self))
  }
}

object SnappyStreamingContext extends Logging {

  private val ACTIVATION_LOCK = new Object()

  private val activeContext = new AtomicReference[SnappyStreamingContext](null)

  private def setActiveContext(snsc: SnappyStreamingContext): Unit = {
    ACTIVATION_LOCK.synchronized {
      activeContext.set(snsc)
    }
  }

  def getActive(): Option[SnappyStreamingContext] = {
    ACTIVATION_LOCK.synchronized {
      Option(activeContext.get())
    }
  }

  def apply(sc: SnappyContext, batchDur: Duration): SnappyStreamingContext = {
    val snsc = activeContext.get()
    if (snsc != null) snsc
    else ACTIVATION_LOCK.synchronized {
      val snsc = activeContext.get()
      if (snsc != null) snsc
      else {
        val snsc = new SnappyStreamingContext(sc, batchDur)
        snsc.remember(Milliseconds(300*1000))
        setActiveContext(snsc)
        snsc
      }
    }
  }

  def start(): Unit = {
    val snsc = getActive().get
    snsc.start()
  }

  def stop(stopSparkContext: Boolean = false,
      stopGracefully: Boolean = true): Unit = {
    val snsc = getActive().get
    if (snsc != null) {
      snsc.stop(stopSparkContext, stopGracefully)
      snsc.snappyContext.clearCache()
      SnappyContext.stop()
      setActiveContext(null)
    }
  }
}

trait StreamPlan {
  def rowStream: DStream[InternalRow]
}

