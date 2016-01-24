/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.sql.streaming

import java.util.concurrent.atomic.AtomicReference

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow, ScalaReflection}
import org.apache.spark.sql.execution.RDDConversions
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.sources.SchemaRelationProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, _}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Milliseconds, StreamingContext, StreamingContextState}

/**
  * Provides an ability to manipulate SQL like query on DStream
  */
class SnappyStreamingContext protected[spark](@transient val snappyContext: SnappyContext,
    val batchDur: Duration)
    extends StreamingContext(snappyContext.sparkContext, batchDur) with Serializable {

  self =>

  /**
   * Start the execution of the streams.
   * Also registers population of AQP tables from stream tables if present.
   *
   * @throws IllegalStateException if the StreamingContext is already stopped
   */
  override def start(): Unit = synchronized {
    if (getState() == StreamingContextState.INITIALIZED) {
      // register population of AQP tables from stream tables
      snappyContext.snappyContextFunctions.aqpTablePopulator(snappyContext)
    }
    super.start()
  }

  override def stop(stopSparkContext: Boolean,
      stopGracefully: Boolean): Unit = {
    try {
      super.stop(stopSparkContext, stopGracefully)
    } finally {
      // force invalidate all the cached relations to remove any stale streams
      SnappyStoreHiveCatalog.registerRelationDestroy()
      StreamBaseRelation.clearStreams()
    }
  }

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
    // register a dummy task so that the DStream gets started
    // TODO: need to remove once we add proper registration of registerCQ
    // streams in catalog and possible AQP structures on top
    dStream.foreachRDD(rdd => Unit)
    dStream
  }

  def getSchemaDStream(tableName: String): SchemaDStream = {
    val plan = snappyContext.catalog.lookupRelation(tableName)
    snappy.unwrapSubquery(plan) match {
      case LogicalRelation(sr: StreamPlan, _) => new SchemaDStream(self,
        LogicalDStreamPlan(sr.schema.toAttributes, sr.rowStream)(self))
      case _ =>
        throw new AnalysisException(s"Table $tableName not a stream table")
    }
  }

  /**
    * Creates a [[SchemaDStream]] from an DStream of Product (e.g. case classes).
    */
  def createSchemaDStream[A <: Product : TypeTag]
  (stream: DStream[A]): SchemaDStream = {
    val schema = ScalaReflection.schemaFor[A].dataType.asInstanceOf[StructType]
    val rowStream = stream.transform(rdd => RDDConversions.productToRowRdd
    (rdd, schema.map(_.dataType)))
    val logicalPlan = LogicalDStreamPlan(schema.toAttributes, rowStream)(self)
    new SchemaDStream(self, logicalPlan)
  }

  def createSchemaDStream(rowStream: DStream[Row], schema: StructType): SchemaDStream = {
    val converter = CatalystTypeConverters.createToScalaConverter(schema)
    val logicalPlan = LogicalDStreamPlan(schema.toAttributes,
      rowStream.map(converter(_).asInstanceOf[InternalRow]))(self)
    new SchemaDStream(self, logicalPlan)
  }

  SnappyStreamingContext.setActiveContext(self)
}

object SnappyStreamingContext extends Logging {

  private val ACTIVATION_LOCK = new Object()

  private val activeContext = new AtomicReference[SnappyStreamingContext](null)

  private def setActiveContext(snsc: SnappyStreamingContext): Unit = {
    ACTIVATION_LOCK.synchronized {
      activeContext.set(snsc)
    }
  }

  def getActive: Option[SnappyStreamingContext] = {
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
        snsc.remember(Milliseconds(300 * 1000))
        setActiveContext(snsc)
        snsc
      }
    }
  }

  def start(): Unit = getActive match {
    case Some(snsc) => snsc.start()
    case None =>
  }

  def stop(stopSparkContext: Boolean = false,
      stopGracefully: Boolean = true): Unit = {
    getActive match {
      case Some(snsc) =>
        snsc.stop(stopSparkContext, stopGracefully)
        snsc.snappyContext.clearCache()
        // SnappyContext.stop()
        setActiveContext(null)
      case None =>
    }
  }
}

trait StreamPlan {
  def rowStream: DStream[InternalRow]

  def schema: StructType
}

trait StreamPlanProvider extends SchemaRelationProvider
