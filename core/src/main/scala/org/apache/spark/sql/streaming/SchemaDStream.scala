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

import org.apache.spark.api.java.function.{VoidFunction => JVoidFunction, VoidFunction2 => JVoidFunction2}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.collection.WrappedInternalRow
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.ShuffleExchange
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SnappySession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, SnappyStreamingContext, Time}

/**
 * A SQL based DStream with support for schema/Product
 * This class offers the ability to manipulate SQL query on DStreams
 * It is similar to SchemaRDD, which offers the similar functions
 * Internally, RDD of each batch duration is treated as a small
 * table and CQs are evaluated on those small tables
 * Some of the abstraction and code is borrowed from the project:
 * https://github.com/Intel-bigdata/spark-streamingsql
 *
 * @param snsc
 * @param queryExecution
 *
 */
final class SchemaDStream(@transient val snsc: SnappyStreamingContext,
    @transient val queryExecution: QueryExecution)
    extends DStream[Row](snsc) {

  @transient private val snappySession: SnappySession = snsc.snappySession

  @transient private val catalog = snappySession.sessionState.catalog

  def this(ssc: SnappyStreamingContext, logicalPlan: LogicalPlan) =
    this(ssc, ssc.snappySession.sessionState.executePlan(logicalPlan))

  /**
   * Apply a function to each DataFrame in this SchemaDStream. This is an output operator, so
   * 'this' SchemaDStream will be registered as an output stream and therefore materialized.
   */
  def foreachDataFrame(foreachFunc: DataFrame => Unit): Unit = {
    foreachDataFrame(foreachFunc, needsConversion = true)
  }

  /**
   * Apply a function to each DataFrame in this SchemaDStream. This is an output operator, so
   * 'this' SchemaDStream will be registered as an output stream and therefore materialized.
   */
  def foreachDataFrame(foreachFunc: DataFrame => Unit, needsConversion: Boolean): Unit = {
    val func = (rdd: RDD[Row]) => {
      foreachFunc(snappySession.createDataFrame(rdd, this.schema, needsConversion))
    }
    this.foreachRDD(func)
  }

  /**
   * Apply a function to each DataFrame in this SchemaDStream. This is an output operator, so
   * 'this' SchemaDStream will be registered as an output stream and therefore materialized.
   */
  def foreachDataFrame(foreachFunc: JVoidFunction[DataFrame]): Unit = {
    val func = (rdd: RDD[Row]) => {
      foreachFunc.call(snappySession.createDataFrame(rdd, this.schema, needsConversion = true))
    }
    this.foreachRDD(func)
  }

  /**
   * Apply a function to each DataFrame in this SchemaDStream. This is an output operator, so
   * 'this' SchemaDStream will be registered as an output stream and therefore materialized.
   */
  def foreachDataFrame(foreachFunc: JVoidFunction[DataFrame], needsConversion: Boolean): Unit = {
    val func = (rdd: RDD[Row]) => {
      foreachFunc.call(snappySession.createDataFrame(rdd, this.schema, needsConversion))
    }
    this.foreachRDD(func)
  }

  /**
   * Apply a function to each DataFrame in this SchemaDStream. This is an output operator, so
   * 'this' SchemaDStream will be registered as an output stream and therefore materialized.
   */
  def foreachDataFrame(foreachFunc: JVoidFunction2[DataFrame, Time]): Unit = {
    val func = (rdd: RDD[Row], time: Time) => {
      foreachFunc.call(snappySession.createDataFrame(rdd, this.schema, needsConversion = true),
        time)
    }
    this.foreachRDD(func)
  }


  /**
   * Apply a function to each DataFrame in this SchemaDStream. This is an output operator, so
   * 'this' SchemaDStream will be registered as an output stream and therefore materialized.
   */
  def foreachDataFrame(foreachFunc: (DataFrame, Time) => Unit): Unit = {
    foreachDataFrame(foreachFunc, needsConversion = true)
  }

  /**
   * Apply a function to each DataFrame in this SchemaDStream. This is an output operator, so
   * 'this' SchemaDStream will be registered as an output stream and therefore materialized.
   */
  def foreachDataFrame(foreachFunc: (DataFrame, Time) => Unit, needsConversion: Boolean): Unit = {
    val func = (rdd: RDD[Row], time: Time) => {
      foreachFunc(snappySession.createDataFrame(rdd, this.schema, needsConversion), time)
    }
    this.foreachRDD(func)
  }

  /** Persist the RDDs of this SchemaDStream with the given storage level */
  override def persist(level: StorageLevel): SchemaDStream = {
    super.persist(level)
    this
  }

  /** Persist RDDs of this SchemaDStream with the default storage level (MEMORY_ONLY_SER) */
  override def persist(): SchemaDStream = persist(StorageLevel.MEMORY_ONLY_SER)

  /** Persist RDDs of this SchemaDStream with the default storage level (MEMORY_ONLY_SER) */
  override def cache(): SchemaDStream = persist()

  /**
   * Enable periodic checkpointing of RDDs of this SchemaDStream
   *
   * @param interval Time interval after which generated RDD will be checkpointed
   */
  override def checkpoint(interval: Duration): SchemaDStream = {
    super.checkpoint(interval)
    this
  }

  /** Return a new SchemaDStream containing only the elements that satisfy a predicate. */
  override def filter(filterFunc: Row => Boolean): SchemaDStream = {
    snsc.createSchemaDStream(filter(filterFunc), schema)
  }

  /**
   * Return a new SchemaDStream with an increased or decreased level of parallelism.
   * Each RDD in the returned SchemaDStream has exactly numPartitions partitions.
   */
  override def repartition(numPartitions: Int): SchemaDStream = {
    snsc.createSchemaDStream(transform(_.repartition(numPartitions)), schema)
  }

  /** Registers this SchemaDStream as a table in the catalog. */
  def registerAsTable(tableName: String): Unit = {
    catalog.registerTable(
      catalog.newQualifiedTempTableName(tableName),
      logicalPlan)
  }

  /** Returns the schema of this SchemaDStream (represented by
   * a [[StructType]]). */
  def schema: StructType = queryExecution.analyzed.schema

  /** List of parent DStreams on which this DStream depends on */
  override def dependencies: List[DStream[InternalRow]] = parentStreams.toList

  /** Time interval after which the DStream generates a RDD */
  override def slideDuration: Duration = {
    parentStreams.head.slideDuration
  }

  @transient val logicalPlan: LogicalPlan = queryExecution.logical
  match {
    case _: InsertIntoTable =>
      throw new IllegalStateException(s"logical plan ${queryExecution.logical} " +
          s"is not supported currently")
    case _ => queryExecution.logical
  }

  private val _cachedField = {
    val f = classOf[ShuffleExchange].getDeclaredFields.find(
      _.getName.contains("cachedShuffleRDD")).get
    f.setAccessible(true)
    f
  }

  private def executionPlan: SparkPlan = {
    queryExecution.executedPlan.foreach {
      case s: ShuffleExchange => _cachedField.set(s, null)
      case _ =>
    }
    queryExecution.executedPlan
  }

  /** Method that generates a RDD for the given time */
  override def compute(validTime: Time): Option[RDD[Row]] = {
    StreamBaseRelation.setValidTime(validTime)
    val schema = this.schema
    Some(executionPlan.execute().mapPartitionsInternal { itr =>
      val wrappedRow = new WrappedInternalRow(schema)
      itr.map(row => wrappedRow.internalRow = row )
    })
  }

  @transient private lazy val parentStreams = {
    def traverse(plan: SparkPlan): Seq[DStream[InternalRow]] = plan match {
      case x: StreamPlan => x.rowStream :: Nil
      case _ => plan.children.flatMap(traverse)
    }
    val streams = traverse(queryExecution.executedPlan)
    streams
  }

  /**
   * Returns all column names as an array.
   */
  def columns: Array[String] = schema.fields.map(_.name)

  def printSchema(): Unit = {
    println(schema.treeString) // scalastyle:ignore
  }
}
