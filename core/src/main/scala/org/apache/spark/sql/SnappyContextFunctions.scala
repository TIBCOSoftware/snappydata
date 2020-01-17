/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package org.apache.spark.sql

import io.snappydata.SnappyDataFunctions
import io.snappydata.sql.catalog.CatalogObjectType

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.streaming.StreamBaseRelation
import org.apache.spark.sql.types.StructType

class SnappyContextFunctions {

  def clear(): Unit = {}

  def clearStatic(): () => Unit = () => {}

  def postRelationCreation(relation: Option[BaseRelation], session: SnappySession): Unit = {}

  def registerSnappyFunctions(session: SnappySession): Unit = {
    val registry = session.sessionState.functionRegistry
    SnappyDataFunctions.builtin.foreach(fn => registry.registerFunction(fn._1, fn._2, fn._3))
  }

  def createTopK(session: SnappySession, tableName: String,
      keyColumnName: String, schema: StructType,
      topkOptions: Map[String, String], ifExists: Boolean): Boolean =
    throw new UnsupportedOperationException("missing aqp jar")

  def dropTopK(session: SnappySession, topKName: String): Unit =
    throw new UnsupportedOperationException("missing aqp jar")

  def insertIntoTopK(session: SnappySession, rows: RDD[Row],
      topKName: String, time: Long): Unit =
    throw new UnsupportedOperationException("missing aqp jar")

  def queryTopK(session: SnappySession, topKName: String,
      startTime: String, endTime: String, k: Int): DataFrame =
    throw new UnsupportedOperationException("missing aqp jar")

  def queryTopK(session: SnappySession, topK: String,
      startTime: Long, endTime: Long, k: Int): DataFrame =
    throw new UnsupportedOperationException("missing aqp jar")

  def queryTopKRDD(session: SnappySession, topK: String,
      startTime: String, endTime: String, schema: StructType): RDD[InternalRow] =
    throw new UnsupportedOperationException("missing aqp jar")

  protected[sql] def collectSamples(session: SnappySession, rows: RDD[Row],
      aqpTables: Seq[String], time: Long): Unit =
    throw new UnsupportedOperationException("missing aqp jar")

  def createSampleDataFrameContract(session: SnappySession, df: DataFrame,
      logicalPlan: LogicalPlan): SampleDataFrameContract =
    throw new UnsupportedOperationException("missing aqp jar")

  def convertToStratifiedSample(options: Map[String, Any], session: SnappySession,
      logicalPlan: LogicalPlan): LogicalPlan =
    throw new UnsupportedOperationException("missing aqp jar")

  def isStratifiedSample(logicalPlan: LogicalPlan): Boolean =
    throw new UnsupportedOperationException("missing aqp jar")

  def withErrorDataFrame(df: DataFrame, error: Double,
      confidence: Double, behavior: String): DataFrame =
    throw new UnsupportedOperationException("missing aqp jar")

  def newSQLParser(snappySession: SnappySession): SnappySqlParser =
    new SnappySqlParser(snappySession)

  def aqpTablePopulator(session: SnappySession): Unit = {
    // register blank tasks for the stream tables so that the streams start
    session.sessionState.catalog.getDataSourceRelations[StreamBaseRelation](
      CatalogObjectType.Stream).foreach(_.rowStream.foreachRDD(_ => Unit))
  }

  def sql[T](fn: => T): T = fn
}
