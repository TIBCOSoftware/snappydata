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

import io.snappydata.Constant

import org.apache.spark.sql.SampleDataFrameContract.ErrorRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.collection.MultiColumnOpenHashMap
import org.apache.spark.sql.execution.QueryExecution

import org.apache.spark.sql.sources.StatCounter

import scala.collection.mutable

final class SampleDataFrame(@transient val snappySession: SnappySession,
    @transient override val logicalPlan: LogicalPlan)
    extends DataFrame(snappySession, logicalPlan, DataFrameUtil.encoder(snappySession,
      logicalPlan)) with Serializable {

  @transient var implementor: SampleDataFrameContract =
    createSampleDataFrameContract

  def registerSampleTable(tableName: String, baseTable: Option[String]): Unit =
    implementor.registerSampleTable(tableName, baseTable)

  def errorStats(columnName: String,
      groupBy: Set[String] = Set.empty): MultiColumnOpenHashMap[StatCounter] =
    implementor.errorStats(columnName, groupBy)

  def errorEstimateAverage(columnName: String, confidence: Double,
      groupByColumns: Set[String] = Set.empty): mutable.Map[Row, ErrorRow] =
    implementor.errorEstimateAverage(columnName, confidence, groupByColumns)

  private def createSampleDataFrameContract =
    snappySession.snappyContextFunctions.createSampleDataFrameContract(snappySession,
      this, logicalPlan)
}

final class DataFrameWithTime(_snappySession: SnappySession,
    _logicalPlan: LogicalPlan, val time: Long)
    extends DataFrame(_snappySession, _logicalPlan, DataFrameUtil.encoder(
      _snappySession, _logicalPlan)) with Serializable

case class AQPDataFrame(@transient snappySession: SnappySession,
    @transient qe: QueryExecution) extends DataFrame(snappySession, qe,
    DataFrameUtil.encoder(snappySession, qe)) {

  def withError(error: Double,
      confidence: Double = Constant.DEFAULT_CONFIDENCE,
      behavior: String = Constant.DEFAULT_BEHAVIOR): DataFrame =
    snappySession.snappyContextFunctions.withErrorDataFrame(this, error,
      confidence, behavior)
}

object DataFrameUtil {

  def encoder(sparkSession: SparkSession,
      logicalPlan: LogicalPlan): ExpressionEncoder[Row] = {
    val qe = sparkSession.sessionState.executePlan(logicalPlan)
    qe.assertAnalyzed()
    RowEncoder(qe.analyzed.schema)
  }

  def encoder(sparkSession: SparkSession,
      qe: QueryExecution): ExpressionEncoder[Row] = {
    qe.assertAnalyzed()
    RowEncoder(qe.analyzed.schema)
  }
}
