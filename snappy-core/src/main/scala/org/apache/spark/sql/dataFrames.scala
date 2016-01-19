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
package org.apache.spark.sql

import org.apache.spark.sql.SampleDataFrameContract.ErrorRow
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.collection.MultiColumnOpenHashMap

import org.apache.spark.sql.sources.StatCounter

import scala.collection.mutable

final class SampleDataFrame(@transient override val sqlContext: SnappyContext,
    @transient override val logicalPlan: LogicalPlan)
    extends DataFrame(sqlContext, logicalPlan) with Serializable {

  @transient var implementor: SampleDataFrameContract =
    createSampleDataFrameContract

  def registerSampleTable(tableName: String): Unit =
    implementor.registerSampleTable(tableName)

  override def registerTempTable(tableName: String): Unit =
    registerSampleTable(tableName)

  def errorStats(columnName: String,
      groupBy: Set[String] = Set.empty): MultiColumnOpenHashMap[StatCounter] =
    implementor.errorStats(columnName, groupBy)

  def errorEstimateAverage(columnName: String, confidence: Double,
      groupByColumns: Set[String] = Set.empty): mutable.Map[Row, ErrorRow] =
    implementor.errorEstimateAverage(columnName, confidence, groupByColumns)

  private def createSampleDataFrameContract =
    sqlContext.snappyContextFunctions.createSampleDataFrameContract(sqlContext,
      this, logicalPlan)
}

final class DataFrameWithTime(@transient _sqlContext: SnappyContext,
    @transient _logicalPlan: LogicalPlan, val time: Long)
    extends DataFrame(_sqlContext, _logicalPlan) with Serializable
