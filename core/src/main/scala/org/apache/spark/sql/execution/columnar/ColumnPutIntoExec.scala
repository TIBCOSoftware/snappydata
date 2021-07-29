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
package org.apache.spark.sql.execution.columnar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, UnsafeRow}
import org.apache.spark.sql.execution.{BinaryExecNode, ExecutePlan, SparkPlan}
import org.apache.spark.sql.types.LongType


case class ColumnPutIntoExec(insertPlan: SparkPlan,
    updatePlan: SparkPlan) extends BinaryExecNode {

  override lazy val output: Seq[Attribute] = AttributeReference(
    "count", LongType)() :: Nil

  override def left: SparkPlan = insertPlan

  override def right: SparkPlan = updatePlan

  override protected def doExecute(): RDD[InternalRow] = {
    sqlContext.sparkContext.parallelize(sideEffectResult, 1)
  }

  override def executeCollect(): Array[InternalRow] = sideEffectResult

  override def executeTake(limit: Int): Array[InternalRow] =
    sideEffectResult.take(limit)

  protected lazy val sideEffectResult: Array[InternalRow] = {
    // mark the child plans to skip releasing the locks which is only done by the top-level putInto
    updatePlan.foreach {
      case exec: ExecutePlan => exec.clearSession = false
      case _ =>
    }
    insertPlan.foreach {
      case exec: ExecutePlan => exec.clearSession = false
      case _ =>
    }
    // First update the rows which are present in the table. This is necessary because
    // inserts can cause rollover causing updates to fail (or need to add delayed rollover
    // to ColumnInsertExec though that can be done only for partitioned tables else rollover
    // in one bucket can cause another parallel update task on the same bucket to fail).
    val u = updatePlan.executeCollect().map(_.getLong(0)).toSeq.foldLeft(0L)(_ + _)
    // Then insert the rows which are not there in the table
    val i = insertPlan.executeCollect().map(_.getLong(0)).toSeq.foldLeft(0L)(_ + _)
    val resultRow = new UnsafeRow(1)
    val data = new Array[Byte](32)
    resultRow.pointTo(data, 32)
    resultRow.setLong(0, i + u)
    Array(resultRow)
  }
}
