/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, UnsafeRow}
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.types.LongType


case class ColumnPutIntoExec(insertPlan: SparkPlan,
    updatePlan: SparkPlan) extends BinaryExecNode {

  override lazy val output: Seq[Attribute] = AttributeReference(
    "count", LongType)() :: Nil

  override def left: SparkPlan = insertPlan

  override def right: SparkPlan = updatePlan

  override protected def doExecute(): RDD[InternalRow] = {
    val resultRow = executeCollect()
    sqlContext.sparkContext.parallelize(resultRow, 1)
  }

  override def executeCollect(): Array[InternalRow] = {
    try {
      collectResults()
    } finally {
      sqlContext.sparkSession.asInstanceOf[SnappySession].clearPutInto()
    }
  }

  private def collectResults(): Array[InternalRow] = {
    // First update the rows which are present in the table
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
