/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.columnar.impl.ColumnDelta
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan, UnaryExecNode}

/**
 * On top of sort merge join of two child relations.
 */
abstract class BaseDeltaInsertExec(child: SparkPlan) extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output

  /** Specifies how data is partitioned across different nodes in the cluster. */
  override def outputPartitioning: Partitioning = child.outputPartitioning

  /** Specifies any partition requirements on the input data for this operator. */
  // override def requiredChildDistribution: Seq[Distribution] =
  //  Seq.fill(children.size)(UnspecifiedDistribution)

  /** Specifies how data is ordered in each partition. */
  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  /** Specifies sort order for each partition requirements on the input data for this operator. */
  // override def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq.fill(children.size)(Nil)

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def supportCodegen: Boolean = false

  override def inputRDDs(): Seq[RDD[InternalRow]] = if (child.isInstanceOf[SortMergeJoinExec]) {
    child.asInstanceOf[SortMergeJoinExec].inputRDDs()
  } else Nil

  override def doProduce(ctx: CodegenContext): String = if (child.isInstanceOf[SortMergeJoinExec]) {
    child.asInstanceOf[SortMergeJoinExec].doProduce(ctx)
  } else ""
}

case class DeltaInsertExec(child: SparkPlan) extends BaseDeltaInsertExec(child) {

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val keyAttributes = ColumnDelta.mutableKeyAttributes.map(_.name)
    val out = output.map(_.name)
    val keyAttributeIndices = Seq(out.indexOf(keyAttributes.head), out.indexOf(keyAttributes(1)),
      out.indexOf(keyAttributes(2)), out.indexOf(keyAttributes(3)))

    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
      var lastRowOrdinal: Long = Long.MinValue
      var lastBatchId: Long = Long.MinValue
      var lastBucketOrdinal: Integer = Int.MinValue
      var lastBatchNumrows: Integer = Int.MinValue
      iter.dropWhile { row =>
        keyAttributeIndices.forall(i => row.isNullAt(i))
      }.filter { row =>
        val allNulls = keyAttributeIndices.forall(i => row.isNullAt(i))
        if (!allNulls) {
          lastRowOrdinal = row.getLong(keyAttributeIndices.head)
          lastBatchId = row.getLong(keyAttributeIndices(1))
          lastBucketOrdinal = row.getInt(keyAttributeIndices(2))
          lastBatchNumrows = row.getInt(keyAttributeIndices(3))
        }
        allNulls
      }.map { row =>
        numOutputRows += 1
        row.setLong(keyAttributeIndices.head, lastRowOrdinal)
        row.setLong(keyAttributeIndices(1), lastBatchId)
        row.setInt(keyAttributeIndices(2), lastBucketOrdinal)
        row.setInt(keyAttributeIndices(3), lastBatchNumrows)
        row
      }
    }
  }
}

case class DirectInsertExec(child: SparkPlan) extends BaseDeltaInsertExec(child) {

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
      iter.takeWhile { row =>
        val allNulls = output.indices.forall(i => row.isNullAt(i))
        if (!allNulls) {
          numOutputRows += 1
        }
        !allNulls
      }
    }
  }
}
