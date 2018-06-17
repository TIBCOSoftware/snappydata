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
    val out = output
    // TODO VB: remove this
    // scalastyle:off println
    println(s" DeltaInsertExec $out")
    // scalastyle:on println
    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
      iter.filter { row =>
        out.indices.foreach(i => {
          val attr = out(i)
          print(s" [$i, ${row.get(i, attr.dataType)}]")
        })
        // TODO VB: remove this
        // scalastyle:off println
        println()
        // scalastyle:on println
        numOutputRows += 1
        true
      }
    }
  }
}

case class DirectInsertExec(child: SparkPlan) extends BaseDeltaInsertExec(child) {

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val out = output
    // TODO VB: remove this
    // scalastyle:off println
    println(s" DirectInsertExec $out")
    // scalastyle:on println
    child.execute().mapPartitionsWithIndexInternal { (index, iter) =>
      iter.filter { row =>
        out.indices.foreach(i => {
          val attr = out(i)
          print(s" [$i, ${row.get(i, attr.dataType)}]")
        })
        // TODO VB: remove this
        // scalastyle:off println
        println()
        // scalastyle:on println
        numOutputRows += 1
        true
      }
    }
  }
}
