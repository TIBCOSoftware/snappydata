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
package org.apache.spark.sql.execution

import scala.collection.AbstractIterator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, SinglePartition}
import org.apache.spark.sql.collection.ToolsCallbackInit
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.sources.{BaseRelation, PrunedUnsafeFilteredScan}
import org.apache.spark.sql.types.StructType

/** Physical plan node for scanning data from an DataSource scan RDD.
 * If user knows that the data is partitioned or replicated across
 * all nodes this SparkPla can be used to avoid expensive shuffle
 * and Broadcast joins. This plan overrides outputPartitioning and
 * make it inline with the partitioning of the underlying DataSource */
private[sql] case class PartitionedPhysicalRDD(
    output: Seq[Attribute],
    rdd: RDD[InternalRow],
    numPartitions: Int,
    numBuckets: Int,
    partitionColumns: Seq[Expression],
    extraInformation: String) extends LeafExecNode with CodegenSupport {

  override lazy val schema: StructType = StructType.fromAttributes(output)

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val scanTime = longMetric("scanTime")
    rdd.mapPartitionsPreserve { itr =>
      new AbstractIterator[InternalRow] {

        var nRows = 0L
        val start = System.nanoTime()

        def hasNext: Boolean = {
          if (itr.hasNext) true
          else {
            numOutputRows += nRows
            scanTime += (System.nanoTime() - start) / 1000000L
            false
          }
        }

        def next(): InternalRow = {
          nRows += 1
          itr.next()
        }
      }
    }
  }

  /** Specifies how data is partitioned across different nodes in the cluster. */
  override lazy val outputPartitioning: Partitioning = {
    if (numPartitions == 1) {
      SinglePartition
    }
    else {
      val callbacks = ToolsCallbackInit.toolsCallback
      if (callbacks != null) {
        callbacks.getOrderlessHashPartitioning(partitionColumns, numPartitions, numBuckets)
      } else {
        HashPartitioning(partitionColumns, numPartitions)
      }
    }
  }

  private[sql] override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan and consume time"))

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    rdd :: Nil
  }

  override def doProduce(ctx: CodegenContext): String = {
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    val scanTime = metricTerm(ctx, "scanTime")
    // PartitionedPhysicalRDD always just has one input
    val input = ctx.freshName("input")
    val numRows = ctx.freshName("numRows")
    val start = ctx.freshName("start")
    ctx.addMutableState("scala.collection.Iterator", input,
      s"$input = inputs[0];")
    val exprRows = output.zipWithIndex.map { case (a, i) =>
      BoundReference(i, a.dataType, a.nullable)
    }
    val row = ctx.freshName("row")
    ctx.INPUT_ROW = row
    ctx.currentVars = null
    val columnsRowInput = exprRows.map(_.genCode(ctx))
    s"""
       |long $numRows = 0L;
       |long $start = System.nanoTime();
       |try {
       |  while ($input.hasNext()) {
       |    InternalRow $row = (InternalRow)$input.next();
       |    $numRows++;
       |    ${consume(ctx, columnsRowInput, row).trim}
       |    if (shouldStop()) return;
       |  }
       |} finally {
       |  $numOutputRows.add($numRows);
       |  $scanTime.add((System.nanoTime() - $start)/1000000L);
       |}
     """.stripMargin
  }

  override def simpleString: String = "Partitioned Scan " + extraInformation +
      " , Requested Columns = " + output.mkString("[", ",", "]") +
      " partitionColumns = " + partitionColumns.mkString("[", ",", "]" +
      " numBuckets= " + numBuckets +
      " numPartitions= " + numPartitions)
}

private[sql] object PartitionedPhysicalRDD {
  def createFromDataSource(
      output: Seq[Attribute],
      numPartition: Int,
      numBuckets: Int,
      partitionColumns: Seq[Expression],
      rdd: RDD[InternalRow],
      relation: BaseRelation): PartitionedPhysicalRDD = {
    PartitionedPhysicalRDD(output, rdd, numPartition, numBuckets, partitionColumns,
      relation.toString)
  }
}

trait PartitionedDataSourceScan extends PrunedUnsafeFilteredScan {

  def numPartitions: Int

  def numBuckets: Int

  def partitionColumns: Seq[String]
}
