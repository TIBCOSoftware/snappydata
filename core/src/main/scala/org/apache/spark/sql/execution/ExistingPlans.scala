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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, SinglePartition}
import org.apache.spark.sql.collection.ToolsCallbackInit
import org.apache.spark.sql.execution.columnar.impl.BaseColumnFormatRelation
import org.apache.spark.sql.execution.columnar.{ColumnTableScan, ConnectionType}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.row.RowFormatRelation
import org.apache.spark.sql.sources.{PrunedUnsafeFilteredScan, SamplingRelation}
import org.apache.spark.sql.types._

/** Physical plan node for scanning data from an DataSource scan RDD.
 * If user knows that the data is partitioned or replicated across
 * all nodes this SparkPla can be used to avoid expensive shuffle
 * and Broadcast joins. This plan overrides outputPartitioning and
 * make it inline with the partitioning of the underlying DataSource */
private[sql] abstract class PartitionedPhysicalRDD(
    output: Seq[Attribute],
    rdd: RDD[Any],
    numPartitions: Int,
    numBuckets: Int,
    partitionColumns: Seq[Expression],
    baseRelation: PartitionedDataSourceScan)
    extends LeafExecNode with CodegenSupport {

  private val extraInformation = baseRelation.toString

  override lazy val schema: StructType = StructType.fromAttributes(output)

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    rdd.asInstanceOf[RDD[InternalRow]] :: Nil
  }

  protected override def doExecute(): RDD[InternalRow] = {
    WholeStageCodegenExec(this).execute()
  }

  /** Specifies how data is partitioned across different nodes in the cluster. */
  override lazy val outputPartitioning: Partitioning = {
    if (numPartitions == 1) {
      SinglePartition
    } else if (partitionColumns.nonEmpty) {
      val callbacks = ToolsCallbackInit.toolsCallback
      if (callbacks != null) {
        callbacks.getOrderlessHashPartitioning(partitionColumns,
          numPartitions, numBuckets)
      } else {
        HashPartitioning(partitionColumns, numPartitions)
      }
    } else super.outputPartitioning
  }

  private[sql] override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def simpleString: String = "Partitioned Scan " + extraInformation +
      " , Requested Columns = " + output.mkString("[", ",", "]") +
      " partitionColumns = " + partitionColumns.mkString("[", ",", "]" +
      " numBuckets= " + numBuckets +
      " numPartitions= " + numPartitions)
}

private[sql] object PartitionedPhysicalRDD {
  def createFromDataSource(
      output: Seq[Attribute],
      numPartitions: Int,
      numBuckets: Int,
      partitionColumns: Seq[Expression],
      rdd: RDD[Any],
      otherRDDs: Seq[RDD[InternalRow]],
      relation: PartitionedDataSourceScan): PartitionedPhysicalRDD =
    relation match {
      case r: BaseColumnFormatRelation =>
        ColumnTableScan(output, rdd, otherRDDs, numPartitions, numBuckets,
          partitionColumns, r.scanAsUnsafeRows, relation)
      case r: SamplingRelation =>
        ColumnTableScan(output, rdd, otherRDDs, numPartitions, numBuckets,
          partitionColumns, r.baseRelation.scanAsUnsafeRows, relation)
      case _: RowFormatRelation =>
        if (otherRDDs.nonEmpty) {
          throw new UnsupportedOperationException(
            "Row table scan cannot handle other RDDs")
        }
        RowTableScan(output, rdd, numPartitions, numBuckets,
          partitionColumns, relation)
    }
}

trait PartitionedDataSourceScan extends PrunedUnsafeFilteredScan {

  def schema: StructType

  def numPartitions: Int

  def numBuckets: Int

  def partitionColumns: Seq[String]

  def connectionType: ConnectionType.Value
}
