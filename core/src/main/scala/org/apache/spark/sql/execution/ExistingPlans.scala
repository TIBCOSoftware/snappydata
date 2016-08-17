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
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, Attribute, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, SinglePartition}
import org.apache.spark.sql.collection.ToolsCallbackInit
import org.apache.spark.sql.sources.{PrunedUnsafeFilteredScan, BaseRelation}
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
    partitionColumns: Seq[Expression],
    extraInformation: String) extends LeafExecNode {

  override lazy val schema: StructType = StructType.fromAttributes(output)

  protected override def doExecute(): RDD[InternalRow] = {
    rdd.mapPartitionsInternal { iter =>
      val proj = UnsafeProjection.create(schema)
      iter.map(proj)
    }
  }

  override def outputsUnsafeRows: Boolean = true

  /** Specifies how data is partitioned across different nodes in the cluster. */
  override lazy val outputPartitioning: Partitioning = {
    if (numPartitions == 1) SinglePartition
    else {
      val callbacks = ToolsCallbackInit.toolsCallback
      if (callbacks != null) {
        callbacks.getOrderlessHashPartitioning(partitionColumns, numPartitions)
      } else {
        HashPartitioning(partitionColumns, numPartitions)
      }
    }
  }

  override def simpleString: String = "Partitioned Scan " + extraInformation +
      " , Requested Columns = " + output.mkString("[", ",", "]") +
      " partitionColumns = " + partitionColumns.mkString("[", ",", "]")
}

private[sql] object PartitionedPhysicalRDD {
  def createFromDataSource(
      output: Seq[Attribute],
      numPartition: Int,
      partitionColumns: Seq[Expression],
      rdd: RDD[InternalRow],
      relation: BaseRelation): PartitionedPhysicalRDD = {
    PartitionedPhysicalRDD(output, rdd, numPartition, partitionColumns,
      relation.toString)
  }
}

trait PartitionedDataSourceScan extends PrunedUnsafeFilteredScan {

  def numPartitions: Int

  def partitionColumns: Seq[String]
}
