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
package org.apache.spark.sql.execution.joins

import java.io.{ObjectOutputStream, IOException}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.util.Utils
import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}


/**
 * :: DeveloperApi ::
 * Performs an local hash join of two child relations.  If a relation (out of a datasource) is already replicated
 * accross all nodes then rather than doing a Broadcast join which can be expensive, this join just
 * scans through the single partition of the replicated relation while streaming through the other relation
 */
@DeveloperApi
case class LocalJoin(leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    left: SparkPlan,
    right: SparkPlan)
    extends BinaryNode with HashJoin {


  override private[sql] lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createLongMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createLongMetric(sparkContext, "number of right rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning


  override def requiredChildDistribution: Seq[Distribution] =
    UnspecifiedDistribution :: UnspecifiedDistribution :: Nil

  /**
   * Overridden by concrete implementations of SparkPlan.
   * Produces the result of the query as an RDD[InternalRow]
   */
  override protected def doExecute(): RDD[InternalRow] = {
    val (numBuildRows, numStreamedRows) = buildSide match {
      case BuildLeft => (longMetric("numLeftRows"), longMetric("numRightRows"))
      case BuildRight => (longMetric("numRightRows"), longMetric("numLeftRows"))
    }
    val numOutputRows = longMetric("numOutputRows")

    narrowPartitions(buildPlan.execute(), streamedPlan.execute(), true) {
      (buildIter, streamIter) => {
        val hashed = HashedRelation(buildIter, numBuildRows, buildSideKeyGenerator)
        hashJoin(streamIter, numStreamedRows, hashed, numOutputRows)
      }
    }
  }

  def narrowPartitions(buildRDD: RDD[InternalRow], streamRDD: RDD[InternalRow], preservesPartitioning: Boolean)
      (f: (Iterator[InternalRow], Iterator[InternalRow]) => Iterator[InternalRow]): NarrowPartitionsRDD = {
    val sc = buildRDD.sparkContext
    new NarrowPartitionsRDD(sc, sc.clean(f),
      buildRDD, streamRDD, preservesPartitioning)
  }
}


private[spark] class NarrowPartitionsRDD(
    @transient sc: SparkContext,
    var f: (Iterator[InternalRow], Iterator[InternalRow]) => Iterator[InternalRow],
    var buildRDD: RDD[InternalRow],
    var streamRDD: RDD[InternalRow],
    preservesPartitioning: Boolean = false)
    extends RDD[InternalRow](sc, Seq(new OneToOneDependency(streamRDD))) {

  override def compute(s: Partition, context: TaskContext): Iterator[InternalRow] = {
    val partitions = s.asInstanceOf[NarrowPartitionsPartition]
    f(buildRDD.iterator(partitions.buildPartition, context),
      streamRDD.iterator(partitions.streamPartition, context))
  }

  override def getPartitions: Array[Partition] = {
    val numParts = streamRDD.partitions.length
    val part = buildRDD.partitions.head
    Array.tabulate[Partition](numParts) { i =>
      val streamLocs = streamRDD.preferredLocations(streamRDD.partitions(i))
      val buildLocs = buildRDD.preferredLocations(part)
      val exactMatchLocations = streamLocs.intersect(buildLocs)
      val locs = if (!exactMatchLocations.isEmpty) exactMatchLocations else
                         (streamLocs ++ buildLocs).distinct

      new NarrowPartitionsPartition(part.index, buildRDD, i, streamRDD , locs)
    }
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    s.asInstanceOf[NarrowPartitionsPartition].preferredLocations
  }

  override def clearDependencies() {
    super.clearDependencies()
    buildRDD = null
    streamRDD = null
    f = null
  }

}

private[spark] class NarrowPartitionsPartition(
    buildIdx : Int,
    @transient var buildRDD: RDD[InternalRow],
    streamIdx: Int,
    @transient var streamRDD: RDD[InternalRow],
    @transient val preferredLocations: Seq[String])
    extends Partition {
  override val index: Int = streamIdx
  var buildPartition = buildRDD.partitions(buildIdx)
  var streamPartition = streamRDD.partitions(streamIdx)

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    buildPartition = buildRDD.partitions(buildIdx)
    streamPartition = streamRDD.partitions(streamIdx)

    oos.defaultWriteObject()
  }
}
