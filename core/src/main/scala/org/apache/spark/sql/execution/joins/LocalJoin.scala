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

import java.io.{IOException, ObjectOutputStream}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.storage.{RDDBlockId, StorageLevel}
import org.apache.spark.util.Utils
import org.apache.spark.{OneToOneDependency, Partition, SparkContext, SparkEnv, TaskContext}


/**
  * :: DeveloperApi ::
  * Performs an local hash join of two child relations. If a relation
  * (out of a datasource) is already replicated accross all nodes then rather
  * than doing a Broadcast join which can be expensive, this join just
  * scans through the single partition of the replicated relation while
  * streaming through the other relation.
  */
@DeveloperApi
case class LocalJoin(leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    condition: Option[Expression],
    joinType: JoinType,
    left: SparkPlan,
    right: SparkPlan)
    extends BinaryExecNode with HashJoin {


  override private[sql] lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createMetric(sparkContext, "number of right rows"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

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
    val context = TaskContext.get()
    def hashedRelationIter(buildIter: Iterator[InternalRow]): HashedRelation = {
      HashedRelation(buildIter, buildKeys, taskMemoryManager = context.taskMemoryManager())
    }

    val buildRDD = buildPlan.execute()
    val streamRDD = streamedPlan.execute()

    val sc = buildRDD.sparkContext
    val hashedRDD = new HashRelationRDD(sc, buildRDD,
      streamRDD.partitions.length, sc.clean(hashedRelationIter))

    narrowPartitions(hashedRDD, streamRDD, preservesPartitioning = true) {
      (hashedIter, streamIter) => {
        val hashed = hashedIter.next()
        join(streamIter, hashed, numOutputRows)
      }
    }
  }

  def narrowPartitions(hashedRDD: RDD[HashedRelation], streamRDD: RDD[InternalRow],
      preservesPartitioning: Boolean)
      (f: (Iterator[HashedRelation], Iterator[InternalRow])
          => Iterator[InternalRow]): NarrowPartitionsRDD = {
    val sc = hashedRDD.sparkContext
    new NarrowPartitionsRDD(sc, sc.clean(f),
      hashedRDD, streamRDD, preservesPartitioning)
  }
}

// Helper object jut to take a global sync across tasks.
// Furture helper methods can come here
private[spark] object HashRelationRDD

private[spark] class HashRelationRDD(
    sc: SparkContext,
    var buildRDD: RDD[InternalRow],
    val maxPartitions: Int,
    var f: (Iterator[InternalRow]) => HashedRelation
    ) extends RDD[HashedRelation](sc, Seq(new OneToOneDependency(buildRDD))) {

  override def compute(s: Partition,
      context: TaskContext): Iterator[HashedRelation] = {

    val blockId = RDDBlockId(this.id, s.index)

    val rel1 = SparkEnv.get.blockManager.getSingle(blockId) match {
      case Some(x) => x.asInstanceOf[HashedRelation]
      case None =>
        HashRelationRDD.synchronized {
          SparkEnv.get.blockManager.getSingle(blockId) match {
            case Some(x) => x.asInstanceOf[HashedRelation]
            case None =>
              val hashedRelation = f(buildRDD.iterator(s, context))
              SparkEnv.get.blockManager.putSingle(blockId, hashedRelation,
                StorageLevel.MEMORY_AND_DISK_SER, tellMaster = false)

              hashedRelation
          }

        }
    }

    Seq(rel1).iterator
  }

  override def getPartitions: Array[Partition] = {
    buildRDD.partitions
  }


  override def getPreferredLocations(s: Partition): Seq[String] = {
    buildRDD.preferredLocations(s)
  }
}

private[spark] class NarrowPartitionsRDD(_sc: SparkContext,
    var f: (Iterator[HashedRelation], Iterator[InternalRow]) => Iterator[InternalRow],
    var hashedRDD: RDD[HashedRelation],
    var streamRDD: RDD[InternalRow],
    preservesPartitioning: Boolean = false)
    extends RDD[InternalRow](_sc, Seq(new OneToOneDependency(streamRDD))) {

  override def compute(s: Partition, context: TaskContext): Iterator[InternalRow] = {
    val partitions = s.asInstanceOf[NarrowPartitionsPartition]
    f(hashedRDD.iterator(partitions.buildPartition, context),
      streamRDD.iterator(partitions.streamPartition, context))
  }

  override def getPartitions: Array[Partition] = {
    val numParts = streamRDD.partitions.length
    val part = hashedRDD.partitions.head
    Array.tabulate[Partition](numParts) { i =>
      val streamLocs = streamRDD.preferredLocations(streamRDD.partitions(i))
      val buildLocs = hashedRDD.preferredLocations(part)
      val exactMatchLocations = streamLocs.intersect(buildLocs)
      val locs = if (exactMatchLocations.nonEmpty) exactMatchLocations
      else (streamLocs ++ buildLocs).distinct

      new NarrowPartitionsPartition(part.index, hashedRDD, i, streamRDD, locs)
    }
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    s.asInstanceOf[NarrowPartitionsPartition].preferredLocations
  }

  override def clearDependencies() {
    super.clearDependencies()
    hashedRDD = null
    streamRDD = null
    f = null
  }

}

private[spark] class NarrowPartitionsPartition(
    buildIdx: Int,
    @transient var hashedRDD: RDD[HashedRelation],
    streamIdx: Int,
    @transient var streamRDD: RDD[InternalRow],
    @transient val preferredLocations: Seq[String])
    extends Partition {
  override val index: Int = streamIdx
  var buildPartition = hashedRDD.partitions(buildIdx)
  var streamPartition = streamRDD.partitions(streamIdx)

  @throws[IOException]
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    buildPartition = hashedRDD.partitions(buildIdx)
    streamPartition = streamRDD.partitions(streamIdx)

    oos.defaultWriteObject()
  }
}
