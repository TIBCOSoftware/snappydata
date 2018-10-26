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
package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, ClusteredDistribution, Distribution, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.SparkPlan

/**
 * Base trait for joins used in SnappyData. Currently this allows
 * children to have subsets of join keys as partitioning columns
 * without introducing a shuffle.
 */
trait SnappyJoinLike extends SparkPlan {

  val leftKeys: Seq[Expression]
  val rightKeys: Seq[Expression]
  val joinType: JoinType
  val left: SparkPlan
  val right: SparkPlan
  val leftSizeInBytes: BigInt
  val rightSizeInBytes: BigInt

  override def requiredChildDistribution: Seq[Distribution] = {
    // if left or right side is already distributed on a subset of keys
    // then use the same partitioning (for the larger side to reduce exchange)
    val leftClustered = ClusteredDistribution(leftKeys)
    val rightClustered = ClusteredDistribution(rightKeys)
    val leftPartitioning = left.outputPartitioning
    val rightPartitioning = right.outputPartitioning
    // if either side is broadcast then return defaults
    if (leftPartitioning.isInstanceOf[BroadcastDistribution] ||
        rightPartitioning.isInstanceOf[BroadcastDistribution] ||
        // if both sides are unknown then return defaults too
        (leftPartitioning.isInstanceOf[UnknownPartitioning] &&
            rightPartitioning.isInstanceOf[UnknownPartitioning])) {
      leftClustered :: rightClustered :: Nil
    } else {
      // try subsets of the keys on each side
      val leftSubset = getSubsetAndIndices(leftPartitioning, leftKeys)
      val rightSubset = getSubsetAndIndices(rightPartitioning, rightKeys)
      leftSubset match {
        case Some((l, li)) => rightSubset match {
          case Some((r, ri)) =>
            // check if key indices of both sides match
            if (li == ri) {
              ClusteredDistribution(l) :: ClusteredDistribution(r) :: Nil
            } else {
              // choose the bigger plan
              if (leftSizeInBytes > rightSizeInBytes) {
                ClusteredDistribution(l) ::
                    ClusteredDistribution(li.map(rightKeys.apply)) :: Nil
              } else {
                ClusteredDistribution(ri.map(leftKeys.apply)) ::
                    ClusteredDistribution(r) :: Nil
              }
            }
          case None => ClusteredDistribution(l) ::
              ClusteredDistribution(li.map(rightKeys.apply)) :: Nil
        }
        case None => rightSubset match {
          case Some((r, ri)) => ClusteredDistribution(ri.map(leftKeys.apply)) ::
              ClusteredDistribution(r) :: Nil
          case None => leftClustered :: rightClustered :: Nil
        }
      }
    }
  }

  /**
   * Optionally return result if partitioning is a subset of given join keys,
   * and if so then return the subset as well as the indices of subset keys
   * in the join keys (in order).
   */
  protected def getSubsetAndIndices(partitioning: Partitioning,
      keys: Seq[Expression]): Option[(Seq[Expression], Seq[Int])] = {
    val numColumns = Utils.getNumColumns(partitioning)
    if (keys.length > numColumns) {
      keys.indices.combinations(numColumns).map(s => s.map(keys.apply) -> s)
          .find(p => partitioning.satisfies(ClusteredDistribution(p._1)))
    } else None
  }
}
