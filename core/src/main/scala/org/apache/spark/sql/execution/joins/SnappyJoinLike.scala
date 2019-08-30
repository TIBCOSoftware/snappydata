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
package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, ClusteredDistribution, Distribution, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SnappyHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}

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
      val leftSubsets = getSubsetsAndIndices(leftPartitioning, leftKeys, left)
      val rightSubsets = getSubsetsAndIndices(rightPartitioning, rightKeys, right)
      // find the subsets with matching indices else pick the first ones from the two
      var rightSubset = rightSubsets.headOption
      val leftSubset =
        if (leftSubsets.isEmpty) None
        else if (rightSubsets.isEmpty) Some(leftSubsets.head)
        else {
          leftSubsets.find(p => rightSubsets.find(_._2 == p._2) match {
            case None => false
            case x => rightSubset = x; true
          }).orElse(leftSubsets.headOption)
        }
      leftSubset match {
        case Some((l, li)) => rightSubset match {
          case Some((r, ri)) =>
            // check if key indices of both sides match
            if (li == ri) {
              ClusteredDistribution(l) :: ClusteredDistribution(r) :: Nil
            } else {
              // choose the bigger plan to match distribution and reduce shuffle
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
   * and if so then return the subset as well as the indices of subset keys in the
   * join keys (in order). Also unwraps aliases in the keys for matching against
   * partitioning and returns a boolean indicating whether alias was unwrapped or not.
   */
  protected def getSubsetsAndIndices(partitioning: Partitioning,
      keys: Seq[Expression], child: SparkPlan): Seq[(Seq[Expression], Seq[Int])] = {
    val numColumns = Utils.getNumColumns(partitioning)
    if (keys.length >= numColumns) {
      var combination: Seq[Expression] = null
      keys.indices.combinations(numColumns).collect {
        case s if partitioning.satisfies(ClusteredDistribution {
          combination = s.map(keys.apply)
          combination
        }) => (combination, s)
        case s if partitioning.satisfies(ClusteredDistribution {
          combination = unAlias(s.map(keys.apply), child)
          combination
        }) => (combination, s)
      }.toSeq
    } else Nil
  }

  /**
   * Remove the extra Aliases added by aggregates (e.g. k1 projection in
   * "select k1, max(k2) from t1 group by k1") so partitioning can match against the base
   * Attribute for such cases rather than the aliased one which will never match
   * introducing an unnecessary shuffle.
   *
   * Since the aggregate plans generate a new Attribute for the projected grouping keys,
   * hence this method has to go down to the resultExpressions of an aggregate and remove
   * aliases at that level to get hold of the base Attribute.
   */
  private def unAlias(exprs: Seq[Expression], child: SparkPlan): Seq[Expression] = {
    // all aggregate implementation will have output as outputExpressions.map(_.toAttribute)
    // so search for matching attribute in output, go to the corresponding outputExpression
    // and remove aliases from that to get the base Attribute
    def matchAggregate(output: Seq[Attribute], outputExpressions: Seq[NamedExpression],
        result: Array[Expression]): Unit = {
      for (i <- output.indices) {
        val pos = result.indexOf(output(i))
        if (pos != -1) {
          // only unwrap Attributes inside Aliases
          val substitute = Utils.unAlias(outputExpressions(i), classOf[Attribute])
          if (substitute ne outputExpressions(i)) {
            result(pos) = substitute
          }
        }
      }
    }

    def searchAggregate(plan: SparkPlan, result: Array[Expression]): Unit = plan match {
      // resolve if possible in this aggregate and keep searching the tree afterwards
      case a: SnappyHashAggregateExec => matchAggregate(a.output, a.resultExpressions, result)
        searchAggregate(a.child, result)
      case a: HashAggregateExec => matchAggregate(a.output, a.resultExpressions, result)
        searchAggregate(a.child, result)
      case a: SortAggregateExec => matchAggregate(a.output, a.resultExpressions, result)
        searchAggregate(a.child, result)
      case u: UnaryExecNode if u.outputPartitioning == u.child.outputPartitioning =>
        searchAggregate(u.child, result)
      case p if p.children.exists(_.outputPartitioning == p.outputPartitioning) =>
        searchAggregate(p.children.find(_.outputPartitioning == p.outputPartitioning).get, result)
      case _ =>
    }

    val result = exprs.toArray
    searchAggregate(child, result)
    result
  }
}
