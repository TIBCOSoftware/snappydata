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
package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.DefaultPlanner
import org.apache.spark.sql.streaming._


/**
  * This trait is an extension to SparkPlanner and introduces number of
  * enhancements specific to SnappyData.
  */
private[sql] trait SnappyStrategies {

  self: DefaultPlanner =>

  object SnappyStrategies extends Strategy {

    def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      sampleSnappyCase(plan)
    }
  }

  /** Stream related strategies to map stream specific logical plan to physical plan */
    object StreamQueryStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case LogicalDStreamPlan(output, rowStream) =>
        PhysicalDStreamPlan(output, rowStream) :: Nil
      case WindowLogicalPlan(d, s, LogicalDStreamPlan(output, rowStream), _) =>
        WindowPhysicalPlan(d, s, PhysicalDStreamPlan(output, rowStream)) :: Nil
      case WindowLogicalPlan(d, s, l@LogicalRelation(t: StreamPlan, _, _), _) =>
        WindowPhysicalPlan(d, s, PhysicalDStreamPlan(l.output, t.rowStream)) :: Nil
      case WindowLogicalPlan(d, s, child, _) => throw new AnalysisException(
        s"Unexpected child $child for WindowLogicalPlan")
      case _ => Nil
    }
  }

  object LocalJoinStrategies extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, left, CanLocalJoin(right)) =>
        makeLocalHashJoin(leftKeys, rightKeys, left, right, condition, joins.BuildRight)
      case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, CanLocalJoin(left), right) =>
        makeLocalHashJoin(leftKeys, rightKeys, left, right, condition, joins.BuildLeft)
      case _ => Nil
    }

    object CanLocalJoin {
      def unapply(plan: LogicalPlan): Option[LogicalPlan] = plan match {
        case PhysicalOperation(projects, filters,
        l@LogicalRelation(t: PartitionedDataSourceScan, _, _)) =>
          if (t.numBuckets == 1) Some(plan) else None
        case PhysicalOperation(projects, filters,
        Join(left, right, _, _)) =>
          val leftPlan = CanLocalJoin.unapply(left)
          val rightPlan = CanLocalJoin.unapply(right)
          // If join is a result of join of replicated tables, this
          // join result should also be a local join with any other table
          leftPlan match {
            case Some(_) => rightPlan match {
              case Some(_) => Some(plan)
              case None => None
            }
            case None => None
          }
        case PhysicalOperation(_, _, node) if node.children.size == 1 =>
          CanLocalJoin.unapply(node.children.head) match {
            case Some(_) => Some(plan)
            case None => None
          }

        case x => None
      }
    }

    private[this] def makeLocalHashJoin(
        leftKeys: Seq[Expression],
        rightKeys: Seq[Expression],
        left: LogicalPlan,
        right: LogicalPlan,
        condition: Option[Expression],
        side: joins.BuildSide): Seq[SparkPlan] = {

      val localHashJoin = execution.joins.LocalJoin(
        leftKeys, rightKeys, side, condition, Inner, planLater(left), planLater(right))
      condition.map(FilterExec(_, localHashJoin)).getOrElse(localHashJoin) :: Nil
    }
  }

}
