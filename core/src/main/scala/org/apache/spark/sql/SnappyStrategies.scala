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

import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Final, ImperativeAggregate, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, NamedExpression, RowOrdering}
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, PhysicalAggregation, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{AggUtils, CollectAggregateExec, SnappyHashAggregateExec}
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
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition,
      left, right) if canBuildRight(joinType) && canLocalJoin(right) =>
        makeLocalHashJoin(leftKeys, rightKeys, left, right, condition,
          joinType, joins.BuildRight, true)
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition,
      left, right) if canBuildLeft(joinType) && canLocalJoin(left) =>
          makeLocalHashJoin(leftKeys, rightKeys, left, right, condition,
            joinType, joins.BuildLeft, true)
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if canBuildRight(joinType) && canBuildLocalHashMap(right) ||
            !RowOrdering.isOrderable(leftKeys) =>
        makeLocalHashJoin(leftKeys, rightKeys, left, right, condition,
          joinType, joins.BuildRight, false)
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if canBuildLeft(joinType) && canBuildLocalHashMap(left) ||
            !RowOrdering.isOrderable(leftKeys) =>
        makeLocalHashJoin(leftKeys, rightKeys, left, right, condition,
          joinType, joins.BuildLeft, false)
      case _ => Nil
    }

    /**
     * Matches a plan whose size is small enough to build a hash table.
     *
     */
    private def canBuildLocalHashMap(plan: LogicalPlan): Boolean = {
      plan.statistics.sizeInBytes <
          io.snappydata.Property.LocalHashJoinSize.configEntry.getConf[Long](conf)
    }

    private def canBuildRight(joinType: JoinType): Boolean = joinType match {
      case Inner | LeftOuter | LeftSemi | LeftAnti => true
      case j: ExistenceJoin => true
      case _ => false
    }

    private def canBuildLeft(joinType: JoinType): Boolean = joinType match {
      case Inner | RightOuter => true
      case _ => false
    }

    private def canLocalJoin(plan: LogicalPlan): Boolean = {
      plan match {
        case PhysicalOperation(projects, filters,
        l@LogicalRelation(t: PartitionedDataSourceScan, _, _)) =>
          t.numBuckets == 1
        case PhysicalOperation(projects, filters,
        Join(left, right, _, _)) =>
          // If join is a result of join of replicated tables, this
          // join result should also be a local join with any other table
          canLocalJoin(left) && canLocalJoin(right)
        case PhysicalOperation(_, _, node) if node.children.size == 1 =>
          canLocalJoin(node.children.head)

        case _ => false
      }
    }

    private[this] def makeLocalHashJoin(
        leftKeys: Seq[Expression],
        rightKeys: Seq[Expression],
        left: LogicalPlan,
        right: LogicalPlan,
        condition: Option[Expression],
        joinType: JoinType,
        side: joins.BuildSide,
        replicatedTableJoin: Boolean): Seq[SparkPlan] = {
      joins.LocalJoin(leftKeys, rightKeys, side, condition,
        joinType, planLater(left), planLater(right), replicatedTableJoin) :: Nil
    }
  }

}

/**
 * Used to plan the aggregate operator for expressions using the optimized
 * SnappyData aggregation operators.
 *
 * Adapted from Spark's Aggregation strategy.
 */
object SnappyAggregation extends Strategy {

  var enableOptimizedAggregation = true

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ReturnAnswer(rootPlan) => applyAggregation(rootPlan, isRootPlan = true)
    case _ => applyAggregation(plan, isRootPlan = false)
  }

  def applyAggregation(plan: LogicalPlan,
      isRootPlan: Boolean): Seq[SparkPlan] = plan match {
    case PhysicalAggregation(groupingExpressions, aggregateExpressions,
    resultExpressions, child) if enableOptimizedAggregation =>

      val (functionsWithDistinct, functionsWithoutDistinct) =
        aggregateExpressions.partition(_.isDistinct)
      if (functionsWithDistinct.map(_.aggregateFunction.children)
          .distinct.length > 1) {
        // This is a sanity check. We should not reach here when we have
        // multiple distinct column sets. The MultipleDistinctRewriter
        // should take care this case.
        sys.error("You hit a query analyzer bug. Please report your query " +
            "to Spark user mailing list.")
      }

      val aggregateOperator =
        if (aggregateExpressions.map(_.aggregateFunction)
            .exists(!_.supportsPartial)) {
          if (functionsWithDistinct.nonEmpty) {
            sys.error("Distinct columns cannot exist in Aggregate " +
                "operator containing aggregate functions which don't " +
                "support partial aggregation.")
          } else {
            aggregate.AggUtils.planAggregateWithoutPartial(
              groupingExpressions,
              aggregateExpressions,
              resultExpressions,
              planLater(child))
          }
        } else if (functionsWithDistinct.isEmpty) {
          planAggregateWithoutDistinct(
            groupingExpressions,
            aggregateExpressions,
            resultExpressions,
            planLater(child),
            isRootPlan)
        } else {
          planAggregateWithOneDistinct(
            groupingExpressions,
            aggregateExpressions,
            functionsWithDistinct,
            functionsWithoutDistinct,
            resultExpressions,
            planLater(child))
        }

      aggregateOperator

    case _ => Nil
  }

  def supportCodegen(aggregateExpressions: Seq[AggregateExpression]): Boolean = {
    // ImperativeAggregate is not supported right now in code generation.
    !aggregateExpressions.exists(_.aggregateFunction
        .isInstanceOf[ImperativeAggregate])
  }

  def planAggregateWithoutDistinct(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan,
      isRootPlan: Boolean): Seq[SparkPlan] = {
    // Check if we can use SnappyHashAggregateExec.

    if (!supportCodegen(aggregateExpressions)) {
      return AggUtils.planAggregateWithoutDistinct(groupingExpressions,
        aggregateExpressions, resultExpressions, child)
    }

    // 1. Create an Aggregate Operator for partial aggregations.

    val groupingAttributes = groupingExpressions.map(_.toAttribute)
    val partialAggregateExpressions = aggregateExpressions.map(_.copy(
      mode = Partial))
    val partialAggregateAttributes = partialAggregateExpressions.flatMap(
      _.aggregateFunction.aggBufferAttributes)
    val partialResultExpressions = groupingAttributes ++
        partialAggregateExpressions.flatMap(_.aggregateFunction
            .inputAggBufferAttributes)

    val partialAggregate = SnappyHashAggregateExec(
      requiredChildDistributionExpressions = None,
      groupingExpressions = groupingExpressions,
      aggregateExpressions = partialAggregateExpressions,
      aggregateAttributes = partialAggregateAttributes,
      initialInputBufferOffset = 0,
      __resultExpressions = partialResultExpressions,
      child = child)

    // 2. Create an Aggregate Operator for final aggregations.
    val finalAggregateExpressions = aggregateExpressions.map(_.copy(
      mode = Final))
    // The attributes of the final aggregation buffer, which is presented
    // as input to the result projection:
    val finalAggregateAttributes = finalAggregateExpressions.map(
      _.resultAttribute)

    val finalHashAggregate = SnappyHashAggregateExec(
      requiredChildDistributionExpressions = Some(groupingAttributes),
      groupingExpressions = groupingAttributes,
      aggregateExpressions = finalAggregateExpressions,
      aggregateAttributes = finalAggregateAttributes,
      initialInputBufferOffset = groupingExpressions.length,
      __resultExpressions = resultExpressions,
      child = partialAggregate)

    val finalAggregate = if (isRootPlan) {
      // Special CollectAggregateExec plan for top-level simple aggregations
      // which can be performed on the driver itself rather than an exchange.
      CollectAggregateExec(basePlan = finalHashAggregate,
        child = partialAggregate)
    } else finalHashAggregate
    finalAggregate :: Nil
  }

  def planAggregateWithOneDistinct(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      functionsWithDistinct: Seq[AggregateExpression],
      functionsWithoutDistinct: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): Seq[SparkPlan] = {
    // Check if we can use SnappyHashAggregateExec.

    if (!supportCodegen(aggregateExpressions)) {
      return AggUtils.planAggregateWithoutDistinct(groupingExpressions,
        aggregateExpressions, resultExpressions, child)
    }

    // functionsWithDistinct is guaranteed to be non-empty. Even though it
    // may contain more than one DISTINCT aggregate function, all of those
    // functions will have the same column expressions.
    // For example, it would be valid for functionsWithDistinct to be
    // [COUNT(DISTINCT foo), MAX(DISTINCT foo)], but [COUNT(DISTINCT bar),
    // COUNT(DISTINCT foo)] is disallowed because those two distinct
    // aggregates have different column expressions.
    val distinctExpressions = functionsWithDistinct.head
        .aggregateFunction.children
    val namedDistinctExpressions = distinctExpressions.map {
      case ne: NamedExpression => ne
      case other => Alias(other, other.toString)()
    }
    val distinctAttributes = namedDistinctExpressions.map(_.toAttribute)
    val groupingAttributes = groupingExpressions.map(_.toAttribute)

    // 1. Create an Aggregate Operator for partial aggregations.
    val partialAggregate: SparkPlan = {
      val aggregateExpressions = functionsWithoutDistinct.map(_.copy(
        mode = Partial))
      val aggregateAttributes = aggregateExpressions.map(_.resultAttribute)
      // We will group by the original grouping expression, plus an
      // additional expression for the DISTINCT column. For example, for
      // AVG(DISTINCT value) GROUP BY key, the grouping expressions
      // will be [key, value].
      SnappyHashAggregateExec(
        requiredChildDistributionExpressions = None,
        groupingExpressions = groupingExpressions ++ namedDistinctExpressions,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = 0,
        __resultExpressions = groupingAttributes ++ distinctAttributes ++
            aggregateExpressions.flatMap(_.aggregateFunction
                .inputAggBufferAttributes),
        child = child)
    }

    // 2. Create an Aggregate Operator for partial merge aggregations.
    val partialMergeAggregate: SparkPlan = {
      val aggregateExpressions = functionsWithoutDistinct.map(_.copy(
        mode = PartialMerge))
      val aggregateAttributes = aggregateExpressions.map(_.resultAttribute)
      SnappyHashAggregateExec(
        requiredChildDistributionExpressions =
            Some(groupingAttributes ++ distinctAttributes),
        groupingExpressions = groupingAttributes ++ distinctAttributes,
        aggregateExpressions = aggregateExpressions,
        aggregateAttributes = aggregateAttributes,
        initialInputBufferOffset = (groupingAttributes ++
            distinctAttributes).length,
        __resultExpressions = groupingAttributes ++ distinctAttributes ++
            aggregateExpressions.flatMap(_.aggregateFunction
                .inputAggBufferAttributes),
        child = partialAggregate)
    }

    // 3. Create an Aggregate operator for partial aggregation (for distinct)
    val distinctColumnAttributeLookup = distinctExpressions.zip(
      distinctAttributes).toMap
    val rewrittenDistinctFunctions = functionsWithDistinct.map {
      // Children of an AggregateFunction with DISTINCT keyword has already
      // been evaluated. At here, we need to replace original children
      // to AttributeReferences.
      case agg@AggregateExpression(aggregateFunction, mode, true, _) =>
        aggregateFunction.transformDown(distinctColumnAttributeLookup)
            .asInstanceOf[AggregateFunction]
    }

    val partialDistinctAggregate: SparkPlan = {
      val mergeAggregateExpressions = functionsWithoutDistinct.map(_.copy(
        mode = PartialMerge))
      // The attributes of the final aggregation buffer, which is presented
      // as input to the result projection:
      val mergeAggregateAttributes = mergeAggregateExpressions.map(
        _.resultAttribute)
      val (distinctAggregateExpressions, distinctAggregateAttributes) =
        rewrittenDistinctFunctions.zipWithIndex.map { case (func, i) =>
          // We rewrite the aggregate function to a non-distinct
          // aggregation because its input will have distinct arguments.
          // We just keep the isDistinct setting to true, so when users
          // look at the query plan, they still can see distinct aggregations.
          val expr = AggregateExpression(func, Partial, isDistinct = true)
          // Use original AggregationFunction to lookup attributes,
          // which is used to build aggregateFunctionToAttribute.
          val attr = functionsWithDistinct(i).resultAttribute
          (expr, attr)
        }.unzip

      val partialAggregateResult = groupingAttributes ++
          mergeAggregateExpressions.flatMap(_.aggregateFunction
              .inputAggBufferAttributes) ++
          distinctAggregateExpressions.flatMap(_.aggregateFunction
              .inputAggBufferAttributes)
      SnappyHashAggregateExec(
        requiredChildDistributionExpressions = None,
        groupingExpressions = groupingAttributes,
        aggregateExpressions = mergeAggregateExpressions ++
            distinctAggregateExpressions,
        aggregateAttributes = mergeAggregateAttributes ++
            distinctAggregateAttributes,
        initialInputBufferOffset = (groupingAttributes ++
            distinctAttributes).length,
        __resultExpressions = partialAggregateResult,
        child = partialMergeAggregate)
    }

    // 4. Create an Aggregate Operator for the final aggregation.
    val finalAndCompleteAggregate: SparkPlan = {
      val finalAggregateExpressions = functionsWithoutDistinct.map(_.copy(
        mode = Final))
      // The attributes of the final aggregation buffer, which is presented
      // as input to the result projection:
      val finalAggregateAttributes = finalAggregateExpressions.map(
        _.resultAttribute)

      val (distinctAggregateExpressions, distinctAggregateAttributes) =
        rewrittenDistinctFunctions.zipWithIndex.map { case (func, i) =>
          // We rewrite the aggregate function to a non-distinct
          // aggregation because its input will have distinct arguments.
          // We just keep the isDistinct setting to true, so when users
          // look at the query plan, they still can see distinct aggregations.
          val expr = AggregateExpression(func, Final, isDistinct = true)
          // Use original AggregationFunction to lookup attributes,
          // which is used to build aggregateFunctionToAttribute.
          val attr = functionsWithDistinct(i).resultAttribute
          (expr, attr)
        }.unzip

      SnappyHashAggregateExec(
        requiredChildDistributionExpressions = Some(groupingAttributes),
        groupingExpressions = groupingAttributes,
        aggregateExpressions = finalAggregateExpressions ++
            distinctAggregateExpressions,
        aggregateAttributes = finalAggregateAttributes ++
            distinctAggregateAttributes,
        initialInputBufferOffset = groupingAttributes.length,
        __resultExpressions = resultExpressions,
        child = partialDistinctAggregate)
    }

    finalAndCompleteAggregate :: Nil
  }
}
