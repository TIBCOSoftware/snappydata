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

import org.apache.spark.sql.JoinStrategy._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Complete, Final, ImperativeAggregate, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeSet, Expression, NamedExpression, RowOrdering}
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, PhysicalAggregation, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.catalyst.plans.physical.ClusteredDistribution
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{AggUtils, CollectAggregateExec, SnappyHashAggregateExec}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, Exchange}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight}
import org.apache.spark.sql.internal.{DefaultPlanner, SQLConf}
import org.apache.spark.sql.sources.IndexableRelation
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
      case WindowLogicalPlan(_, _, child, _) => throw new AnalysisException(
        s"Unexpected child $child for WindowLogicalPlan")
      case _ => Nil
    }
  }

  object LocalJoinStrategies extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition,
      left, right) if canBuildRight(joinType) && canLocalJoin(right) =>
        makeLocalHashJoin(leftKeys, rightKeys, left, right, condition,
          joinType, joins.BuildRight, replicatedTableJoin = true)
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition,
      left, right) if canBuildLeft(joinType) && canLocalJoin(left) =>
        makeLocalHashJoin(leftKeys, rightKeys, left, right, condition,
          joinType, joins.BuildLeft, replicatedTableJoin = true)

      // check for collocated joins before going for broadcast
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if isCollocatedJoin(joinType, left, leftKeys, right, rightKeys) =>
        val buildLeft = canBuildLeft(joinType) && canBuildLocalHashMap(left, conf)
        if (buildLeft && left.statistics.sizeInBytes < right.statistics.sizeInBytes) {
          makeLocalHashJoin(leftKeys, rightKeys, left, right, condition,
            joinType, joins.BuildLeft, replicatedTableJoin = false)
        } else if (canBuildRight(joinType) && canBuildLocalHashMap(right, conf)) {
          makeLocalHashJoin(leftKeys, rightKeys, left, right, condition,
            joinType, joins.BuildRight, replicatedTableJoin = false)
        } else if (buildLeft) {
          makeLocalHashJoin(leftKeys, rightKeys, left, right, condition,
            joinType, joins.BuildLeft, replicatedTableJoin = false)
        } else if (RowOrdering.isOrderable(leftKeys)) {
          joins.SortMergeJoinExec(leftKeys, rightKeys, joinType, condition,
            planLater(left), planLater(right)) :: Nil
        } else Nil

      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if canBuildRight(joinType) && canBroadcast(right, conf) =>
        if (skipBroadcastRight(joinType, left, right, conf)) {
          Seq(joins.BroadcastHashJoinExec(leftKeys, rightKeys, joinType,
            BuildLeft, condition, planLater(left), planLater(right)))
        } else {
          Seq(joins.BroadcastHashJoinExec(leftKeys, rightKeys, joinType,
            BuildRight, condition, planLater(left), planLater(right)))
        }
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if canBuildLeft(joinType) && canBroadcast(left, conf) =>
        Seq(joins.BroadcastHashJoinExec(leftKeys, rightKeys, joinType,
          BuildLeft, condition, planLater(left), planLater(right)))

      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if canBuildRight(joinType) && canBuildLocalHashMap(right, conf) ||
            !RowOrdering.isOrderable(leftKeys) =>
        if (canBuildLeft(joinType) && canBuildLocalHashMap(left, conf) &&
            left.statistics.sizeInBytes < right.statistics.sizeInBytes) {
          makeLocalHashJoin(leftKeys, rightKeys, left, right, condition,
            joinType, joins.BuildLeft, replicatedTableJoin = false)
        } else {
          makeLocalHashJoin(leftKeys, rightKeys, left, right, condition,
            joinType, joins.BuildRight, replicatedTableJoin = false)
        }
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if canBuildLeft(joinType) && canBuildLocalHashMap(left, conf) ||
            !RowOrdering.isOrderable(leftKeys) =>
        makeLocalHashJoin(leftKeys, rightKeys, left, right, condition,
          joinType, joins.BuildLeft, replicatedTableJoin = false)

      case _ => Nil
    }

    private def getCollocatedPartitioning(joinType: JoinType,
        leftPlan: LogicalPlan, leftKeys: Seq[Expression],
        rightPlan: LogicalPlan, rightKeys: Seq[Expression],
        checkBroadcastJoin: Boolean): (Seq[Expression], Seq[Int], Int) = {

      def getKeyOrder(joinKeys: Seq[Expression],
          partitioning: Seq[Expression]): Seq[Int] = {
        val keyOrder = joinKeys.map { k =>
          val i = partitioning.indexWhere(_.semanticEquals(k))
          if (i < 0) return Nil
          i
        }
        keyOrder
      }

      def getCompatiblePartitioning(plan: LogicalPlan,
          joinKeys: Seq[Expression]): (Seq[Expression], Seq[Int], Int) = plan match {
        case PhysicalOperation(_, _, r@LogicalRelation(
        scan: PartitionedDataSourceScan, _, _)) =>
          // send back numPartitions=1 for replicated table since collocated
          if (scan.numBuckets == 1) return (Nil, Nil, 1)

          val partCols = scan.partitionColumns.map(colName =>
            r.resolveQuoted(colName, self.snappySession.sessionState.analyzer.resolver)
                .getOrElse(throw new AnalysisException(
                  s"""Cannot resolve column "$colName" among (${r.output})""")))
          // check if join keys match (or are subset of) partitioning columns
          val keyOrder = getKeyOrder(joinKeys, partCols)
          if (keyOrder.nonEmpty) (partCols, keyOrder, scan.numBuckets)
          // return partitioning in any case when checking for broadcast
          else if (checkBroadcastJoin) (partCols, Nil, scan.numBuckets)
          else (Nil, Nil, -1)

        case PhysicalOperation(_, _, ExtractEquiJoinKeys(jType, lKeys, rKeys,
        _, left, right)) =>
          // If join is a result of collocated join, then the result can
          // also be a collocated join with other tables if compatible
          // Also the result of a broadcast join will be partitioned
          // on the other table, so allow collocation for the result.
          // Below will return partitioning columns of the result but the key
          // order of those in its join are not useful, rather need to determine
          // the key order as passed to the method.
          val (cols, _, numPartitions) = getCollocatedPartitioning(
            jType, left, lKeys, right, rKeys, checkBroadcastJoin = true)
          // check if the partitioning of the result is compatible with current
          val keyOrder = getKeyOrder(joinKeys, cols)
          if (keyOrder.nonEmpty) (cols, keyOrder, numPartitions)
          else (Nil, Nil, -1)

        case _ => (Nil, Nil, -1)
      }

      val (leftCols, leftKeyOrder, leftNumPartitions) = getCompatiblePartitioning(
        leftPlan, leftKeys)
      val (rightCols, rightKeyOrder, rightNumPartitions) = getCompatiblePartitioning(
        rightPlan, rightKeys)
      if (leftKeyOrder.nonEmpty && leftNumPartitions == rightNumPartitions &&
          leftKeyOrder == rightKeyOrder) {
        (leftCols, leftKeyOrder, leftNumPartitions)
      } else if (leftNumPartitions == 1) {
        // replicate table is always collocated (used by recursive call for Join)
        (rightCols, rightKeyOrder, rightNumPartitions)
      } else if (rightNumPartitions == 1) {
        // replicate table is always collocated (used by recursive call for Join)
        (leftCols, leftKeyOrder, leftNumPartitions)
      } else {
        if (checkBroadcastJoin) {
          // check if one of the sides will be broadcast, then in that case
          // indicate that collocation is still possible for joins further down
          if (canBuildRight(joinType) && canBroadcast(rightPlan, conf)) {
            if (skipBroadcastRight(joinType, leftPlan, rightPlan, conf)) {
              // resulting partitioning will be of the side not broadcast
              (rightCols, rightKeyOrder, rightNumPartitions)
            } else {
              // resulting partitioning will be of the side not broadcast
              (leftCols, leftKeyOrder, leftNumPartitions)
            }
          } else if (canBuildLeft(joinType) && canBroadcast(leftPlan, conf)) {
            // resulting partitioning will be of the side not broadcast
            (rightCols, rightKeyOrder, rightNumPartitions)
          } else (Nil, Nil, -1)
        } else (Nil, Nil, -1)
      }
    }

    private def isCollocatedJoin(joinType: JoinType, leftPlan: LogicalPlan,
        leftKeys: Seq[Expression], rightPlan: LogicalPlan,
        rightKeys: Seq[Expression]): Boolean = {
      getCollocatedPartitioning(joinType, leftPlan, leftKeys,
        rightPlan, rightKeys, checkBroadcastJoin = false)._2.nonEmpty
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
      // if there is an index on the current stream side then use
      // IndexJoin which will use index instead of iterator
      val (indexScan, indexSide, indexProjects, indexFilters) = side match {
        case joins.BuildRight if canBuildLeft(joinType) => left match {
          case PhysicalOperation(projects, filters,
          lr@LogicalRelation(indexed: IndexableRelation, _, _)) =>
            val indexColumns = rightKeys.collect {
              case a: Alias if a.child.isInstanceOf[NamedExpression] =>
                a.child.asInstanceOf[NamedExpression].name
              case ne: NamedExpression => ne.name
            }
            if (indexColumns.isEmpty) (None, side, Stream.empty, Stream.empty)
            else {
              val requiredColumns = (AttributeSet(projects) ++
                  AttributeSet(filters)).map(lr.attributeMap).toSeq
              indexed.getIndexScan(indexColumns, requiredColumns) match {
                case None => (None, side, Stream.empty, Stream.empty)
                case s@Some(_) => (s, joins.BuildLeft, projects, filters)
              }
            }
        }
        case joins.BuildLeft if canBuildRight(joinType) => right match {
          case PhysicalOperation(projects, filters,
          lr@LogicalRelation(index: IndexableRelation, _, _)) =>
            val indexColumns = leftKeys.collect {
              case a: Alias if a.child.isInstanceOf[NamedExpression] =>
                a.child.asInstanceOf[NamedExpression].name
              case ne: NamedExpression => ne.name
            }
            if (indexColumns.isEmpty) (None, side, Stream.empty, Stream.empty)
            else {
              val requiredColumns = (AttributeSet(projects) ++
                  AttributeSet(filters)).map(lr.attributeMap).toSeq
              index.getIndexScan(indexColumns, requiredColumns) match {
                case None => (None, side, Stream.empty, Stream.empty)
                case s@Some(_) => (s, joins.BuildRight, projects, filters)
              }
            }
          case _ => (None, side, Stream.empty, Stream.empty)
        }
        case _ => (None, side, Stream.empty, Stream.empty)
      }
      indexScan match {
        case None =>
          joins.LocalJoin(leftKeys, rightKeys, side, condition,
            joinType, planLater(left), planLater(right),
            left.statistics.sizeInBytes, right.statistics.sizeInBytes,
            replicatedTableJoin) :: Nil
        case Some(scan) =>
          new joins.IndexJoin(leftKeys, rightKeys, side, condition,
            joinType, planLater(left), planLater(right),
            left.statistics.sizeInBytes, right.statistics.sizeInBytes,
            replicatedTableJoin, scan,
            indexSide, indexProjects, indexFilters) :: Nil
      }
    }
  }
}

private[sql] object JoinStrategy {

  def skipBroadcastRight(joinType: JoinType, left: LogicalPlan,
      right: LogicalPlan, conf: SQLConf): Boolean = {
    canBuildLeft(joinType) && canBroadcast(left, conf) &&
        left.statistics.sizeInBytes < right.statistics.sizeInBytes
  }

  /**
   * Matches a plan whose output should be small enough to be used in broadcast join.
   */
  def canBroadcast(plan: LogicalPlan, conf: SQLConf): Boolean = {
    plan.statistics.isBroadcastable ||
        plan.statistics.sizeInBytes <= conf.autoBroadcastJoinThreshold
  }

  /**
   * Matches a plan whose size is small enough to build a hash table.
   */
  def canBuildLocalHashMap(plan: LogicalPlan, conf: SQLConf): Boolean = {
    plan.statistics.sizeInBytes <=
        io.snappydata.Property.HashJoinSize.get(conf)
  }

  def isLocalJoin(plan: LogicalPlan): Boolean = plan match {
    case ExtractEquiJoinKeys(joinType, _, _, _, left, right) =>
      (canBuildRight(joinType) && canLocalJoin(right)) ||
          (canBuildLeft(joinType) && canLocalJoin(left))
    case _ => false
  }

  def canLocalJoin(plan: LogicalPlan): Boolean = {
    plan match {
      case PhysicalOperation(_, _, LogicalRelation(
      t: PartitionedDataSourceScan, _, _)) =>
        t.numBuckets == 1
      case PhysicalOperation(_, _, Join(left, right, _, _)) =>
        // If join is a result of join of replicated tables, this
        // join result should also be a local join with any other table
        canLocalJoin(left) && canLocalJoin(right)
      case PhysicalOperation(_, _, node) if node.children.size == 1 =>
        canLocalJoin(node.children.head)

      case _ => false
    }
  }

  def canBuildRight(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftOuter | LeftSemi | LeftAnti => true
    case _: ExistenceJoin => true
    case _ => false
  }

  def canBuildLeft(joinType: JoinType): Boolean = joinType match {
    case Inner | RightOuter => true
    case _ => false
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
      __resultExpressions = partialResultExpressions,
      child = child,
      hasDistinct = false)

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
      __resultExpressions = resultExpressions,
      child = partialAggregate,
      hasDistinct = false)

    val finalAggregate = if (isRootPlan && groupingAttributes.isEmpty) {
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
      return AggUtils.planAggregateWithOneDistinct(groupingExpressions,
        functionsWithDistinct, functionsWithoutDistinct,
        resultExpressions, child)
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
        __resultExpressions = groupingAttributes ++ distinctAttributes ++
            aggregateExpressions.flatMap(_.aggregateFunction
                .inputAggBufferAttributes),
        child = child,
        hasDistinct = true)
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
        __resultExpressions = groupingAttributes ++ distinctAttributes ++
            aggregateExpressions.flatMap(_.aggregateFunction
                .inputAggBufferAttributes),
        child = partialAggregate,
        hasDistinct = true)
    }

    // 3. Create an Aggregate operator for partial aggregation (for distinct)
    val distinctColumnAttributeLookup = distinctExpressions.zip(
      distinctAttributes).toMap
    val rewrittenDistinctFunctions = functionsWithDistinct.map {
      // Children of an AggregateFunction with DISTINCT keyword has already
      // been evaluated. At here, we need to replace original children
      // to AttributeReferences.
      case AggregateExpression(aggregateFunction, _, true, _) =>
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
        __resultExpressions = partialAggregateResult,
        child = partialMergeAggregate,
        hasDistinct = true)
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
        __resultExpressions = resultExpressions,
        child = partialDistinctAggregate,
        hasDistinct = true)
    }

    finalAndCompleteAggregate :: Nil
  }
}

/**
 * Rule to collapse the partial and final aggregates if the grouping keys
 * match or are superset of the child distribution.
 */
case class CollapseCollocatedPlans(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    // collapse aggregates including removal of exchange completely if possible
    case agg@SnappyHashAggregateExec(Some(groupingAttributes), _,
    finalAggregateExpressions, _, resultExpressions, child, false)
      if groupingAttributes.nonEmpty &&
          // not for child classes (like AQP extensions)
          agg.getClass == classOf[SnappyHashAggregateExec] &&
          // also skip for bootstrap which depends on partial+final
          !agg.output.exists(_.name.contains("bootstrap")) =>
      val partialAggregate = child match {
        case s: SnappyHashAggregateExec => s
        case e: Exchange => e.child.asInstanceOf[SnappyHashAggregateExec]
        case o => throw new IllegalStateException(
          s"unexpected child ${o.simpleString} of ${agg.simpleString}")
      }
      val partitioning = partialAggregate.child.outputPartitioning
      val numColumns = Utils.getNumColumns(partitioning)
      val satisfied = if (groupingAttributes.length == numColumns) {
        if (partitioning.satisfies(ClusteredDistribution(groupingAttributes))) {
          Some(groupingAttributes)
        } else None
      } else if (numColumns > 0) {
        groupingAttributes.combinations(numColumns).find(p =>
          partitioning.satisfies(ClusteredDistribution(p)))
      } else None
      satisfied match {
        case distributionKeys: Some[_] =>
          val completeAggregateExpressions = finalAggregateExpressions
              .map(_.copy(mode = Complete))
          val completeAggregateAttributes = completeAggregateExpressions
              .map(_.resultAttribute)
          // apply EnsureRequirements just to be doubly sure since this rule is
          // applied after EnsureRequirements when outputPartitioning is final
          EnsureRequirements(session.sessionState.conf)(SnappyHashAggregateExec(
            requiredChildDistributionExpressions = distributionKeys,
            groupingExpressions = partialAggregate.groupingExpressions,
            aggregateExpressions = completeAggregateExpressions,
            aggregateAttributes = completeAggregateAttributes,
            __resultExpressions = resultExpressions,
            child = partialAggregate.child,
            hasDistinct = false))

        case _ => agg
      }
  }
}
