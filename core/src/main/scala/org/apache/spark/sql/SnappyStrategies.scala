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
package org.apache.spark.sql

import java.sql.SQLWarning

import scala.util.control.NonFatal

import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import io.snappydata.{Constant, Property, QueryHint}

import org.apache.spark.sql.JoinStrategy._
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Complete, Final, ImperativeAggregate, Partial, PartialMerge}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, Literal, NamedExpression, RowOrdering}
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, PhysicalAggregation}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, HashPartitioning}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, Inner, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.{AggUtils, CollectAggregateExec, SnappyHashAggregateExec}
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.exchange.{EnsureRequirements, Exchange, ShuffleExchange}
import org.apache.spark.sql.execution.sources.PhysicalScan
import org.apache.spark.sql.hive.SnappySessionState
import org.apache.spark.sql.internal.{JoinQueryPlanning, LogicalPlanWithHints, SQLConf}
import org.apache.spark.sql.sources.SamplingRelation
import org.apache.spark.sql.streaming._

/**
 * This trait is an extension to SparkPlanner and introduces number of
 * enhancements specific to SnappyData.
 */
private[sql] trait SnappyStrategies {

  self: SnappySessionState =>

  object SnappyStrategies extends Strategy {

    def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      sampleSnappyCase(plan)
    }
  }

  def isDisabled: Boolean = disableStoreOptimizations

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

  object HashJoinStrategies extends Strategy with JoinQueryPlanning {

    /** Try to apply a given join hint. Returns Nil if apply failed else the resulting plan. */
    private def applyJoinHint(joinHint: String, joinType: JoinType, leftKeys: Seq[Expression],
        rightKeys: Seq[Expression], condition: Option[Expression],
        left: LogicalPlan, right: LogicalPlan, buildSide: joins.BuildSide,
        buildPlan: LogicalPlan, canBuild: JoinType => Boolean): Seq[SparkPlan] = joinHint match {
      case Constant.JOIN_TYPE_HASH =>
        if (canBuild(joinType)) {
          // don't hash join beyond 10GB estimated size because that is likely a mistake
          val buildSize = buildPlan.statistics.sizeInBytes
          if (buildSize > math.max(JoinStrategy.getMaxHashJoinSize(conf),
            10L * 1024L * 1024L * 1024L)) {
            snappySession.addWarning(new SQLWarning(s"Plan hint ${QueryHint.JoinType}=" +
                s"$joinHint for ${right.simpleString} skipped for ${joinType.sql} " +
                s"JOIN on columns=$rightKeys due to large estimated buildSize=$buildSize. " +
                s"Increase session property ${Property.HashJoinSize.name} to force.",
              SQLState.LANG_INVALID_JOIN_STRATEGY))
            return Nil
          }
          makeLocalHashJoin(leftKeys, rightKeys, left, right, condition, joinType,
            buildSide, replicatedTableJoin = allowsReplicatedJoin(buildPlan))
        } else Nil
      case Constant.JOIN_TYPE_BROADCAST =>
        if (canBuild(joinType)) {
          // don't broadcast beyond 1GB estimated size because that is likely a mistake
          val buildSize = buildPlan.statistics.sizeInBytes
          if (buildSize > math.max(conf.autoBroadcastJoinThreshold, 1L * 1024L * 1024L * 1024L)) {
            snappySession.addWarning(new SQLWarning(s"Plan hint ${QueryHint.JoinType}=" +
                s"$joinHint for ${right.simpleString} skipped for ${joinType.sql} " +
                s"JOIN on columns=$rightKeys due to large estimated buildSize=$buildSize. " +
                s"Increase session property ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key} to force.",
              SQLState.LANG_INVALID_JOIN_STRATEGY))
            return Nil
          }
          joins.BroadcastHashJoinExec(leftKeys, rightKeys, joinType,
            buildSide, condition, planLater(left), planLater(right)) :: Nil
        } else Nil
      case Constant.JOIN_TYPE_SORT =>
        if (RowOrdering.isOrderable(leftKeys)) {
          new joins.SnappySortMergeJoinExec(leftKeys, rightKeys, joinType, condition,
            planLater(left), planLater(right), left.statistics.sizeInBytes,
            right.statistics.sizeInBytes) :: Nil
        } else Nil
      case _ => throw new ParseException(s"Unknown joinType hint '$joinHint'. " +
          s"Expected one of ${Constant.ALLOWED_JOIN_TYPE_HINTS}")
    }

    def apply(plan: LogicalPlan): Seq[SparkPlan] =
      if (isDisabled || snappySession.disableHashJoin) {
        Nil
      } else {
        plan match {
          case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right) =>
            // check for explicit hints first and whether it is possible to apply them
            val rightHint = JoinStrategy.getJoinHint(right)
            rightHint match {
              case None =>
              case Some(joinHint) =>
                applyJoinHint(joinHint, joinType, leftKeys, rightKeys, condition,
                  left, right, joins.BuildRight, right, canBuildRight(right)) match {
                  case Nil => snappySession.addWarning(new SQLWarning(s"Plan hint " +
                      s"${QueryHint.JoinType}=$joinHint for ${right.simpleString} cannot be " +
                      s"applied for ${joinType.sql} JOIN on columns=$rightKeys. " +
                      s"Will try on the other side of join: " +
                      s"${left.simpleString}.", SQLState.LANG_INVALID_JOIN_STRATEGY))
                  case result => return result
                }
            }
            (if (rightHint.isEmpty) JoinStrategy.getJoinHint(left) else rightHint) match {
              case None =>
              case Some(joinHint) =>
                applyJoinHint(joinHint, joinType, leftKeys, rightKeys, condition,
                  left, right, joins.BuildLeft, left, canBuildLeft(left)) match {
                  case Nil => snappySession.addWarning(new SQLWarning(s"Plan hint " +
                      s"${QueryHint.JoinType}=$joinHint for ${left.simpleString} cannot be " +
                      s"applied for ${joinType.sql} " +
                      s"JOIN on columns=$leftKeys", SQLState.LANG_INVALID_JOIN_STRATEGY))
                  case result => return result
                }
            }

            // check for hash join with replicated table first
            if (canBuildRight(right)(joinType) && allowsReplicatedJoin(right)) {
              makeLocalHashJoin(leftKeys, rightKeys, left, right, condition,
                joinType, joins.BuildRight, replicatedTableJoin = true)
            } else if (canBuildLeft(left)(joinType) && allowsReplicatedJoin(left)) {
              makeLocalHashJoin(leftKeys, rightKeys, left, right, condition,
                joinType, joins.BuildLeft, replicatedTableJoin = true)
            }
            // check for collocated joins before going for broadcast
            else if (isCollocatedJoin(joinType, left, leftKeys, right, rightKeys)) {
              val buildLeft = canBuildLeft(left)(joinType) && canBuildLocalHashMap(left, conf)
              if (buildLeft && left.statistics.sizeInBytes < right.statistics.sizeInBytes) {
                makeLocalHashJoin(leftKeys, rightKeys, left, right, condition,
                  joinType, joins.BuildLeft, replicatedTableJoin = false)
              } else if (canBuildRight(right)(joinType) && canBuildLocalHashMap(right, conf)) {
                makeLocalHashJoin(leftKeys, rightKeys, left, right, condition,
                  joinType, joins.BuildRight, replicatedTableJoin = false)
              } else if (buildLeft) {
                makeLocalHashJoin(leftKeys, rightKeys, left, right, condition,
                  joinType, joins.BuildLeft, replicatedTableJoin = false)
              } else if (RowOrdering.isOrderable(leftKeys)) {
                new joins.SnappySortMergeJoinExec(leftKeys, rightKeys, joinType, condition,
                  planLater(left), planLater(right), left.statistics.sizeInBytes,
                  right.statistics.sizeInBytes) :: Nil
              } else Nil
            }
            // broadcast joins preferred over exchange+local hash join or SMJ
            else if (canBuildRight(right)(joinType) && canBroadcast(right, conf)) {
              if (skipBroadcastRight(joinType, left, right, conf)) {
                joins.BroadcastHashJoinExec(leftKeys, rightKeys, joinType,
                  joins.BuildLeft, condition, planLater(left), planLater(right)) :: Nil
              } else {
                joins.BroadcastHashJoinExec(leftKeys, rightKeys, joinType,
                  joins.BuildRight, condition, planLater(left), planLater(right)) :: Nil
              }
            } else if (canBuildLeft(left)(joinType) && canBroadcast(left, conf)) {
              joins.BroadcastHashJoinExec(leftKeys, rightKeys, joinType,
                joins.BuildLeft, condition, planLater(left), planLater(right)) :: Nil
            }
            // prefer local hash join after exchange over sort merge join if size is small enough
            else if (canBuildRight(right)(joinType) && canBuildLocalHashMap(right, conf) ||
                !RowOrdering.isOrderable(leftKeys)) {
              if (canBuildLeft(left)(joinType) && canBuildLocalHashMap(left, conf) &&
                  left.statistics.sizeInBytes < right.statistics.sizeInBytes) {
                makeLocalHashJoin(leftKeys, rightKeys, left, right, condition,
                  joinType, joins.BuildLeft, replicatedTableJoin = false)
              } else {
                makeLocalHashJoin(leftKeys, rightKeys, left, right, condition,
                  joinType, joins.BuildRight, replicatedTableJoin = false)
              }
            } else if (canBuildLeft(left)(joinType) && canBuildLocalHashMap(left, conf) ||
                !RowOrdering.isOrderable(leftKeys)) {
              makeLocalHashJoin(leftKeys, rightKeys, left, right, condition,
                joinType, joins.BuildLeft, replicatedTableJoin = false)
            } else if (RowOrdering.isOrderable(leftKeys)) {
              new joins.SnappySortMergeJoinExec(leftKeys, rightKeys, joinType, condition,
                planLater(left), planLater(right), left.statistics.sizeInBytes,
                right.statistics.sizeInBytes) :: Nil
            } else Nil

          case _ => Nil
        }
      }

    private def getCollocatedPartitioning(joinType: JoinType,
        leftPlan: LogicalPlan, leftKeys: Seq[Expression],
        rightPlan: LogicalPlan, rightKeys: Seq[Expression],
        checkBroadcastJoin: Boolean): (Seq[NamedExpression], Seq[Int], Int) = {

      def getCompatiblePartitioning(plan: LogicalPlan,
          joinKeys: Seq[Expression]): (Seq[NamedExpression], Seq[Int], Int) = plan match {
        case PhysicalScan(_, _, child) => child match {
          case r@LogicalRelation(scan: PartitionedDataSourceScan, _, _) =>
            // send back numPartitions=1 for replicated table since collocated
            if (!scan.isPartitioned) return (Nil, Nil, 1)

            // use case-insensitive resolution since partitioning columns during
            // creation could be using the same as opposed to during scan
            val partCols = scan.partitionColumns.map(colName =>
              r.resolveQuoted(colName, analysis.caseInsensitiveResolution)
                  .getOrElse(throw new AnalysisException(
                    s"""Cannot resolve column "$colName" among (${r.output})""")))
            // check if join keys match (or are subset of) partitioning columns
            val (keyOrder, joinKeySubsetOfPart) = getKeyOrder(plan, joinKeys, partCols)
            if (joinKeySubsetOfPart) (partCols, keyOrder, scan.numBuckets)
            // return partitioning in any case when checking for broadcast
            else if (checkBroadcastJoin) (partCols, Nil, scan.numBuckets)
            else (Nil, Nil, -1)

          case ExtractEquiJoinKeys(jType, lKeys, rKeys, _, left, right) =>
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
            val (keyOrder, joinKeySubsetOfPart) = getKeyOrder(plan, joinKeys, cols)
            if (joinKeySubsetOfPart) (cols, keyOrder, numPartitions)
            else (Nil, Nil, -1)

          case _ => (Nil, Nil, -1)
        }
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
          if (canBuildRight(rightPlan)(joinType) && canBroadcast(rightPlan, conf)) {
            if (skipBroadcastRight(joinType, leftPlan, rightPlan, conf)) {
              // resulting partitioning will be of the side not broadcast
              (rightCols, rightKeyOrder, rightNumPartitions)
            } else {
              // resulting partitioning will be of the side not broadcast
              (leftCols, leftKeyOrder, leftNumPartitions)
            }
          } else if (canBuildLeft(leftPlan)(joinType) && canBroadcast(leftPlan, conf)) {
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
      joins.HashJoinExec(leftKeys, rightKeys, side, condition,
        joinType, planLater(left), planLater(right),
        left.statistics.sizeInBytes, right.statistics.sizeInBytes,
        replicatedTableJoin) :: Nil
    }
  }

  object SnappyAggregation extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = if (isDisabled) {
      Nil
    } else {
      new SnappyAggregationStrategy(planner).apply(plan)
    }
  }
}

private[sql] object JoinStrategy {

  def skipBroadcastRight(joinType: JoinType, left: LogicalPlan,
      right: LogicalPlan, conf: SQLConf): Boolean = {
    canBuildLeft(left)(joinType) && canBroadcast(left, conf) &&
        left.statistics.sizeInBytes < right.statistics.sizeInBytes
  }

  /**
   * Check for joinType query hint. A return value of Some(hint) indicates the query hint
   * for the join operation, if any, else this returns None.
   */
  private[sql] def getJoinHint(plan: LogicalPlan): Option[String] = plan match {
    case l: LogicalPlanWithHints => l.hints.get(QueryHint.JoinType.toString) match {
      case Some(v) =>
        val specifiedJoinHint = v.toLowerCase()
        if (Constant.ALLOWED_JOIN_TYPE_HINTS.contains(specifiedJoinHint)) {
          Some(specifiedJoinHint)
        } else {
          throw new ParseException(s"Unknown joinType hint '$v'. " +
              s"Expected one of ${Constant.ALLOWED_JOIN_TYPE_HINTS}")
        }
      case None => None
    }
    case _: BroadcastHint => Some(Constant.JOIN_TYPE_BROADCAST)
    case _: Filter | _: Project | _: LocalLimit =>
      getJoinHint(plan.asInstanceOf[UnaryNode].child)
    case _ => None
  }

  /**
   * Matches a plan whose output should be small enough to be used in broadcast join.
   */
  def canBroadcast(plan: LogicalPlan, conf: SQLConf): Boolean = plan.statistics.isBroadcastable ||
        plan.statistics.sizeInBytes <= conf.autoBroadcastJoinThreshold


  def getMaxHashJoinSize(conf: SQLConf): Long = {
    ExternalStoreUtils.sizeAsBytes(Property.HashJoinSize.get(conf),
      Property.HashJoinSize.name, -1, Long.MaxValue)
  }

  /**
   * Matches a plan whose size is small enough to build a hash table.
   */
  def canBuildLocalHashMap(plan: LogicalPlan, conf: SQLConf): Boolean = {
    plan.statistics.sizeInBytes <= getMaxHashJoinSize(conf) &&
      plan.collectFirst {
        case lr: LogicalRelation => lr.relation
      }.map(_ match {
      case sr: SamplingRelation => sr.canBeOnBuildSide
      case _ => true
    }).getOrElse(true)
  }

  def isReplicatedJoin(plan: LogicalPlan): Boolean = plan match {
    case ExtractEquiJoinKeys(joinType, _, _, _, left, right) =>
      (canBuildRight(right)(joinType) && allowsReplicatedJoin(right)) ||
          (canBuildLeft(left)(joinType) && allowsReplicatedJoin(left))
    case _ => false
  }

  def allowsReplicatedJoin(plan: LogicalPlan): Boolean = {
    plan match {
      case PhysicalScan(_, _, child) => child match {
        case LogicalRelation(t: PartitionedDataSourceScan, _, _) => !t.isPartitioned
        /* && (t match {
          case _: SamplingRelation => false
          case _ => true
        }) */
        case _: Filter | _: Project | _: LocalLimit => allowsReplicatedJoin(child.children.head)
        case ExtractEquiJoinKeys(joinType, _, _, _, left, right) =>
          allowsReplicatedJoin(left) && allowsReplicatedJoin(right) &&
              (canBuildLeft(left)(joinType) || canBuildRight(right)(joinType))
        case _ => false
      }
      case _ => false
    }
  }

  def canBuildRight(right: LogicalPlan)(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftOuter | LeftSemi | LeftAnti => right.collectFirst {
       case lr: LogicalRelation => lr.relation
     }.map(_ match {
      case sr: SamplingRelation => sr.canBeOnBuildSide
      case _ => true
     }).getOrElse(true)
   case _: ExistenceJoin => true
   case _ => false
  }

  def canBuildLeft(left: LogicalPlan)(joinType: JoinType ): Boolean = joinType match {
   case Inner | RightOuter => left.collectFirst {
      case lr: LogicalRelation => lr.relation
    }.map(_ match {
      case sr: SamplingRelation => sr.canBeOnBuildSide
      case _ => true
    }).getOrElse(true)
   case _ => false
  }
}

/**
 * Used to plan the aggregate operator for expressions using the optimized
 * SnappyData aggregation operators.
 *
 * Adapted from Spark's Aggregation strategy.
 */
class SnappyAggregationStrategy(planner: SparkPlanner)
    extends Strategy {

  private val maxAggregateInputSize = {
    // if below throws exception then clear the property from conf
    // else every query will fail in planning here (even reset using SQL will fail)
    try {
      ExternalStoreUtils.sizeAsBytes(Property.HashAggregateSize.get(planner.conf),
        Property.HashAggregateSize.name, -1, Long.MaxValue)
    } catch {
      case NonFatal(e) => planner.conf.unsetConf(Property.HashAggregateSize.name); throw e
    }
  }

  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case ReturnAnswer(rootPlan) => applyAggregation(rootPlan, isRootPlan = true)
    case _ => applyAggregation(plan, isRootPlan = false)
  }

  def applyAggregation(plan: LogicalPlan,
      isRootPlan: Boolean): Seq[SparkPlan] = plan match {
    case PhysicalAggregation(groupingExpressions, aggregateExpressions,
    resultExpressions, child) if maxAggregateInputSize == 0 ||
        child.statistics.sizeInBytes <= maxAggregateInputSize =>

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

  def supportsCodegen(aggregateExpressions: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression]): Boolean = {
    planner.conf.wholeStageEnabled &&
    // ImperativeAggregate is not supported in code generation.
    !aggregateExpressions.exists(_.aggregateFunction
        .isInstanceOf[ImperativeAggregate]) &&
    // aggregate and result expressions should be code-generated
    !(aggregateExpressions ++ resultExpressions).exists(_.find(e => !e.isInstanceOf[Literal] &&
        !e.foldable && e.isInstanceOf[CodegenFallback]).nonEmpty)
  }

  def planAggregateWithoutDistinct(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan,
      isRootPlan: Boolean): Seq[SparkPlan] = {

    // Check if we can use SnappyHashAggregateExec.
    if (!supportsCodegen(aggregateExpressions, resultExpressions)) {
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
      CollectAggregateExec(partialAggregate)(finalHashAggregate)
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
    if (!supportsCodegen(aggregateExpressions, resultExpressions)) {
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
 * match or are superset of the child distribution. Also introduces exchange
 * when inserting into a partitioned table if number of partitions don't match.
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

    case t: TableExec =>
      val addShuffle = if (t.partitioned) {
        // force shuffle when inserting into a table with different partitions
        t.child.outputPartitioning.numPartitions != t.outputPartitioning.numPartitions
      } else false
      if (addShuffle) {
        t.withNewChildren(Seq(ShuffleExchange(HashPartitioning(
          t.requiredChildDistribution.head.asInstanceOf[ClusteredDistribution]
              .clustering, t.numBuckets), t.child)))
      } else t
  }
}

/**
 * Rule to insert a helper plan to collect information for other entities
 * like parameterized literals.
 */
case class InsertCachedPlanFallback(session: SnappySession, topLevel: Boolean)
    extends Rule[SparkPlan] {
  private def addFallback(plan: SparkPlan): SparkPlan = {
    // skip fallback plan when optimizations are already disabled,
    // or if the plan is not a top-level one e.g. a subquery or inside
    // CollectAggregateExec (only top-level plan will catch and retry
    //   with disabled optimizations)
    if (!topLevel || session.sessionState.disableStoreOptimizations) plan
    else plan match {
      // TODO: disabled for StreamPlans due to issues but can it require fallback?
      case _: StreamPlan => plan
      case _: CollectAggregateExec => CodegenSparkFallback(plan, session)
      case _ if !Property.TestDisableCodeGenFlag.get(session.sessionState.conf) ||
       session.sessionState.conf.contains("snappydata.connection") =>
        CodegenSparkFallback(plan, session)
      case _ => plan
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = addFallback(plan)
}

/**
 * Plans scalar subqueries like the Spark's PlanSubqueries but uses customized
 * ScalarSubquery to insert a tokenized literal instead of literal value embedded
 * in code to allow generated code re-use and improve performance substantially.
 */
case class TokenizeSubqueries(sparkSession: SparkSession) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    plan.transformAllExpressions {
      case subquery: catalyst.expressions.ScalarSubquery =>
        val executedPlan = new QueryExecution(sparkSession, subquery.plan).executedPlan
        new TokenizedScalarSubquery(SubqueryExec(s"subquery${subquery.exprId.id}",
          executedPlan), subquery.exprId)
      case catalyst.expressions.PredicateSubquery(query, Seq(e: Expression), _, exprId) =>
        val executedPlan = new QueryExecution(sparkSession, query).executedPlan
        InSubquery(e, SubqueryExec(s"subquery${exprId.id}", executedPlan), exprId)
    }
  }
}
