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

package org.apache.spark.sql.sources

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.internal.cache.{AbstractRegion, ColocationHelper, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc

import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.{AnalysisException, SnappySession}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedFunction, UnresolvedGenerator, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, Coalesce, Expression, Literal, PredicateHelper, SubqueryExpression, UnresolvedWindowExpression}
import org.apache.spark.sql.catalyst.optimizer.ReorderJoin
import org.apache.spark.sql.catalyst.planning.ExtractFiltersAndInnerJoins._
import org.apache.spark.sql.catalyst.{expressions, plans}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.columnar.impl.{BaseColumnFormatRelation, ColumnFormatRelation, IndexColumnFormatRelation}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.row.RowFormatRelation
import org.apache.spark.sql.sources.Entity.{INDEX, INDEX_RELATION, TABLE}

object RuleUtils extends PredicateHelper {

  private def getIndex(catalog: SnappyStoreHiveCatalog, name: String) = {
    val relation = catalog.lookupRelation(catalog.newQualifiedTableName(name))
    relation match {
      case LogicalRelation(i: IndexColumnFormatRelation, _, _) => Some(relation)
      case _ => None
    }
  }

  def fetchIndexes(snappySession: SnappySession,
      table: LogicalPlan): Seq[(LogicalPlan, Seq[LogicalPlan])] = table.collect {
    case l@LogicalRelation(p: ParentRelation, _, _) =>
      val catalog = snappySession.sessionCatalog
      (l.asInstanceOf[LogicalPlan], p.getDependents(catalog).flatMap(getIndex(catalog, _)))
  }

  def getJoinKeys(left: LogicalPlan,
      right: LogicalPlan,
      joinConditions: Seq[Expression]): Seq[(Expression, Expression)] = {

    // see caller notes about mixed referenced join keys getting filtered out.
    val joinedRefs = left.outputSet ++ right.outputSet
    val predicates = joinConditions.flatMap(splitConjunctivePredicates).
        filter(_.references.subsetOf(joinedRefs))

    predicates.
        filterNot(_.references.subsetOf(left.outputSet)).
        filterNot(_.references.subsetOf(right.outputSet)).
        flatMap {
          case expressions.EqualTo(l, r) if canEvaluate(l, left) && canEvaluate(r, right) =>
            Some((l, r))
          case expressions.EqualTo(l, r) if canEvaluate(l, right) && canEvaluate(r, left) =>
            Some((r, l))
          // Replace null with default value for joining key, then those rows with null in it could
          // be joined together
          case expressions.EqualNullSafe(l, r) if canEvaluate(l, left) && canEvaluate(r, right) =>
            Some((Coalesce(Seq(l, Literal.default(l.dataType))),
                Coalesce(Seq(r, Literal.default(r.dataType)))))
          case expressions.EqualNullSafe(l, r) if canEvaluate(l, right) && canEvaluate(r, left) =>
            Some((Coalesce(Seq(r, Literal.default(r.dataType))),
                Coalesce(Seq(l, Literal.default(l.dataType)))))
          case other => None
        }
  }

  @tailrec
  def canTraverseLeftToRight(source: Seq[LogicalPlan], target: LogicalPlan,
      replicatedReachablePaths: Seq[List[LogicalPlan]]): Boolean = {

    if (source.isEmpty) {
      return false
    } else if (source.exists(_ == target)) {
      true
    } else if (replicatedReachablePaths.isEmpty) {
      false
    } else {
      var currentReachablePaths = replicatedReachablePaths
      val newChains = source.flatMap { rep1 =>
        val (otherSide, remainingPaths) = currentReachablePaths.foldLeft(
          (Seq.empty[LogicalPlan], currentReachablePaths)) {
          case ((otherKey, current), plan) =>
            plan match {
              case l :: r :: o if o.isEmpty & (l == rep1) =>
                ((otherKey ++ Some(r)), current.filterNot(_ == plan))
              case l :: r :: o if o.isEmpty & (r == rep1) =>
                ((otherKey ++ Some(l)), current.filterNot(_ == plan))
              case _ => ((otherKey, current))
            }
        }

        currentReachablePaths = remainingPaths
        otherSide
      }

      canTraverseLeftToRight(newChains, target, currentReachablePaths)
    }
  }

  protected[sql] def applyDefaultAction[A](entity: (PartialPlan, A), withFilters: Boolean)
      (implicit snappySession: SnappySession, addToDefault: (PartialPlan, A) => PartialPlan):
  PartialPlan = entity match {
    // handles replicated & non-colocated logical plan
    case (finalPlan, table: LogicalPlan)
      if !finalPlan.replaced.contains(table) =>
      val (tableFilters, _) = RuleUtils.partitionBy(table.outputSet, finalPlan.conditions)
      if (tableFilters.isEmpty && withFilters) {
        return finalPlan
      }
      val joinRefs = finalPlan.outputSet ++ table.outputSet
      val (tableJoinConditions, otherJoinConditions) = RuleUtils.partitionBy(joinRefs,
        finalPlan.conditions)
      val pTabOrIndex = RuleUtils.chooseIndexForFilter(table, tableFilters)
      if ((tableJoinConditions.toSet -- tableFilters.toSet).nonEmpty || finalPlan.curPlan == null) {
        val newPlan = finalPlan.copy(
          curPlan = RuleUtils.createJoin(finalPlan.curPlan, pTabOrIndex.map(_.index)
              .getOrElse(table),
            tableJoinConditions),
          replaced = finalPlan.replaced ++ pTabOrIndex,
          outputSet = joinRefs,
          input = finalPlan.input.filterNot(_ == table),
          conditions =
              if (finalPlan.curPlan == null) finalPlan.conditions else otherJoinConditions)
        addToDefault(newPlan, table.asInstanceOf[A])
      } else {
        finalPlan
      }

    // handles colocated with filters replacement
    case (finalPlan, replacement: Replacement)
      if !finalPlan.replaced.contains(replacement) =>
      val (tableFilters, _) = RuleUtils.partitionBy(replacement.table.outputSet,
        finalPlan.conditions)
      if (tableFilters.isEmpty && withFilters) {
        return finalPlan
      }
      val joinRefs = finalPlan.outputSet ++ replacement.table.outputSet
      val (pTabJoinConditions, otherJoinConditions) = RuleUtils.partitionBy(joinRefs,
        finalPlan.conditions)
      assert((pTabJoinConditions.toSet -- tableFilters.toSet).nonEmpty || finalPlan.curPlan == null,
        s"joinConditions ${pTabJoinConditions.mkString(" && ")} " +
            s"filterConditions ${tableFilters.mkString(" && ")}")
      val newPlan = finalPlan.copy(
        curPlan = RuleUtils.createJoin(finalPlan.curPlan, replacement.index, pTabJoinConditions),
        replaced = finalPlan.replaced ++ Some(replacement),
        outputSet = joinRefs,
        input = finalPlan.input.filterNot(_ == replacement.table),
        conditions =
            if (finalPlan.curPlan == null) finalPlan.conditions else otherJoinConditions)
      addToDefault(newPlan, replacement.asInstanceOf[A])
  }

  protected[sql] def createJoin(curPlan: LogicalPlan,
      planToAdd: LogicalPlan, toJoinWith: Seq[Expression]) = if (curPlan == null) {
    planToAdd
  } else {
    assert(toJoinWith.nonEmpty, "We shouldn't favor this in between because it creates cartesian" +
        " product.")
    Join(curPlan, planToAdd, Inner, toJoinWith.reduceLeftOption(expressions.And))
  }

  protected[sql] def partitionBy(allColumns: AttributeSet, expressions: Seq[Expression]):
  (Seq[Expression], Seq[Expression]) = expressions.partition(e =>
    e.references.subsetOf(allColumns) && !SubqueryExpression.hasCorrelatedSubquery(e))

  protected[sql] def returnPlan(partial: PartialPlan) = CompletePlan(ReorderJoin.createOrderedJoin(
    if (partial.curPlan == null) partial.input else Seq(partial.curPlan) ++ partial.input,
    partial.conditions),
    partial.replaced ++ partial.input.map(t => Replacement(t, t)))

  protected[sql] def chooseIndexForFilter(child: LogicalPlan, conditions: Seq[Expression])
      (implicit snappySession: SnappySession) = {

    val columnGroups = conditions.collect {
      case expressions.EqualTo(l, r) => l.collectFirst { case a: AttributeReference => a }.orElse {
        r.collectFirst { case a: AttributeReference => a }
      }
      case expressions.EqualNullSafe(l, r) => l.collectFirst { case a: AttributeReference => a }
          .orElse {
            r.collectFirst { case a: AttributeReference => a }
          }
    }.groupBy(_.map(_.qualifier)).collect { case (table, cols)
      if table.nonEmpty & table.get.nonEmpty => (
        table.get.get,
        cols.collect { case a if a.nonEmpty => a.get })
    }

    val satisfyingPartitionColumns = for {
      (table, indexes) <- RuleUtils.fetchIndexes(snappySession, child)
      filterCols <- columnGroups.collectFirst {
        case (t, predicates) if predicates.nonEmpty =>
          table match {
            case LogicalRelation(b: ColumnFormatRelation, _, _) if b.table.indexOf(t) > 0 =>
              predicates
            case SubqueryAlias(alias, _) if alias.equals(t) =>
              predicates
            case _ => Seq.empty
          }
      } if filterCols.nonEmpty

      matchedIndexes = indexes.collect {
        case idx@LogicalRelation(ir: IndexColumnFormatRelation, _, _)
          if ir.partitionColumns.length <= filterCols.length &
              ir.partitionColumns.forall(p => filterCols.exists(f =>
                f.name.equalsIgnoreCase(p))) =>
          (ir.partitionColumns.length, idx.asInstanceOf[LogicalPlan])
      } if matchedIndexes.nonEmpty

    } yield {
      Replacement(table, matchedIndexes.maxBy(_._1)._2)
    }

    if (satisfyingPartitionColumns.isEmpty) {
      None
    } else {
      Some(satisfyingPartitionColumns.maxBy {
        r => r.index.statistics.sizeInBytes
      })
    }
  }

}

object Entity {
  type TABLE = LogicalPlan
  type INDEX_RELATION = BaseColumnFormatRelation
  type INDEX = LogicalPlan

  def isColocated(left: LogicalPlan, right: LogicalPlan): Boolean = {
    val leftRelation = unwrapBaseColumnRelation(left)
    val rightRelation = unwrapBaseColumnRelation(right)
    if (leftRelation.isEmpty || rightRelation.isEmpty) {
      return false
    }
    val leftRegion = Misc.getRegionForTable(leftRelation.get.resolvedName, true)
    val leftLeader = leftRegion.asInstanceOf[AbstractRegion] match {
      case pr: PartitionedRegion => ColocationHelper.getLeaderRegionName(pr)
    }
    val rightRegion = Misc.getRegionForTable(rightRelation.get.resolvedName, true)
    val rightLeader = rightRegion.asInstanceOf[AbstractRegion] match {
      case pr: PartitionedRegion => ColocationHelper.getLeaderRegionName(pr)
    }
    leftLeader.equals(rightLeader)
  }

  def unwrapBaseColumnRelation(
      plan: LogicalPlan): Option[BaseColumnFormatRelation] = plan collectFirst {
    case LogicalRelation(relation: BaseColumnFormatRelation, _, _) =>
      relation
    case SubqueryAlias(alias, LogicalRelation(relation: BaseColumnFormatRelation, _, _)) =>
      relation
  }

  private def findR(p: Any) = p match {
    case UnresolvedAttribute(_) |
         UnresolvedFunction(_, _, _) |
         UnresolvedAlias(_, _) |
         UnresolvedWindowExpression(_, _) |
         UnresolvedGenerator(_, _) |
         UnresolvedStar(_) => true
    case _ => false
  }

  def hasUnresolvedReferences(plan: LogicalPlan): Boolean = plan.find { l =>
    l.productIterator.exists(findR) || l.expressions.exists(e =>
      findR(e) || e.productIterator.exists(findR) ||
          e.references.exists(findR)
    )
  }.nonEmpty

}

object HasColocatedEntities {

  type ReturnType = (
      Seq[(INDEX_RELATION, INDEX_RELATION)], Seq[ColocatedReplacements]
      )

  def unapply(tables: (LogicalPlan, LogicalPlan))(implicit snappySession: SnappySession):
  Option[ReturnType] = {
    val (left, right) = tables

    /** now doing a one-to-one mapping of lefttable and its indexes with
      * right table and its indexes. Following example explains the combination
      * generator.
      *
      * val l = Seq(1, 2, 3)
      * val r = Seq(4, 5)
      * l.zip(Seq.fill(l.length)(r)).flatMap {
      * case (leftElement, rightList) => rightList.flatMap { e =>
      * Seq((leftElement, e))
      * }
      * }.foreach(println)
      * will output :
      * (1, 4)
      * (1, 5)
      * (2, 4)
      * (2, 5)
      * (3, 4)
      * (3, 5)
      *
      */
    val leftRightEntityMapping = for {
      (leftTable, leftIndexes) <- RuleUtils.fetchIndexes(snappySession, left)
      (rightTable, rightIndexes) <- RuleUtils.fetchIndexes(snappySession, right)
      leftSeq = Seq(leftTable) ++ leftIndexes
      rightSeq = Seq(rightTable) ++ rightIndexes
    } yield {
      leftSeq.zip(Seq.fill(leftSeq.length)(rightSeq)) flatMap {
        case (leftElement, rightList) => rightList.flatMap(e => Seq((leftElement, e)))
      }
    }

    // right now not expecting multiple tables in left & right hand side.
    //      assert(leftRightEntityMapping.size <= 1)

    val mappings = leftRightEntityMapping.flatMap { mappedElements =>
      val (leftTable, rightTable) = mappedElements(0) // first pairing is always (table, table)
      for {
        (leftPlan, rightPlan) <- mappedElements
        leftRelation = Entity.unwrapBaseColumnRelation(leftPlan) if leftRelation.nonEmpty
        rightRelation = Entity.unwrapBaseColumnRelation(rightPlan) if rightRelation.nonEmpty
        if Entity.isColocated(leftPlan, rightPlan)
      } yield {
        val leftReplacement = leftTable match {
          case _: LogicalRelation => Replacement(leftTable, leftPlan)
          case subquery@SubqueryAlias(alias, _) =>
            Replacement(subquery, SubqueryAlias(alias, leftPlan))
        }
        val rightReplacement = rightTable match {
          case _: LogicalRelation => Replacement(rightTable, rightPlan)
          case subquery@SubqueryAlias(alias, _) =>
            Replacement(subquery, SubqueryAlias(alias, rightPlan))
        }
        ((leftRelation.get, rightRelation.get),
            ColocatedReplacements(ArrayBuffer(leftReplacement, rightReplacement)))
      }
    }

    if (mappings.nonEmpty) {
      Some(mappings.unzip)
    } else {
      None
    }
  }

}

/**
  * Table to table or Table to index replacement.
  */
case class Replacement(table: TABLE, index: INDEX) {
  def numPartitioningCols: Int = index match {
    case LogicalRelation(b: BaseColumnFormatRelation, _, _) => b.partitionColumns.length
    case _ => 0
  }

  override def toString: String = (
      table match {
        case LogicalRelation(b: BaseColumnFormatRelation, _, _) => b.table
        case _ => table.toString()
      }) + " ----> " + (
      index match {
        case LogicalRelation(b: BaseColumnFormatRelation, _, _) => b.table
        case LogicalRelation(r: RowFormatRelation, _, _) => r.table
        case _ => index.toString()
      })
}

object Replacement {
  implicit val replacementOrd = Ordering[(Int, BigInt)].reverse.on[Replacement] { r => (
      r.index.asInstanceOf[LogicalRelation].relation.asInstanceOf[BaseColumnFormatRelation]
          .partitionColumns.length, r.index.statistics.sizeInBytes)
  }
}

case class ColocatedReplacements private(chains: ArrayBuffer[Replacement]) {

  override def equals(other: Any): Boolean = other match {
    case cr: ColocatedReplacements if chains.nonEmpty & cr.chains.nonEmpty =>
      Entity.isColocated(chains(0).index, cr.chains(0).index)
    case _ => false
  }

  def merge(current: ColocatedReplacements): ColocatedReplacements = {
    current.chains.foreach { r =>
      chains.find(_.index == r.index) match {
        case None => chains += r
        case _ =>
      }
    }
    chains.sorted
    this
  }

  def numPartitioningColumns: Int = chains.headOption.map(_.numPartitioningCols).getOrElse(0)

  def estimatedSize: BigInt = chains.map(_.index.statistics.sizeInBytes).sum

  override def toString: String = chains.mkString("\n")
}

/**
  * Captures chain of colocated group of table -> index replacements.
  */
object ColocatedReplacements {

  val empty: ColocatedReplacements = new ColocatedReplacements(ArrayBuffer.empty[Replacement])

  implicit val colocatedRepOrd = Ordering[(Int, Int, BigInt)].reverse.on[ColocatedReplacements] {
    rep => (rep.chains.length, rep.numPartitioningColumns, rep.estimatedSize)
  }

}

/**
  * This we have to copy from spark patterns.scala because we want handle single table with
  * filters as well.
  *
  * This will have another advantage later if we decide to move our rule to the last instead of
  * injecting just after ReorderJoin, whereby additional nodes like Project requires handling.
  */
object ExtractFiltersAndInnerJoins extends PredicateHelper {

  // flatten all inner joins, which are next to each other
  def flattenJoin(plan: LogicalPlan):
  (Seq[LogicalPlan], Seq[Expression]) = plan match {
    case Join(left, right, Inner, cond) =>
      val (plans, conditions) = flattenJoin(left)
      (plans ++ Seq(right), conditions ++ cond.toSeq)

    case plans.logical.Filter(filterCondition, j@Join(left, right, Inner, joinCondition)) =>
      val (plans, conditions) = flattenJoin(j)
      (plans, conditions ++ splitConjunctivePredicates(filterCondition))

    case _ => (Seq(plan), Seq())
  }

  def unapply(plan: LogicalPlan):
  // tables, joinConditions, filterConditions
  Option[(Seq[LogicalPlan], Seq[Expression])] = plan match {
    case f@plans.logical.Filter(filterCondition, j@Join(_, _, Inner, _)) =>
      Some(flattenJoin(f))
    case j@Join(_, _, Inner, _) =>
      Some(flattenJoin(j))
    case f@plans.logical.Filter(filterCondition, child) =>
      Some(Seq(child), splitConjunctivePredicates(filterCondition))
    case _ => None
  }
}

trait SubPlan {
  def currentColocatedGroup: ColocatedReplacements =
    throw new AnalysisException("Unexpected call")
}

case class PartialPlan(curPlan: LogicalPlan, replaced: Seq[Replacement], outputSet: AttributeSet,
    input: Seq[LogicalPlan],
    conditions: Seq[Expression],
    colocatedGroups: Seq[ColocatedReplacements],
    partitioned: Seq[LogicalPlan],
    replicates: Seq[LogicalPlan],
    others: Seq[LogicalPlan]) extends SubPlan {
  var curColocatedIndex = 0

  override def currentColocatedGroup: ColocatedReplacements = colocatedGroups(curColocatedIndex)

  override def toString: String = if (curPlan != null) curPlan.toString() else "No Plan yet"

  /**
    * Apply on multiple entities one by one validating common conditions.
    */
  def /:[A](plansToApply: Seq[A])
      (specializedHandling: PartialFunction[(PartialPlan, A), PartialPlan])
      (implicit snappySession: SnappySession): SubPlan = {
    (this /: plansToApply) {
      case (finalPlan, _) if finalPlan.input.isEmpty =>
        finalPlan
      case (finalPlan, _: LogicalPlan) if finalPlan.input.size == 1 =>
        finalPlan // ApplyRest will take care of last table and all filters.
      case (finalPlan, table: LogicalPlan) if finalPlan.replaced.contains(table) =>
        finalPlan
      case (finalPlan, replacement: Replacement) if finalPlan.replaced.contains(replacement) =>
        finalPlan
      case (partial, table) if specializedHandling.isDefinedAt(partial, table) =>
        specializedHandling.lift(partial, table).get
    }
  }

}

case class CompletePlan(plan: LogicalPlan, replaced: Seq[Replacement]) extends SubPlan

