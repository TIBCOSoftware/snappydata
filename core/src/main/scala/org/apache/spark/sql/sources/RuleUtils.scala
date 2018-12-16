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

package org.apache.spark.sql.sources

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.internal.cache.{AbstractRegion, ColocationHelper, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.sql.catalog.CatalogObjectType

import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute, UnresolvedFunction, UnresolvedGenerator, UnresolvedStar}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference, AttributeSet, Coalesce, Expression, Literal, PredicateHelper, SubqueryExpression, UnresolvedWindowExpression}
import org.apache.spark.sql.catalyst.optimizer.ReorderJoin
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.{expressions, plans}
import org.apache.spark.sql.execution.PartitionedDataSourceScan
import org.apache.spark.sql.execution.columnar.impl.{BaseColumnFormatRelation, ColumnFormatRelation, IndexColumnFormatRelation}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.row.RowFormatRelation
import org.apache.spark.sql.internal.SnappySessionCatalog
import org.apache.spark.sql.sources.Entity.{INDEX, INDEX_RELATION, TABLE}
import org.apache.spark.sql.{AnalysisException, SnappySession}

object RuleUtils extends PredicateHelper {

  private def getIndex(catalog: SnappySessionCatalog, table: CatalogTable): Option[INDEX] = {
    val relation = catalog.resolveRelation(table.identifier)
    relation match {
      case LogicalRelation(_: IndexColumnFormatRelation, _, _) => Some(relation)
      case _ => None
    }
  }

  def fetchIndexes(snappySession: SnappySession,
      table: LogicalPlan): Seq[(LogicalPlan, Seq[LogicalPlan])] = {
    val catalog = snappySession.sessionCatalog
    table.collect {
      case l@LogicalRelation(p: PartitionedDataSourceScan, _, _) =>
        val (schemaName, table) = JdbcExtendedUtils.getTableWithSchema(
          p.table, null, Some(snappySession))
        (l.asInstanceOf[LogicalPlan], catalog.externalCatalog.getDependentsFromProperties(
          schemaName, table, includeTypes = CatalogObjectType.Index :: Nil)
            .flatMap(getIndex(catalog, _)))
    }
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
          (Nil.asInstanceOf[Seq[LogicalPlan]], currentReachablePaths)) {
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

  protected[sql] def returnPlan(partial: PartialPlan) = {
    val input = if (partial.curPlan == null) partial.input
    else Seq(partial.curPlan) ++ partial.input
    CompletePlan(ReorderJoin.createOrderedJoin(input.map((_, Inner)),
      partial.conditions), partial.replaced ++ partial.input.map(t => Replacement(t, t)))
  }

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
            case SubqueryAlias(alias, _, _) if alias.equals(t) =>
              predicates
            case _ => Nil
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
    case SubqueryAlias(alias, LogicalRelation(relation: BaseColumnFormatRelation, _, _), _) =>
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


  def replaceAttribute(condition: Expression,
      attributeMapping: Map[Attribute, Attribute]): Expression = {
    condition.transformUp {
      case a: Attribute =>
        attributeMapping.find({
          case (source, _) => source.exprId == a.exprId
        }).map({ case (t, i) => i.withQualifier(t.qualifier) }).getOrElse(a)
    }
  }
}

object HasColocatedEntities {

  type ReturnType = (
      Seq[(INDEX_RELATION, INDEX_RELATION)], Seq[ReplacementSet]
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
          case subquery@SubqueryAlias(alias, _, v) =>
            Replacement(subquery, SubqueryAlias(alias, leftPlan, None))
        }
        val rightReplacement = rightTable match {
          case _: LogicalRelation => Replacement(rightTable, rightPlan)
          case subquery@SubqueryAlias(alias, _, _) =>
            Replacement(subquery, SubqueryAlias(alias, rightPlan, None))
        }
        ((leftRelation.get, rightRelation.get),
            ReplacementSet(ArrayBuffer(leftReplacement, rightReplacement), Nil))
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
case class Replacement(table: TABLE, index: INDEX, isPartitioned: Boolean = true)
    extends PredicateHelper {

  def isReplacable: Boolean = table != index


  val indexAttributes = index.output.collect { case ar: AttributeReference => ar }

  val tableToIndexAttributeMap = AttributeMap(table.output.map {
    case f: AttributeReference =>
      val newA = indexAttributes.find(_.name.equalsIgnoreCase(f.name)).
          getOrElse(throw new IllegalStateException(
            s"Field $f not found in ${indexAttributes}"))
      (f, newA)
    case a => throw new AssertionError(s"UnHandled Attribute ${a} in table" +
        s" ${table.output.mkString(",")}")
  })

  private var _replacedEntity: LogicalPlan = null

  def numPartitioningCols: Int = index match {
    case LogicalRelation(b: BaseColumnFormatRelation, _, _) => b.partitionColumns.length
    case _ => 0
  }

  override def toString: String = {
    "" + (table match {
      case LogicalRelation(b: BaseColumnFormatRelation, _, _) => b.table
      case _ => table.toString()
    }) + " ----> " +
        (index match {
          case LogicalRelation(b: BaseColumnFormatRelation, _, _) => b.table
          case LogicalRelation(r: RowFormatRelation, _, _) => r.table
          case _ => index.toString()
        })
  }

  def mappedConditions(conditions: Seq[Expression]): Seq[Expression] =
    conditions.map(Entity.replaceAttribute(_, tableToIndexAttributeMap))

  protected[sources] def replacedPlan(conditions: Seq[Expression]): LogicalPlan = {
    if (_replacedEntity == null) {
      val tableConditions = conditions.filter(canEvaluate(_, table))
      _replacedEntity = if (tableConditions.isEmpty) {
        index
      } else {
        plans.logical.Filter(mappedConditions(tableConditions).reduce(expressions.And), index)
      }
    }
    _replacedEntity
  }

  def estimatedSize(conditions: Seq[Expression]): BigInt =
    replacedPlan(conditions).statistics.sizeInBytes

}

/**
 * A set of possible replacements of table to indexes.
 * <br>
 * <strong>Note:</strong> The chain if consists of multiple partitioned tables, they must satisfy
 * colocation criteria.
 *
 * @param chain      Multiple replacements.
 * @param conditions user provided join + filter conditions.
 */
case class ReplacementSet(chain: ArrayBuffer[Replacement],
    conditions: Seq[Expression])
    extends Ordered[ReplacementSet] with PredicateHelper {

  lazy val bestJoinOrder: Seq[Replacement] = {
    val (part, rep) = chain.partition(_.isPartitioned)
    // pick minimum number of replicated tables required to fulfill colocated join order.
    val feasibleJoinPlan = Seq.range(0, chain.length - part.length + 1).flatMap(elem =>
      rep.combinations(elem).map(part ++ _).
        flatMap(_.permutations).filter(hasJoinConditions)).filter(_.nonEmpty)

    if(feasibleJoinPlan.isEmpty) {
      Nil
    } else {
      val all = feasibleJoinPlan.sortBy { jo =>
        estimateSize(jo)
      }(implicitly[Ordering[BigInt]].reverse)

      all.head
    }
  }

  lazy val bestPlanEstimatedSize = estimateSize(bestJoinOrder)

  lazy val bestJoinOrderConditions = joinConditions(bestJoinOrder)

  private def joinConditions(joinOrder: Seq[Replacement]) = {
    val refs = joinOrder.map(_.table.outputSet).reduce(_ ++ _)
    conditions.filter(_.references.subsetOf(refs))
  }

  private def estimateSize(joinOrder: Seq[Replacement]): BigInt = {
    if (joinOrder.isEmpty) {
      return BigInt(0)
    }
    var newConditions = joinConditions(joinOrder)
    newConditions = joinOrder.foldLeft(newConditions) { case (nc, e) =>
      e.mappedConditions(nc)
    }

    val sz = joinOrder.map(_.replacedPlan(conditions)).zipWithIndex.foldLeft(BigInt(0)) {
      case (tot, (table, depth)) if depth == 2 => tot + table.statistics.sizeInBytes
      case (tot, (table, depth)) => tot + (table.statistics.sizeInBytes * depth)
    }

    sz
  }

  private def hasJoinConditions(replacements: Seq[Replacement]): Boolean = {
    replacements.sliding(2).forall(_.toList match {
      case table1 :: table2 :: _ =>
        RuleUtils.getJoinKeys(table1.table, table2.table, conditions).nonEmpty
      case _ => false
    })
  }

  override def equals(other: Any): Boolean = other match {
    case cr: ReplacementSet if chain.nonEmpty & cr.chain.nonEmpty =>
      Entity.isColocated(chain(0).index, cr.chain(0).index)
    case _ => false
  }

  def merge(current: ReplacementSet): ReplacementSet = {
    current.chain.foreach { r =>
      chain.find(_.index == r.index) match {
        case None => chain += r
        case _ =>
      }
    }
    this
  }

  def numPartitioningColumns: Int = chain.headOption.map(_.numPartitioningCols).getOrElse(0)

  override def toString: String = chain.mkString("\n")

  override def compare(that: ReplacementSet): Int =
    bestJoinOrder.length compareTo that.bestJoinOrder.length match {
      case 0 => // for equal length chain, sort by smallest size
        bestPlanEstimatedSize compare that.bestPlanEstimatedSize match {
          // in case sizes are same (like in unit test we made it equal) pick the greater one
          case 0 => -(bestJoinOrderConditions.length compare that.bestJoinOrderConditions.length)
          case r => r
        }
      case c => -c // sort by largest chain
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
  def currentColocatedGroup: ReplacementSet =
    throw new AnalysisException("Unexpected call")
}

case class PartialPlan(curPlan: LogicalPlan, replaced: Seq[Replacement], outputSet: AttributeSet,
    input: Seq[LogicalPlan],
    conditions: Seq[Expression],
    colocatedGroups: Seq[ReplacementSet],
    partitioned: Seq[LogicalPlan],
    replicates: Seq[LogicalPlan],
    others: Seq[LogicalPlan]) extends SubPlan {
  var curColocatedIndex = 0

  override def currentColocatedGroup: ReplacementSet = colocatedGroups(curColocatedIndex)

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
