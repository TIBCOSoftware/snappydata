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

import scala.collection.mutable

import com.gemstone.gemfire.internal.cache.{AbstractRegion, ColocationHelper, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.QueryHint

import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedAlias, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, BinaryComparison, Cast, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.columnar.impl.{BaseColumnFormatRelation, ColumnFormatRelation, IndexColumnFormatRelation}
import org.apache.spark.sql.execution.datasources.LogicalRelation

/** This rule changing table to index relations cannot be done in optimize phase
  * without intrusive changes to the optimizedPlan. Also, didn't wanted to bypass
  * checkAnalysis phase after we replace table with index effecting all TreeNode.resolved
  * references.
  */
case class ResolveIndex(snappySession: SnappySession) extends Rule[LogicalPlan]
    with PredicateHelper {

  lazy val catalog = snappySession.sessionState.catalog

  lazy val analyzer = snappySession.sessionState.analyzer

  override def apply(plan: LogicalPlan): LogicalPlan = replaceWithIndex(plan, { optimizedPlan =>

    /** Having decided to change the table with index during analysis phase,
      * we are faced with issues like 'select * from a, b where a.c1 = b.c1'
      * will have join condition in the Filter. It is the optimizer which does
      * many such intelligent Predicate push down etc. So, we walkthrough an
      * optimized plan here to determine whether its possible and beneficial
      * to replace table with index but we apply the transformation on the
      * ongoing analysis plan.
      */
    optimizedPlan.flatMap {
      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, extraConditions, left, right) =>
        (left, right) match {
          case HasColocatedEntities(colocatedEntities, leftReplacements, rightReplacements) =>

            val joinKeys = leftKeys.zip(rightKeys).flatMap { case (leftExp, rightExp) =>
              val l = leftExp.collectFirst({ case a: AttributeReference => a })
              val r = rightExp.collectFirst({ case a: AttributeReference => a })
              (l, r) match {
                case (Some(x), Some(y)) => Seq((x, y))
                case _ => Seq.empty[(AttributeReference, AttributeReference)]
              }
            }

            def hasJoinKeys(leftPCol: String, rightPCol: String): Boolean = {
              joinKeys.exists({ case (lA, rA) =>
                lA.name.equalsIgnoreCase(leftPCol) && rA.name.equalsIgnoreCase(rightPCol) ||
                    lA.name.equalsIgnoreCase(rightPCol) && rA.name.equalsIgnoreCase(leftPCol)
              })
            }

            val satisfyingJoinCond = colocatedEntities.zipWithIndex.filter({
              case ((leftE, rightE), idx) =>
                val matchedCols = leftE.partitionColumns.zip(rightE.partitionColumns).
                    foldLeft(0) {
                      case (cnt, partitionColumn)
                        if (hasJoinKeys _).tupled(partitionColumn) => cnt + 1
                      case (cnt, _) => cnt
                    }
                matchedCols == leftE.partitionColumns.length
            }).sortBy({ case ((p, _), _) => p.partitionColumns.length }).takeRight(1)

            satisfyingJoinCond.nonEmpty match {
              case true =>
                val (_, idx) = satisfyingJoinCond(0)
                Seq(leftReplacements(idx), rightReplacements(idx))
              case false =>
                logDebug("join condition insufficient for matching any colocation columns ")
                Nil
            }
          case _ => Nil
        }

      case f@org.apache.spark.sql.catalyst.plans.logical.Filter(cond, child) =>

        val tableToIndexes = child.collect({
          case l@LogicalRelation(p: ParentRelation, _, _) =>
            val catalog = snappySession.sessionCatalog
            (l.asInstanceOf[LogicalPlan], p.getDependents(catalog).map(idx =>
              catalog.lookupRelation(catalog.newQualifiedTableName(idx))))
          case s@SubqueryAlias(alias, child@LogicalRelation(p: ParentRelation, _, _)) =>
            val catalog = snappySession.sessionCatalog
            (s.asInstanceOf[LogicalPlan], p.getDependents(catalog).map(idx =>
              catalog.lookupRelation(catalog.newQualifiedTableName(idx))))
        })

        val columnGroups = splitConjunctivePredicates(cond).collect({
          case expressions.EqualTo(l, r) =>
            l.collectFirst({ case a: AttributeReference => a }).orElse({
              r.collectFirst({ case a: AttributeReference => a })
            })
          case expressions.EqualNullSafe(l, r) =>
            l.collectFirst({ case a: AttributeReference => a }).orElse({
              r.collectFirst({ case a: AttributeReference => a })
            })
        }).groupBy(_.map(_.qualifier)).collect {
          case (table, cols) if table.nonEmpty & table.get.nonEmpty =>
            (table.get.get, cols.collect({ case a if a.nonEmpty => a.get }))
        }

        val satisfyingPartitionColumns = for {
          (table, indexes) <- tableToIndexes

          filterCols <- columnGroups.collectFirst { case (t, predicates) if predicates.nonEmpty =>
            table match {
              case LogicalRelation(b: ColumnFormatRelation, _, _) if b.table.indexOf(t) > 0 =>
                predicates
              case SubqueryAlias(alias, _) if alias.equals(t) =>
                predicates
            }
          } if filterCols.nonEmpty

          matchedIndexes = indexes.collect {
            case idx@LogicalRelation(ir: IndexColumnFormatRelation, _, _)
              if ir.partitionColumns.length <= filterCols.length =>
              // TODO: match column names
              (ir.partitionColumns.length, idx.asInstanceOf[LogicalPlan])
          } if matchedIndexes.nonEmpty

        } yield {
          (table, matchedIndexes.maxBy(_._1)._2)
        }

        // now we can apply cost based optimization to pick up which index to prefer.
        if (satisfyingPartitionColumns.isEmpty) {
          Seq.empty
        } else {
          Seq(satisfyingPartitionColumns.maxBy({ case (_, idx) => idx.statistics.sizeInBytes}))
        }

      case _ => Nil
    }
  })

  private def replaceWithIndex(plan: LogicalPlan,
      findReplacementCandidates: (LogicalPlan => Seq[(LogicalPlan, LogicalPlan)])) = {

    val explicitIndexHint = getIndexHints

    val replacementMap = if (explicitIndexHint.nonEmpty) {
      plan flatMap {
        case table@LogicalRelation(colRelation: ColumnFormatRelation, _, _) =>
          explicitIndexHint.get(colRelation.table) match {
            case Some(index) => Seq((table, index))
            case _ => Nil
          }
        case subQuery@SubqueryAlias(alias, child) =>
          explicitIndexHint.get(alias) match {
            case Some(index) => Seq((subQuery, SubqueryAlias(alias, index)))
            case _ => Nil
          }
        case _ => Nil
      }
    } else {
      val hasUnresolvedReferences = plan.find(_.references.exists {
        case UnresolvedAttribute(_) => true
        case _ => false
      }).isDefined

      if (hasUnresolvedReferences) {
        // try once all references are resolved.
        Seq.empty
      } else {
        findReplacementCandidates(snappySession.sessionState.optimizer.execute(plan))
      }
    }

    if (replacementMap.isEmpty) {
      plan
    } else {
      // now lets modify the analysis plan with index
      val newPlan = plan transformUp {
        case q: LogicalPlan =>
          val replacement = replacementMap.collect {
            case (plan, replaceWith) if plan.fastEquals(q)
                && !plan.fastEquals(replaceWith) => replaceWith
          }.reduceLeftOption((acc, p_) => if (!p_.fastEquals(acc)) p_ else acc)

          replacement.getOrElse(q)
      }

      newPlan transformUp {
        case q: LogicalPlan =>
          q transformExpressionsUp {
            case a: AttributeReference =>
              q.resolveChildren(Seq(a.qualifier.getOrElse(""), a.name),
                analyzer.resolver).getOrElse(a)
          }
      }
    }
  }

  private def getIndexHints = {
    val indexHint = QueryHint.Index.toString
    snappySession.queryHints.collect {
      case (hint, value) if hint.startsWith(indexHint) =>
        val tableOrAlias = hint.substring(indexHint.length)
        val key = catalog.lookupRelationOption(
          catalog.newQualifiedTableName(tableOrAlias)) match {
          case Some(relation@LogicalRelation(cf: BaseColumnFormatRelation, _, _)) =>
            cf.table
          case _ => tableOrAlias
        }

        val index = catalog.lookupRelation(
          snappySession.getIndexTable(catalog.newQualifiedTableName(value)))

        (key, index)
    }
  }

  object HasColocatedEntities {

    type ReturnType = (
        Seq[(BaseColumnFormatRelation, BaseColumnFormatRelation)], // left & right relation
            Seq[(LogicalPlan, LogicalPlan)], // Seq[left (tablePlan, indexPlan)]
            Seq[(LogicalPlan, LogicalPlan)] // Seq[right (tablePlan, indexPlan)]
        )

    def unapply(tables: (LogicalPlan, LogicalPlan)):
    Option[ReturnType] = {
      val (left, right) = tables

      val leftIndexes = fetchIndexes2(left)
      val rightIndexes = fetchIndexes2(right)

      /** now doing a one-to-one mapping of lefttable and its indexes with
        * right table and its indexes. Following example explains the mapping
        * and collect method on the $cols is to pick colocated relations.
        *
        * val l = Seq(1, 2, 3)
        * val r = Seq(4, 5)
        * l.zip(Seq.fill(l.length)(r)).flatMap {
        * case (leftElement, rightList) => rightList.flatMap { e =>
        * Seq((l, e))
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
      val (leftPair, rightPair) = leftIndexes.zip(Seq.fill(leftIndexes.length)(rightIndexes)).
          flatMap { case (l, r) => r.flatMap(e => Seq((l, e))) }.
          collect {
            case plans@ExtractBaseColumnFormatRelation(leftEntity, rightEntity)
              if isColocated(leftEntity, rightEntity) =>
              val (leftPlan, rightPlan) = plans
              ((leftEntity, leftPlan), (rightEntity, rightPlan))
          }.unzip

      leftPair.nonEmpty match {
        case true =>
          val (leftBase, leftEntities) = leftPair.unzip
          val (rightBase, rightEntities) = rightPair.unzip
          // Note: fetchIndexes returned with 0th element as the table relation itself.
          Some((leftBase.zip(rightBase), leftEntities.map(e => (leftIndexes(0), e)),
              rightEntities.map(e => (rightIndexes(0), e))))
        case false =>
          logDebug(s"Nothing is colocated between the tables $leftIndexes and $rightIndexes")
          None
      }
    }
  }

  private def isColocated(left: BaseColumnFormatRelation, right: BaseColumnFormatRelation) = {

    val leftRegion = Misc.getRegionForTable(left.resolvedName, true)
    val leftLeader = leftRegion.asInstanceOf[AbstractRegion] match {
      case pr: PartitionedRegion => ColocationHelper.getLeaderRegionName(pr)
    }

    val rightRegion = Misc.getRegionForTable(right.resolvedName, true)
    val rightLeader = rightRegion.asInstanceOf[AbstractRegion] match {
      case pr: PartitionedRegion => ColocationHelper.getLeaderRegionName(pr)
    }

    leftLeader.equals(rightLeader)
  }

  private def fetchIndexes2(plan: LogicalPlan): Seq[LogicalPlan] =
    EliminateSubqueryAliases(plan) match {
      case l@LogicalRelation(p: ParentRelation, _, _) =>
        val catalog = snappySession.sessionCatalog
        Seq(l) ++ p.getDependents(catalog).map(idx =>
          catalog.lookupRelation(catalog.newQualifiedTableName(idx)))
      case q: LogicalPlan => q.children.flatMap(fetchIndexes2(_))
      case _ => Nil
    }

}

// end of ResolveIndex

object ExtractBaseColumnFormatRelation {

  type pairOfTables = (BaseColumnFormatRelation, BaseColumnFormatRelation)

  def unapply(arg: (LogicalPlan, LogicalPlan)): Option[pairOfTables] = {
    (extractBoth _).tupled(arg)
  }

  def unapply(arg: LogicalPlan): Option[BaseColumnFormatRelation] = {
    arg collectFirst {
      case entity@LogicalRelation(relation: BaseColumnFormatRelation, _, _) =>
        relation
    }
  }

  private def extractBoth(left: LogicalPlan, right: LogicalPlan) = {
    unapply(left) match {
      case Some(v1) => unapply(right) match {
        case Some(v2) => Some(v1, v2)
        case _ => None
      }
      case _ => None
    }
  }

}

