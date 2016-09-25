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
import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.internal.cache.{AbstractRegion, ColocationHelper, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.QueryHint

import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedAlias, UnresolvedAttribute, UnresolvedFunction, UnresolvedGenerator, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, AttributeSet, BinaryComparison, Cast, Expression, PredicateHelper, UnresolvedWindowExpression}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.columnar.impl.{BaseColumnFormatRelation, ColumnFormatRelation, IndexColumnFormatRelation}
import org.apache.spark.sql.execution.datasources.LogicalRelation


/**
  * Replace table with index hint
  */
case class ResolveQueryHints(snappySession: SnappySession) extends Rule[LogicalPlan]
{
  lazy val catalog = snappySession.sessionState.catalog

  lazy val analyzer = snappySession.sessionState.analyzer

  override def apply(plan: LogicalPlan): LogicalPlan = {

    val explicitIndexHint = getIndexHints

    if (explicitIndexHint.isEmpty) {
      return plan
    }

    plan transformUp {
        case table@LogicalRelation(colRelation: ColumnFormatRelation, _, _) =>
          explicitIndexHint.get(colRelation.table).getOrElse(table)
        case subQuery@SubqueryAlias(alias, LogicalRelation(_, _, _)) =>
          explicitIndexHint.get(alias) match {
            case Some(index) => SubqueryAlias(alias, index)
            case _ => subQuery
          }
    } transformUp {
      case q: LogicalPlan =>
        q transformExpressionsUp {
          case a: AttributeReference =>
            q.resolveChildren(Seq(a.qualifier.getOrElse(""), a.name),
              analyzer.resolver).getOrElse(a)
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

}

/**
  * Replace table with index if colocation criteria is satisfied.
  */
case class ResolveIndex(snappySession: SnappySession) extends Rule[LogicalPlan]
    with PredicateHelper {

  lazy val catalog = snappySession.sessionState.catalog

  lazy val analyzer = snappySession.sessionState.analyzer

  override def apply(plan: LogicalPlan): LogicalPlan = replaceWithIndex(plan, { optimizedPlan =>

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

          (table, indexes) <- fetchIndexes(child)

          filterCols <- columnGroups.collectFirst { case (t, predicates) if predicates.nonEmpty =>
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
          (table, matchedIndexes.maxBy(_._1)._2)
        }

        // now we can apply cost based optimization to pick up which index to prefer.
        if (satisfyingPartitionColumns.isEmpty) {
          Seq.empty
        } else {
          Seq(satisfyingPartitionColumns.maxBy({ case (_, idx) => idx.statistics.sizeInBytes }))
        }

      case _ => Nil
    }
  })

  private def replaceWithIndex(plan: LogicalPlan,
      findReplacementCandidates: (LogicalPlan => Seq[(LogicalPlan, LogicalPlan)])) = {

    val replacementMap = {

      if (snappySession.queryHints.exists {
        case (hint, _) if hint.startsWith(QueryHint.Index.toString) =>
          true
        case _ => false
      }) {
        Seq.empty
      }

      def findR(p: Any) = p match {
        case UnresolvedAttribute(_) |
             UnresolvedFunction(_, _, _) |
             UnresolvedAlias(_, _) |
             UnresolvedWindowExpression(_, _) |
             UnresolvedGenerator(_, _) |
             UnresolvedStar(_) => true
        case _ => false
      }

      val hasUnresolvedReferences = plan.find {l =>
        l.productIterator.exists(findR) || l.expressions.exists({ e =>
              findR(e) || e.productIterator.exists(findR) ||
                  e.references.exists(findR)
          })
      }.nonEmpty

      if (hasUnresolvedReferences) {
        // try once all references are resolved.
        Seq.empty
      } else {
        findReplacementCandidates(plan)
      }
    }

    if (replacementMap.isEmpty) {
      plan
    } else {

      val newAttributesMap = new mutable.HashMap[AttributeReference, AttributeReference]()

      // now lets modify the analysis plan with index
      val newPlan = plan transformUp {
        case q: LogicalRelation =>
          val replacement = replacementMap.collect {
            case (plan, replaceWith) if plan.fastEquals(q)
                && !plan.fastEquals(replaceWith) => replaceWith
          }.reduceLeftOption((acc, p_) => if (!p_.fastEquals(acc)) p_ else acc)

          replacement.map(newP => {
            val newAttributes = newP.schema.toAttributes
            q.output.foreach({
              case f: AttributeReference =>
                newAttributesMap(f) = newAttributes.find(_.name.equalsIgnoreCase(f.name)).
                    getOrElse(throw new IllegalStateException(
                      s"Field $f not found in ${newAttributes}"))
            })

            newP
          }).getOrElse(q)
      }

      if (newAttributesMap.nonEmpty) {
        newPlan transformUp {
          case q: LogicalPlan =>
            q transformExpressionsUp {
              case a: AttributeReference => newAttributesMap.getOrElse(a, a)
            }
        }
      } else {
        newPlan
      }
    }
  }

  private def fetchIndexes(table: LogicalPlan) = table.collect {
    case l@LogicalRelation(p: ParentRelation, _, _) =>
      val catalog = snappySession.sessionCatalog
      (l.asInstanceOf[LogicalPlan], p.getDependents(catalog).map(idx =>
        catalog.lookupRelation(catalog.newQualifiedTableName(idx))))
    case s@SubqueryAlias(alias, child@LogicalRelation(p: ParentRelation, _, _)) =>
      val catalog = snappySession.sessionCatalog
      (s.asInstanceOf[LogicalPlan], p.getDependents(catalog).map(idx =>
        catalog.lookupRelation(catalog.newQualifiedTableName(idx))))
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
        (leftTable, leftIndexes) <- fetchIndexes(left)
        (rightTable, rightIndexes) <- fetchIndexes(right)
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

          leftRelation = unwrapBaseColumnRelation(leftPlan) if leftRelation.nonEmpty
          rightRelation = unwrapBaseColumnRelation(rightPlan) if rightRelation.nonEmpty

          if isColocated(leftRelation.get, rightRelation.get)
        } yield {

          val leftReplacement = leftTable match {
            case _: LogicalRelation => (leftTable, leftPlan)
            case subquery@SubqueryAlias(alias, _) =>
              (subquery, SubqueryAlias(alias, leftPlan))
          }

          val rightReplacement = rightTable match {
            case _: LogicalRelation => (rightTable, rightPlan)
            case subquery@SubqueryAlias(alias, _) =>
              (subquery, SubqueryAlias(alias, rightPlan))
          }

          ((leftRelation.get, rightRelation.get), leftReplacement, rightReplacement)
        }
      }

      if (mappings.nonEmpty) {
        Some(mappings.unzip3)
      } else {
        None
      }
    }

  }

  private def unwrapBaseColumnRelation(
      plan: LogicalPlan): Option[BaseColumnFormatRelation] = plan collectFirst {
    case LogicalRelation(relation: BaseColumnFormatRelation, _, _) =>
      relation
    case SubqueryAlias(alias, LogicalRelation(relation: BaseColumnFormatRelation, _, _)) =>
      relation
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
