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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BinaryComparison, Cast, Expression, PredicateHelper}
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

  override def apply(plan: LogicalPlan): LogicalPlan = {

    val indexHint = QueryHint.WithIndex.toString
    val explicitIndexHint = snappySession.queryHints.collect {
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

    val replacementMap = if (explicitIndexHint.isEmpty) {
      val hasUnresolvedReferences = plan.find (_.references.exists {
        case UnresolvedAttribute(_) => true
        case _ => false
      }).isDefined

      if (hasUnresolvedReferences) {
        // try once all references are resolved.
        return plan
      }
      /** Having decided to change the table with index during analysis phase,
        * we are faced with issues like 'select * from a, b where a.c1 = b.c1'
        * will have join condition in the Filter. It is the optimizer which does
        * many such intelligent Predicate push down etc. So, we walkthrough an
        * optimized plan here to determine whether its possible and beneficial
        * to replace table with index but we apply the transformation on the
        * ongoing analysis plan.
        */
      val optimizedPlan = snappySession.sessionState.optimizer.execute(plan)
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
          val tabToIdxReplacementMap = extractIndexTransformations(cond, child)
          Nil
        case _ => Nil
      }
    } else {
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
    }

    replacementMap.nonEmpty match {
      case true =>
        // now lets modify the analysis plan with index
        val newPlan = plan transformUp {
          case q: LogicalPlan =>
            val replacement = replacementMap.collect {
              case (plan, replaceWith) if plan.fastEquals(q)
                  && ! plan.fastEquals(replaceWith) => replaceWith
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
      case false => plan
    }

  }

  object HasColocatedEntities {

    type ReturnType = (Seq[(BaseColumnFormatRelation, BaseColumnFormatRelation)],
        Seq[(LogicalPlan, LogicalPlan)], Seq[(LogicalPlan, LogicalPlan)])

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
        *   case (leftElement, rightList) => rightList.flatMap { e =>
        *     Seq((l, e))
        *   }
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
          Some((leftBase.zip(rightBase), leftEntities.map(e => (leftIndexes(0), e)),
              rightEntities.map(e => (rightIndexes(0), e))))
        case false =>
          logDebug(s"Nothing is colocated between the tables $leftIndexes and $rightIndexes")
          None
      }
    }
  }


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


  private def replaceTableWithIndex(tabToIdxReplacementMap: Map[String, Option[LogicalPlan]],
      plan: LogicalPlan) = {
    plan transformUp {
      case l@LogicalRelation(b: ColumnFormatRelation, _, _) =>
        tabToIdxReplacementMap.find {
          case (t, Some(index)) =>
            b.table.indexOf(t) >= 0
          case (t, None) => false
        }.flatMap(_._2).getOrElse(l)

      case s@SubqueryAlias(alias, child@LogicalRelation(p: ParentRelation, _, _)) =>
        tabToIdxReplacementMap.withDefaultValue(None)(alias) match {
          case Some(i) => s.copy(child = i)
          case _ => s
        }
    }

  }

  private def extractIndexTransformations(cond: Expression, child: LogicalPlan) = {
    val tableToIndexes = fetchIndexes(child)
    val cols = splitConjunctivePredicates(cond)
    val columnGroups = segregateColsByTables(cols)

    getBestMatchingIndex(columnGroups, tableToIndexes)
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

  private def fetchIndexes(plan: LogicalPlan): mutable.HashMap[LogicalPlan, Seq[LogicalPlan]] =
    plan.children.foldLeft(mutable.HashMap.empty[LogicalPlan, Seq[LogicalPlan]]) {
      case (m, table) =>
        val newVal: Seq[LogicalPlan] = m.getOrElse(table,
          table match {
            case l@LogicalRelation(p: ParentRelation, _, _) =>
              val catalog = snappySession.sessionCatalog
              p.getDependents(catalog).map(idx =>
                catalog.lookupRelation(catalog.newQualifiedTableName(idx)))
            case SubqueryAlias(alias, child@LogicalRelation(p: ParentRelation, _, _)) =>
              val catalog = snappySession.sessionCatalog
              p.getDependents(catalog).map(idx =>
                catalog.lookupRelation(catalog.newQualifiedTableName(idx)))
            case _ => Nil
          }
        )
        // we are capturing same set of indexes for one or more
        // SubqueryAlias. This comes handy while matching columns
        // that is aliased for lets say self join.
        // select x.*, y.* from tab1 x, tab2 y where x.parentID = y.id
        m += (table -> newVal)
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

  private def unwrapReference(input: Expression) = input.references.collectFirst {
    case a: AttributeReference => a
  }

  private def segregateColsByTables(cols: Seq[Expression]) = cols.
      foldLeft(mutable.MutableList.empty[AttributeReference]) {
        case (m, col@BinaryComparison(left, _)) =>
          unwrapReference(left).foldLeft(m) {
            case (m, leftA: AttributeReference) =>
              m.find(_.fastEquals(leftA)) match {
                case None => m += leftA
                case _ => m
              }
          }
        case (m, _) => m
      }.groupBy(_.qualifier.get)

  private def getBestMatchingIndex(
      columnGroups: Map[String, mutable.MutableList[AttributeReference]],
      tableToIndexes: mutable.HashMap[LogicalPlan, Seq[LogicalPlan]]) = columnGroups.map {
    case (table, colList) =>
      val indexes = tableToIndexes.find { case (tableRelation, _) => tableRelation match {
        case l@LogicalRelation(b: ColumnFormatRelation, _, _) =>
          b.table.indexOf(table) >= 0
        case SubqueryAlias(alias, _) => alias.equals(table)
        case _ => false
      }
      }.map(_._2).getOrElse(Nil)

      val index = indexes.foldLeft(Option.empty[LogicalPlan]) {
        case (max, i) => i match {
          case l@LogicalRelation(idx: IndexColumnFormatRelation, _, _) =>
            idx.partitionColumns.filter(c => colList.exists(_.name.equalsIgnoreCase(c))) match {
              case predicatesMatched
                if predicatesMatched.length == idx.partitionColumns.length =>
                val existing = max.getOrElse(l)
                existing match {
                  case LogicalRelation(eIdx: IndexColumnFormatRelation, _, _)
                    if (idx.partitionColumns.length > eIdx.partitionColumns.length) =>
                    Some(l)
                  case _ => Some(existing)
                }
              case _ => max
            }
        }
      }

      if (index.isDefined) {
        logInfo(s"Picking up $index for $table")
      }

      (table, index)
  }.filter({ case (_, idx) => idx.isDefined })


}
