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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import io.snappydata.QueryHint._

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.optimizer.ReorderJoin
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.{expressions, plans}
import org.apache.spark.sql.execution.PartitionedDataSourceScan
import org.apache.spark.sql.execution.columnar.impl.{BaseColumnFormatRelation, ColumnFormatRelation, IndexColumnFormatRelation}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.Entity.{INDEX_RELATION, TABLE}
import org.apache.spark.sql.{SnappySession, SparkSupport}


/**
 * Replace table with index hint
 */
case class ResolveQueryHints(snappySession: SnappySession)
    extends Rule[LogicalPlan] with SparkSupport {

  private def catalog = snappySession.sessionState.catalog

  private def analyzer = snappySession.sessionState.analyzer

  override def apply(plan: LogicalPlan): LogicalPlan = {

    val explicitIndexHint = getIndexHints

    if (explicitIndexHint.isEmpty) {
      return plan
    }

    plan transformUp {
      case lr: LogicalRelation if lr.relation.isInstanceOf[ColumnFormatRelation] =>
        explicitIndexHint.getOrElse(lr.relation.asInstanceOf[ColumnFormatRelation].table,
          Some(lr)).get
      case s: SubqueryAlias if s.child.isInstanceOf[LogicalRelation] &&
          s.child.asInstanceOf[LogicalRelation].relation.isInstanceOf[IndexColumnFormatRelation] =>
        explicitIndexHint.get(s.alias) match {
          case Some(Some(index)) => internals.newSubqueryAlias(s.alias, index)
          case _ => s
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

  private def getIndexHints: mutable.Map[String, Option[LogicalPlan]] = {
    val indexHint = Index
    val hints = snappySession.queryHints
    if (hints.isEmpty) mutable.Map.empty
    else hints.asScala.collect {
      case (hint, value) if hint.startsWith(indexHint) =>
        val tableOrAlias = hint.substring(indexHint.length)
        val tableIdent = snappySession.tableIdentifier(tableOrAlias)
        val key = if (catalog.tableExists(tableIdent)) {
          catalog.resolveRelation(tableIdent) match {
            case lr: LogicalRelation if lr.relation.isInstanceOf[BaseColumnFormatRelation] =>
              lr.relation.asInstanceOf[BaseColumnFormatRelation].table
            case _ => tableOrAlias
          }
        } else tableOrAlias
        val index = if (value.trim.length == 0) {
          // if blank index mentioned,
          // we don't validate to find the index, instead consider that user is
          // disabling optimizer to choose index all together.
          None
        } else {
          val tableIdent = snappySession.tableIdentifier(value)
          Some(catalog.resolveRelation(snappySession.getIndexTable(tableIdent)))
        }

        (key, index)
    }
  }

}

/**
 * Replace table with index if colocation criteria is satisfied.
 */
case class ResolveIndex(implicit val snappySession: SnappySession) extends Rule[LogicalPlan]
    with PredicateHelper {

  private def createColocatedJoins(input: Seq[LogicalPlan],
      conditions: Seq[Expression],
      visited: mutable.HashSet[LogicalPlan]): CompletePlan = {

    if (input.size == 1) {
      val table = input.head
      val pTabOrIndex = RuleUtils.chooseIndexForFilter(table, conditions)
      val replacement = if (conditions.nonEmpty) {
        plans.logical.Filter(conditions.reduceLeft(expressions.And),
          pTabOrIndex.map(_.index).getOrElse(table))
      } else {
        pTabOrIndex.map(_.index).getOrElse(table)
      }
      return CompletePlan(replacement, pTabOrIndex.toSeq)
    }

    val joinOrderHints = JoinOrderStrategy.getJoinOrderHints

    type TableList = ArrayBuffer[LogicalPlan]
    // split the input tables into partitioned, replicate and unhandled list.
    val (partitioned, replicates, others) =
      ((new TableList, new TableList, new TableList) /: input) {
        case (splitted@(part, rep, _),
        l: LogicalRelation) if l.relation.isInstanceOf[PartitionedDataSourceScan] =>
          if (l.relation.asInstanceOf[PartitionedDataSourceScan].partitionColumns.nonEmpty) {
            part += l
          } else {
            rep += l
          }
          splitted
        case (splitted@(_, _, other), l) =>
          other += l
          splitted
      }

    val (generated, userDefined) = getPossibleColocatedJoins(partitioned, replicates, others,
      conditions).toList.partition { case (_, _, _, isGenerated) => isGenerated }


    // lets try with userDefined join conditions first. If we find any colocation worthy set
    // then we will not attempt for generated colocated joins. Although filter push down may still
    // happen & table replaced with index.
    val joinPossibilities = userDefined.flatMap { case (leftPlan, rightPlan, joinKeys, _) =>
      getPossibleReplacements(leftPlan, rightPlan, joinKeys)
    }

    // if we think its okay to try both, just merge them here.
    val colocationTrials = if (joinPossibilities.isEmpty ||
        joinOrderHints.contains(IncludeGeneratedPaths)) {
      // now try with the generated possibilities.
      generated.flatMap { case (leftPlan, rightPlan, joinKeys, _) =>
        getPossibleReplacements(leftPlan, rightPlan, joinKeys)
      }
    } else {
      joinPossibilities
    }

    val colocationGroups = (ArrayBuffer.empty[ReplacementSet] /: colocationTrials) {
      case (mergedList, current) =>
        mergedList.find(_ equals current) match {
          case Some(existing) => existing.merge(current)
          case None => mergedList += current.copy(conditions = conditions)
        }
        mergedList
    }.sorted.toList

    val (ncf, nonColocated) = partitioned.filterNot {
      case _ if colocationGroups.isEmpty => false
      case p => colocationGroups.head.chain.exists(_.table equals p)
    }.partition(t => conditions.exists(canEvaluate(_, t)))


    val nonColocatedWithFilters = ncf.map(r => RuleUtils.chooseIndexForFilter(r, conditions)
        .getOrElse(Replacement(r, r)))

    val replicatesWithColocated = ReplacementSet(replicates.map(
      r => Replacement(r, r, isPartitioned = false)) ++
        (if (colocationGroups.nonEmpty) colocationGroups.head.chain else Nil), conditions)

    val replicatesWithNonColocatedHavingFilters = nonColocatedWithFilters.map(nc =>
      ReplacementSet(replicates.map(r => Replacement(r, r)) ++ Some(nc), conditions)).sorted

    val smallerNC = replicatesWithNonColocatedHavingFilters.indexWhere(
      _.bestPlanEstimatedSize < replicatesWithColocated.bestPlanEstimatedSize)

    val finalJoinOrder = ArrayBuffer.empty[Replacement]
    var newJoinConditions: Seq[Expression] = conditions

    def mapReferences(conditions: Seq[Expression], r: Replacement): Seq[Expression] = {
      r.mappedConditions(conditions)
    }

    var curPlan = CompletePlan(null, Nil)

    // there are no Non-Colocated tables of lesser cost than colocation chain in consideration.
    if (smallerNC == -1) {

      finalJoinOrder ++= replicatesWithColocated.bestJoinOrder
      curPlan = curPlan.copy(replaced = curPlan.replaced ++
          replicatesWithColocated.bestJoinOrder.filter(_.isReplacable))
      newJoinConditions = replicatesWithColocated.bestJoinOrder.
          foldLeft(newJoinConditions)(mapReferences)

      finalJoinOrder ++= nonColocatedWithFilters
      curPlan = curPlan.copy(replaced = curPlan.replaced ++
          nonColocatedWithFilters.filter(_.isReplacable))
      newJoinConditions = nonColocatedWithFilters.foldLeft(newJoinConditions)(mapReferences)

      finalJoinOrder ++= nonColocated.map(r => Replacement(r, r))
    } else {
      for (_ <- 0 to smallerNC) {
        // pack NC tables first.
      }
    }

    val newInput = finalJoinOrder.map(_.index) ++
        input.filterNot(i => finalJoinOrder.exists(_.table == i))

    curPlan = curPlan.copy(plan = ReorderJoin.createOrderedJoin(
      newInput.map((_, Inner)), newJoinConditions))


    // add to the visited list, as plan.flatMap is transformDown which results into child join
    // nodes coming in after outer join node is serviced by flattening. LogicalPlan is unique
    // objects and hence multiple aliasing to the same table shouldn't interfere with each other
    // in this lookup.
    input.foreach(visited += _)
    curPlan
  }

  /** Generating combinations of PRs 2 at a time and then evaluate colocation. if p1 -> p2 -> p3
   * like colocation chain can occur, we can potentially use multiple indexes. That we can
   * determine by reducing these combinations of 2 and merge into longer chain.
   *
   * The choice of colocated group to `replace` will be made firstly based on more
   * feasibility check on LogicalPlan.outputSet, and then cost based computed by considering
   * filter and size. Later selectivity can be introduced.
   *
   * This approach have a limitation
   * {{{
   * 1. select .. from t1 join t2 on t1.pc1 = t2.pc2 and t1.pc3 = t2.pc4 join t3 on t1.pc1 =
   * t3.pc5 and t2.pc4 = t3.pc6.
   *
   *   This kind of mixed back referencing will right now fail to fetch joinKeys because we
   *   are trying 2 at a time. Creating all possible combinations and eliminating one by one
   *   is too computation intensive & increases code complexity. This kind of scenario can be
   *   handled by providing user guideline. "Use either t1 or t2 partitioning cols
   *   uniformly in a join condition".
   * }}}
   *
   * if lets say colocated condition is mentioned like A -> B -> REP -> C
   * where we know some index b/w A, B and C are colocated regions, then this should form a
   * single chain.
   *
   * whereas A -> B cross-join C -> D  should NOT form colocation chain because of cartesian
   * product.
   *
   * here we are just generating possible joining PRs
   */
  private def getPossibleColocatedJoins(partitioned: ArrayBuffer[TABLE],
      replicates: ArrayBuffer[TABLE],
      others: ArrayBuffer[TABLE],
      joinConditions: Seq[Expression]) = {

    partitioned.toList.combinations(2).flatMap {
      case left :: right :: rest if rest.isEmpty =>

        val userDefinedJoinKeys = RuleUtils.getJoinKeys(left, right, joinConditions)

        if (userDefinedJoinKeys.isEmpty) {
          // okay, there is no direct colocation criteria mentioned.
          // probing with replicated in between. we will prefer these only when user have
          // not provided any other join condition involving either of this left/right pair.
          // like t1 -> t2 -> t3 join conditions are mentioned but while trying t1 -> t3 we won't
          // find any user defined join keys. Its okay to skip trying just t1 -> t3 indirect
          // colocation because eventually colocation chain when merged with the help of t2 as
          // common between the pairs, the result will be t1 -> t2 -> t3.

          val leftReplicateMixedConditions = replicates.map(r => (r, RuleUtils.getJoinKeys(left,
            r, joinConditions))).filter { case (_, c) => c.nonEmpty }

          val rightReplicateMixedConditions = replicates.map(r => (r, RuleUtils.getJoinKeys(right,
            r, joinConditions))).filter { case (_, c) => c.nonEmpty }

          if (leftReplicateMixedConditions.isEmpty || rightReplicateMixedConditions.isEmpty) {
            // we don't have enough join conditions to satisfy colocation.
            None
          } else {

            val replicateToReplicateJoined = if (replicates.toList.size > 2) {
              replicates.toList.combinations(2).filter {
                case l :: r :: o if o.isEmpty =>
                  // there can be additional mixed conditions which will get filtered out,
                  // that is okay because we just want to ensure here that 2 rep tables are
                  // not creating cartesian product. Although its harmless as far as I can think
                  // but right now enforcing it, as explained in generateJoinKeys,
                  // reduces complexity.
                  RuleUtils.getJoinKeys(l, r, joinConditions).nonEmpty
              }.toList
            } else {
              replicates.toList match {
                case l :: r :: o if o.isEmpty
                    & RuleUtils.getJoinKeys(l, r, joinConditions).nonEmpty =>
                  List(replicates.toList)
                case _ :: o if o.isEmpty =>
                  List(replicates.toList)
                case _ => List(List.empty[Entity.TABLE])
              }
            }

            val joinKeys = generateJoinKeys(left,
              leftReplicateMixedConditions,
              right,
              rightReplicateMixedConditions,
              replicateToReplicateJoined,
              replicates)

            if (joinKeys.nonEmpty) {
              Some((left, right, joinKeys, true /* isGenerated */ ))
            } else {
              None
            }
          }

        } else {
          Some((left, right, userDefinedJoinKeys, false /* isGenerated */ ))
        }
    }

  }

  private def getPossibleReplacements(left: LogicalPlan,
      right: LogicalPlan, joinConditions: Seq[(Expression, Expression)]) = {
    (left, right) match {
      case HasColocatedEntities(colocatedEntities, replacements) =>
        val joinKeys = joinConditions.flatMap {
          case (leftA: AttributeReference, rightA: AttributeReference) =>
            Seq((leftA.name, rightA.name))
          case _ => Nil
        }

        val hasJoinKeys = Function.tupled[INDEX_RELATION, INDEX_RELATION, Boolean] {
          case (leftE, rightE) =>
            // at some point of time we need to do expression matching b/w partitionCol & input
            // joinKey.
            leftE.partitionColumns.zip(rightE.partitionColumns).forall {
              case (leftPCol, rightPCol) =>
                joinKeys.exists { case (lA, rA) =>
                  lA.equalsIgnoreCase(leftPCol) && rA.equalsIgnoreCase(rightPCol) ||
                      lA.equalsIgnoreCase(rightPCol) && rA.equalsIgnoreCase(leftPCol)
                }
            }
        }

        val result = colocatedEntities.zipWithIndex.collect {
          case (entities, idx) if hasJoinKeys(entities) => replacements(idx)
        }

        if (result.isEmpty) {
          val (leftKeys, rightKeys) = joinKeys.unzip
          logDebug(s"insufficient partitioning column join keys leftKeys:$leftKeys | " +
              s"rightKeys:$rightKeys")
        }

        result
      case _ => Nil
    }

  }

  /**
   * Generating join keys poses a pairing problem 'coz user haven't explicitly given join
   * relation and partitioning cols hash computation is ordered. Therefore, we cannot
   * pair join keys orderless. for example:
   *
   * {{{
   * given following tables:
   *   create t1 partition by column (c1, c2);
   *   create t2 partition by column(c2, c1);
   *   create rep1 (rc1, rc2, rc3, rc4);
   * }}}
   *
   * {{{
   * some query can colocate and some cannot.
   * select * from t1 join rep on t1.c1 = rc1 and t1.c2 = rc2 join t2 on rc3 = t2.c1 & rc4 = t2.c2
   * // shouldn't colocate 'coz partition col is not orderless. here sequence becomes important !.
   *
   * select * from t1 join rep on t1.c1 = rc1 and t1.c2 = rc2 join t2 on rc3 = t2.c2 & rc4 = t2.c1
   * // will colocate 'coz per sequence of join cols mentioned, t1.c1 will match with t2.c2.
   *
   * select * from t1 join rep on t1.c1 = rc1 and t1.c2 = rc2 join t2 on rc2 = t2.c1 & rc1 = t2.c2
   * // will colocate 'coz connecting attributes of replicated tables clarifies the p.cols mapping
   * b/w tables.
   * }}}
   *
   * situation becomes complicated when 2 or more replicated tables are in between. we may land
   * up into mix of above last 2 conditions.
   *
   * {{{
   *   and then we have query like below assuming t1(partition_by(c1)) and t2(partition_by(c7))
   *
   *   select * from t1 join rep r1 on t1.c1 = r1.c1 join rep r2 on t1.c1 = r2.xx and
   *   t1.c2 = r2.c3 and r1.c4 = r2.c5 join t2 on r2.c6 = t2.c7
   * }}}
   *
   * so, right now for every predicate on left, we will simply pair 1 to 1 with right PR
   * predicate and check for colocation. Simple user guideline will be to mention join
   * conditions in uniform order i.e.if first table p.cols are mentioned as t1.c2 = rc2 and t1
   * .c1 = rc1 then, user must mention t2 table's join condition in reverse order too like
   * t2.c1 = rc4 and t2.c2 = rc3
   *
   * note: we don't care how many cols are involved in between replicated tables, till the time
   * replicates in between are joined and finally t1 and t2 have enough join conditions with the
   * replicates, colocation should be safe.
   *
   * {{{
   *
   *  more examples:
   *
   * following query will attempt colocation check b/w t1 & t2 only with generatedJoinKeys as
   * ((t1.c1, t2.c2), (t1.c2, t2.c1))
   *
   *   select * from t1 join r1 on t1.c1 = r1.rc1 and t1.c2 = r1.rc2 join r2 on r1.rc3 = r2.rc4
   *   join t2 on t2.c2 = r2.rc5 and t2.c1 = r2.rc6
   *
   * notice it won't attempt generatedJoinKeys with ((t1.c1, t2.c1), (t1.c2, t2.c2)) because
   * partitioning columns are no more orderless.
   *
   * OTOH, following query won't even attempt to generateJoinKeys due to no connecting
   * relation between the replicates.
   *
   *   select * from t1 join r1 on t1.c1 = r1.rc1 and t1.c2 = r1.rc2 join r2 on t1.c3 = r2.rc4
   *   join t2 on t2.c2 = r2.rc5 and t2.c1 = r2.rc6
   * }}}
   *
   */
  private def generateJoinKeys(
      leftPartitionedTable: LogicalPlan,
      leftRepMixedConditions: Seq[(LogicalPlan, Seq[(Expression, Expression)])],
      rightPartitionedTable: LogicalPlan,
      rightRepMixedConditions: Seq[(LogicalPlan, Seq[(Expression, Expression)])],
      replicatedReachablePaths: Seq[List[LogicalPlan]],
      replicates: Seq[LogicalPlan]) = {

    // ideally this should try left.foreach { right.foreach{ ... } } and not do a flatMap
    // instead, choose best joinKeys out of multiple access path.
    leftRepMixedConditions.zip(rightRepMixedConditions).flatMap {
      case ((leftRepTable, leftJoinKeys), (rightRepTable, rightJoinKeys))
        if leftJoinKeys.length == rightJoinKeys.length &
            RuleUtils.canTraverseLeftToRight(Seq(leftRepTable), rightRepTable,
              replicatedReachablePaths) =>

        leftJoinKeys.zip(rightJoinKeys).map {
          case ((leftPartitionedKey, _), (rightPartitionedKey, _)) =>
            (leftPartitionedKey, rightPartitionedKey)
        }
      case _ => None
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val joinOrderHints = JoinOrderStrategy.getJoinOrderHints
    val enabled: Boolean = io.snappydata.Property.EnableExperimentalFeatures.
        get(snappySession.snappyContext.conf)
    if (!enabled) {
      return plan
    }
    val hints = snappySession.queryHints
    if (!hints.isEmpty && hints.asScala.exists {
      case (hint, _) => hint.startsWith(Index) &&
          !joinOrderHints.contains(ContinueOptimizations)
    } || Entity.hasUnresolvedReferences(plan)) {
      return plan
    }

    val newAttributesMap = new ArrayBuffer[(AttributeReference, AttributeReference)]()
    val visited = new mutable.HashSet[LogicalPlan]

    def notVisited(input: Seq[LogicalPlan]) = !input.exists(p => {
      // already resolved to some index. lets not try anything here.
      // this is just for speed, where this rule gets executed so many times.
      visited(p)
    })

    val newPlan = plan.transformDown {
      case ExtractFiltersAndInnerJoins(input, conditions)
        if conditions.nonEmpty && notVisited(input) =>
        val completePlan = createColocatedJoins(input, conditions, visited)
        completePlan.replaced.foreach {
          case r if r.table != r.index =>
            val newAttributes = r.index.output.collect { case ar: AttributeReference => ar }
            r.table.output.foreach {
              case f: AttributeReference =>
                val newA = newAttributes.find(_.name.equalsIgnoreCase(f.name)).
                    getOrElse(throw new IllegalStateException(
                      s"Field $f not found in $newAttributes"))
                newAttributesMap ++= Some((f, newA))
            }
          case _ =>
        }
        completePlan.plan
    }

    if (newAttributesMap.nonEmpty) {
      val retVal = newPlan transformUp {
        case q: LogicalPlan =>
          q transformExpressionsUp {
            case a: AttributeReference => newAttributesMap.find({
              case (tableA, _) => tableA.exprId == a.exprId
            }).map({ case (t, i) => i.withQualifier(t.qualifier) }).getOrElse(a)
          }
      }
      retVal
    } else {
      newPlan
    }

  }

}

// end of ResolveIndex

