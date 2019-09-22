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

package org.apache.spark.sql.sources

import io.snappydata.QueryHint._

import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{expressions, plans}

/**
  * Trait to apply different join order policies like Replicates with filters first, then largest
  * colocated group, and finally non-colocated with filters, if any.
  *
  * One can change the ordering policies as part of query hints and later can be admin provided
  * externally against a regex based query pattern.
  *
  * e.g. select * from /*+ joinOrder(replicates+filters, non-colocated+filters) */ table1, table2
  * where ....
  *
  * note: I think this should be at the query level instead of per select scope i.e. something like
  * /*+ joinOrder(replicates+filters, non-colocated+filters) */ select * from tab1, (select xx
  * from tab2, tab3 where ... ), tab4 where ...
  */
abstract class JoinOrderStrategy extends PredicateHelper {

  final def apply(subPlan: SubPlan, withFilters: Boolean)
      (implicit snappySession: SnappySession): SubPlan = subPlan match {
    case c: CompletePlan => c
    case finalPlan: PartialPlan if finalPlan.input.isEmpty =>
      CompletePlan(finalPlan.curPlan, finalPlan.replaced)
    // single table case.
    case partial: PartialPlan if partial.curPlan == null && partial.input.size == 1 =>
      val table = partial.input.head
      val pTabOrIndex = RuleUtils.chooseIndexForFilter(table, partial.conditions)
      val replacement = if (partial.conditions.nonEmpty) {
        plans.logical.Filter(partial.conditions.reduceLeft(expressions.And),
          pTabOrIndex.map(_.index).getOrElse(table))
      } else {
        pTabOrIndex.map(_.index).getOrElse(table)
      }
      CompletePlan(replacement, pTabOrIndex.toSeq)
    case partial: PartialPlan => apply(partial, withFilters)
  }

  // todo: make it abstract.
  protected def apply(part: PartialPlan, withFilters: Boolean)(
      implicit snappySession: SnappySession): SubPlan = part

  def shortName: String

  def unapply(input: String): Option[JoinOrderStrategy] =
    if (shortName == input) Some(this) else None
}

object JoinOrderStrategy {

  private def defaultSeq: Seq[JoinOrderStrategy] = Seq(
    Replicates,
    ColocatedWithFilters,
    LargestColocationChain,
    NonColocated,
    ApplyRest // alert!! must be last rule and shouldn't be skipped.
  )

  def getJoinOrderHints(implicit snappySession: SnappySession): Seq[JoinOrderStrategy] = {
    snappySession.queryHints.get(JoinOrder) match {
      case null => defaultSeq
      case hints =>
        parse(hints.split(",")) ++
            Some(ApplyRest) // alert!! must be last rule and shouldn't be skipped.
    }
  }

  private def parse(input: Seq[String]): Seq[JoinOrderStrategy] = input.map {
    case ContinueOptimizations(c) => c
    case IncludeGeneratedPaths(c) => c
    case Replicates(c) => c
    case ColocatedWithFilters(c) => c
    case LargestColocationChain(c) => c
    case NonColocated(c) => c
    case c => throw new Exception(s"Unknown JoinOrderStrategy [ $c ]")
  }
}

/**
  * Put replicated tables with filters first.
  * If we find only one replicated table with filter, we try that with
  * largest colocated group.
  */
case object Replicates extends JoinOrderStrategy {
  override def shortName: String = ""// JOS.ReplicateWithFilters

  implicit def addToDef(newPlan: PartialPlan, repTab: LogicalPlan): PartialPlan = newPlan.copy(
    replicates = newPlan.replicates.filterNot(_ == repTab))

  override def apply(partial: PartialPlan, withFilters: Boolean)
      (implicit snappySession: SnappySession): SubPlan =
    (partial.replicates /: partial) { case p => RuleUtils.applyDefaultAction(p, withFilters) }
}

/**
  * Pick the current colocated group and put tables with filters with the currently built plan.
  */
case object ColocatedWithFilters extends JoinOrderStrategy {
  override def shortName: String = ""// JOS.CollocatedWithFilters

  implicit def addToDef(newPlan: PartialPlan, replacement: Replacement): PartialPlan = newPlan

  override def apply(partial: PartialPlan, _ignored: Boolean)
      (implicit snappySession: SnappySession): SubPlan =
    if (partial.colocatedGroups.isEmpty) {
      partial
    } else {
      (partial.currentColocatedGroup.chain /: partial) {
        case a => RuleUtils.applyDefaultAction(a, withFilters = true)
      }
    }
}

/**
  * Put rest of the colocated table joins after applying ColocatedWithFilters.
  */
case object LargestColocationChain extends JoinOrderStrategy {
  override def shortName: String = ""// JOS.LargestCollocationChain

  override def apply(partial: PartialPlan, _ignored: Boolean)
      (implicit snappySession: SnappySession): SubPlan = {
    if (partial.colocatedGroups.isEmpty) {
      return partial
    }
    (partial.currentColocatedGroup.chain /: partial) {
      case (finalPlan, replacement: Replacement) =>
        val joinRefs = finalPlan.outputSet ++ replacement.table.outputSet
        val (pTabJoinConditions, otherJoinConditions) = RuleUtils.partitionBy(joinRefs,
          finalPlan.conditions)
        if (pTabJoinConditions.nonEmpty || finalPlan.curPlan == null) {
          finalPlan.copy(
            curPlan = RuleUtils.createJoin(finalPlan.curPlan, replacement.index,
              pTabJoinConditions),
            replaced = finalPlan.replaced ++ Some(replacement),
            outputSet = joinRefs,
            input = finalPlan.input.filterNot(_ == replacement.table),
            conditions =
                if (finalPlan.curPlan == null) finalPlan.conditions else otherJoinConditions)
        } else {
          finalPlan
        }
    }
  }
}

/**
  * Tables considered non-colocated according to currentColocatedGroup with Filters are put into
  * join condition.
  */
case object NonColocated extends JoinOrderStrategy {
  override def shortName: String = ""// JOS.NonCollocatedWithFilters

  implicit def addToDef(newPlan: PartialPlan, pTab: LogicalPlan): PartialPlan = newPlan.copy(
    partitioned = newPlan.partitioned.filterNot(_ == pTab))

  override def apply(partial: PartialPlan, withFilters: Boolean)
      (implicit snappySession: SnappySession): SubPlan =
    if (partial.colocatedGroups.nonEmpty) {
      // if applied first, then consider topmost colocated group as potential candidate.
      // in case that doesn't turns out right, we might have to try to reapply all the rules
      // with next colocation scheme much like RuleExecutor which is fixedPoint. Right now not
      // handling any of it because Spark is getting a CBO as we speak and we might have to redo
      // this logic anyway.
      val nonColocated = partial.partitioned.filter(p =>
        !partial.currentColocatedGroup.chain.exists(_ != p))

      (nonColocated /: partial) { case a => RuleUtils.applyDefaultAction(a, withFilters) }
    } else {
      partial
    }

}

/**
  * Simply assemble rest of the tables as per user defined join order.
  */
case object ApplyRest extends JoinOrderStrategy {
  override def shortName: String = ""

  override def apply(partial: PartialPlan, withFilters: Boolean)
      (implicit snappySession: SnappySession): SubPlan = {

    var newPlan: SubPlan = partial
    newPlan = Replicates(newPlan, withFilters = false)
    newPlan = NonColocated(newPlan, withFilters = false)

    newPlan match {
      case p: PartialPlan =>
        // if none of the rules got applied, nothing is possible then. We just reassemble & return.
        RuleUtils.returnPlan(p)
      case _ => newPlan
    }
  }
}

/**
  * This doesn't require any alteration to joinOrder as such.
  */
case object ContinueOptimizations extends JoinOrderStrategy {
  override def shortName: String = ""// HintNames.JoinOrder_ContinueOptimizations
}

/**
  * This hint too doesn't require any implementation as such.
  */
case object IncludeGeneratedPaths extends JoinOrderStrategy {
  override def shortName: String = ""// HintNames.JoinOrder_IncludeGeneratedPaths
}
