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
package org.apache.spark.sql.execution

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.SnappySession.CachedKey
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, LiteralValue, ParamLiteral, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.CachedPlanHelperExec.REFERENCES_KEY
import org.apache.spark.sql.execution.joins.BroadcastHashJoinExec

case class CachedPlanHelperExec(childPlan: CodegenSupport, @transient session: SnappySession)
    extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def child: SparkPlan = childPlan

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    val childRDDs = childPlan.inputRDDs()
    childRDDs
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    // cannot flatten out the references buffer here since the values may not
    // have been populated yet
    session.getContextObject[ArrayBuffer[ArrayBuffer[Any]]](REFERENCES_KEY) match {
      case Some(references) => references += ctx.references
      case None => session.addContextObject(REFERENCES_KEY,
        ArrayBuffer[ArrayBuffer[Any]](ctx.references))
    }
    // keep a map of the first broadcasthashjoinexec plan and the corresponding ref array
    // collect the broadcasthashjoins in this wholestage and the references array
    var nextStageStarted = false
    var alreadyGotBroadcastNode = false
    childPlan transformDown {
      case bchj: BroadcastHashJoinExec => {
        println(s"Got a bchj = ${bchj} and nextstagestarted = ${nextStageStarted}")
        if (!nextStageStarted) {
          // The below assertion was kept thinking that there will be just one
          // broadcasthashjoin in one stage but there can be more than one and so
          // removing the assertion and instead adding this information in the context
          // indicating the above layer not to cache this plan.
          // assert(!alreadyGotBroadcastNode, "only one broadcast plans expected per wholestage")
          if (alreadyGotBroadcastNode) {
            session.getContextObject[mutable.Map[BroadcastHashJoinExec, ArrayBuffer[Any]]](
              CachedPlanHelperExec.NOCACHING_KEY) match {
              case Some(flag) => true
              case None => session.addContextObject(CachedPlanHelperExec.NOCACHING_KEY, true)
            }
          }
          session.getContextObject[mutable.Map[BroadcastHashJoinExec, ArrayBuffer[Any]]](
            CachedPlanHelperExec.BROADCASTS_KEY) match {
            case Some(map) => map += bchj -> ctx.references
            case None => session.addContextObject(CachedPlanHelperExec.BROADCASTS_KEY,
              mutable.Map(bchj -> ctx.references))
          }
          alreadyGotBroadcastNode = true
        }
        bchj transformAllExpressions {
          case pl: ParamLiteral =>
            session.getContextObject[mutable.Map[BroadcastHashJoinExec, ArrayBuffer[Any]]](
              CachedPlanHelperExec.NOCACHING_KEY) match {
              case Some(flag) => true
              case None => session.addContextObject(CachedPlanHelperExec.NOCACHING_KEY, true)
            }
            pl
        }
        bchj
      }
      case cp: CachedPlanHelperExec => {
        nextStageStarted = true
      }
      cp
    }
    // val map = session.getContextObject[mutable.Map[BroadcastHashJoinExec, ArrayBuffer[Any]]](
    //  CachedPlanHelperExec.BROADCASTS_KEY)
//    if (map != null && map.nonEmpty) {
//      println(s"Priniting broadcast map")
//      println(map.iterator.foreach(println))
//    }

    childPlan.produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String =
    parent.doConsume(ctx, input, row)

  override protected def doExecute(): RDD[InternalRow] = childPlan.execute()
}

object CachedPlanHelperExec extends Logging {

  val REFERENCES_KEY    = "TokenizationReferences"
  val BROADCASTS_KEY    = "TokenizationBroadcasts"
  val WRAPPED_CONSTANTS = "TokenizedConstants"
  val NOCACHING_KEY    =  "TokenizationNoCaching"

  private[sql] def allLiterals(allReferences: Seq[Seq[Any]]): Array[LiteralValue] = {
    allReferences.flatMap(_.collect {
      case l: LiteralValue => l
    }).toSet[LiteralValue].toArray.sortBy(_.position)
  }

  def replaceConstants(literals: Array[LiteralValue], currLogicalPlan: LogicalPlan,
      newpls: mutable.ArrayBuffer[ParamLiteral]): Unit = {
    literals.foreach { case lv@LiteralValue(_, _, p) =>
      lv.value = newpls.find(_.pos == p).get.value
      val y = newpls.find(_.pos == p).get.value
    }
  }
}
