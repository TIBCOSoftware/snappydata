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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.SnappySession.CachedKey
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, LiteralValue, ParamLiteral, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.CachedPlanHelperExec.REFERENCES_KEY

case class CachedPlanHelperExec(childPlan: CodegenSupport, @transient session: SnappySession)
    extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def child: SparkPlan = childPlan

  override def inputRDDs(): Seq[RDD[InternalRow]] = childPlan.inputRDDs()

  override protected def doProduce(ctx: CodegenContext): String = {
    // cannot flatten out the references buffer here since the values may not
    // have been populated yet
    session.getContextObject[ArrayBuffer[ArrayBuffer[Any]]](REFERENCES_KEY) match {
      case Some(references) => references += ctx.references
      case None => session.addContextObject(REFERENCES_KEY,
        ArrayBuffer[ArrayBuffer[Any]](ctx.references))
    }
    childPlan.produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String =
    parent.doConsume(ctx, input, row)

  override protected def doExecute(): RDD[InternalRow] = childPlan.execute()
}

object CachedPlanHelperExec extends Logging {

  val REFERENCES_KEY = "TokenizationReferences"

  private[sql] def allLiterals(allReferences: Seq[Seq[Any]]): Array[LiteralValue] = {
    allReferences.flatMap(_.collect {
      case l: LiteralValue => l
    }).toSet[LiteralValue].toArray.sortBy(_.position)
  }

  def collectParamLiteralNodes(lp: LogicalPlan, literals: Array[LiteralValue]): Unit = {
    lp transformAllExpressions {
      case p: ParamLiteral => {
        if (p.pos <= literals.length) {
        assert(p.pos == literals(p.pos - 1).position)
          literals(p.pos - 1).value = p.l.value
        }
        p
      }
    }
  }

  def replaceConstants(literals: Array[LiteralValue], currLogicalPlan: LogicalPlan): Unit = {
    if (literals.length > 0) {
      collectParamLiteralNodes(currLogicalPlan, literals)
    }
  }
}
