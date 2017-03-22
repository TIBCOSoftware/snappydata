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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, LiteralValue, ParamLiteral, ParamConstantsValue, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.SnappySession.CachedKey
import org.apache.spark.sql.catalyst.plans.physical.Partitioning

import com.pivotal.gemfirexd.internal.iapi.sql.ParameterValueSet

case class CachedPlanHelperExec(childPlan: CodegenSupport)
    extends UnaryExecNode with CodegenSupport {

  override def output: Seq[Attribute] = child.output

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def child: SparkPlan = childPlan

  override def inputRDDs(): Seq[RDD[InternalRow]] = childPlan.inputRDDs()

  override protected def doProduce(ctx: CodegenContext): String = {
    CachedPlanHelperExec.addReferencesToCurrentKey(ctx.references)
    childPlan.produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String =
    parent.doConsume(ctx, input, row)

  override protected def doExecute(): RDD[InternalRow] = childPlan.execute()
}

object CachedPlanHelperExec extends Logging {
  var contextReferences: mutable.Map[CachedKey,
      mutable.MutableList[mutable.ArrayBuffer[Any]]] = new
          mutable.HashMap[CachedKey, mutable.MutableList[mutable.ArrayBuffer[Any]]]

  def addReferencesToCurrentKey(references: mutable.ArrayBuffer[Any]): Unit = {
    var x = contextReferences.getOrElse(SnappySession.currCachedKey, {
      val l = new mutable.MutableList[ArrayBuffer[Any]]
      logDebug(s"Putting new reference list = ${l} against key = ${SnappySession.currCachedKey}")
      contextReferences.put(SnappySession.currCachedKey, l)
      l
    })
    x += references
  }

  private def allLiterals(): Array[LiteralValue] = {
    var lls = new ArrayBuffer[LiteralValue]()
    val refs = contextReferences.getOrElse(SnappySession.currCachedKey,
      throw new IllegalStateException("Expected a cached reference object"))
    refs.foreach(ctxrefs => {
      ctxrefs.filter(
        { p: Any => p.isInstanceOf[LiteralValue] }).map(lls +=
          _.asInstanceOf[LiteralValue])
    })
    lls.sortBy(_.position).toArray
  }

  def allParamConstants(): Array[ParamConstantsValue] = {
    var lls = new ArrayBuffer[ParamConstantsValue]()
    val refs = contextReferences.getOrElse(SnappySession.currCachedKey,
      throw new IllegalStateException("Expected a cached reference object"))
    refs.foreach(ctxrefs => {
      ctxrefs.filter(
        { p: Any => p.isInstanceOf[ParamConstantsValue] }).map(lls +=
          _.asInstanceOf[ParamConstantsValue])
    })
    lls.sortBy(_.position).toArray
  }

  def collectParamLiteralNodes(lp: LogicalPlan, literals: Array[LiteralValue]): Unit = {
    lp transformAllExpressions {
      case p: ParamLiteral => {
        literals(p.pos - 1).value = p.l.value
        p
      }
    }
  }

  def collectParamConstantsValueNodes(pvs: ParameterValueSet,
      allParamConstantsValue: Array[ParamConstantsValue]): Unit = {
    (0 until pvs.getParameterCount) foreach(i => {
      allParamConstantsValue(i).setValue(pvs.getParameter(i))
    })
  }

  def replaceConstants(currLogicalPlan: LogicalPlan): Unit = {
    val literals = allLiterals()
    if (literals.length > 0) {
      collectParamLiteralNodes(currLogicalPlan, literals)
    }
  }

  def replaceParamConstants(pvs: ParameterValueSet): Unit = {
    val allParamConstantsValue = allParamConstants()
    assert(allParamConstantsValue != null)
    assert(pvs != null)
    // Note: Reason if allParamConstantsValue.size is zero
    // ParamLiteral.doGenCode will not generate ParamConstantsValue if
    // any ParamConstants has NullType, as set in SnappyParser.
    // DataType in ParamConstants needs to be updated.
    assert(allParamConstantsValue.length == pvs.getParameterCount,
      s"Unequal param count: pvs-count=${pvs.getParameterCount}" +
          s" param-count=${allParamConstantsValue.length}")
    if (allParamConstantsValue.length > 0) {
      collectParamConstantsValueNodes(pvs, allParamConstantsValue)
    }
  }
}
