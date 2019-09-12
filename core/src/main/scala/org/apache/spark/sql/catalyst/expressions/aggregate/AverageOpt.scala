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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Add, Expression}
import org.apache.spark.sql.types.{ByteType, CalendarIntervalType, DecimalType, ShortType}

/**
 * Optimizes Spark's [[Average]] to reduce generated code.
 */
class AverageOpt(child: Expression) extends Average(child) with ImplicitCastForAdd {

  override lazy val updateExpressions: Seq[Expression with Serializable] = {
    val sum = aggBufferAttributes.head
    val sumDataType = sum.dataType
    val add = childWithCast(child, sumDataType)
    val count = CountAdd(aggBufferAttributes(1), add :: Nil)
    val avg = new AvgAdd(sum, add)
    avg.count = count
    count.avg = avg
    avg :: count :: Nil
  }

  override lazy val mergeExpressions: Seq[Expression] = {
    val sum = aggBufferAttributes.head
    val count = aggBufferAttributes(1)
    Seq(new SumAdd(sum.left, sum.right), new SumAdd(count.left, count.right))
  }
}

class AvgAdd(sum: Expression, add: Expression) extends Add(sum, add) {

  private[aggregate] var count: CountAdd = _

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val newExp = new AvgAdd(newArgs(0).asInstanceOf[Expression],
      newArgs(1).asInstanceOf[Expression])
    newExp.count = count
    newExp.count.avg = newExp
    newExp
  }

  override def withNewChildren(newChildren: Seq[Expression]): Expression = {
    val newExp = new AvgAdd(newChildren.head, newChildren(1))
    newExp.count = count
    newExp.count.avg = newExp
    newExp
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val sumEv = sum.genCode(ctx)
    val addEv = add.genCode(ctx)
    val sumVar = sumEv.value
    val addVar = addEv.value
    val nonNullCode = dataType match {
      case _: DecimalType => s"$sumVar = $sumVar != null ? $sumVar.$$plus($addVar) : $addVar;"
      case ByteType | ShortType => s"$sumVar = (${ctx.javaType(dataType)})($sumVar + $addVar);"
      case CalendarIntervalType => s"$sumVar = $sumVar != null ? $sumVar.add($addVar) : $addVar;"
      case _ => s"$sumVar += $addVar;"
    }
    // evaluate count inside and let "isNull" be determined from count
    val countVar = ctx.freshName("avgCount")
    ctx.addMutableState("long", countVar, "")

    val countEv = count.count.genCode(ctx)
    val countEvVar = countEv.value
    val code = if (add.nullable) {
      s"""
        ${sumEv.code}
        ${countEv.code}
        ${addEv.code}
        if (!${addEv.isNull}) {
          $nonNullCode
          $countVar = ++$countEvVar;
        }
      """
    } else {
      s"""
        ${sumEv.code}
        ${countEv.code}
        ${addEv.code}
        $nonNullCode
        $countVar = ++$countEvVar;
      """
    }
    count.countEv = ExprCode(code = "", isNull = "false", value = countVar)
    count.genCtx = ctx
    sumEv.copy(code = code, isNull = s"($countEvVar == 0)")
  }
}
