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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{DataType, LongType}

/**
 * Extends Spark's [[Count]] to reduce generated code.
 */
class CountExp(children: Seq[Expression]) extends Count(children) {

  override lazy val updateExpressions: Seq[Expression] = {
    CountAdd(aggBufferAttributes.head, children.toList) :: Nil
  }
}

case class CountAdd(count: Expression, exprs: List[Expression]) extends Expression {

  override def children: Seq[Expression] = count :: exprs

  override def foldable: Boolean = false

  override def nullable: Boolean = false

  override def dataType: DataType = LongType

  private lazy val nullableChildren = exprs.filter(_.nullable)

  @transient private[aggregate] var countEv: ExprCode = _
  @transient private[aggregate] var genCtx: CodegenContext = _
  private[aggregate] var avg: AvgAdd = _

  override def makeCopy(newArgs: Array[AnyRef]): Expression = {
    val newExp = CountAdd(newArgs(0).asInstanceOf[Expression],
      newArgs(1).asInstanceOf[List[Expression]])
    newExp.avg = avg
    if (avg ne null) newExp.avg.count = newExp
    newExp
  }

  override def withNewChildren(newChildren: Seq[Expression]): Expression = {
    val newExp = CountAdd(newChildren.head, newChildren.tail.toList)
    newExp.avg = avg
    if (avg ne null) newExp.avg.count = newExp
    newExp
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    if ((countEv eq null) || (genCtx ne ctx)) {
      countEv = count.genCode(ctx)
      genCtx = ctx
      val countVar = countEv.value
      val code = if (nullableChildren.isEmpty) s"${countEv.code}\n$countVar++;"
      else {
        val exprs = ctx.generateExpressions(nullableChildren)
        countEv.code + exprs.map(_.code).mkString("\n", "\n", "\n") +
            exprs.map("!" + _.isNull).mkString("if (", " || ", s") $countVar++;")
      }
      countEv = countEv.copy(code = code)
    }
    countEv
  }

  override def eval(input: InternalRow): Any = {
    val countVal = count.eval(input)
    if (nullableChildren.isEmpty) countVal.asInstanceOf[Long] + 1L
    else {
      val vals = exprs.map(_.eval(input))
      for (v <- vals) {
        if (v != null) return countVal.asInstanceOf[Long] + 1L
      }
      countVal
    }
  }
}
