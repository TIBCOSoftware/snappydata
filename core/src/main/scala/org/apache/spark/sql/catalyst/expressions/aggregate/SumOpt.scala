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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Add, Cast, Expression}
import org.apache.spark.sql.types._

/**
 * Optimizes Spark's [[Sum]] to reduce generated code.
 */
class SumOpt(child: Expression) extends Sum(child) with ImplicitCastForAdd {

  override lazy val updateExpressions: Seq[Expression] = {
    val sum = aggBufferAttributes.head
    val add = childWithCast(child, sum.dataType)
    new SumAdd(sum, add) :: Nil
  }
}

/**
 * Common trait for aggregates that will do the required cast to result type
 * either implicitly or in their generated code without an explicit CAST operator.
 */
trait ImplicitCastForAdd {

  def childWithCast(child: Expression, requiredType: DataType): Expression = {
    // shave off unnecessary cast to long for integral types
    val noCast = child match {
      case Cast(c, LongType) if c.dataType.isInstanceOf[IntegralType] => c
      case _ => child
    }
    // cast to double/long will happen implicitly in java code
    noCast.dataType match {
      case d if d == requiredType => noCast
      case _: IntegralType if requiredType == DoubleType || requiredType == LongType => noCast
      case _ => Cast(noCast, requiredType)
    }
  }
}

class SumAdd(sum: Expression, add: Expression) extends Add(sum, add) {

  protected def genCodeForNonNullAdditive(ctx: CodegenContext, dataType: DataType,
      sumEv: ExprCode, addVar: String, resultIsNull: String): String = {
    val sumVar = sumEv.value
    val resultFalse = if (resultIsNull == "false") "" else s"$resultIsNull = false;\n"
    val resultCode = dataType match {
      case _: DecimalType => s"$sumVar = $sumVar.$$plus($addVar);"
      case ByteType | ShortType => s"$sumVar = (${ctx.javaType(dataType)})($sumVar + $addVar);"
      case CalendarIntervalType => s"$sumVar = $sumVar.add($addVar);"
      case _ => s"$sumVar += $addVar;"
    }
    if (sumEv.isNull == "false") resultFalse + resultCode
    else {
      s"""${resultFalse}if (${sumEv.isNull}) {
          $sumVar = $addVar;
        } else {
          $resultCode
        }"""
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val sumEv = sum.genCode(ctx)
    val addEv = add.genCode(ctx)
    val (evIsNull, isNullCode) =
      if (nullable) ev.isNull -> s"boolean ${ev.isNull} = ${sumEv.isNull};\n" else "false" -> ""
    val nonNullCode = genCodeForNonNullAdditive(ctx, dataType, sumEv, addEv.value, evIsNull)
    val code = if (add.nullable && addEv.isNull != "false") {
      s"""
        ${sumEv.code}$isNullCode
        ${addEv.code}
        if (!${addEv.isNull}) {
          $nonNullCode
        }
      """
    } else {
      s"""
        ${sumEv.code}$isNullCode
        ${addEv.code}
        $nonNullCode
      """
    }
    ev.copy(code = code, isNull = evIsNull, value = sumEv.value)
  }
}
