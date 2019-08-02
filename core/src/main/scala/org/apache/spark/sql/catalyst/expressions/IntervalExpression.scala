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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{AbstractDataType, CalendarIntervalType, DataType, LongType}
import org.apache.spark.unsafe.types.CalendarInterval

case class IntervalExpression(children: Seq[Expression], units: Seq[Long])
    extends Expression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] =
    if (children.length == 1) LongType :: Nil else Seq.fill(children.length)(LongType)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.isEmpty) {
      TypeCheckResult.TypeCheckFailure(s"At least one child expression required for $toString")
    } else if (children.length != units.length) {
      TypeCheckResult.TypeCheckFailure(
        s"Number of children ${children.length} must match units ${units.length}: $toString")
    } else if (children.length == 1) {
      val dt = children.head.dataType
      if (dt == LongType) TypeCheckResult.TypeCheckSuccess
      else TypeCheckResult.TypeCheckFailure(s"Expected LongType input but got $dt for $toString")
    } else {
      if (children.forall(_.dataType == LongType)) TypeCheckResult.TypeCheckSuccess
      else {
        TypeCheckResult.TypeCheckFailure(
          s"Expected LongType inputs but got ${children.map(_.dataType)} for $toString")
      }
    }
  }

  override def dataType: DataType = CalendarIntervalType

  override def foldable: Boolean =
    if (children.length == 1) children.head.foldable else children.forall(_.foldable)

  override def deterministic: Boolean =
    if (children.length == 1) children.head.deterministic else children.forall(_.deterministic)

  override def nullable: Boolean =
    if (children.length == 1) children.head.nullable else children.exists(_.nullable)

  override def eval(input: InternalRow): Any = {
    val v = children.head.eval(input)
    if (v == null) return null
    var result = evalSingle(v.asInstanceOf[Long], units.head)
    if (children.length == 1) result
    else {
      for (i <- 1 until children.length) {
        val r = children(i).eval(input)
        if (r == null) result = null
        else if (result ne null) {
          result = result.add(evalSingle(r.asInstanceOf[Long], units(i)))
        }
      }
      result
    }
  }

  private def evalSingle(v: Long, unit: Long): CalendarInterval = {
    var months = 0
    var micros = 0L
    unit match {
      case -1 => months = v.toInt * 12
      case -2 => months = v.toInt
      case _ => micros = v * unit
    }
    new CalendarInterval(months, micros)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val months = ctx.freshName("months")
    val micros = ctx.freshName("micros")
    val intervalClass = classOf[CalendarInterval].getName
    val nullable = this.nullable
    if (children.length == 1) {
      val childGen = children.head.genCode(ctx)
      val childIsNull = if (nullable) childGen.isNull else "false"
      val code =
        s"""
           |${childGen.code}
           |$intervalClass ${ev.value};
           |${doGenCodeSingle(childGen.value, childIsNull, ev.value,
              units.head.toString, months, micros, intervalClass)}
        """.stripMargin
      if (childIsNull == "false") {
        ev.copy(code = code, isNull = "false")
      } else {
        ev.copy(code = code + s"boolean ${ev.isNull} = ${ev.value} == null;\n")
      }
    } else {
      val index = ctx.freshName("i")
      val unitsArr = ctx.addReferenceObj("unitsArr", units.toArray, "long[]")
      val childValueArr = ctx.freshName("valueArr")
      val childIsNullArr = ctx.freshName("isNullArr")
      val result = ctx.freshName("result")
      val childGens = children.map(_.genCode(ctx))
      val size = childGens.length
      val initArr = childGens.indices.map { i =>
        s"""
           |$childValueArr[$i] = ${childGens(i).value};
           |${if (nullable) s"$childIsNullArr[$i] = ${childGens(i).isNull};" else ""}
        """.stripMargin
      }.mkString("")
      val childIsNull = if (nullable) s"$childIsNullArr[$index]" else "false"
      val code =
        s"""
           |${childGens.map(_.code).mkString("\n")}
           |long[] $childValueArr = new long[$size];
           |${if (nullable) s"boolean[] $childIsNullArr = new boolean[$size];" else ""}
           |$intervalClass ${ev.value} = null;
           |$initArr
           |for (int $index = 0; $index < $size; $index++) {
           |  $intervalClass $result;
           |  ${doGenCodeSingle(s"$childValueArr[$index]", childIsNull, result,
                s"$unitsArr[$index]", months, micros, intervalClass)}
           |  if ($result == null) {
           |    ${ev.value} = null;
           |    break;
           |  }
           |  ${ev.value} = ${ev.value} != null ? ${ev.value}.add($result) : $result;
           |}
        """.stripMargin
      if (nullable) ev.copy(code = code + s"boolean ${ev.isNull} = ${ev.value} == null;\n")
      else ev.copy(code = code, isNull = "false")
    }
  }

  private def doGenCodeSingle(value: String, isNull: String, result: String,
      unit: String, months: String, micros: String, intervalClass: String): String = {
    s"""
       |if ($isNull) {
       |  $result = null;
       |} else {
       |  int $months = 0;
       |  long $micros = 0L;
       |  if ($unit == -1) $months = ((int)$value) * 12;
       |  else if ($unit == -2) $months = (int)$value;
       |  else $micros = $value * $unit;
       |  $result = new $intervalClass($months, $micros);
       |}
    """.stripMargin
  }
}
