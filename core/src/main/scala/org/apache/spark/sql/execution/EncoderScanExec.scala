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
package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, BindReferences, Expression}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.types.DateType

/**
 * Efficient SparkPlan with code generation support to consume an RDD
 * that has an [[ExpressionEncoder]].
 */
case class EncoderScanExec(rdd: RDD[Any], encoder: ExpressionEncoder[Any],
    isFlat: Boolean, output: Seq[Attribute])
    extends LeafExecNode with CodegenSupport {

  override protected def doExecute(): RDD[InternalRow] = {
    rdd.mapPartitionsInternal(_.map(encoder.toRow))
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    Seq(rdd.asInstanceOf[RDD[InternalRow]])
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    val dateTimeClass = DateTimeUtils.getClass.getName.replace("$", "")
    val iterator = ctx.freshName("iterator")
    ctx.addMutableState("scala.collection.Iterator", iterator,
      s"$iterator = inputs[0];")

    val javaTypeName = encoder.clsTag.runtimeClass.getName
    val objVar = ctx.freshName("object")

    val expressions = encoder.serializer.map(
      BindReferences.bindReference(_, output))

    ctx.INPUT_ROW = null
    // for non-flat objects row cannot be null and exception
    // will be thrown right at the start
    val (nullVar, nullCheck) = if (isFlat) {
      (s"($objVar == null)", "")
    } else {
      ("false",
          s"""
             |if ($objVar == null) {
             |  throw new RuntimeException("top level null input object");
             |}""")
    }
    ctx.currentVars = Seq(ExprCode("", nullVar, objVar))
    val declarations = new StringBuilder

    def optimizeDate(expr: Expression): ExprCode = expr match {
      case s@StaticInvoke(_, _, "fromJavaDate", inputValue :: Nil, _) =>
        // optimization to re-use previous date since it may remain
        // same for a while in many cases
        val prevJavaDate = ctx.freshName("prevJavaDate")
        val prevDate = ctx.freshName("prevDate")
        declarations.append(s"java.sql.Date $prevJavaDate = null;\n")
        declarations.append(s"int $prevDate = 0;\n")
        val inputDate = inputValue.genCode(ctx)
        val javaDate = inputDate.value
        val ev = s.genCode(ctx)
        val code = if (ev.isNull == "false") {
          s"""
             |${inputDate.code}
             |int ${ev.value} = -1;
             |if ($prevJavaDate != null &&
             |    $prevJavaDate.getTime() == $javaDate.getTime()) {
             |  ${ev.value} = $prevDate;
             |} else {
             |  $prevJavaDate = $javaDate;
             |  $prevDate = $dateTimeClass.fromJavaDate($javaDate);
             |  ${ev.value} = $prevDate;
             |}
          """.stripMargin
        } else {
          s"""
             |${inputDate.code}
             |boolean ${ev.isNull};
             |int ${ev.value} = -1;
             |if (${inputDate.isNull}) {
             |  ${ev.isNull} = true;
             |} else if ($prevJavaDate != null &&
             |    $prevJavaDate.getTime() == $javaDate.getTime()) {
             |  ${ev.value} = $prevDate;
             |  ${ev.isNull} = false;
             |} else {
             |  $prevJavaDate = $javaDate;
             |  $prevDate = $dateTimeClass.fromJavaDate($javaDate);
             |  ${ev.value} = $prevDate;
             |  ${ev.isNull} = false;
             |}
          """.stripMargin
        }
        ev.copy(code = code)

      case Alias(child, _) => optimizeDate(child)

      case _ => expr.genCode(ctx)
    }

    val input = expressions.map { expr =>
      val dataType = Utils.getSQLDataType(expr.dataType)
      val ev = dataType match {
        case DateType => optimizeDate(expr)
        case _ => expr.genCode(ctx)
      }
      ev
      // The following code makes some of the Spark tests to fail
      // check org.apache.spark.sql.SnappyDataFrameSuite.except - nullability.
      // Reason was if primitives were not null checked it used to give default -1 for
      // null ints
      // Hence the below code was erronous and after fixing null handing in above date field
      // it works for all cases.
      
      /* if (ctx.isPrimitiveType(dataType)) {
        ev.copy(isNull = "false")
      } else {
        ev
      } */
    }

    s"""
       |$declarations
       |while ($iterator.hasNext()) {
       |  final $javaTypeName $objVar = ($javaTypeName)$iterator.next();
       |  $nullCheck
       |  ${consume(ctx, input).trim}
       |  if (shouldStop()) return;
       |}
    """.stripMargin
  }
}

class EncoderPlan[T](rdd: RDD[T], val encoder: ExpressionEncoder[T],
    val isFlat: Boolean, output: Seq[Attribute], session: SparkSession)
    extends LogicalRDD(output, rdd.asInstanceOf[RDD[InternalRow]])(session) {

  override def newInstance(): EncoderPlan.this.type = {
    val newRDD = super.newInstance().asInstanceOf[LogicalRDD]
    new EncoderPlan(rdd, encoder, isFlat,
      newRDD.output, session).asInstanceOf[this.type]
  }
}
