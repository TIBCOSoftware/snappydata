/*
 * Copyright (c) 2017-2020 TIBCO Software Inc. All rights reserved.
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
package org.apache.spark.sql.internal

import org.apache.spark.sql.{SparkInternals, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogColumnStat
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan, Pivot}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{DataType, StructField}

/**
 * Implementation of [[SparkInternals]] for Spark 2.4.4.
 */
class Spark244Internals extends Spark232Internals {

  override def columnStatToMap(stat: Any, colName: String,
      dataType: DataType): Map[String, String] = {
    stat.asInstanceOf[CatalogColumnStat].toMap(colName)
  }

  override def columnStatFromMap(table: String, field: StructField,
      map: Map[String, String]): Option[ColumnStat] = {
    CatalogColumnStat.fromMap(table, field.name, map).map(_.toPlanStat(field.name, field.dataType))
  }

  private def exprValue(v: String, javaClass: Class[_]): ExprValue = v match {
    case "false" => FalseLiteral
    case "true" => TrueLiteral
    case _ => VariableValue(v, javaClass)
  }

  override def newExprCode(code: String, isNull: String,
      value: String, javaClass: Class[_]): ExprCode = {
    ExprCode(CodeBlock(code :: Nil, EmptyBlock :: Nil),
      isNull = exprValue(isNull, java.lang.Boolean.TYPE),
      value = exprValue(value, javaClass))
  }

  override def copyExprCode(ev: ExprCode, code: String, isNull: String,
      value: String, javaClass: Class[_]): ExprCode = {
    val codeBlock =
      if (code eq null) ev.code
      else if (code.isEmpty) EmptyBlock
      else CodeBlock(code :: Nil, EmptyBlock :: Nil)
    ev.copy(codeBlock,
      if (isNull ne null) VariableValue(isNull, java.lang.Boolean.TYPE) else ev.isNull,
      if (value ne null) VariableValue(value, javaClass) else ev.value)
  }

  override def resetCode(ev: ExprCode): Unit = {
    ev.code = EmptyBlock
  }

  override def exprCodeIsNull(ev: ExprCode): String = ev.isNull.code

  override def exprCodeValue(ev: ExprCode): String = ev.value.code

  override def javaType(dt: DataType, ctx: CodegenContext): String = CodeGenerator.javaType(dt)

  override def boxedType(javaType: String, ctx: CodegenContext): String = {
    CodeGenerator.boxedType(javaType)
  }

  override def defaultValue(dt: DataType, ctx: CodegenContext): String = {
    CodeGenerator.defaultValue(dt)
  }

  override def isPrimitiveType(javaType: String, ctx: CodegenContext): Boolean = {
    CodeGenerator.isPrimitiveType(javaType)
  }

  override def primitiveTypeName(javaType: String, ctx: CodegenContext): String = {
    CodeGenerator.primitiveTypeName(javaType)
  }

  override def getValue(input: String, dataType: DataType, ordinal: String,
      ctx: CodegenContext): String = {
    CodeGenerator.getValue(input, dataType, ordinal)
  }

  override def optionalQueryPreparations(session: SparkSession): Seq[Rule[SparkPlan]] = Nil

  override def newPivot(groupByExprs: Seq[NamedExpression], pivotColumn: Expression,
      pivotValues: Seq[Expression], aggregates: Seq[Expression], child: LogicalPlan): Pivot = {
    Pivot(if (groupByExprs.isEmpty) None else Some(groupByExprs), pivotColumn, pivotValues,
      aggregates, child)
  }
}
