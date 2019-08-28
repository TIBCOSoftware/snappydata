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
package org.apache.spark.sql.execution.row

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, ExpressionCanonicalizer}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sources.JdbcExtendedUtils.quotedName
import org.apache.spark.sql.sources.{ConnectionProperties, DestroyRelation, JdbcExtendedUtils}
import org.apache.spark.sql.types.StructType

/**
 * Generated code plan for updates in a row table.
 */
case class RowUpdateExec(child: SparkPlan, resolvedName: String,
    partitionColumns: Seq[String], partitionExpressions: Seq[Expression],
    numBuckets: Int, isPartitioned: Boolean, tableSchema: StructType,
    relation: Option[DestroyRelation], updateColumns: Seq[Attribute],
    updateExpressions: Seq[Expression], keyColumns: Seq[Attribute],
    connProps: ConnectionProperties, onExecutor: Boolean) extends RowExec {

  assert(updateColumns.length == updateExpressions.length)

  override protected def opType: String = "Update"

  override def nodeName: String = "RowUpdate"

  override protected def doProduce(ctx: CodegenContext): String = {
    val sql = new StringBuilder
    sql.append("UPDATE ").append(quotedName(resolvedName, escapeQuotes = true)).append(" SET ")
    JdbcExtendedUtils.fillColumnsClause(sql, updateColumns.map(_.name),
      escapeQuotes = true, separator = ", ")
    sql.append(" WHERE ")
    JdbcExtendedUtils.fillColumnsClause(sql, keyColumns.map(_.name), escapeQuotes = true)
    super.doProduce(ctx, sql.toString())
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode],
      row: ExprCode): String = {
    ctx.INPUT_ROW = null
    ctx.currentVars = input
    // bind the update expressions followed by key columns
    val allExpressions = updateExpressions ++ keyColumns
    val boundUpdateExpr = allExpressions.map(
      u => ExpressionCanonicalizer.execute(BindReferences.bindReference(u, child.output)))
    val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(boundUpdateExpr)
    val effectiveCodes = subExprs.codes.mkString("\n")
    val stmtInput = ctx.withSubExprEliminationExprs(subExprs.states) {
      boundUpdateExpr.map(_.genCode(ctx))
    }
    ctx.currentVars = null

    val stmtSchema = StructType(getUpdateSchema(updateExpressions) ++
        StructType.fromAttributes(keyColumns))
    val code = super.doConsume(ctx, stmtInput, stmtSchema)
    if (effectiveCodes.isEmpty) code else effectiveCodes + code
  }
}
