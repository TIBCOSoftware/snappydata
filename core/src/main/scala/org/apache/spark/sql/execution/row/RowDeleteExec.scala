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
 * Generated code plan for deletes in a row table.
 */
case class RowDeleteExec(child: SparkPlan, resolvedName: String,
    partitionColumns: Seq[String], partitionExpressions: Seq[Expression],
    numBuckets: Int, isPartitioned: Boolean, tableSchema: StructType,
    relation: Option[DestroyRelation], keyColumns: Seq[Attribute],
    connProps: ConnectionProperties, onExecutor: Boolean) extends RowExec {

  override protected def opType: String = "Delete"

  override def nodeName: String = "RowDelete"

  override protected def doProduce(ctx: CodegenContext): String = {
    val sql = new StringBuilder
    sql.append("DELETE FROM ").append(quotedName(resolvedName, escapeQuotes = true))
        .append(" WHERE ")
    JdbcExtendedUtils.fillColumnsClause(sql, keyColumns.map(_.name), escapeQuotes = true)
    super.doProduce(ctx, sql.toString())
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode],
      row: ExprCode): String = {
    ctx.INPUT_ROW = null
    ctx.currentVars = input
    // bind the key columns
    val stmtInput = ctx.generateExpressions(keyColumns.map(
      u => ExpressionCanonicalizer.execute(BindReferences.bindReference(u, child.output))))
    ctx.currentVars = null

    val stmtSchema = StructType.fromAttributes(keyColumns)
    super.doConsume(ctx, stmtInput, stmtSchema)
  }
}
