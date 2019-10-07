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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.sources.{ConnectionProperties, DestroyRelation, JdbcExtendedUtils}
import org.apache.spark.sql.types.StructType

/**
 * Generated code plan for bulk inserts/puts into a row table.
 */
case class RowInsertExec(child: SparkPlan, putInto: Boolean,
    partitionColumns: Seq[String], partitionExpressions: Seq[Expression],
    numBuckets: Int, isPartitioned: Boolean, tableSchema: StructType,
    relation: Option[DestroyRelation], onExecutor: Boolean, resolvedName: String,
    connProps: ConnectionProperties) extends RowExec {

  override def opType: String = if (putInto) "Put" else "Inserted"

  override protected def isInsert: Boolean = true

  override def nodeName: String = if (putInto) "RowPut" else "RowInsert"

  override protected def doProduce(ctx: CodegenContext): String = {
    val rowInsertStr = JdbcExtendedUtils.getInsertOrPutString(resolvedName,
      tableSchema, putInto, escapeQuotes = true)
    super.doProduce(ctx, rowInsertStr)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode],
      row: ExprCode): String = super.doConsume(ctx, input, tableSchema)
}
