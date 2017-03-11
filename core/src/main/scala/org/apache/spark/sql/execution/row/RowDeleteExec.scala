/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences}
import org.apache.spark.sql.execution.{SparkPlan, TableMutationExec}
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Generated code plan for deletes in a row table.
 */
case class RowDeleteExec(child: SparkPlan, resolvedName: String,
    tableSchema: StructType, keyColumns: Seq[Attribute],
    connProps: ConnectionProperties, onExecutor: Boolean)
    extends TableMutationExec(forUpdate = false) {

  @transient private var result: String = _
  @transient private var stmt: String = _
  @transient private var batchCount: String = _

  override protected def doProduce(ctx: CodegenContext): String = {
    val (initCode, endCode) = connectionCodes(ctx)
    result = ctx.freshName("result")
    ctx.addMutableState("long", result, s"$result = -1L;")
    stmt = ctx.freshName("statement")
    batchCount = ctx.freshName("batchCount")
    val childProduce = doChildProduce(ctx)
    s"""
       |if ($result >= 0L) return;
       |$initCode
       |try {
       |  final java.sql.PreparedStatement $stmt = $connTerm.prepareStatement(
       |    "${s"DELETE FROM $resolvedName WHERE $whereClauseString"}");
       |  $result = 0L;
       |  int $batchCount = 0;
       |  $childProduce
       |  if ($batchCount > 0) {
       |    final int[] counts = $stmt.executeBatch();
       |    for (int count : counts) {
       |      $result += count;
       |    }
       |    $stmt.close();
       |    $connTerm.commit();
       |    ${consume(ctx, Seq(ExprCode("", "false", result)))}
       |  }
       |} catch (java.sql.SQLException sqle) {
       |  throw new java.io.IOException(sqle.toString(), sqle);
       |}$endCode
    """.stripMargin
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode],
      row: ExprCode): String = {
    val batchSize = connProps.executorConnProps
        .getProperty("batchsize", "1000").toInt

    ctx.INPUT_ROW = null
    ctx.currentVars = input
    // bind the key columns
    val stmtInput = ctx.generateExpressions(keyColumns.map(
      u => ExpressionCanonicalizer.execute(BindReferences.bindReference(
        u, child.output))), doSubexpressionElimination = true)
    ctx.currentVars = null

    val stmtSchema = StructType.fromAttributes(keyColumns)
    val schemaTerm = ctx.addReferenceObj("stmtSchema", stmtSchema)
    val schemaFields = ctx.freshName("schemaFields")
    val structFieldClass = classOf[StructField].getName

    s"""
       |${evaluateVariables(stmtInput)}
       |final $structFieldClass[] $schemaFields = $schemaTerm.fields();
       |${CodeGeneration.genStmtSetters(stmtSchema.fields,
          connProps.dialect, stmtInput, stmt, schemaFields, ctx)}
       |$batchCount++;
       |$stmt.addBatch();
       |if (($batchCount % $batchSize) == 0) {
       |  final int[] counts = $stmt.executeBatch();
       |  for (int count : counts) {
       |    $result += count;
       |  }
       |  $batchCount = 0;
       |}
    """.stripMargin
  }
}
