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

import java.sql.{Connection, PreparedStatement}

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.{SparkPlan, TableInsertExec}
import org.apache.spark.sql.sources.{ConnectionProperties, DestroyRelation, JdbcExtendedUtils}
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Generated code plan for bulk insertion into a row table.
 */
case class RowInsertExec(_child: SparkPlan, upsert: Boolean,
    partitionColumns: Seq[String], _partitionExpressions: Seq[Expression],
    _numBuckets: Int, tableSchema: StructType, relation: Option[DestroyRelation],
    onExecutor: Boolean, resolvedName: String, connProps: ConnectionProperties)
    extends TableInsertExec(_child, partitionColumns, _partitionExpressions,
      _numBuckets, tableSchema, relation, onExecutor) {

  private[sql] var statementRef = -1

  private var stmt: String = _
  private var rowCount: String = _
  private var result: String = _

  override protected def doProduce(ctx: CodegenContext): String = {
    stmt = ctx.freshName("stmt")
    rowCount = ctx.freshName("rowCount")
    result = ctx.freshName("result")
    ctx.addMutableState("int", result, s"$result = -1;")
    val numInsertedRowsMetric = if (onExecutor) null
    else metricTerm(ctx, "numInsertedRows")
    val numInsertions = ctx.freshName("numInsertions")
    val connectionClass = classOf[Connection].getName
    val statementClass = classOf[PreparedStatement].getName
    val utilsClass = ExternalStoreUtils.getClass.getName

    val (stmtCode, open, close) = if (onExecutor) {
      // actual connection will be filled into references before execution
      statementRef = ctx.references.length
      val stmtObj = ctx.addReferenceObj("stmt", null, statementClass)
      (s"final $statementClass $stmt = $stmtObj;", "", "")
    } else {
      val conn = ctx.freshName("connection")
      val props = ctx.addReferenceObj("connectionProperties", connProps)
      ctx.addMutableState(connectionClass, conn, "")
      val rowInsertStr = JdbcExtendedUtils.getInsertOrPutString(resolvedName,
        tableSchema, upsert, escapeQuotes = true)
      (
          s"""final $statementClass $stmt = $conn.prepareStatement(
              "$rowInsertStr");""",
          s"""$conn = $utilsClass.MODULE$$.getConnection(
               "$resolvedName", $props, true);""",
          s""" finally {
               try {
                |$conn.commit();
                 $conn.close();
               } catch (java.sql.SQLException sqle) {
                 throw new java.io.IOException(sqle.toString(), sqle);
               }
             }
          """)
    }
    val childProduce = doChildProduce(ctx)
    // no need to stop in iteration at any point
    ctx.addNewFunction("shouldStop",
      s"""
         |@Override
         |protected final boolean shouldStop() {
         |  return false;
         |}
      """.stripMargin)
    s"""
       |if ($result >= 0) return;
       |$open
       |try {
       |  int $rowCount = 0;
       |  $result = 0;
       |  $stmtCode
       |  $childProduce
       |  if ($rowCount > 0) {
       |    final int $numInsertions = $stmt.executeBatch().length;
       |    $result += $numInsertions;
       |    ${if (numInsertedRowsMetric eq null) ""
              else s"$numInsertedRowsMetric.${metricAdd(numInsertions)};"}
       |  }
       |  $stmt.close();
       |  ${consume(ctx, Seq(ExprCode("", "false", result)))}
       |} catch (java.sql.SQLException sqle) {
       |  throw new java.io.IOException(sqle.toString(), sqle);
       |}$close
    """.stripMargin
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode],
      row: ExprCode): String = {
    val schemaTerm = ctx.addReferenceObj("schema", tableSchema)
    val schemaFields = ctx.freshName("schemaFields")
    val structFieldClass = classOf[StructField].getName
    val batchSize = connProps.executorConnProps
        .getProperty("batchsize", "1000").toInt
    val numInsertedRowsMetric = if (onExecutor) null
    else metricTerm(ctx, "numInsertedRows")
    val numInsertions = ctx.freshName("numInsertions")
    s"""
       |final $structFieldClass[] $schemaFields = $schemaTerm.fields();
       |${CodeGeneration.genStmtSetters(tableSchema.fields,
          connProps.dialect, input, stmt, schemaFields, ctx)}
       |$rowCount++;
       |$stmt.addBatch();
       |if (($rowCount % $batchSize) == 0) {
       |  final int $numInsertions = $stmt.executeBatch().length;
       |  $result += $numInsertions;
       |  ${if (numInsertedRowsMetric eq null) ""
            else s"$numInsertedRowsMetric.${metricAdd(numInsertions)};"}
       |  $rowCount = 0;
       |}
    """.stripMargin
  }
}
