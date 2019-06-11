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

package org.apache.spark.sql.execution.row

import java.sql.Connection

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.execution.TableExec
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Base class for bulk row table insert, update, put, delete operations.
 */
trait RowExec extends TableExec {

  @transient private[sql] var connRef = -1
  @transient protected var connTerm: String = _
  @transient protected var stmt: String = _
  @transient protected var rowCount: String = _
  @transient protected var result: String = _

  def resolvedName: String

  def connProps: ConnectionProperties

  override def simpleString: String = s"$nodeName($resolvedName) partitioning=" +
      s"${partitionColumns.mkString("[", ",", "]")} numBuckets=$numBuckets"

  protected def connectionCodes(ctx: CodegenContext): (String, String, String) = {
    val connectionClass = classOf[Connection].getName
    connTerm = ctx.freshName("connection")
    // onExecutor will never be true in case of ColumnDelete/Update
    if (onExecutor) {
      // actual connection will be filled into references before execution
      connRef = ctx.references.length
      // connObj position in the array is connRef
      val connObj = ctx.addReferenceObj("conn", null, connectionClass)
      (s"final $connectionClass $connTerm = $connObj;", "", "")
    } else {
      val utilsClass = ExternalStoreUtils.getClass.getName
      ctx.addMutableState(connectionClass, connTerm, "")
      val props = ctx.addReferenceObj("connectionProperties", connProps)
      val initCode =
        s"""
           |$connTerm = $utilsClass.MODULE$$.getConnection(
           |    "$resolvedName", $props, true);""".stripMargin
      val endCode =
        s""" finally {
           |  try {
           |    $connTerm.commit();
           |    $connTerm.close();
           |  } catch (java.sql.SQLException sqle) {
           |    // ignore exception in close
           |  }
           |}""".stripMargin
      (initCode, s"", endCode)
    }
  }

  protected def executeBatchCode(numOperations: String,
      numOpRowsMetric: String): String =
    s"""
       |int $numOperations = 0;
       |for (int count : $stmt.executeBatch()) {
       |  $numOperations += count;
       |}
       |$result += $numOperations;
       |${if (numOpRowsMetric eq null) ""
          else s"$numOpRowsMetric.${metricAdd(numOperations)};"}""".stripMargin

  protected def doProduce(ctx: CodegenContext, pstmtStr: String,
      produceAddonCode: () => String = () => ""): String = {
    val (initCode, commitCode, endCode) = connectionCodes(ctx)
    result = ctx.freshName("result")
    stmt = ctx.freshName("statement")
    rowCount = ctx.freshName("rowCount")
    val numOpRowsMetric = if (onExecutor) null
    else metricTerm(ctx, s"num${opType}Rows")
    val numOperations = ctx.freshName("numOperations")
    val childProduce = doChildProduce(ctx)
    val mutateTable = ctx.freshName("mutateTable")

    ctx.addMutableState("java.sql.PreparedStatement", stmt, "")
    ctx.addMutableState("long", result, s"$result = -1L;")
    ctx.addMutableState("long", rowCount, "")
    ctx.addNewFunction(mutateTable,
      s"""
         |private void $mutateTable() throws java.io.IOException, java.sql.SQLException {
         |  $childProduce
         |  if ($rowCount > 0) {
         |    ${executeBatchCode(numOperations, numOpRowsMetric)}
         |  }
         |  $stmt.close();
         |  ${produceAddonCode()}
         |}
      """.stripMargin)
    s"""
       |if ($result >= 0L) return;
       |$initCode
       |try {
       |  $stmt = $connTerm.prepareStatement("$pstmtStr");
       |  $result = 0L;
       |  $mutateTable();
       |  ${consume(ctx, Seq(ExprCode("", "false", result)))}
       |} catch (java.sql.SQLException sqle) {
       |  throw new java.io.IOException(sqle.toString(), sqle);
       |}$commitCode
       |$endCode
    """.stripMargin
  }

  protected def getUpdateSchema(updateExpressions: Seq[Expression]): Seq[StructField] = {
    var seq = -1
    updateExpressions.map {
      case ne: NamedExpression =>
        val a = ne.toAttribute
        StructField(a.name, a.dataType, a.nullable, a.metadata)
      case e: Expression =>
        seq += 1
        StructField(s"UpdateCol_$seq", e.dataType, e.nullable)
    }
  }

  protected def doConsume(ctx: CodegenContext, input: Seq[ExprCode],
      schema: StructType): String = {
    val schemaTerm = ctx.addReferenceObj("schema", schema)
    val schemaFields = ctx.freshName("schemaFields")
    val structFieldClass = classOf[StructField].getName
    ctx.addMutableState(s"$structFieldClass[]", schemaFields,
      s"$schemaFields = $schemaTerm.fields();")
    val batchSize = connProps.executorConnProps
        .getProperty("batchsize", "1000").toInt
    val numOpRowsMetric = if (onExecutor) null
    else metricTerm(ctx, s"num${opType}Rows")
    val numOperations = ctx.freshName("numOperations")

    val inputCode = evaluateVariables(input)
    val functionCalls = schema.indices.map { col =>
      val f = schema(col)
      val isNull = ctx.freshName("isNull")
      val field = ctx.freshName("field")
      val ev = input(col)
      val dataType = ctx.javaType(f.dataType)
      val columnSetterFunction = ctx.freshName("setColumnOfRow")
      val columnSetterCode = CodeGeneration.getColumnSetterFragment(col, f.dataType,
        connProps.dialect, ev.copy(isNull = isNull, value = field), stmt, schemaFields, ctx)
      ctx.addNewFunction(columnSetterFunction,
        s"""
           |private void $columnSetterFunction(final boolean $isNull,
           |    final $dataType $field) throws java.sql.SQLException {
           |  $columnSetterCode
           |}
        """.stripMargin)
      s"$columnSetterFunction(${ev.isNull}, ${ev.value});"
    }.mkString("\n")
    s"""
       |$inputCode
       |$functionCalls
       |$rowCount++;
       |$stmt.addBatch();
       |if (($rowCount % $batchSize) == 0) {
       |  ${executeBatchCode(numOperations, numOpRowsMetric)}
       |  $rowCount = 0;
       |}
    """.stripMargin
  }
}
