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

package org.apache.spark.sql.execution.columnar

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, ExpressionCanonicalizer}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.encoding.ColumnDeleteEncoder
import org.apache.spark.sql.execution.columnar.impl.ColumnDelta
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.row.RowExec
import org.apache.spark.sql.sources.{ConnectionProperties, DestroyRelation}
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types.StructType

/**
 * Generated code plan for deletes into a column table.
 * This extends [[RowExec]] to generate the combined code for row buffer deletes.
 */
case class ColumnDeleteExec(child: SparkPlan, columnTable: String,
    partitionColumns: Seq[String], partitionExpressions: Seq[Expression],
    numBuckets: Int, tableSchema: StructType, externalStore: ExternalStore,
    relation: Option[DestroyRelation], keyColumns: Seq[Attribute],
    connProps: ConnectionProperties, onExecutor: Boolean) extends RowExec {

  override def resolvedName: String = externalStore.tableName

  override protected def opType: String = "Delete"

  override def nodeName: String = "ColumnDelete"

  override lazy val metrics: Map[String, SQLMetric] = {
    if (onExecutor) Map.empty
    else Map(
      "numDeleteRows" -> SQLMetrics.createMetric(sparkContext,
        "number of deletes in row buffer"),
      "numDeleteColumnBatchRows" -> SQLMetrics.createMetric(sparkContext,
        "number of deletes in column batches"))
  }

  @transient private var batchOrdinal: String = _
  @transient private var batchIdTerm: String = _
  @transient private var finishDelete: String = _
  @transient private var deleteMetric: String = _

  override protected def doProduce(ctx: CodegenContext): String = {
    val sql = new StringBuilder
    sql.append("DELETE FROM ").append(resolvedName)
        .append(s" WHERE ${StoreUtils.SHADOW_COLUMN_NAME}=?")

    super.doProduce(ctx, sql.toString(), () =>
      s"""
         |if ($batchOrdinal > 0) {
         |  $finishDelete(null); // force a finish
         |}
      """.stripMargin)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode],
      row: ExprCode): String = {
    val cursor = ctx.freshName("cursor")
    val lastColumnBatchId = ctx.freshName("lastColumnBatchId")
    val deleteEncoder = ctx.freshName("deleteEncoder")
    batchOrdinal = ctx.freshName("batchOrdinal")
    batchIdTerm = ctx.freshName("batchId")
    val bucketId = ctx.freshName("bucketId")
    finishDelete = ctx.freshName("finishDelete")
    deleteMetric = if (onExecutor) null else metricTerm(ctx, "numDeleteColumnBatchRows")

    val deleteEncoderClass = classOf[ColumnDeleteEncoder].getName

    ctx.addMutableState(deleteEncoderClass, deleteEncoder, "")
    ctx.addMutableState("long", cursor, "")
    ctx.addMutableState("int", batchOrdinal, "")
    ctx.addMutableState("UTF8String", batchIdTerm, "")
    ctx.addMutableState("UTF8String", lastColumnBatchId, "")
    ctx.addMutableState("int", bucketId, "")
    val externalStoreTerm = ctx.addReferenceObj("externalStore", externalStore)

    ctx.INPUT_ROW = null
    ctx.currentVars = input
    // bind the key columns
    val keysInput = ctx.generateExpressions(keyColumns.map(
      u => ExpressionCanonicalizer.execute(BindReferences.bindReference(
        u, child.output))), doSubexpressionElimination = true)
    ctx.currentVars = null

    // first column in keyColumns should be ordinal in the column
    assert(keyColumns.head.name.equalsIgnoreCase(ColumnDelta.mutableKeyNames.head))
    // second column in keyColumns should be column batchId
    assert(keyColumns(1).name.equalsIgnoreCase(ColumnDelta.mutableKeyNames(1)))
    // last column in keyColumns should be bucketId
    assert(keyColumns(2).name.equalsIgnoreCase(ColumnDelta.mutableKeyNames(2)))

    val numKeys = keyColumns.length
    val positionTerm = keysInput(keysInput.length - numKeys).value

    val keyVarsCode =
      s"""
         |${evaluateVariables(keysInput)}
         |$batchIdTerm = ${keysInput(keysInput.length - numKeys + 1).value};
         |$bucketId = ${keysInput(keysInput.length - numKeys + 2).value}
      """.stripMargin
    // row buffer needs to select the rowId for the ordinal
    val rowConsume = super.doConsume(ctx, keysInput,
      StructType(Seq(ColumnDelta.mutableKeyFields.head)))

    val initializeEncoder =
      s"""
         |$deleteEncoder = new $deleteEncoderClass();
         |$cursor = $deleteEncoder.initialize(8); // start with a default size
      """.stripMargin
    ctx.addNewFunction(finishDelete,
      s"""
         |private void $finishDelete(UTF8String $batchIdTerm) {
         |  if ($batchIdTerm != $lastColumnBatchId && ($batchIdTerm == null ||
         |      !$batchIdTerm.equals($lastColumnBatchId))) {
         |    if ($lastColumnBatchId == null) {
         |      // first call
         |      $lastColumnBatchId = $batchIdTerm;
         |      return;
         |    }
         |    // finish previous encoder, put into table and re-initialize
         |    final java.nio.ByteBuffer buffer = $deleteEncoder.finish($cursor);
         |    // delete puts an empty stats row to denote that there are changes
         |    $externalStoreTerm.storeDelete($columnTable, buffer,
         |        new byte[] { 0, 0, 0, 0 }, $bucketId, $lastColumnBatchId.toString());
         |    $result += $batchOrdinal;
         |    ${if (deleteMetric eq null) "" else s"$deleteMetric.${metricAdd(batchOrdinal)};"}
         |    $initializeEncoder
         |    $lastColumnBatchId = $batchIdTerm;
         |    $batchOrdinal = 0;
         |  }
         |}
      """.stripMargin)

    s"""
       |$keyVarsCode
       |if ($batchIdTerm != null) {
       |  // finish and apply update if the next column batch ID is seen
       |  $finishDelete($batchIdTerm);
       |  // write to the encoder
       |  $cursor = $deleteEncoder.writeInt($positionTerm, $cursor);
       |  $batchOrdinal++;
       |} else {
       |  $rowConsume
       |}
    """.stripMargin
  }
}
