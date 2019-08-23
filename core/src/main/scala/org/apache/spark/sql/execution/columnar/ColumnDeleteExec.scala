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

package org.apache.spark.sql.execution.columnar

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, ExpressionCanonicalizer}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.encoding.ColumnDeleteEncoder
import org.apache.spark.sql.execution.columnar.impl.ColumnDelta
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.row.RowExec
import org.apache.spark.sql.sources.JdbcExtendedUtils.quotedName
import org.apache.spark.sql.sources.{ConnectionProperties, DestroyRelation, JdbcExtendedUtils}
import org.apache.spark.sql.store.{CompressionCodecId, StoreUtils}
import org.apache.spark.sql.types.StructType

/**
 * Generated code plan for deletes into a column table.
 * This extends [[RowExec]] to generate the combined code for row buffer deletes.
 */
case class ColumnDeleteExec(child: SparkPlan, columnTable: String,
    partitionColumns: Seq[String], partitionExpressions: Seq[Expression],
    numBuckets: Int, isPartitioned: Boolean, tableSchema: StructType,
    externalStore: ExternalStore, appendableRelation: JDBCAppendableRelation,
    keyColumns: Seq[Attribute], connProps: ConnectionProperties,
    onExecutor: Boolean) extends ColumnExec {

  override def relation: Option[DestroyRelation] = Some(appendableRelation)

  val compressionCodec: CompressionCodecId.Type = CompressionCodecId.fromName(
    appendableRelation.getCompressionCodec)

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

  override def simpleString: String = s"${super.simpleString} compression=$compressionCodec"

  @transient private var batchOrdinal: String = _
  @transient private var finishDelete: String = _
  @transient private var deleteMetric: String = _
  @transient protected var txId: String = _
  @transient protected var success: String = _

  override protected def doProduce(ctx: CodegenContext): String = {
    val sql = new StringBuilder
    sql.append("DELETE FROM ").append(quotedName(resolvedName, escapeQuotes = true))
        .append(" WHERE ")
    // only the ordinalId is required apart from partitioning columns
    if (keyColumns.length > 4) {
      JdbcExtendedUtils.fillColumnsClause(sql, keyColumns.dropRight(4).map(_.name),
        escapeQuotes = true)
      sql.append(" AND ")
    }
    sql.append(StoreUtils.ROWID_COLUMN_NAME).append("=?")

    super.doProduce(ctx, sql.toString(), () =>
      s"""
         |if ($batchOrdinal > 0) {
         |  $finishDelete($invalidUUID, -1, -1); // force a finish
         |}
         |$taskListener.setSuccess();
      """.stripMargin)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode],
      row: ExprCode): String = {
    val position = ctx.freshName("position")
    val lastColumnBatchId = ctx.freshName("lastColumnBatchId")
    val lastBucketId = ctx.freshName("lastBucketId")
    val lastNumRows = ctx.freshName("lastNumRows")
    val deleteEncoder = ctx.freshName("deleteEncoder")
    batchOrdinal = ctx.freshName("batchOrdinal")
    finishDelete = ctx.freshName("finishDelete")
    deleteMetric = if (onExecutor) null else metricTerm(ctx, "numDeleteColumnBatchRows")

    val deleteEncoderClass = classOf[ColumnDeleteEncoder].getName

    val initializeEncoder =
      s"""
         |$deleteEncoder = new $deleteEncoderClass();
         |$position = $deleteEncoder.initialize(8); // start with a default size
      """.stripMargin

    ctx.addMutableState(deleteEncoderClass, deleteEncoder, "")
    ctx.addMutableState("int", position, initializeEncoder)
    ctx.addMutableState("int", batchOrdinal, "")
    ctx.addMutableState("long", lastColumnBatchId, s"$lastColumnBatchId = $invalidUUID;")
    ctx.addMutableState("int", lastBucketId, "")
    ctx.addMutableState("int", lastNumRows, "")

    val tableName = ctx.addReferenceObj("columnTable", columnTable, "java.lang.String")

    // last three columns in keyColumns should be internal ones
    val keyCols = keyColumns.takeRight(4)
    assert(keyCols.head.name.equalsIgnoreCase(ColumnDelta.mutableKeyNames.head))
    assert(keyCols(1).name.equalsIgnoreCase(ColumnDelta.mutableKeyNames(1)))
    assert(keyCols(2).name.equalsIgnoreCase(ColumnDelta.mutableKeyNames(2)))
    assert(keyCols(3).name.equalsIgnoreCase(ColumnDelta.mutableKeyNames(3)))

    // bind the key columns (including partitioning columns if present)
    ctx.INPUT_ROW = null
    ctx.currentVars = input
    val keysInput = ctx.generateExpressions(keyColumns.map(
      u => ExpressionCanonicalizer.execute(BindReferences.bindReference(u, child.output))))
    ctx.currentVars = null

    val keyVars = keysInput.takeRight(4)
    val ordinalIdVar = keyVars.head.value
    val batchIdVar = keyVars(1).value
    val bucketVar = keyVars(2).value
    val numRowsVar = keyVars(3).value
    val externalStoreTerm = ctx.addReferenceObj("externalStore", externalStore)
    val keyVarsCode = evaluateVariables(keysInput)
    // row buffer needs to select the rowId and partitioning columns so drop last three
    val rowConsume = super.doConsume(ctx, keysInput.dropRight(3),
      StructType(getUpdateSchema(keyColumns.dropRight(3))))

    ctx.addNewFunction(finishDelete,
      s"""
         |private void $finishDelete(long batchId, int bucketId, int numRows) {
         |  if (batchId == $invalidUUID || batchId != $lastColumnBatchId) {
         |    if ($lastColumnBatchId == $invalidUUID) {
         |      // first call
         |      $lastColumnBatchId = batchId;
         |      $lastBucketId = bucketId;
         |      $lastNumRows = numRows;
         |      return;
         |    }
         |    // finish previous encoder, put into table and re-initialize
         |    final java.nio.ByteBuffer buffer = $deleteEncoder.finish($position, $lastNumRows);
         |    $externalStoreTerm.storeDelete($tableName, buffer, $lastBucketId,
         |        $lastColumnBatchId, ${compressionCodec.id}, new scala.Some($connTerm));
         |    $result += $batchOrdinal;
         |    ${if (deleteMetric eq null) "" else s"$deleteMetric.${metricAdd(batchOrdinal)};"}
         |    $initializeEncoder
         |    $lastColumnBatchId = batchId;
         |    $lastBucketId = bucketId;
         |    $lastNumRows = numRows;
         |    $batchOrdinal = 0;
         |  }
         |}
      """.stripMargin)

    s"""
       |$keyVarsCode
       |if ($batchIdVar != $invalidUUID) {
       |  // finish and apply delete if the next column batch ID is seen
       |  if ($batchIdVar != $lastColumnBatchId) {
       |    $finishDelete($batchIdVar, $bucketVar, $numRowsVar);
       |  }
       |  // write to the encoder
       |  $position = $deleteEncoder.writeInt($position, (int)$ordinalIdVar);
       |  $batchOrdinal++;
       |} else {
       |  $rowConsume
       |}
    """.stripMargin
  }
}
