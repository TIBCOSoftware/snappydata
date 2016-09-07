/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, Expression}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoding
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{PartitionedDataSourceScan, PartitionedPhysicalRDD, RowTableScan}
import org.apache.spark.sql.types._

/**
 * Physical plan node for scanning data from a SnappyData column table RDD.
 * If user knows that the data is partitioned across all nodes, this SparkPlan
 * can be used to avoid expensive shuffle and Broadcast joins.
 * This plan overrides outputPartitioning and makes it inline with the
 * partitioning of the underlying DataSource.
 */
private[sql] final case class ColumnTableScan(
    output: Seq[Attribute],
    rdd: RDD[Any],
    otherRDDs: Seq[RDD[InternalRow]],
    numPartitions: Int,
    numBuckets: Int,
    partitionColumns: Seq[Expression],
    scanUnsafeRows: Boolean,
    @transient baseRelation: PartitionedDataSourceScan)
    extends PartitionedPhysicalRDD(output, rdd, numPartitions, numBuckets,
      partitionColumns, baseRelation) {

  private[sql] override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext,
      "number of output rows"),
    "numRowBufferOutput" -> SQLMetrics.createMetric(sparkContext,
      "number of output rows from the row buffer")) ++ (
      if (otherRDDs.isEmpty) Map.empty
      else Map("numOtherRows" -> SQLMetrics.createMetric(sparkContext,
        "number of output rows from other RDDs")))

  private val allRDDs = if (otherRDDs.isEmpty) rdd
  else rdd.sparkContext.union((Seq(rdd) ++ otherRDDs).asInstanceOf[Seq[RDD[Any]]])

  private val internalRowPartitionIndex = rdd.getNumPartitions

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    allRDDs.asInstanceOf[RDD[InternalRow]] :: Nil
  }

  override def doProduce(ctx: CodegenContext): String = {
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    val numRowBufferOutput = metricTerm(ctx, "numRowBufferOutput")
    val numOtherRows =
      if (otherRDDs.isEmpty) null else metricTerm(ctx, "numOtherRows")
    val numRows = ctx.freshName("numRows")
    // PartitionedPhysicalRDD always has one input.
    // It returns an iterator of iterators (row + column)
    // except when doing union with multiple RDDs where other
    // RDDs return iterator of UnsafeRows.
    val rowInput = ctx.freshName("rowInput")
    val colInput = ctx.freshName("colInput")
    if (otherRDDs.isEmpty) {
      ctx.addMutableState("scala.collection.Iterator",
        rowInput, s"$rowInput = (scala.collection.Iterator)inputs[0].next();")
      ctx.addMutableState("scala.collection.Iterator",
        colInput, s"$colInput = (scala.collection.Iterator)inputs[0].next();")
    } else {
      ctx.addMutableState("scala.collection.Iterator",
        rowInput,
        s"""
            $rowInput = (partitionIndex < $internalRowPartitionIndex)
                ? (scala.collection.Iterator)inputs[0].next() : inputs[0];
        """)
      ctx.addMutableState("scala.collection.Iterator",
        colInput,
        s"""
            $colInput = (partitionIndex < $internalRowPartitionIndex)
                ? (scala.collection.Iterator)inputs[0].next() : null;
        """)
    }
    ctx.currentVars = null

    // if this is a partition for "otherRDDs" then just do a normal UnsafeRow
    // scan and return (e.g. for sample tables)
    val otherRDDScan = if (otherRDDs.isEmpty) ""
    else {
      val row = ctx.freshName("row")
      ctx.INPUT_ROW = row
      val columnsRowInput = output.zipWithIndex.map { case (a, index) =>
        BoundReference(index, a.dataType, a.nullable).genCode(ctx)
      }
      s"""
         |// check if this partition is for other UnsafeRow iterators
         |if (partitionIndex >= $internalRowPartitionIndex) {
         |  // iterate UnsafeRows from the RDD
         |  long $numRows = 0L;
         |  try {
         |    while ($rowInput.hasNext()) {
         |      final UnsafeRow $row = (UnsafeRow)$rowInput.next();
         |      $numRows++;
         |      ${consume(ctx, columnsRowInput, row).trim}
         |      if (shouldStop()) return;
         |    }
         |    return;
         |  } finally {
         |    $numOtherRows.add($numRows);
         |  }
         |}
       """.stripMargin
    }

    val rowScanCode = baseRelation.connectionType match {
      case ConnectionType.Embedded =>
        // row buffer returns CompactExecRow iterator
        RowTableScan.doProduceWithoutProjection(ctx, rowInput,
          numRowBufferOutput, output, baseRelation, this)
      case _ =>
        // row buffer returns ResultSet
        RowTableScan.doProduceWithProjection(ctx, rowInput,
          numRowBufferOutput, output, baseRelation, this)
    }

    ctx.currentVars = null
    if (scanUnsafeRows) {
      val row = ctx.freshName("row")
      ctx.INPUT_ROW = row
      val columnsRowInput = output.zipWithIndex.map { case (a, index) =>
        BoundReference(index, a.dataType, a.nullable).genCode(ctx)
      }
      s"""
         |// start with the check for other RDDs being scanned
         |$otherRDDScan
         |// then iterate the row buffer
         |$rowScanCode
         |// iterate UnsafeRows from column table next
         |long $numRows = 0L;
         |try {
         |  while ($colInput.hasNext()) {
         |    final UnsafeRow $row = (UnsafeRow)$colInput.next();
         |    $numRows++;
         |    ${consume(ctx, columnsRowInput, row).trim}
         |    if (shouldStop()) return;
         |  }
         |} finally {
         |  $numOutputRows.add($numRows);
         |}
      """.stripMargin
    } else {
      val cachedBatchClass = classOf[CachedBatch].getName
      val decoderClass = classOf[ColumnEncoding].getName
      val batch = ctx.freshName("batch")
      val numBatchRows = ctx.freshName("numBatchRows")
      val batchOrdinal = ctx.freshName("batchOrdinal")
      val buffers = ctx.freshName("buffers")

      // need DataType and nullable to get decoder in generated code
      val fields = ctx.addReferenceObj("fields", output.toArray, "Attribute[]")
      val initCode = new StringBuilder
      val moveNextCode = new StringBuilder
      val columnsInput = output.zipWithIndex.map { case (a, index) =>
        val decoder = ctx.freshName("decoder")
        val buffer = ctx.freshName("buffer")
        initCode.append(
          s"""
            final byte[] $buffer = $buffers[$index];
            final $decoderClass $decoder = $decoderClass.getColumnDecoder(
              $buffer, $numBatchRows, $fields[$index]);
          """)
        val isNullVar = if (a.nullable) ctx.freshName("nullable") else null
        moveNextCode.append(genCodeColumnNext(ctx, decoder, buffer,
          batchOrdinal, a.dataType, isNullVar)).append('\n')
        genCodeColumnBuffer(ctx, decoder, buffer,
          batchOrdinal, a.dataType, isNullVar)
      }
      s"""
         |// start with the check for other RDDs being scanned
         |$otherRDDScan
         |// then iterate the row buffer
         |$rowScanCode
         |// iterate cached batches from column table next
         |long $numRows = 0L;
         |try {
         |  while ($colInput.hasNext()) {
         |    $cachedBatchClass $batch = ($cachedBatchClass)$colInput.next();
         |    final int $numBatchRows = $batch.numRows();
         |    final byte[][] $buffers = $batch.buffers();
         |    ${initCode.toString()}
         |    for (int $batchOrdinal = 0; $batchOrdinal < $numBatchRows;
         |        $batchOrdinal++) {
         |      ${moveNextCode.toString()}
         |      $numRows++;
         |      ${consume(ctx, columnsInput).trim}
         |      if (shouldStop()) return;
         |    }
         |  }
         |} catch (RuntimeException re) {
         |  throw re;
         |} catch (Exception e) {
         |  throw new RuntimeException(e);
         |} finally {
         |  $numOutputRows.add($numRows);
         |}
      """.stripMargin
    }
  }

  private def genCodeColumnNext(ctx: CodegenContext, decoder: String,
      buffer: String, batchOrdinal: String, dataType: DataType,
      isNullVar: String): String = {
    val sqlType = Utils.getSQLDataType(dataType)
    val jt = ctx.javaType(sqlType)
    val moveNext = sqlType match {
      case _ if ctx.isPrimitiveType(jt) =>
        s"$decoder.next${ctx.primitiveTypeName(jt)}($buffer);"
      case StringType => s"$decoder.nextUTF8String($buffer);"
      case t: DecimalType =>
        s"$decoder.nextDecimal($buffer, ${t.precision});"
      case CalendarIntervalType => s"$decoder.nextInterval($buffer);"
      case BinaryType | _: ArrayType | _: MapType | _: StructType =>
        s"$decoder.nextBinary($buffer)"
      case NullType => ""
      case _ =>
        throw new UnsupportedOperationException(s"unknown type $sqlType")
    }
    if (isNullVar != null) {
      val nullCode =
        s"final boolean $isNullVar = $decoder.readNull($buffer, $batchOrdinal);"
      if (moveNext.isEmpty) nullCode
      else s"$nullCode\nif (!$isNullVar) $moveNext"
    } else moveNext
  }

  private def genCodeColumnBuffer(ctx: CodegenContext, decoder: String,
      buffer: String, batchOrdinal: String, dataType: DataType,
      isNullVar: String): ExprCode = {
    val sqlType = Utils.getSQLDataType(dataType)
    val jt = ctx.javaType(sqlType)
    val extract = sqlType match {
      case _ if ctx.isPrimitiveType(jt) =>
        s"$decoder.read${ctx.primitiveTypeName(jt)}($buffer)"
      case StringType => s"$decoder.readUTF8String($buffer)"
      case t: DecimalType =>
        s"$decoder.readDecimal($buffer, ${t.precision}, ${t.scale})"
      case BinaryType => s"$decoder.readBinary($buffer)"
      case CalendarIntervalType => s"$decoder.readInterval($buffer)"
      case _: ArrayType => s"$decoder.readArray($buffer)"
      case _: MapType => s"$decoder.readMap($buffer)"
      case t: StructType => s"$decoder.readStruct($buffer, ${t.size})"
      case NullType => "null"
      case _ =>
        throw new UnsupportedOperationException(s"unknown type $sqlType")
    }
    val col = ctx.freshName("col")
    if (isNullVar != null) {
      val code =
        s"final $jt $col = $isNullVar ? ${ctx.defaultValue(jt)} : $extract;"
      ExprCode(code, isNullVar, col)
    } else {
      ExprCode(s"final $jt $col = $extract;", "false", col)
    }
  }
}
