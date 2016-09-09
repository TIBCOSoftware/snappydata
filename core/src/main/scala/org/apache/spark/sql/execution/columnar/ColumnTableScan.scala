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
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoding
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.row.{ResultSetEncodingAdapter, ResultSetTraversal, UnsafeRowEncodingAdapter, UnsafeRowHolder}
import org.apache.spark.sql.execution.{PartitionedDataSourceScan, PartitionedPhysicalRDD}
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
    "numRowsBufferOutput" -> SQLMetrics.createMetric(sparkContext,
      "number of output rows from row buffer")) ++ (
      if (otherRDDs.isEmpty) Map.empty
      else Map("numRowsOtherRDDs" -> SQLMetrics.createMetric(sparkContext,
        "number of output rows from other RDDs")))

  private val allRDDs = if (otherRDDs.isEmpty) rdd
  else rdd.sparkContext.union((Seq(rdd) ++ otherRDDs)
      .asInstanceOf[Seq[RDD[Any]]])

  private val otherRDDsPartitionIndex = rdd.getNumPartitions

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    allRDDs.asInstanceOf[RDD[InternalRow]] :: Nil
  }

  override def doProduce(ctx: CodegenContext): String = {
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    val numRowsBufferOutput = metricTerm(ctx, "numRowsBufferOutput")
    val numRowsOtherOutput =
      if (otherRDDs.isEmpty) null else metricTerm(ctx, "numRowsOtherRDDs")
    val numRows = ctx.freshName("numRows")
    val numRowsBuffer = s"${numRows}RowBuffer"
    val numRowsOther = s"${numRows}OtherRDDs"
    // PartitionedPhysicalRDD always has one input.
    // It returns an iterator of iterators (row + column)
    // except when doing union with multiple RDDs where other
    // RDDs return iterator of UnsafeRows.
    val rowInput = ctx.freshName("rowInput")
    val colInput = ctx.freshName("colInput")
    val input = ctx.freshName("input")
    val rs = ctx.freshName("resultSet")
    val rsIterClass = classOf[ResultSetTraversal].getName
    if (otherRDDs.isEmpty) {
      ctx.addMutableState("scala.collection.Iterator",
        rowInput, s"$rowInput = (scala.collection.Iterator)inputs[0].next();")
      ctx.addMutableState("scala.collection.Iterator",
        colInput, s"$colInput = (scala.collection.Iterator)inputs[0].next();")
    } else {
      ctx.addMutableState("scala.collection.Iterator",
        rowInput,
        s"""
            $rowInput = (partitionIndex < $otherRDDsPartitionIndex)
                ? (scala.collection.Iterator)inputs[0].next() : inputs[0];
        """)
      ctx.addMutableState("scala.collection.Iterator",
        colInput,
        s"""
            $colInput = (partitionIndex < $otherRDDsPartitionIndex)
                ? (scala.collection.Iterator)inputs[0].next() : null;
        """)
    }

    ctx.currentVars = null
    if (scanUnsafeRows) {
      val row = ctx.freshName("row")
      ctx.INPUT_ROW = row
      s"""
         |// for testing just iterate UnsafeRows from column table
         |long $numRows = 0L;
         |try {
         |  while ($colInput.hasNext()) {
         |    final UnsafeRow $row = (UnsafeRow)$colInput.next();
         |    $numRows++;
         |    ${consume(ctx, null, row).trim}
         |    if (shouldStop()) return;
         |  }
         |} finally {
         |  $numOutputRows.add($numRows);
         |}
      """.stripMargin
    } else {
      val cachedBatchClass = classOf[CachedBatch].getName
      val decoderClass = classOf[ColumnEncoding].getName
      val rsDecoderClass = classOf[ResultSetEncodingAdapter].getName
      val unsafeDecoderClass = classOf[UnsafeRowEncodingAdapter].getName
      val unsafeHolderClass = classOf[UnsafeRowHolder].getName
      val batch = ctx.freshName("batch")
      val numBatchRows = ctx.freshName("numBatchRows")
      val batchOrdinal = ctx.freshName("batchOrdinal")
      val buffers = ctx.freshName("buffers")
      val inputIsRow = s"${input}IsRow"
      val inputIsOtherRDD = s"${input}IsOtherRDD"
      val unsafeHolder =
        if (otherRDDs.isEmpty) null else ctx.freshName("unsafeHolder")

      // need DataType and nullable to get decoder in generated code
      val fields = ctx.addReferenceObj("fields", output.toArray, "Attribute[]")
      val initCode = new StringBuilder
      val rsInitCode = new StringBuilder
      val otherRDDsInitCode = new StringBuilder
      val bufferInitCode = new StringBuilder
      val moveNextCode = new StringBuilder

      if (otherRDDs.isEmpty) {
        rsInitCode.append(
          s"final java.sql.ResultSet $rs = (($rsIterClass)$rowInput).rs();")
      } else {
        rsInitCode.append(
          s"""
            final java.sql.ResultSet $rs = $inputIsOtherRDD ? null
                : (($rsIterClass)$rowInput).rs();
          """
        )
        otherRDDsInitCode.append(
          s"""
            // check if this partition is for otherRDDs iteration
            final boolean $inputIsOtherRDD =
              partitionIndex >= $otherRDDsPartitionIndex;
            // initialize otherRDDs adapters for required columns
            final $unsafeHolderClass $unsafeHolder = new $unsafeHolderClass();
          """.stripMargin
        )
      }

      val nextRowSnippet = if (otherRDDs.isEmpty) s"$rowInput.next();"
      else {
        s"""
          if ($inputIsOtherRDD) {
            $unsafeHolder.setRow((UnsafeRow)$rowInput.next());
          } else {
            $rowInput.next();
          }
        """
      }
      val incrementNumRowsSnippet = if (otherRDDs.isEmpty) s"$numRowsBuffer++;"
      else {
        s"""
          if ($inputIsOtherRDD) {
            $numRowsOther++;
          } else {
            $numRowsBuffer++;
          }
        """
      }
      val incrementOtherRows = if (otherRDDs.isEmpty) ""
      else s"$numRowsOtherOutput.add($numRowsOther);"

      val columnsInput = output.zipWithIndex.map { case (a, index) =>
        val decoder = ctx.freshName("decoder")
        val rsDecoder = ctx.freshName("rsDecoder")
        val unsafeDecoder =
          if (otherRDDs.isEmpty) null else ctx.freshName("unsafeDecoder")
        val buffer = ctx.freshName("buffer")
        rsInitCode.append(
          s"""
             // TODO: SW: for ResultSet, more efficient will be to not push
             // projection and use baseSchema index below
             final $rsDecoderClass $rsDecoder = new $rsDecoderClass($rs,
               ${index + 1});
          """
        )
        if (otherRDDs.isEmpty) {
          initCode.append(
            s"""
              byte[] $buffer = null;
              $decoderClass $decoder = $rsDecoder;
            """)
        } else {
          otherRDDsInitCode.append(
            s"""
              final $unsafeDecoderClass $unsafeDecoder = new $unsafeDecoderClass(
                $unsafeHolder, ${baseRelation.schema.fieldIndex(a.name)});
            """
          )
          initCode.append(
            s"""
              byte[] $buffer = null;
              $decoderClass $decoder = null;
              if ($inputIsOtherRDD) {
                $decoder = $unsafeDecoder;
              } else {
                $decoder = $rsDecoder;
              }
            """)
        }
        bufferInitCode.append(
          s"""
            $buffer = $buffers[$index];
            $decoder = $decoderClass.getColumnDecoder($buffer,
              $fields[$index]);
          """)
        val notNullVar = if (a.nullable) ctx.freshName("notNull") else null
        moveNextCode.append(genCodeColumnNext(ctx, decoder, buffer,
          batchOrdinal, a.dataType, notNullVar)).append('\n')
        genCodeColumnBuffer(ctx, decoder, buffer, a.dataType, notNullVar)
      }

      s"""
         |// Combined iterator for cached batches from column table
         |// and ResultSet from row buffer. Also takes care of otherRDDs
         |// case when partition is of otherRDDs by iterating over it
         |// using an UnsafeRow adapter.
         |long $numRows = 0L;
         |long $numRowsBuffer = 0L;
         |long $numRowsOther = 0L;
         |scala.collection.Iterator $input = $rowInput;
         |boolean $inputIsRow = true;
         |// initialize the ResultSet and other RDD adapters
         |${rsInitCode.toString()}
         |${otherRDDsInitCode.toString()}
         |// initialize the decoders for ResultSet and other RDD iteration
         |${initCode.toString()}
         |try {
         |  while (true) {
         |    if (!$input.hasNext()) {
         |      if ($input == $rowInput) {
         |        $input = $colInput;
         |        $inputIsRow = false;
         |        if ($input == null || !$input.hasNext()) {
         |          break;
         |        }
         |      } else {
         |        break;
         |      }
         |    }
         |    final byte[][] $buffers;
         |    final int $numBatchRows;
         |    if ($inputIsRow) {
         |      $nextRowSnippet
         |      $buffers = null;
         |      $numBatchRows = 1;
         |    } else {
         |      $cachedBatchClass $batch = ($cachedBatchClass)$colInput.next();
         |      $buffers = $batch.buffers();
         |      $numBatchRows = $batch.numRows();
         |      // initialize the buffers and decoders
         |      ${bufferInitCode.toString()}
         |    }
         |    for (int $batchOrdinal = 0; $batchOrdinal < $numBatchRows;
         |        $batchOrdinal++) {
         |      ${moveNextCode.toString()}
         |      $numRows++;
         |      if ($inputIsRow) {
         |        $incrementNumRowsSnippet
         |      }
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
         |  $numRowsBufferOutput.add($numRowsBuffer);
         |  $incrementOtherRows
         |}
      """.stripMargin
    }
  }

  private def genCodeColumnNext(ctx: CodegenContext, decoder: String,
      buffer: String, batchOrdinal: String, dataType: DataType,
      notNullVar: String): String = {
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
    if (notNullVar != null) {
      val nullCode =
        s"final byte $notNullVar = $decoder.notNull($buffer, $batchOrdinal);"
      if (moveNext.isEmpty) nullCode
      else s"$nullCode\nif ($notNullVar == 1) $moveNext"
    } else moveNext
  }

  private def genCodeColumnBuffer(ctx: CodegenContext, decoder: String,
      buffer: String, dataType: DataType, notNullVar: String): ExprCode = {
    val sqlType = Utils.getSQLDataType(dataType)
    val jt = ctx.javaType(sqlType)
    val extract = sqlType match {
      case DateType => s"$decoder.readDate($buffer)"
      case TimestampType => s"$decoder.readTimestamp($buffer)"
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
    if (notNullVar != null) {
      val nullVar = ctx.freshName("nullVal")
      val code =
        s"""
          final $jt $col;
          final boolean $nullVar;
          if ($notNullVar == 1) {
            $col = $extract;
            $nullVar = false;
          } else {
            if ($notNullVar == 0) {
              $col = ${ctx.defaultValue(jt)};
              $nullVar = true;
            } else {
              $col = $extract;
              $nullVar = $decoder.wasNull();
            }
          }
        """
      ExprCode(code, nullVar, col)
    } else {
      ExprCode(s"final $jt $col = $extract;", "false", col)
    }
  }
}
