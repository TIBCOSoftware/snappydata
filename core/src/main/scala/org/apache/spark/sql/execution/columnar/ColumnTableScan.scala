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
/*
 * UnionScanRDD taken from Spark's UnionRDD having the license below.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.columnar

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.store.{OffHeapCompactExecRowWithLobs, ResultWasNull, RowFormatter}

import org.apache.spark.rdd.{RDD, UnionPartition}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoding
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.row.{ResultSetEncodingAdapter, ResultSetTraversal, UnsafeRowEncodingAdapter, UnsafeRowHolder}
import org.apache.spark.sql.execution.{PartitionedDataSourceScan, PartitionedPhysicalScan}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._
import org.apache.spark.{Dependency, Partition, RangeDependency, SparkContext, TaskContext}

/**
 * Physical plan node for scanning data from a SnappyData column table RDD.
 * If user knows that the data is partitioned across all nodes, this SparkPlan
 * can be used to avoid expensive shuffle and Broadcast joins.
 * This plan overrides outputPartitioning and makes it inline with the
 * partitioning of the underlying DataSource.
 */
private[sql] final case class ColumnTableScan(
    output: Seq[Attribute],
    dataRDD: RDD[Any],
    otherRDDs: Seq[RDD[InternalRow]],
    numPartitions: Int,
    numBuckets: Int,
    partitionColumns: Seq[Expression],
    @transient baseRelation: PartitionedDataSourceScan)
    extends PartitionedPhysicalScan(output, dataRDD, numPartitions, numBuckets,
      partitionColumns, baseRelation.asInstanceOf[BaseRelation]) {

  private[sql] override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext,
      "number of output rows"),
    "numRowsBufferOutput" -> SQLMetrics.createMetric(sparkContext,
      "number of output rows from row buffer")) ++ (
      if (otherRDDs.isEmpty) Map.empty
      else Map("numRowsOtherRDDs" -> SQLMetrics.createMetric(sparkContext,
        "number of output rows from other RDDs")))

  private val allRDDs = if (otherRDDs.isEmpty) rdd
  else new UnionScanRDD(rdd.sparkContext, (Seq(rdd) ++ otherRDDs)
      .asInstanceOf[Seq[RDD[Any]]])

  private val isOffHeap = GemFireXDUtils.getGemFireContainer(
    baseRelation.table, true).isOffHeap

  private val otherRDDsPartitionIndex = rdd.getNumPartitions

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    allRDDs.asInstanceOf[RDD[InternalRow]] :: Nil
  }

  override def doProduce(ctx: CodegenContext): String = {
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    val numRowsBufferOutput = metricTerm(ctx, "numRowsBufferOutput")
    val numRowsOtherOutput =
      if (otherRDDs.isEmpty) null else metricTerm(ctx, "numRowsOtherRDDs")
    val isEmbedded = baseRelation.connectionType match {
      case ConnectionType.Embedded => true
      case _ => false
    }
    // PartitionedPhysicalRDD always has one input.
    // It returns an iterator of iterators (row + column)
    // except when doing union with multiple RDDs where other
    // RDDs return iterator of UnsafeRows.
    val rowInput = ctx.freshName("rowInput")
    val colInput = ctx.freshName("colInput")
    val input = ctx.freshName("input")
    val inputIsRow = s"${input}IsRow"
    val inputIsOtherRDD = s"${input}IsOtherRDD"
    val rs = ctx.freshName("resultSet")
    val rsIterClass = classOf[ResultSetTraversal].getName
    val unsafeHolder = if (otherRDDs.isEmpty) null
    else ctx.freshName("unsafeHolder")
    val unsafeHolderClass = classOf[UnsafeRowHolder].getName

    val colItrClass = if (!isEmbedded) classOf[CachedBatchIteratorOnRS].getName
    else if (isOffHeap) classOf[OffHeapLobsIteratorOnScan].getName
    else classOf[ByteArraysIteratorOnScan].getName

    if (otherRDDs.isEmpty) {
      ctx.addMutableState("scala.collection.Iterator",
        rowInput, s"$rowInput = (scala.collection.Iterator)inputs[0].next();")
      ctx.addMutableState(colItrClass, colInput,
        s"$colInput = ($colItrClass)inputs[0].next();")
      ctx.addMutableState("java.sql.ResultSet", rs,
        s"$rs = (($rsIterClass)$rowInput).rs();")
    } else {
      ctx.addMutableState("boolean", inputIsOtherRDD,
        s"$inputIsOtherRDD = (partitionIndex >= $otherRDDsPartitionIndex);")
      ctx.addMutableState("scala.collection.Iterator", rowInput,
        s"""
            $rowInput = $inputIsOtherRDD ? inputs[0]
                : (scala.collection.Iterator)inputs[0].next();
        """)
      ctx.addMutableState(colItrClass, colInput,
        s"$colInput = $inputIsOtherRDD ? null : ($colItrClass)inputs[0].next();")
      ctx.addMutableState("java.sql.ResultSet", rs,
        s"$rs = $inputIsOtherRDD ? null : (($rsIterClass)$rowInput).rs();")
      ctx.addMutableState(unsafeHolderClass, unsafeHolder,
        s"$unsafeHolder = new $unsafeHolderClass();")
    }
    ctx.addMutableState("scala.collection.Iterator", input,
      s"$input = $rowInput;")
    ctx.addMutableState("boolean", inputIsRow, s"$inputIsRow = true;")

    ctx.currentVars = null
    val cachedBatchClass = classOf[CachedBatch].getName
    val execRowClass = classOf[OffHeapCompactExecRowWithLobs].getName
    val decoderClass = classOf[ColumnEncoding].getName
    val wasNullClass = classOf[ResultWasNull].getName
    val rsAdapterClass = classOf[ResultSetEncodingAdapter].getName
    val rowAdapterClass = classOf[UnsafeRowEncodingAdapter].getName
    val rowFormatterClass = classOf[RowFormatter].getName
    val batch = ctx.freshName("batch")
    val numBatchRows = s"${batch}NumRows"
    val batchIndex = s"${batch}Index"
    val buffers = s"${batch}Buffers"
    val rowFormatter = s"${batch}RowFormatter"

    val numRowsBuffer = ctx.freshName("numRowsBuffer")
    val numRowsOther = s"${numRowsBuffer}Other"

    ctx.addMutableState("byte[][]", buffers, s"$buffers = null;")
    ctx.addMutableState("int", numBatchRows, s"$numBatchRows = 0;")
    ctx.addMutableState("int", batchIndex, s"$batchIndex = 0;")

    // need DataType and nullable to get decoder in generated code
    val fields = ctx.addReferenceObj("fields", output.toArray, "Attribute[]")
    val columnBufferInitCode = new StringBuilder
    val bufferInitCode = new StringBuilder
    val cursorUpdateCode = new StringBuilder
    val moveNextCode = new StringBuilder

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
    else {
      s"""
        $numOutputRows.add($numRowsOther);
        $numRowsOtherOutput.add($numRowsOther);
      """
    }

    val columnsInput = output.zipWithIndex.map { case (a, index) =>
      val decoder = ctx.freshName("decoder")
      val cursor = s"${decoder}Cursor"
      val buffer = ctx.freshName("buffer")
      val cursorVar = s"cursor$index"
      val decoderVar = s"decoder$index"
      val bufferVar = s"buffer$index"
      // projections are not pushed in embedded mode for optimized access
      val baseIndex = baseRelation.schema.fieldIndex(a.name)
      val rsPosition = if (isEmbedded) baseIndex + 1 else index + 1

      val bufferPosition = baseIndex + PartitionedPhysicalScan.CT_COLUMN_START

      ctx.addMutableState("byte[]", buffer, s"$buffer = null;")
      if (otherRDDs.isEmpty) {
        ctx.addMutableState(decoderClass, decoder,
          s"$decoder = new $rsAdapterClass($rs, $rsPosition);")
      } else {
        ctx.addMutableState(decoderClass, decoder,
          s"""
            if ($inputIsOtherRDD) {
              $decoder = new $rowAdapterClass($unsafeHolder, $baseIndex);
            } else {
              $decoder = new $rsAdapterClass($rs, $rsPosition);
            }
          """
        )
      }
      ctx.addMutableState("long", cursor, s"$cursor = 0L;")
      if (!isEmbedded) {
        columnBufferInitCode.append(s"$buffer = $buffers[$index];")
      } else if (isOffHeap) {
        columnBufferInitCode.append(
          s"""
            $buffer = $batch.getAsBytes($bufferPosition, ($wasNullClass)null);
          """)
      } else {
        columnBufferInitCode.append(
          s"$buffer = $rowFormatter.getLob($buffers, $bufferPosition);")
      }
      columnBufferInitCode.append(
        s"""
          $decoder = $decoderClass.getColumnDecoder($buffer,
            $fields[$index]);
          // intialize the decoder and store the starting cursor position
          $cursor = $decoder.initializeDecoding($buffer, $fields[$index]);
        """)
      bufferInitCode.append(
        s"""
          final $decoderClass $decoderVar = $decoder;
          final byte[] $bufferVar = $buffer;
          long $cursorVar = $cursor;
        """
      )
      cursorUpdateCode.append(s"$cursor = $cursorVar;\n")
      val notNullVar = if (a.nullable) ctx.freshName("notNull") else null
      moveNextCode.append(genCodeColumnNext(ctx, decoderVar, bufferVar,
        cursorVar, "batchOrdinal", a.dataType, notNullVar)).append('\n')
      genCodeColumnBuffer(ctx, decoderVar, bufferVar, cursorVar,
        a.dataType, notNullVar)
    }

    val batchInit = if (!isEmbedded) {
      s"""
        final $cachedBatchClass $batch = ($cachedBatchClass)$colInput.next();
        $buffers = $batch.buffers();
        $numBatchRows = $batch.numRows();
      """
    } else if (isOffHeap) {
      s"""
        final $execRowClass $batch = ($execRowClass)$colInput.next();
        $numBatchRows = $batch.getAsInt(
          ${PartitionedPhysicalScan.CT_NUMROWS_POSITION}, ($wasNullClass)null);
      """
    } else {
      s"""
        final $rowFormatterClass $rowFormatter = $colInput.rowFormatter();
        $buffers = (byte[][])$colInput.next();
        $numBatchRows = $rowFormatter.getAsInt(
          ${PartitionedPhysicalScan.CT_NUMROWS_POSITION}, $buffers[0],
          ($wasNullClass)null);
      """
    }
    val nextBatch = ctx.freshName("nextBatch")
    ctx.addNewFunction(nextBatch,
      s"""
         |private boolean $nextBatch() throws Exception {
         |  if ($buffers != null) return true;
         |  // get next batch or row (latter for non-batch source iteration)
         |  if ($input == null) return false;
         |  if (!$input.hasNext()) {
         |    if ($input == $rowInput) {
         |      $input = $colInput;
         |      $inputIsRow = false;
         |      if ($input == null || !$input.hasNext()) {
         |        return false;
         |      }
         |    } else {
         |      return false;
         |    }
         |  }
         |  if ($inputIsRow) {
         |    $nextRowSnippet
         |    $numBatchRows = 1;
         |  } else {
         |    $batchInit
         |    $numOutputRows.add($numBatchRows);
         |    // initialize the column buffers and decoders
         |    ${columnBufferInitCode.toString()}
         |  }
         |  $batchIndex = 0;
         |  return true;
         |}
      """.stripMargin)

    s"""
       |// Combined iterator for cached batches from column table
       |// and ResultSet from row buffer. Also takes care of otherRDDs
       |// case when partition is of otherRDDs by iterating over it
       |// using an UnsafeRow adapter.
       |long $numRowsBuffer = 0L;
       |long $numRowsOther = 0L;
       |try {
       |  while ($nextBatch()) {
       |    final int numRows = $numBatchRows;
       |    ${bufferInitCode.toString()}
       |    final boolean isRow = $inputIsRow;
       |    for (int batchOrdinal = $batchIndex; batchOrdinal < numRows;
       |         batchOrdinal++) {
       |      ${moveNextCode.toString()}
       |      if (isRow) {
       |        $incrementNumRowsSnippet
       |      }
       |      ${consume(ctx, columnsInput).trim}
       |      if (shouldStop()) {
       |        // increment index for premature return
       |        $batchIndex = batchOrdinal + 1;
       |        // set the cursors
       |        ${cursorUpdateCode.toString()}
       |        return;
       |      }
       |    }
       |    $buffers = null;
       |  }
       |} catch (RuntimeException re) {
       |  throw re;
       |} catch (Exception e) {
       |  throw new RuntimeException(e);
       |} finally {
       |  $numOutputRows.add($numRowsBuffer);
       |  $numRowsBufferOutput.add($numRowsBuffer);
       |  $incrementOtherRows
       |}
    """.stripMargin
  }

  private def genCodeColumnNext(ctx: CodegenContext, decoder: String,
      buffer: String, cursorVar: String, batchOrdinal: String,
      dataType: DataType, notNullVar: String): String = {
    val sqlType = Utils.getSQLDataType(dataType)
    val jt = ctx.javaType(sqlType)
    val moveNext = sqlType match {
      case _ if ctx.isPrimitiveType(jt) =>
        val typeName = ctx.primitiveTypeName(jt)
        s"$cursorVar = $decoder.next$typeName($buffer, $cursorVar);"
      case StringType =>
        s"$cursorVar = $decoder.nextUTF8String($buffer, $cursorVar);"
      case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS =>
        s"$cursorVar = $decoder.nextLongDecimal($buffer, $cursorVar);"
      case _: DecimalType =>
        s"$cursorVar = $decoder.nextDecimal($buffer, $cursorVar);"
      case CalendarIntervalType =>
        s"$cursorVar = $decoder.nextInterval($buffer, $cursorVar);"
      case BinaryType | _: ArrayType | _: MapType | _: StructType =>
        s"$cursorVar = $decoder.nextBinary($buffer, $cursorVar);"
      case NullType => ""
      case _ =>
        throw new UnsupportedOperationException(s"unknown type $sqlType")
    }
    if (notNullVar != null) {
      val nullCode =
        s"final int $notNullVar = $decoder.notNull($buffer, $batchOrdinal);"
      if (moveNext.isEmpty) nullCode
      else s"$nullCode\nif ($notNullVar == 1) $moveNext"
    } else moveNext
  }

  private def genCodeColumnBuffer(ctx: CodegenContext, decoder: String,
      buffer: String, cursorVar: String, dataType: DataType,
      notNullVar: String): ExprCode = {
    val sqlType = Utils.getSQLDataType(dataType)
    val jt = ctx.javaType(sqlType)
    val extract = sqlType match {
      case DateType => s"$decoder.readDate($buffer, $cursorVar)"
      case TimestampType => s"$decoder.readTimestamp($buffer, $cursorVar)"
      case _ if ctx.isPrimitiveType(jt) =>
        s"$decoder.read${ctx.primitiveTypeName(jt)}($buffer, $cursorVar)"
      case StringType => s"$decoder.readUTF8String($buffer, $cursorVar)"
      case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS =>
        s"$decoder.readLongDecimal($buffer, ${d.precision}, ${d.scale}, " +
            s"$cursorVar)"
      case d: DecimalType =>
        s"$decoder.readDecimal($buffer, ${d.precision}, ${d.scale}, $cursorVar)"
      case BinaryType => s"$decoder.readBinary($buffer, $cursorVar)"
      case CalendarIntervalType => s"$decoder.readInterval($buffer, $cursorVar)"
      case _: ArrayType => s"$decoder.readArray($buffer, $cursorVar)"
      case _: MapType => s"$decoder.readMap($buffer, $cursorVar)"
      case t: StructType =>
        s"$decoder.readStruct($buffer, ${t.size}, $cursorVar)"
      case NullType => "null"
      case _ =>
        throw new UnsupportedOperationException(s"unknown type $sqlType")
    }
    val col = ctx.freshName("col")
    if (notNullVar != null) {
      val nullVar = ctx.freshName("nullVal")
      // For ResultSets wasNull() is always a post-facto operation
      // i.e. works only after get has been invoked. However, for column
      // table buffers as well as UnsafeRow adapter, this is not the case
      // and nonNull() should be invoked before get (and get not invoked
      //   at all if nonNull was false). Hence notNull uses tri-state to
      // indicate (true/false/use wasNull) and code below is a tri-switch.
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

/**
 * This class is a simplified copy of Spark's UnionRDD. The reason for
 * having this is to ensure that partition IDs are always assigned in order
 * (which may possibly change in future versions of Spark's UnionRDD)
 * so that a compute on executor can tell owner RDD of the current partition.
 */
private[sql] final class UnionScanRDD[T: ClassTag](
    sc: SparkContext,
    var rdds: Seq[RDD[T]])
    extends RDD[T](sc, Nil) {

  override def getPartitions: Array[Partition] = {
    val array = new Array[Partition](rdds.map(_.getNumPartitions).sum)
    var pos = 0
    for ((rdd, rddIndex) <- rdds.zipWithIndex; split <- rdd.partitions) {
      array(pos) = new UnionPartition(pos, rdd, rddIndex, split.index)
      pos += 1
    }
    array
  }

  override def getDependencies: Seq[Dependency[_]] = {
    val deps = new ArrayBuffer[Dependency[_]]
    var pos = 0
    for (rdd <- rdds) {
      deps += new RangeDependency(rdd, 0, pos, rdd.getNumPartitions)
      pos += rdd.getNumPartitions
    }
    deps
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val part = s.asInstanceOf[UnionPartition[T]]
    parent[T](part.parentRddIndex).iterator(part.parentPartition, context)
  }

  override def getPreferredLocations(s: Partition): Seq[String] =
    s.asInstanceOf[UnionPartition[T]].preferredLocations()

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }
}
