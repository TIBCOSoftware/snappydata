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

import org.apache.spark.rdd.{RDD, UnionPartition}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoding
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.row.{ResultSetEncodingAdapter, ResultSetTraversal, UnsafeRowEncodingAdapter, UnsafeRowHolder}
import org.apache.spark.sql.execution.{PartitionedDataSourceScan, PartitionedPhysicalRDD}
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
  else new UnionScanRDD(rdd.sparkContext, (Seq(rdd) ++ otherRDDs)
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

    if (otherRDDs.isEmpty) {
      ctx.addMutableState("scala.collection.Iterator",
        rowInput, s"$rowInput = (scala.collection.Iterator)inputs[0].next();")
      ctx.addMutableState("scala.collection.Iterator",
        colInput, s"$colInput = (scala.collection.Iterator)inputs[0].next();")
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
      ctx.addMutableState("scala.collection.Iterator", colInput,
        s"""
            $colInput = $inputIsOtherRDD ? null
                : (scala.collection.Iterator)inputs[0].next();
        """)
      ctx.addMutableState("java.sql.ResultSet", rs,
        s"$rs = $inputIsOtherRDD ? null : (($rsIterClass)$rowInput).rs();")
      ctx.addMutableState(unsafeHolderClass, unsafeHolder,
        s"$unsafeHolder = new $unsafeHolderClass();")
    }
    ctx.addMutableState("scala.collection.Iterator", input,
      s"$input = $rowInput;")
    ctx.addMutableState("boolean", inputIsRow, s"$inputIsRow = true;")

    ctx.currentVars = null
    if (scanUnsafeRows) {
      val numRows = ctx.freshName("numRows")
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
      val rsAdapterClass = classOf[ResultSetEncodingAdapter].getName
      val rowAdapterClass = classOf[UnsafeRowEncodingAdapter].getName
      val batch = ctx.freshName("batch")
      val numBatchRows = s"${batch}NumRows"
      val batchIndex = s"${batch}Index"
      val buffers = s"${batch}Buffers"

      val numRowsBuffer = ctx.freshName("numRowsBuffer")
      val numRowsOther = s"${numRowsBuffer}Other"

      ctx.addMutableState("byte[][]", buffers, s"$buffers = null;")
      ctx.addMutableState("int", numBatchRows, s"$numBatchRows = 0;")
      ctx.addMutableState("int", batchIndex, s"$batchIndex = 0;")

      // need DataType and nullable to get decoder in generated code
      val fields = ctx.addReferenceObj("fields", output.toArray, "Attribute[]")
      val columnBufferInitCode = new StringBuilder
      val bufferInitCode = new StringBuilder
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
      val isEmbedded = baseRelation.connectionType match {
        case ConnectionType.Embedded => true
        case _ => false
      }

      val columnsInput = output.zipWithIndex.map { case (a, index) =>
        val decoder = ctx.freshName("decoder")
        val buffer = ctx.freshName("buffer")
        val decoderVar = s"decoder$index"
        val bufferVar = s"buffer$index"
        // projections are not pushed in embedded mode for optimized access
        val baseIndex = baseRelation.schema.fieldIndex(a.name)
        val rsPosition = if (isEmbedded) baseIndex + 1 else index + 1

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
        columnBufferInitCode.append(
          s"""
            $buffer = $buffers[$index];
            $decoder = $decoderClass.getColumnDecoder($buffer,
              $fields[$index]);
          """)
        bufferInitCode.append(
          s"""
            final $decoderClass $decoderVar = $decoder;
            final byte[] $bufferVar = $buffer;
          """
        )
        val notNullVar = if (a.nullable) ctx.freshName("notNull") else null
        moveNextCode.append(genCodeColumnNext(ctx, decoderVar, bufferVar,
          "batchOrdinal", a.dataType, notNullVar)).append('\n')
        genCodeColumnBuffer(ctx, decoderVar, bufferVar, a.dataType, notNullVar)
      }

      val nextBatch = ctx.freshName("nextBatch")
      ctx.addNewFunction(nextBatch,
        s"""
           |private boolean $nextBatch() {
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
           |    $cachedBatchClass $batch = ($cachedBatchClass)$colInput.next();
           |    $buffers = $batch.buffers();
           |    $numBatchRows = $batch.numRows();
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
        s"$decoder.nextBinary($buffer);"
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
