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
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, ExpressionCanonicalizer}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoding
import org.apache.spark.sql.execution.columnar.impl.BaseColumnFormatRelation
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.row.{ResultSetEncodingAdapter, ResultSetTraversal, UnsafeRowEncodingAdapter, UnsafeRowHolder}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String
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
    numBuckets: Int,
    partitionColumns: Seq[Expression],
    partitionColumnAliases: Seq[Seq[Attribute]],
    @transient baseRelation: PartitionedDataSourceScan,
    allFilters: Seq[Expression],
    schemaAttributes: Seq[AttributeReference],
    isForSampleReservoirAsRegion: Boolean = false)
    extends PartitionedPhysicalScan(output, dataRDD, numBuckets,
      partitionColumns, partitionColumnAliases,
      baseRelation.asInstanceOf[BaseRelation]) with CodegenSupport {

  override def getMetrics: Map[String, SQLMetric] = super.getMetrics ++ Map(
    "numRowsBuffer" -> SQLMetrics.createMetric(sparkContext,
      "number of output rows from row buffer"),
    "columnBatchesSeen" -> SQLMetrics.createMetric(sparkContext,
      "column batches seen"),
    "columnBatchesSkipped" -> SQLMetrics.createMetric(sparkContext,
      "column batches skipped by the predicate")) ++ (
      if (otherRDDs.isEmpty) Map.empty
      else Map("numRowsOtherRDDs" -> SQLMetrics.createMetric(sparkContext,
        "number of output rows from other RDDs")))

  private def generateStatPredicate(ctx: CodegenContext): String = {

    val columnBatchStatistics = relation match {
      case columnRelation: BaseColumnFormatRelation =>
        columnRelation.getColumnBatchStatistics(schemaAttributes)
      case _ => null
    }

    def getColumnBatchStatSchema: Seq[AttributeReference] =
      if (columnBatchStatistics ne null) columnBatchStatistics.schema else Nil

    def statsFor(a: Attribute) = columnBatchStatistics.forAttribute(a)

    // Returned filter predicate should return false iff it is impossible
    // for the input expression to evaluate to `true' based on statistics
    // collected about this partition batch.
    // This code is picked up from InMemoryTableScanExec
    @transient def buildFilter: PartialFunction[Expression, Expression] = {
      case And(lhs: Expression, rhs: Expression)
        if buildFilter.isDefinedAt(lhs) || buildFilter.isDefinedAt(rhs) =>
        (buildFilter.lift(lhs) ++ buildFilter.lift(rhs)).reduce(_ && _)

      case Or(lhs: Expression, rhs: Expression)
        if buildFilter.isDefinedAt(lhs) && buildFilter.isDefinedAt(rhs) =>
        buildFilter(lhs) || buildFilter(rhs)

      case EqualTo(a: AttributeReference, l: Literal) =>
        statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound
      case EqualTo(l: Literal, a: AttributeReference) =>
        statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound

      case LessThan(a: AttributeReference, l: Literal) => statsFor(a).lowerBound < l
      case LessThan(l: Literal, a: AttributeReference) => l < statsFor(a).upperBound

      case LessThanOrEqual(a: AttributeReference, l: Literal) => statsFor(a).lowerBound <= l
      case LessThanOrEqual(l: Literal, a: AttributeReference) => l <= statsFor(a).upperBound

      case GreaterThan(a: AttributeReference, l: Literal) => l < statsFor(a).upperBound
      case GreaterThan(l: Literal, a: AttributeReference) => statsFor(a).lowerBound < l

      case GreaterThanOrEqual(a: AttributeReference, l: Literal) => l <= statsFor(a).upperBound
      case GreaterThanOrEqual(l: Literal, a: AttributeReference) => statsFor(a).lowerBound <= l

      case StartsWith(a: AttributeReference, l: Literal) =>
        // upper bound for column (i.e. LessThan) can be found by going to
        // next value of the last character of literal
        val s = l.value.asInstanceOf[UTF8String]
        val len = s.numBytes()
        val upper = new Array[Byte](len)
        s.writeToMemory(upper, Platform.BYTE_ARRAY_OFFSET)
        var lastCharPos = len - 1
        // check for maximum unsigned value 0xff
        val max = 0xff.toByte // -1
        while (lastCharPos >= 0 && upper(lastCharPos) == max) {
          lastCharPos -= 1
        }
        val stats = statsFor(a)
        if (lastCharPos < 0) { // all bytes are 0xff
          // a >= startsWithPREFIX
          l <= stats.upperBound
        } else {
          upper(lastCharPos) = (upper(lastCharPos) + 1).toByte
          val upperLiteral = Literal(UTF8String.fromAddress(upper,
            Platform.BYTE_ARRAY_OFFSET, len), StringType)

          // a >= startsWithPREFIX && a < startsWithPREFIX+1
          l <= stats.upperBound && stats.lowerBound < upperLiteral
        }

      case IsNull(a: Attribute) => statsFor(a).nullCount > 0
      case IsNotNull(a: Attribute) => statsFor(a).count - statsFor(a).nullCount > 0
    }

    // This code is picked up from InMemoryTableScanExec
    val columnBatchStatFilters: Seq[Expression] = {
      if (relation.isInstanceOf[BaseColumnFormatRelation]) {
        allFilters.flatMap { p =>
          val filter = buildFilter.lift(p)
          val boundFilter = filter.map(BindReferences.bindReference(
            _, columnBatchStatistics.schema, allowFailures = true))

          boundFilter.foreach(_ =>
            filter.foreach(f =>
              logDebug(s"Predicate $p generates partition filter: $f")))

          // If the filter can't be resolved then we are missing required statistics.
          boundFilter.filter(_.resolved)
        }
      } else {
        Seq.empty[Expression]
      }
    }

    val columnBatchStatsSchema = getColumnBatchStatSchema
    val numStatFields = columnBatchStatsSchema.length
    val predicate = ExpressionCanonicalizer.execute(
      BindReferences.bindReference(columnBatchStatFilters
          .reduceOption(And).getOrElse(Literal(true)), columnBatchStatsSchema))
    val statRow = ctx.freshName("statRow")
    val statBytes = ctx.freshName("statBytes")
    ctx.INPUT_ROW = statRow
    ctx.currentVars = null
    val predicateEval = predicate.genCode(ctx)

    val platformClass = classOf[Platform].getName
    val columnBatchesSeen = metricTerm(ctx, "columnBatchesSeen")
    val columnBatchesSkipped = metricTerm(ctx, "columnBatchesSkipped")
    // skip filtering if nothing is to be applied
    if (predicateEval.value == "true" && predicateEval.isNull == "false") {
      return ""
    }
    val filterFunction = ctx.freshName("columnBatchFilter")
    ctx.addNewFunction(filterFunction,
      s"""
         |private boolean $filterFunction(byte[] $statBytes) {
         |  final UnsafeRow $statRow = new UnsafeRow($numStatFields);
         |  $statRow.pointTo($statBytes, $platformClass.BYTE_ARRAY_OFFSET,
         |    $statBytes.length);
         |  $columnBatchesSeen.${metricAdd("1")};
         |  // Skip the column batches based on the predicate
         |  ${predicateEval.code}
         |  if (!${predicateEval.isNull} && ${predicateEval.value}) {
         |    return true;
         |  } else {
         |    $columnBatchesSkipped.${metricAdd("1")};
         |    return false;
         |  }
         |}
       """.stripMargin)
    filterFunction
  }

  private val allRDDs = if (otherRDDs.isEmpty) rdd
  else new UnionScanRDD(rdd.sparkContext, (Seq(rdd) ++ otherRDDs)
      .asInstanceOf[Seq[RDD[Any]]])

  private val isOffHeap = GemFireXDUtils.getGemFireContainer(
    baseRelation.table, true).isOffHeap

  private val otherRDDsPartitionIndex = rdd.getNumPartitions

  @transient private val session =
    sqlContext.sparkSession.asInstanceOf[SnappySession]

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    allRDDs.asInstanceOf[RDD[InternalRow]] :: Nil
  }

  override def doProduce(ctx: CodegenContext): String = {
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    val numRowsBuffer = metricTerm(ctx, "numRowsBuffer")
    val numRowsOther =
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
    val rowInputSRR = ctx.freshName("rowInputSRR")
    val input = ctx.freshName("input")
    val inputIsRow = s"${input}IsRow"
    val inputIsRowSRR = s"${input}IsRowSRR"
    val inputIsOtherRDD = s"${input}IsOtherRDD"
    val rs = ctx.freshName("resultSet")
    val rsIterClass = classOf[ResultSetTraversal].getName
    val unsafeHolder = if (otherRDDs.isEmpty && !isForSampleReservoirAsRegion) null
    else ctx.freshName("unsafeHolder")
    val unsafeHolderClass = classOf[UnsafeRowHolder].getName
    val stratumRowClass = classOf[StratumInternalRow].getName

    // TODO [sumedh]: Asif, why this special treatment for weightage column
    // in the code here? Why not as a normal AttributeReference in the plan
    // (or an extension of it if some special treatment is required)?
    val wrappedRow = if (isForSampleReservoirAsRegion) ctx.freshName("wrappedRow")
    else null
    val (weightVarName, weightAssignCode) = if (isForSampleReservoirAsRegion &&
        output.exists(_.name == Utils.WEIGHTAGE_COLUMN_NAME)) {
      val varName = ctx.freshName("weightage")
      ctx.addMutableState("long", varName, s"$varName = 0;")
      (varName, s"$varName = $wrappedRow.weight();")
    } else ("", "")

    val colItrClass = if (!isEmbedded) classOf[CachedBatchIteratorOnRS].getName
    else if (isOffHeap) classOf[OffHeapLobsIteratorOnScan].getName
    else classOf[ByteArraysIteratorOnScan].getName

    if (otherRDDs.isEmpty) {
      if (isForSampleReservoirAsRegion) {
        ctx.addMutableState("scala.collection.Iterator",
          rowInputSRR, s"$rowInputSRR = (scala.collection.Iterator)inputs[0].next();")
        ctx.addMutableState(unsafeHolderClass, unsafeHolder,
          s"$unsafeHolder = new $unsafeHolderClass();")
        ctx.addMutableState("boolean", inputIsRowSRR, s"$inputIsRowSRR = true;")
      }
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
      if (isForSampleReservoirAsRegion) s"$input = $rowInputSRR;"
      else s"$input = $rowInput;")
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

    ctx.addMutableState("byte[][]", buffers, s"$buffers = null;")
    ctx.addMutableState("int", numBatchRows, s"$numBatchRows = 0;")
    ctx.addMutableState("int", batchIndex, s"$batchIndex = 0;")

    // need DataType and nullable to get decoder in generated code
    // shipping as StructType for efficient serialization
    val planSchema = ctx.addReferenceObj("schema", schema,
      classOf[StructType].getName)
    val columnBufferInitCode = new StringBuilder
    val bufferInitCode = new StringBuilder
    val cursorUpdateCode = new StringBuilder
    val moveNextCode = new StringBuilder
    val reservoirRowFetch =
      s"""
         |$stratumRowClass $wrappedRow = ($stratumRowClass)$rowInputSRR.next();
         |$weightAssignCode
         |$unsafeHolder.setRow((UnsafeRow)$wrappedRow.actualRow());
      """.stripMargin

    val nextRowSnippet = if (otherRDDs.isEmpty) {
      if (isForSampleReservoirAsRegion) {
        s"""
          if ($inputIsRowSRR) {
            $reservoirRowFetch
          } else {
            $rowInput.next();
          }
        """
      } else {
        s"$rowInput.next();"
      }
    } else {
      s"""
        if ($inputIsOtherRDD) {
          $unsafeHolder.setRow((UnsafeRow)$rowInput.next());
        } else {
          $rowInput.next();
        }
      """
    }
    val incrementNumRowsSnippet = if (otherRDDs.isEmpty) {
      s"$numRowsBuffer.${metricAdd("1")};"
    } else {
      s"""
        if ($inputIsOtherRDD) {
          $numRowsOther.${metricAdd("1")};
        } else {
          $numRowsBuffer.${metricAdd("1")};
        }
      """
    }
    val incrementOtherRows = if (otherRDDs.isEmpty) ""
    else s"$numOutputRows.${metricAdd(metricValue(numRowsOther))};"

    val initRowTableDecoders = new StringBuilder
    val batchConsumers = getBatchConsumers(parent)
    val columnsInput = output.zipWithIndex.map { case (attr, index) =>
      val decoder = ctx.freshName("decoder")
      val cursor = s"${decoder}Cursor"
      val buffer = ctx.freshName("buffer")
      val cursorVar = s"cursor$index"
      val decoderVar = s"decoder$index"
      val bufferVar = s"buffer$index"
      // projections are not pushed in embedded mode for optimized access
      val baseIndex = baseRelation.schema.fieldIndex(attr.name)
      val rsPosition = if (isEmbedded) baseIndex + 1 else index + 1

      val bufferPosition = baseIndex + PartitionedPhysicalScan.CT_COLUMN_START

      ctx.addMutableState("byte[]", buffer, s"$buffer = null;")

      val rowDecoderCode = s"$decoder = new $rsAdapterClass($rs, $rsPosition);"
      if (otherRDDs.isEmpty) {
        if (isForSampleReservoirAsRegion) {
          ctx.addMutableState(decoderClass, decoder,
            s"$decoder = new $rowAdapterClass($unsafeHolder, $baseIndex);")
          initRowTableDecoders.append(rowDecoderCode).append('\n')
        } else {
          ctx.addMutableState(decoderClass, decoder, rowDecoderCode)
        }
      } else {
        ctx.addMutableState(decoderClass, decoder,
          s"""
            if ($inputIsOtherRDD) {
              $decoder = new $rowAdapterClass($unsafeHolder, $baseIndex);
            } else {
              $rowDecoderCode
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
          $decoder = $decoderClass.getColumnDecoder(
            $buffer, $planSchema.apply($index));
          // intialize the decoder and store the starting cursor position
          $cursor = $decoder.initializeDecoding(
            $buffer, $planSchema.apply($index));
        """)
      bufferInitCode.append(
        s"""
          final $decoderClass $decoderVar = $decoder;
          final byte[] $bufferVar = $buffer;
          long $cursorVar = $cursor;
        """
      )
      cursorUpdateCode.append(s"$cursor = $cursorVar;\n")
      val notNullVar = if (attr.nullable) ctx.freshName("notNull") else null
      moveNextCode.append(genCodeColumnNext(ctx, decoderVar, bufferVar,
        cursorVar, "batchOrdinal", attr.dataType, notNullVar)).append('\n')
      val (ev, bufferInit) = genCodeColumnBuffer(ctx, decoderVar, bufferVar,
        cursorVar, attr, notNullVar, weightVarName)
      bufferInitCode.append(bufferInit)
      ev
    }

    val filterFunction = generateStatPredicate(ctx)
    // TODO: add filter function for non-embedded mode (using store layer
    //   function that will invoke the above function in independent class)
    val batchInit = if (!isEmbedded) {
      val columnBatchesSeen = metricTerm(ctx, "columnBatchesSeen")
      s"""
        final $cachedBatchClass $batch = ($cachedBatchClass)$colInput.next();
        $columnBatchesSeen.${metricAdd("1")};
        $buffers = $batch.buffers();
        $numBatchRows = $batch.numRows();
      """
    } else if (isOffHeap) {
      val filterCode = if (filterFunction.isEmpty) {
        val columnBatchesSeen = metricTerm(ctx, "columnBatchesSeen")
        s"""final $execRowClass $batch = ($execRowClass)$colInput.next();
          $columnBatchesSeen.${metricAdd("1")};"""
      } else {
        s"""$execRowClass $batch;
          while (true) {
            $batch = ($execRowClass)$colInput.next();
            final byte[] statBytes = $batch.getRowBytes(
              ${PartitionedPhysicalScan.CT_STATROW_POSITION});
            if ($filterFunction(statBytes)) {
              break;
            }
            if (!$colInput.hasNext()) return false;
          }"""
      }
      s"""
        $filterCode
        $numBatchRows = $batch.getAsInt(
          ${PartitionedPhysicalScan.CT_NUMROWS_POSITION}, ($wasNullClass)null);
      """
    } else {
      val filterCode = if (filterFunction.isEmpty) {
        val columnBatchesSeen = metricTerm(ctx, "columnBatchesSeen")
        s"""final $rowFormatterClass $rowFormatter = $colInput.rowFormatter();
          $buffers = (byte[][])$colInput.next();
          $columnBatchesSeen.${metricAdd("1")};"""
      } else {
        s"""$rowFormatterClass $rowFormatter;
           while (true) {
             $rowFormatter = $colInput.rowFormatter();
             $buffers = (byte[][])$colInput.next();
             final byte[] statBytes = $rowFormatter.getLob($buffers,
               ${PartitionedPhysicalScan.CT_STATROW_POSITION});
             if ($filterFunction(statBytes)) {
               break;
             }
             if (!$colInput.hasNext()) return false;
           }"""
      }
      s"""
        $filterCode
        $numBatchRows = $rowFormatter.getAsInt(
          ${PartitionedPhysicalScan.CT_NUMROWS_POSITION}, $buffers[0],
          ($wasNullClass)null);
      """
    }
    val nextBatch = ctx.freshName("nextBatch")
    val switchSRR = if (isForSampleReservoirAsRegion) {
      // triple switch between rowInputSRR, rowInput, colInput
      s"""
         |if ($input == $rowInputSRR) {
         |  $input = $rowInput;
         |  $inputIsRowSRR = false;
         |  $inputIsRow = true;
         |  if ($input.hasNext()) {
         |    $initRowTableDecoders
         |    $input.next();
         |    $numBatchRows = 1;
         |    $incrementNumRowsSnippet
         |    return true;
         |  }
         |  // else fall back to row table consumed case
         |}
      """.stripMargin
    } else ""
    ctx.addNewFunction(nextBatch,
      s"""
         |private boolean $nextBatch() throws Exception {
         |  if ($buffers != null) return true;
         |  // get next batch or row (latter for non-batch source iteration)
         |  if ($input == null) return false;
         |  if (!$input.hasNext()) {
         |    ${switchSRR}if ($input == $rowInput) {
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
         |    $incrementNumRowsSnippet
         |  } else {
         |    $batchInit
         |    $numOutputRows.${metricAdd(numBatchRows)};
         |    // initialize the column buffers and decoders
         |    ${columnBufferInitCode.toString()}
         |  }
         |  $batchIndex = 0;
         |  return true;
         |}
      """.stripMargin)

    val batchConsume = batchConsumers.map(_.batchConsume(ctx, this,
      columnsInput)).mkString("\n")
    val finallyCode = session.evaluateFinallyCode(ctx)
    val consumeCode = consume(ctx, columnsInput).trim

    s"""
       |// Combined iterator for column batches from column table
       |// and ResultSet from row buffer. Also takes care of otherRDDs
       |// case when partition is of otherRDDs by iterating over it
       |// using an UnsafeRow adapter.
       |try {
       |  while ($nextBatch()) {
       |    ${bufferInitCode.toString()}
       |    $batchConsume
       |    final int numRows = $numBatchRows;
       |    for (int batchOrdinal = $batchIndex; batchOrdinal < numRows;
       |         batchOrdinal++) {
       |      ${moveNextCode.toString()}
       |      $consumeCode
       |      if (shouldStop()) {
       |        // increment index for return
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
       |  $numOutputRows.${metricAdd(metricValue(numRowsBuffer))};
       |  $finallyCode
       |  $incrementOtherRows
       |}
    """.stripMargin
  }

  private def getBatchConsumers(parent: CodegenSupport): List[BatchConsumer] = {
    parent match {
      case null => Nil
      case b: BatchConsumer if b.canConsume(this) => b :: getBatchConsumers(
        TypeUtilities.parentMethod.invoke(parent).asInstanceOf[CodegenSupport])
      case _ => getBatchConsumers(TypeUtilities.parentMethod.invoke(parent)
          .asInstanceOf[CodegenSupport])
    }
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
      buffer: String, cursorVar: String, attr: Attribute,
      notNullVar: String, weightVar: String): (ExprCode, String) = {
    val col = ctx.freshName("col")
    var bufferInit = ""
    var dictionaryAssignCode = ""
    var assignCode = ""
    var dictionary = ""
    var dictIndex = ""
    var dictionaryLen = ""
    val sqlType = Utils.getSQLDataType(attr.dataType)
    val jt = ctx.javaType(sqlType)
    var jtDecl = s"final $jt $col;"
    val nullVar = ctx.freshName("nullVal")
    val colAssign = sqlType match {
      case DateType => s"$col = $decoder.readDate($buffer, $cursorVar);"
      case TimestampType =>
        s"$col = $decoder.readTimestamp($buffer, $cursorVar);"
      case _ if ctx.isPrimitiveType(jt) =>
        val typeName = ctx.primitiveTypeName(jt)
        s"$col = $decoder.read$typeName($buffer, $cursorVar);"
      case StringType =>
        dictionary = ctx.freshName("dictionary")
        dictIndex = ctx.freshName("dictionaryIndex")
        dictionaryLen = ctx.freshName("dictionaryLength")
        // initialize index to dictionaryLength - 1 where null value will
        // reside in case there are nulls in the current batch
        jtDecl = s"UTF8String $col; int $dictIndex = $dictionaryLen - 1;"
        bufferInit =
            s"""
               |final UTF8String[] $dictionary = $decoder.getStringDictionary();
               |final int $dictionaryLen =
               |    $dictionary != null ? $dictionary.length : -1;
            """.stripMargin
        dictionaryAssignCode =
            s"$dictIndex = $decoder.readDictionaryIndex($buffer, $cursorVar);"
        val nullCheckAddon =
          if (notNullVar != null) s"if ($notNullVar < 0) $nullVar = $col == null;\n"
          else ""
        assignCode =
          s"($dictionary != null ? $dictionary[$dictIndex] " +
              s": $decoder.readUTF8String($buffer, $cursorVar));\n" +
              s"$nullCheckAddon"

        s"$dictionaryAssignCode\n$col = $assignCode;"
      case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS =>
        s"$col = $decoder.readLongDecimal($buffer, ${d.precision}, " +
            s"${d.scale}, $cursorVar);"
      case d: DecimalType =>
        s"$col = $decoder.readDecimal($buffer, ${d.precision}, " +
            s"${d.scale}, $cursorVar);"
      case BinaryType => s"$col = $decoder.readBinary($buffer, $cursorVar);"
      case CalendarIntervalType =>
        s"$col = $decoder.readInterval($buffer, $cursorVar);"
      case _: ArrayType => s"$col = $decoder.readArray($buffer, $cursorVar);"
      case _: MapType => s"$col = $decoder.readMap($buffer, $cursorVar);"
      case t: StructType =>
        s"$col = $decoder.readStruct($buffer, ${t.size}, $cursorVar);"
      case NullType => s"$col = null;"
      case _ =>
        throw new UnsupportedOperationException(s"unknown type $sqlType")
    }
    if (notNullVar != null) {
      // For ResultSets wasNull() is always a post-facto operation
      // i.e. works only after get has been invoked. However, for column
      // table buffers as well as UnsafeRow adapter, this is not the case
      // and nonNull() should be invoked before get (and get not invoked
      //   at all if nonNull was false). Hence notNull uses tri-state to
      // indicate (true/false/use wasNull) and code below is a tri-switch.
      val code = s"""
          $jtDecl
          final boolean $nullVar;
          if ($notNullVar == 1) {
            $colAssign
            $nullVar = false;
          } else {
            if ($notNullVar == 0) {
              $col = ${ctx.defaultValue(jt)};
              $nullVar = true;
            } else {
              $colAssign
              $nullVar = $decoder.wasNull();
            }
          }
        """
      if (!dictionary.isEmpty) {
        val dictionaryCode =
          s"""
            $jtDecl
            final boolean $nullVar;
            if ($notNullVar == 1) {
              $dictionaryAssignCode
              $nullVar = false;
            } else {
              if ($notNullVar == 0) {
                $nullVar = true;
              } else {
                $dictionaryAssignCode
                $nullVar = $decoder.wasNull();
              }
            }
          """
        session.addExCode(ctx, col :: Nil, attr :: Nil, ExprCodeEx(None,
          dictionaryCode, assignCode, dictionary, dictIndex, dictionaryLen))
      }
      (ExprCode(code, nullVar, col), bufferInit)
    } else {
      if (!dictionary.isEmpty) {
        val dictionaryCode = jtDecl + '\n' + dictionaryAssignCode
        session.addExCode(ctx, col :: Nil, attr :: Nil, ExprCodeEx(None,
          dictionaryCode, assignCode, dictionary, dictIndex, dictionaryLen))
      }
      var code = jtDecl + '\n' + colAssign + '\n'
      if (weightVar != null && attr.name == Utils.WEIGHTAGE_COLUMN_NAME) {
        code += s"if ($col == 1) $col = $weightVar;\n"
      }
      (ExprCode(code, "false", col), bufferInit)
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
