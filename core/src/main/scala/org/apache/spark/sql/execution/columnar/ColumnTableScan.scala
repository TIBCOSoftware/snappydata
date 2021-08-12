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

import io.snappydata.ResultSetWithNull

import org.apache.spark.rdd.{RDD, UnionPartition}
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, ExpressionCanonicalizer}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, RangePartitioning}
import org.apache.spark.sql.collection.{LazyIterator, SharedUtils, Utils}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.encoding._
import org.apache.spark.sql.execution.columnar.impl.{BaseColumnFormatRelation, ColumnDelta}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.row.{ResultSetDecoder, ResultSetTraversal, UnsafeRowDecoder, UnsafeRowHolder}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types._
import org.apache.spark.{Dependency, Logging, Partition, RangeDependency, SparkContext, TaskContext}

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
    relationSchema: StructType,
    allFilters: Seq[Expression],
    schemaAttributes: Seq[AttributeReference],
    caseSensitive: Boolean,
    isForSampleReservoirAsRegion: Boolean = false)
    extends PartitionedPhysicalScan(output, dataRDD, numBuckets,
      partitionColumns, partitionColumnAliases,
      baseRelation.asInstanceOf[BaseRelation]) with CodegenSupport {

  override val nodeName: String = {
    if (baseRelation != null && baseRelation.getClass.getName.contains("Sampl")) "SampleTableScan"
    else "ColumnTableScan"
  }

  override def sameResult(plan: SparkPlan): Boolean = plan match {
    case r: ColumnTableScan => r.baseRelation.table == baseRelation.table &&
        r.numBuckets == numBuckets && r.schema == schema
    case _ => false
  }

  @transient private val MAX_SCHEMA_LENGTH = 40

  override protected def baseOutputPartitioning: Partitioning = {
    // if the column output contains bucketId then do an ordered partition on the same
    // to match ColumnExec.requiredChildDistribution
    output.find(_.name.equalsIgnoreCase(ColumnDelta.BucketId.name)) match {
      case Some(attr) =>
        RangePartitioning(StoreUtils.getColumnUpdateDeleteOrdering(attr) :: Nil, numBuckets)
      case _ => super.baseOutputPartitioning
    }
  }

  override lazy val outputOrdering: Seq[SortOrder] = {
    val buffer = new ArrayBuffer[SortOrder](3)
    // ordered on [bucketId, batchId, ordinal (position within batch)] for update/delete
    Seq(ColumnDelta.BucketId.name, ColumnDelta.BatchId.name, ColumnDelta.RowOrdinal.name).foreach(
      name => output.find(_.name.equalsIgnoreCase(name)) match {
        case Some(attr) => buffer += StoreUtils.getColumnUpdateDeleteOrdering(attr)
        case _ =>
      })
    // either all columns should be present or none
    if (buffer.nonEmpty && buffer.length != 3) {
      throw new IllegalStateException("ColumnTableScan: all three columns (bucketId, batchId, " +
          s"rowOrdinal) must be present for update/delete scan but only found: $buffer")
    }
    buffer
  }

  override def getMetrics: Map[String, SQLMetric] = {
    if (sqlContext eq null) Map.empty
    else super.getMetrics ++ Map(
      "numRowsBuffer" -> SQLMetrics.createMetric(sparkContext,
        "number of output rows from row buffer"),
      "columnBatchesSeen" -> SQLMetrics.createMetric(sparkContext,
        "column batches seen"),
      "updatedColumnCount" -> SQLMetrics.createMetric(sparkContext,
        "total updated columns in batches"),
      "deletedBatchCount" -> SQLMetrics.createMetric(sparkContext,
        "column batches having deletes"),
      "columnBatchesSkipped" -> SQLMetrics.createMetric(sparkContext,
        "column batches skipped by the predicate")) ++ (
        if (otherRDDs.isEmpty) Map.empty
        else Map("numRowsOtherRDDs" -> SQLMetrics.createMetric(sparkContext,
          "number of output rows from other RDDs")))
  }

  override def metricTerm(ctx: CodegenContext, name: String): String =
    if (sqlContext eq null) null else super.metricTerm(ctx, name)

  private val allRDDs = if (otherRDDs.isEmpty) rdd
  else new UnionScanRDD(rdd.sparkContext, (Seq(rdd) ++ otherRDDs)
      .asInstanceOf[Seq[RDD[Any]]])

  private lazy val otherRDDsPartitionIndex = rdd.getNumPartitions


  @transient private val session =
    Option(sqlContext).map(_.sparkSession.asInstanceOf[SnappySession])

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    allRDDs.asInstanceOf[RDD[InternalRow]] :: Nil
  }

  def splitToMethods(ctx: CodegenContext, blocks: ArrayBuffer[String]): String = {
    val apply = ctx.freshName("apply")
    val functions = blocks.zipWithIndex.map { case (body, i) =>
      val name = s"${apply}_$i"
      val code =
        s"""
           |private void $name() {
           |  $body
           |}
         """.stripMargin
      ctx.addNewFunction(name, code)
      name
    }
    functions.map(name => s"$name();").mkString("\n")
  }

  def convertExprToMethodCall(ctx: CodegenContext, expr: ExprCode,
      attr: Attribute, index: Int, batchOrdinal: String): ExprCode = {
    val retValName = ctx.freshName(s"col$index")
    val nullVarForCol = ctx.freshName(s"nullVarForCol$index")
    ctx.addMutableState("boolean", nullVarForCol, "")
    val sqlType = Utils.getSQLDataType(attr.dataType)
    val jt = ctx.javaType(sqlType)
    val name = s"readValue_$index"
    val code =
      s"""
         |private $jt $name(int $batchOrdinal) {
         |  ${expr.code}
         |  $nullVarForCol = ${expr.isNull};
         |  return ${expr.value};
         |}
         """.stripMargin
    ctx.addNewFunction(name, code)
    val exprCode =
      s"""
         |$jt $retValName = $name($batchOrdinal);
       """.stripMargin
    ExprCode(exprCode, s"$nullVarForCol", s"$retValName")
  }

  override def doProduce(ctx: CodegenContext): String = {
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    val numRowsBuffer = metricTerm(ctx, "numRowsBuffer")
    val numRowsOther =
      if (otherRDDs.isEmpty) null else metricTerm(ctx, "numRowsOtherRDDs")
    val embedded = (baseRelation eq null) ||
      (baseRelation.connectionType == ConnectionType.Embedded)
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
    val lazyIteratorClass = classOf[LazyIterator[Any]].getName
    val rsIterClass = classOf[ResultSetTraversal].getName
    val unsafeHolder = if (otherRDDs.isEmpty && !isForSampleReservoirAsRegion) null
    else ctx.freshName("unsafeHolder")
    val updatedColumnCount = metricTerm(ctx, "updatedColumnCount")
    val deletedBatchCount = metricTerm(ctx, "deletedBatchCount")
    val unsafeHolderClass = classOf[UnsafeRowHolder].getName
    val stratumRowClass = classOf[StratumInternalRow].getName

    // TODO [sumedh]: Asif, why this special treatment for weightage column
    // in the code here? Why not as a normal AttributeReference in the plan
    // (or an extension of it if some special treatment is required)?
    val wrappedRow = if (isForSampleReservoirAsRegion) ctx.freshName("wrappedRow")
    else null
    val (weightVarName, weightAssignCode) = if (output.exists(_.name.equalsIgnoreCase(
        Utils.WEIGHTAGE_COLUMN_NAME))) {
      val varName = ctx.freshName("weightage")
      ctx.addMutableState("long", varName, s"$varName = 0;")
      (varName, s"$varName = $wrappedRow.weight();")
    } else ("", "")

    val iteratorClass = "scala.collection.Iterator"
    val colIteratorClass = if (embedded) classOf[ColumnBatchIterator].getName
    else classOf[ColumnBatchIteratorOnRS].getName
    if (otherRDDs.isEmpty) {
      if (isForSampleReservoirAsRegion) {
        ctx.addMutableState(iteratorClass, rowInputSRR,
          s"$rowInputSRR = ($iteratorClass)inputs[0].next();")
        ctx.addMutableState(unsafeHolderClass, unsafeHolder,
          s"$unsafeHolder = new $unsafeHolderClass();")
        ctx.addMutableState("boolean", inputIsRowSRR, s"$inputIsRowSRR = true;")
      }
      ctx.addMutableState(iteratorClass, rowInput,
        s"$rowInput = ($iteratorClass)inputs[0].next();")
      ctx.addMutableState(colIteratorClass, colInput,
        s"$colInput = ($colIteratorClass)inputs[0].next();")
      ctx.addMutableState("java.sql.ResultSet", rs,
        s"$rs = (($rsIterClass)(($lazyIteratorClass)$rowInput).iterator()).rs();")
    } else {
      ctx.addMutableState("boolean", inputIsOtherRDD,
        s"$inputIsOtherRDD = (partitionIndex >= $otherRDDsPartitionIndex);")
      ctx.addMutableState(iteratorClass, rowInput,
        s"$rowInput = $inputIsOtherRDD ? inputs[0] " +
            s": ($iteratorClass)inputs[0].next();")
      ctx.addMutableState(colIteratorClass, colInput,
        s"$colInput = $inputIsOtherRDD ? null : ($colIteratorClass)inputs[0].next();")
      ctx.addMutableState("java.sql.ResultSet", rs,
        s"$rs = $inputIsOtherRDD ? null : " +
            s"(($rsIterClass)(($lazyIteratorClass)$rowInput).iterator()).rs();")
      ctx.addMutableState(unsafeHolderClass, unsafeHolder,
        s"$unsafeHolder = new $unsafeHolderClass();")
    }
    ctx.addMutableState(iteratorClass, input,
      if (isForSampleReservoirAsRegion) s"$input = $rowInputSRR;"
      else s"$input = $rowInput;")
    ctx.addMutableState("boolean", inputIsRow, s"$inputIsRow = true;")

    ctx.currentVars = null
    val encodingClass = ColumnEncoding.encodingClassName
    val decoderClass = classOf[ColumnDecoder].getName
    val updatedDecoderClass = classOf[UpdatedColumnDecoderBase].getName
    val rsDecoderClass = classOf[ResultSetDecoder].getName
    val rsWithNullClass = classOf[ResultSetWithNull].getName
    val rowDecoderClass = classOf[UnsafeRowDecoder].getName
    val deletedDecoderClass = classOf[ColumnDeleteDecoder].getName
    val batch = ctx.freshName("batch")
    val hasUpdates = s"${batch}HasUpdates"
    val numBatchRows = s"${batch}NumRows"
    val batchIndex = s"${batch}Index"
    val buffers = s"${batch}Buffers"
    val numRows = ctx.freshName("numRows")
    val batchOrdinal = ctx.freshName("batchOrdinal")
    val deletedDecoder = s"${batch}Deleted"
    val deletedDecoderLocal = s"${deletedDecoder}Local"
    var deletedDeclaration = ""
    var deletedCheck = ""
    val deletedCount = ctx.freshName("deletedCount")
    var deletedCountCheck = ""

    ctx.addMutableState("java.nio.ByteBuffer", buffers, "")
    ctx.addMutableState("boolean", hasUpdates, "")
    ctx.addMutableState("int", numBatchRows, "")
    ctx.addMutableState("int", batchIndex, "")
    ctx.addMutableState(deletedDecoderClass, deletedDecoder, "")
    ctx.addMutableState("int", deletedCount, "")

    // need DataType and nullable to get decoder in generated code
    // shipping as StructType for efficient serialization
    val planSchema = ctx.addReferenceObj("schema", schema,
      classOf[StructType].getName)
    val columnBufferInit = new StringBuilder
    val bufferInitCode = new StringBuilder
    val closeDecoders = new StringBuilder
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
      if (numRowsBuffer eq null) ""
      else s"$numRowsBuffer.${metricAdd("1")};"
    } else {
      s"""
        if ($inputIsOtherRDD) {
          $numRowsOther.${metricAdd("1")};
        } else {
          $numRowsBuffer.${metricAdd("1")};
        }
      """
    }

    val initRowTableDecoders = new StringBuilder
    val bufferInitCodeBlocks = new ArrayBuffer[String]()

    val isWideSchema = output.length > MAX_SCHEMA_LENGTH
    val batchConsumers = getBatchConsumers(parent)
    // "key" columns for update/delete with reserved names in ColumnDelta.mutableKeyNames
    var columnBatchIdTerm: String = null
    var ordinalIdTerm: String = null
    var bucketIdTerm: String = null

    // this mapper is for the physical columns in the table
    val columnsInputMapper = (attr: Attribute, index: Int, rsIndex: Int) => {
      val decoder = ctx.freshName("decoder")
      val decoderLocal = s"${decoder}Local"
      val updatedDecoder = s"${decoder}Updated"
      val updatedDecoderLocal = s"${decoder}UpdatedLocal"
      val numNullsVar = s"${decoder}NumNulls"
      val buffer = s"${decoder}Buffer"
      val bufferVar = s"${buffer}Object"
      val initBufferFunction = s"${buffer}Init"
      val closeDecoderFunction = s"${decoder}Close"
      if (isWideSchema) {
        ctx.addMutableState("Object", bufferVar, "")
      }
      // projections are not pushed in embedded mode for optimized access
      val baseIndex = Utils.fieldIndex(schemaAttributes, attr.name, caseSensitive)
      val rsPosition = if (embedded) baseIndex + 1 else rsIndex + 1
      val incrementUpdatedColumnCount = if (updatedColumnCount eq null) ""
      else s"\n$updatedColumnCount.${metricAdd("1")};"

      ctx.addMutableState("java.nio.ByteBuffer", buffer, "")
      ctx.addMutableState("int", numNullsVar, "")

      val rowDecoderCode =
        s"$decoder = new $rsDecoderClass(($rsWithNullClass)$rs, $rsPosition);"
      if (otherRDDs.isEmpty) {
        if (isForSampleReservoirAsRegion) {
          ctx.addMutableState(decoderClass, decoder,
            s"$decoder = new $rowDecoderClass($unsafeHolder, $baseIndex);")
          initRowTableDecoders.append(rowDecoderCode).append('\n')
        } else {
          ctx.addMutableState(decoderClass, decoder, rowDecoderCode)
        }
      } else {
        ctx.addMutableState(decoderClass, decoder,
          s"""
            if ($inputIsOtherRDD) {
              $decoder = new $rowDecoderClass($unsafeHolder, $baseIndex);
            } else {
              $rowDecoderCode
            }
          """
        )
      }
      ctx.addMutableState(updatedDecoderClass, updatedDecoder, "")

      ctx.addNewFunction(initBufferFunction,
        s"""
           |private void $initBufferFunction() {
           |  $buffer = $colInput.getColumnLob($baseIndex);
           |  $decoder = $encodingClass.getColumnDecoder($buffer,
           |      $planSchema.apply($index));
           |  // check for updated column
           |  $updatedDecoder = $hasUpdates ? $colInput.getUpdatedColumnDecoder(
           |      $decoder, $planSchema.apply($index), $baseIndex) : null;
           |  if ($updatedDecoder != null) {
           |    $incrementUpdatedColumnCount
           |  }
           |  $numNullsVar = 0;
           |}
        """.stripMargin)
      columnBufferInit.append(s"$initBufferFunction();\n")

      ctx.addNewFunction(closeDecoderFunction,
        s"""
           |private void $closeDecoderFunction() {
           |  if ($decoder != null) {
           |    $decoder.close();
           |  }
           |  if ($updatedDecoder != null) {
           |    $updatedDecoder.close();
           |  }
           |}
        """.stripMargin)
      closeDecoders.append(s"$closeDecoderFunction();\n")

      if (isWideSchema) {
        if (bufferInitCode.length > 1024) {
          bufferInitCodeBlocks.append(bufferInitCode.toString())
          bufferInitCode.clear()
        }

        bufferInitCode.append(
          s"$bufferVar = ($buffer == null || $buffer.isDirect()) ? null : $buffer.array();\n")
      } else {
        bufferInitCode.append(
          s"""
             |final $decoderClass $decoderLocal = $decoder;
             |final $updatedDecoderClass $updatedDecoderLocal = $updatedDecoder;
             |final Object $bufferVar = ($buffer == null || $buffer.isDirect())
             |    ? null : $buffer.array();
          """.stripMargin)
      }

      if (!isWideSchema) {
        genCodeColumnBuffer(ctx, decoderLocal, updatedDecoderLocal, decoder, updatedDecoder,
          bufferVar, batchOrdinal, numNullsVar, attr, weightVarName)
      } else {
        val ev = genCodeColumnBuffer(ctx, decoder, updatedDecoder, decoder, updatedDecoder,
          bufferVar, batchOrdinal, numNullsVar, attr, weightVarName)
        convertExprToMethodCall(ctx, ev, attr, index, batchOrdinal)
      }
    }
    var rsIndex = -1
    val columnsInput = output.zipWithIndex.map {
      case (attr, _) if attr.name.startsWith(ColumnDelta.mutableKeyNamePrefix) =>
        ColumnDelta.mutableKeyMap.get(attr.name) match {
          case Some(ColumnDelta.RowOrdinal) =>
            ordinalIdTerm = ctx.freshName("ordinalId")
            ExprCode("", "false", ordinalIdTerm)
          case Some(ColumnDelta.BatchId) =>
            columnBatchIdTerm = ctx.freshName("columnBatchId")
            ExprCode("", "false", columnBatchIdTerm)
          case Some(ColumnDelta.BucketId) =>
            bucketIdTerm = ctx.freshName("bucketId")
            ExprCode("", "false", bucketIdTerm)
          case Some(ColumnDelta.BatchNumRows) => ExprCode("", "false", numBatchRows)
          case _ => throw new IllegalStateException(s"Unexpected internal attribute $attr")
        }
      case (attr, index) => rsIndex += 1; columnsInputMapper(attr, index, rsIndex)
    }

    if (output.isEmpty) {
      // no columns in a count(.) query
      deletedCountCheck = s" - ($inputIsRow ? 0 : $deletedCount)"
    } else {
      // add deleted column check if there is at least one column to scan
      // (else it is taken care of by deletedCount being reduced from batch size)
      val incrementDeletedBatchCount = if (deletedBatchCount eq null) ""
      else s"\nif ($deletedDecoder != null) $deletedBatchCount.${metricAdd("1")};"
      columnBufferInit.append(
        s"$deletedDecoder = $colInput.getDeletedColumnDecoder();$incrementDeletedBatchCount\n")
      deletedDeclaration =
          s"final $deletedDecoderClass $deletedDecoderLocal = $deletedDecoder;\n"
      deletedCheck = s"if ($deletedDecoderLocal != null && " +
          s"$deletedDecoderLocal.deleted($batchOrdinal)) continue;"
    }

    if (isWideSchema) {
      bufferInitCodeBlocks.append(bufferInitCode.toString())
    }

    val bufferInitCodeStr = if (isWideSchema) {
      splitToMethods(ctx, bufferInitCodeBlocks)
    } else {
      bufferInitCode.toString()
    }

    // for smart connector, the filters are pushed down in the procedure sent to stores
    val filterFunction = if (embedded) ColumnTableScan.generateStatPredicate(ctx,
      relation.isInstanceOf[BaseColumnFormatRelation], schemaAttributes,
      allFilters, numBatchRows, metricTerm, metricAdd, simpleString) else ""
    val statsRow = ctx.freshName("statsRow")
    val colNextBytes = ctx.freshName("colNextBytes")
    val numTableColumns = if (relationSchema.exists(
      _.name.equalsIgnoreCase(ColumnDelta.RowOrdinal.name))) {
      relationSchema.length - ColumnDelta.mutableKeyNames.length // for update/delete
    } else relationSchema.length
    val numColumnsInStatBlob = ColumnStatsSchema.numStatsColumns(numTableColumns)

    val incrementBatchOutputRows = if (numOutputRows ne null) {
      s"$numOutputRows.${metricAdd(s"$numBatchRows - $deletedCount")};"
    } else ""
    val incrementBufferOutputRows = if (numOutputRows ne null) {
      s"$numOutputRows.${metricAdd(metricValue(numRowsBuffer))};"
    } else ""
    val incrementOtherRows = if (otherRDDs.isEmpty) ""
    else s"$numOutputRows.${metricAdd(metricValue(numRowsOther))};"
    val columnBatchesSeen = metricTerm(ctx, "columnBatchesSeen")
    val incrementBatchCount = if (columnBatchesSeen eq null) ""
    else s"$columnBatchesSeen.${metricAdd("1")};"
    val batchAssign =
      s"""
        final java.nio.ByteBuffer $colNextBytes = (java.nio.ByteBuffer)$colInput.next();
        UnsafeRow $statsRow = ${SharedUtils.getClass.getName}.MODULE$$.toUnsafeRow(
          $colNextBytes, $numColumnsInStatBlob);
        $hasUpdates = $colInput.getCurrentDeltaStats() != null; // old delta stats row
        $numBatchRows = $statsRow.getInt(${ColumnStatsSchema.COUNT_INDEX_IN_SCHEMA});
        if ($numBatchRows < 0) { // indicates delta updates
          $hasUpdates = true;
          $numBatchRows = -$numBatchRows;
        }
        $incrementBatchCount
        $buffers = $colNextBytes;
      """
    val batchInit = if (filterFunction.isEmpty) batchAssign else {
      s"""
        while (true) {
          $batchAssign
          // skip filtering if old format delta stats row containing obsolete data is present
          if ($colInput.getCurrentDeltaStats() != null ||
              $filterFunction($statsRow, $numBatchRows)) {
            break;
          }
          if (!$colInput.hasNext()) return false;
        }"""
    }
    val nextBatch = ctx.freshName("nextBatch")
    val closeDecodersFunction = ctx.freshName("closeAllDecoders")
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
         |  $closeDecodersFunction();
         |  // get next batch or row (latter for non-batch source iteration)
         |  if ($input == null) return false;
         |  if (!$input.hasNext()) {
         |    ${switchSRR}if ($input == $rowInput) {
         |      $incrementBufferOutputRows
         |      $incrementOtherRows
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
         |    $deletedCount = $colInput.getDeletedRowCount();
         |    $incrementBatchOutputRows
         |    // initialize the column buffers and decoders
         |    $columnBufferInit
         |  }
         |  $batchIndex = 0;
         |  return true;
         |}
      """.stripMargin)
    ctx.addNewFunction(closeDecodersFunction,
      s"""
         |private void $closeDecodersFunction() {
         |  ${closeDecoders.toString()}
         |}
      """.stripMargin)

    val (assignBatchId, assignOrdinalId) = if (ordinalIdTerm ne null) (
        s"""
           |final boolean $inputIsRow = this.$inputIsRow;
           |final long $columnBatchIdTerm;
           |final int $bucketIdTerm;
           |if ($inputIsRow) {
           |  $columnBatchIdTerm = $invalidUUID;
           |  $bucketIdTerm = -1; // not required for row buffer
           |} else {
           |  $columnBatchIdTerm = $colInput.getCurrentBatchId();
           |  $bucketIdTerm = $colInput.getCurrentBucketId();
           |}
        """.stripMargin,
        // ordinalId is the last column in the row buffer table (exclude virtual columns)
        s"""
           |final long $ordinalIdTerm = $inputIsRow ? $rs.getLong(
           |    ${if (embedded) relationSchema.length - 3 else output.length - 3}) : $batchOrdinal;
        """.stripMargin)
    else ("", "")
    val batchConsume = batchConsumers.map(_.batchConsume(ctx, this,
      columnsInput)).mkString("\n").trim
    val beforeStop = batchConsumers.map(_.beforeStop(ctx, this,
      columnsInput)).mkString("\n").trim
    val finallyCode = session match {
      case Some(s) => s.evaluateFinallyCode(ctx)
      case _ => ""
    }
    val consumeCode = consume(ctx, columnsInput).trim

    s"""
       |// Combined iterator for column batches from column table
       |// and ResultSet from row buffer. Also takes care of otherRDDs
       |// case when partition is of otherRDDs by iterating over it
       |// using an UnsafeRow adapter.
       |try {
       |  while ($nextBatch()) {
       |    $bufferInitCodeStr
       |    $assignBatchId
       |    $batchConsume
       |    $deletedDeclaration
       |    final int $numRows = $numBatchRows$deletedCountCheck;
       |    for (int $batchOrdinal = $batchIndex; $batchOrdinal < $numRows;
       |         $batchOrdinal++) {
       |      $deletedCheck
       |      $assignOrdinalId
       |      $consumeCode
       |      if (shouldStop()) {
       |        $beforeStop
       |        // increment index for return
       |        $batchIndex = $batchOrdinal + 1;
       |        return;
       |      }
       |    }
       |    $buffers = null;
       |  }
       |} catch (java.io.IOException ioe) {
       |  throw ioe;
       |} catch (RuntimeException re) {
       |  throw re;
       |} catch (Exception e) {
       |  throw new java.io.IOException(e.toString(), e);
       |} finally {
       |  $finallyCode
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

  private def genCodeColumnBuffer(ctx: CodegenContext, decoder: String, updateDecoder: String,
      decoderGlobal: String, mutableDecoderGlobal: String, buffer: String, batchOrdinal: String,
      numNullsVar: String, attr: Attribute, weightVar: String): ExprCode = {
    val nonNullPosition = if (attr.nullable) s"$batchOrdinal - $numNullsVar" else batchOrdinal
    val col = ctx.freshName("col")
    val sqlType = Utils.getSQLDataType(attr.dataType)
    val jt = ctx.javaType(sqlType)
    var colAssign = ""
    var updatedAssign = ""
    val typeName = sqlType match {
      case DateType => "Date"
      case TimestampType => "Timestamp"
      case _ if ctx.isPrimitiveType(jt) => ctx.primitiveTypeName(jt)
      case StringType =>
        val dictionaryVar = ctx.freshName("dictionary")
        val dictionaryIndexVar = ctx.freshName("dictionaryIndex")
        val dictionary = ExprCode(
          s"""
             |$dictionaryVar = $mutableDecoderGlobal == null
             |    ? $decoderGlobal.getStringDictionary()
             |    : $mutableDecoderGlobal.getStringDictionary();
          """.stripMargin, s"($dictionaryVar == null)", dictionaryVar)
        val dictionaryIndex = if (attr.nullable) {
          ExprCode(
            s"""
               |${genIfNonNullCode(ctx, decoder, buffer, batchOrdinal, numNullsVar)} {
               |  $dictionaryIndexVar = $updateDecoder == null
               |    ? $decoder.readDictionaryIndex($buffer, $nonNullPosition)
               |    : $updateDecoder.readDictionaryIndex();
               |} else {
               |  $dictionaryIndexVar = $dictionaryVar.size();
               |}
               """.stripMargin, "false", dictionaryIndexVar)
        } else {
          ExprCode(
            s"""
               |$dictionaryIndexVar = $updateDecoder == null
               |    ? $decoder.readDictionaryIndex($buffer, $nonNullPosition)
               |    : $updateDecoder.readDictionaryIndex();
          """.stripMargin, "false", dictionaryIndexVar)
        }
        session.foreach(_.addDictionaryCode(ctx, col,
          DictionaryCode(dictionary, buffer, dictionaryIndex)))
        "UTF8String"
      case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS =>
        colAssign = s"$col = $decoder.readLongDecimal($buffer, ${d.precision}, " +
            s"${d.scale}, $nonNullPosition);"
        updatedAssign = s"readLongDecimal(${d.precision}, ${d.scale})"
        "LongDecimal"
      case d: DecimalType =>
        colAssign = s"$col = $decoder.readDecimal($buffer, ${d.precision}, " +
            s"${d.scale}, $nonNullPosition);"
        updatedAssign = s"readDecimal(${d.precision}, ${d.scale})"
        "Decimal"
      case BinaryType => "Binary"
      case CalendarIntervalType => "Interval"
      case _: ArrayType => "Array"
      case _: MapType => "Map"
      case t: StructType =>
        colAssign = s"$col = $decoder.readStruct($buffer, ${t.length}, $nonNullPosition);"
        updatedAssign = s"readStruct(${t.length})"
        "Struct"
      case _ =>
        throw new UnsupportedOperationException(s"unknown type $sqlType")
    }
    if (colAssign.isEmpty) {
      colAssign = s"$col = $decoder.read$typeName($buffer, $nonNullPosition);"
    }
    if (updatedAssign.isEmpty) {
      updatedAssign = s"read$typeName()"
    }
    updatedAssign = s"$col = $updateDecoder.getCurrentDeltaBuffer().$updatedAssign;"

    val unchangedCode = s"$updateDecoder == null || $updateDecoder.unchanged($batchOrdinal)"
    if (attr.nullable) {
      val isNullVar = ctx.freshName("isNull")
      val defaultValue = ctx.defaultValue(jt)
      val code =
        s"""
           |final $jt $col;
           |boolean $isNullVar = false;
           |if ($unchangedCode) {
           |  ${genIfNonNullCode(ctx, decoder, buffer, batchOrdinal, numNullsVar)} {
           |    $colAssign
           |  } else {
           |    $col = $defaultValue;
           |    $isNullVar = true;
           |  }
           |} else if ($updateDecoder.readNotNull()) {
           |  $updatedAssign
           |} else {
           |  $col = $defaultValue;
           |  $isNullVar = true;
           |}
        """.stripMargin
      ExprCode(code, isNullVar, col)
    } else {
      var code =
        s"""
           |final $jt $col;
           |if ($unchangedCode) $colAssign
           |else $updatedAssign
        """.stripMargin
      if (weightVar != null && attr.name.equalsIgnoreCase(Utils.WEIGHTAGE_COLUMN_NAME)) {
        code += s"if ($col == 1) $col = $weightVar;\n"
      }
      ExprCode(code, "false", col)
    }
  }

  private def genIfNonNullCode(ctx: CodegenContext, decoder: String,
      buffer: String, batchOrdinal: String, numNullsVar: String): String = {
    // nextNullPosition is not updated immediately rather when batchOrdinal
    // goes just past because a column read code can be invoked multiple
    // times for the same batchOrdinal like in SNAP-2118;
    // check below crams in all the conditions in a single check to minimize code
    // repetition of non-null assignment call that is normally inlined by JVM
    // and besides this piece of code should get inlined in any case or else
    // it will be big performance hit for every column nullability check
    val nextNullPosition = ctx.freshName("nextNullPosition")
    s"""
       |int $nextNullPosition = $decoder.getNextNullPosition();
       |if ($batchOrdinal < $nextNullPosition ||
       |    // check case when batchOrdinal has gone just past nextNullPosition
       |    ($batchOrdinal == $nextNullPosition + 1 &&
       |     $batchOrdinal < ($nextNullPosition = $decoder.findNextNullPosition(
       |       $buffer, $nextNullPosition, $numNullsVar++))) ||
       |    // check if batchOrdinal has moved ahead by more than one due to filters
       |    ($batchOrdinal != $nextNullPosition && (($numNullsVar =
       |       $decoder.numNulls($buffer, $batchOrdinal, $numNullsVar)) == 0 ||
       |       $batchOrdinal != $decoder.getNextNullPosition())))""".stripMargin
  }
}

object ColumnTableScan extends Logging {

  def generateStatPredicate(ctx: CodegenContext, isColumnTable: Boolean,
      schemaAttrs: Seq[AttributeReference], allFilters: Seq[Expression], numRowsTerm: String,
      metricTerm: (CodegenContext, String) => String, metricAdd: String => String,
      simpleString: String = ""): String = {

    if ((allFilters eq null) || allFilters.isEmpty) {
      return ""
    }
    val numBatchRows = NumBatchRows(numRowsTerm)
    val (columnBatchStatsMap, columnBatchStats) = if (isColumnTable) {
      val allStats = schemaAttrs.map(a => a -> ColumnStatsSchema(a.name, a.dataType))
      (AttributeMap(allStats),
          ColumnStatsSchema.COUNT_ATTRIBUTE +: allStats.flatMap(_._2.schema))
    } else (null, Nil)

    def statsFor(a: Attribute): ColumnStatsSchema = columnBatchStatsMap(a)

    def filterInList(l: Seq[Expression]): Boolean =
      l.length <= 200 && l.forall(TokenLiteral.isConstant)

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

      case EqualTo(a: AttributeReference, l) if TokenLiteral.isConstant(l) =>
        statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound
      case EqualTo(l, a: AttributeReference) if TokenLiteral.isConstant(l) =>
        statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound

      case In(a: AttributeReference, l) if filterInList(l) =>
        statsFor(a).lowerBound <= Greatest(l) && statsFor(a).upperBound >= Least(l)
      case DynamicInSet(a: AttributeReference, l) if filterInList(l) =>
        statsFor(a).lowerBound <= Greatest(l) && statsFor(a).upperBound >= Least(l)

      case LessThan(a: AttributeReference, l) if TokenLiteral.isConstant(l) =>
        statsFor(a).lowerBound < l
      case LessThan(l, a: AttributeReference) if TokenLiteral.isConstant(l) =>
        l < statsFor(a).upperBound

      case LessThanOrEqual(a: AttributeReference, l) if TokenLiteral.isConstant(l) =>
        statsFor(a).lowerBound <= l
      case LessThanOrEqual(l, a: AttributeReference) if TokenLiteral.isConstant(l) =>
        l <= statsFor(a).upperBound

      case GreaterThan(a: AttributeReference, l) if TokenLiteral.isConstant(l) =>
        l < statsFor(a).upperBound
      case GreaterThan(l, a: AttributeReference) if TokenLiteral.isConstant(l) =>
        statsFor(a).lowerBound < l

      case GreaterThanOrEqual(a: AttributeReference, l) if TokenLiteral.isConstant(l) =>
        l <= statsFor(a).upperBound
      case GreaterThanOrEqual(l, a: AttributeReference) if TokenLiteral.isConstant(l) =>
        statsFor(a).lowerBound <= l

      case StartsWith(a: AttributeReference, l) if TokenLiteral.isConstant(l) =>
        val stats = statsFor(a)
        val pattern = if (l.dataType == StringType) l else Cast(l, StringType)
        StartsWithForStats(stats.upperBound, stats.lowerBound, pattern)

      case IsNull(a: Attribute) => statsFor(a).nullCount > 0
      case IsNotNull(a: Attribute) => numBatchRows > statsFor(a).nullCount
    }

    // This code is picked up from InMemoryTableScanExec
    val columnBatchStatFilters: Seq[Expression] = {
      if (isColumnTable) {
        // first group the filters by the expression types (keeping the original operator order)
        // and then order each group on underlying reference names to give a consistent
        // ordering (else two different runs can generate different code)
        val orderedFilters = new ArrayBuffer[(Class[_], ArrayBuffer[Expression])](2)
        allFilters.foreach { f =>
          if (f.dataType.isInstanceOf[DecimalType] ||
              ColumnWriter.SUPPORTED_STATS_TYPES.contains(f.dataType)) {
            orderedFilters.find(_._1 == f.getClass) match {
              case Some(p) => p._2 += f
              case None =>
                val newBuffer = new ArrayBuffer[Expression](2)
                newBuffer += f
                orderedFilters += f.getClass -> newBuffer
            }
          }
        }
        orderedFilters.flatMap(_._2.sortBy(_.references.map(_.name).toSeq
            .sorted.mkString(","))).flatMap { p =>
          val filter = buildFilter.lift(p)
          val boundFilter = filter.map(BindReferences.bindReference(
            _, columnBatchStats, allowFailures = true))

          boundFilter.foreach(_ =>
            filter.foreach(f =>
              logDebug(s"Predicate $p generates partition filter: $f")))

          // If the filter can't be resolved then we are missing required statistics.
          boundFilter.filter(_.resolved)
        }
      } else Nil
    }

    val predicate = ExpressionCanonicalizer.execute(
      BindReferences.bindReference(columnBatchStatFilters
          .reduceOption(And).getOrElse(Literal(true)), columnBatchStats))
    val statsRow = ctx.freshName("statsRow")
    ctx.INPUT_ROW = statsRow
    ctx.currentVars = null
    val predicateEval = predicate.genCode(ctx)

    // skip filtering if nothing is to be applied
    if (predicateEval.value == "true" && predicateEval.isNull == "false") {
      return ""
    }
    val columnBatchesSkipped = if (metricTerm ne null) {
      metricTerm(ctx, "columnBatchesSkipped")
    } else null
    val addSkippedBatchMetric = if (columnBatchesSkipped ne null) {
      s"$columnBatchesSkipped.${metricAdd("1")};"
    } else ""
    val filterFunction = ctx.freshName("columnBatchFilter")
    logDebug(s"ColumnTableScan: using statistics predicate $predicate for $simpleString")
    ctx.addNewFunction(filterFunction,
      s"""
         |private boolean $filterFunction(UnsafeRow $statsRow, int $numRowsTerm) {
         |  // Skip the column batches based on the predicate
         |  ${predicateEval.code}
         |  if (${predicateEval.isNull} || ${predicateEval.value}) {
         |    return true;
         |  } else {
         |    // add to skipped metric only if both stats say so
         |    $addSkippedBatchMetric
         |    return false;
         |  }
         |}
       """.stripMargin)
    filterFunction
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

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }
}

case class NumBatchRows(varName: String) extends LeafExpression {

  override def nullable: Boolean = false

  override def dataType: DataType = IntegerType

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    ExprCode("", "false", varName)
  }

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException(
      "NumBatchRows.eval not expected to be invoked")

  override def sql: String = s"NumBatchRows($varName)"
}

case class StartsWithForStats(upper: Expression, lower: Expression,
    pattern: Expression) extends Expression {

  // pattern must be a string constant for stats row evaluation
  assert(TokenLiteral.isConstant(pattern))
  assert(pattern.dataType == StringType)

  override final def children: Seq[Expression] = Seq(upper, lower, pattern)

  override def foldable: Boolean = false

  override def nullable: Boolean = false

  override def dataType: DataType = BooleanType

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val upperExpr = upper.genCode(ctx)
    val lowerExpr = lower.genCode(ctx)
    val patternExpr = pattern.genCode(ctx)
    val str = ctx.freshName("str")
    val len = str + "Len"
    val lastCharPos = str + "LastPos"
    val upperBytes = str + "Upper"
    val upperStr = str + "UpperUTF8"
    val result = ev.value
    val code =
      s"""
         |${patternExpr.code}
         |boolean $result = true;
         |if (!${patternExpr.isNull}) {
         |  ${lowerExpr.code}
         |  ${upperExpr.code}
         |  // upper bound for column (i.e. LessThan) can be found by going to
         |  // next value of the last character of literal
         |  int $len = ${patternExpr.value}.numBytes();
         |  byte[] $upperBytes = new byte[$len];
         |  ${patternExpr.value}.writeToMemory($upperBytes, Platform.BYTE_ARRAY_OFFSET);
         |  int $lastCharPos = $len - 1;
         |  // check for maximum unsigned value 0xff
         |  while ($lastCharPos >= 0 && $upperBytes[$lastCharPos] == (byte)-1) {
         |    $lastCharPos--;
         |  }
         |  if ($lastCharPos < 0 || (${lowerExpr.isNull})) { // all bytes are 0xff
         |    // a >= startsWithPREFIX
         |    if (!${upperExpr.isNull}) {
         |      $result = ${patternExpr.value}.compareTo(${upperExpr.value}) <= 0;
         |    }
         |  } else {
         |    $upperBytes[$lastCharPos] = (byte)($upperBytes[$lastCharPos] + 1);
         |    UTF8String $upperStr = UTF8String.fromAddress($upperBytes,
         |      Platform.BYTE_ARRAY_OFFSET, $len);
         |    // a >= startsWithPREFIX && a < startsWithPREFIX+1
         |    $result = ((${upperExpr.isNull}) ||
         |        ${patternExpr.value}.compareTo(${upperExpr.value}) <= 0) &&
         |      ${lowerExpr.value}.compareTo($upperStr) < 0;
         |  }
         |}
         |
      """.stripMargin
    ev.copy(code, "false", result)
  }

  override def eval(input: InternalRow): Any =
    throw new UnsupportedOperationException(
      "StartsWithForStats.eval not expected to be invoked")

  override def sql: String = s"StartsWith($upper, $lower, $pattern)"
}
