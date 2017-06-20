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
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, ExpressionCanonicalizer}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.encoding.{ColumnDecoder, ColumnEncoding, ColumnStatsSchema}
import org.apache.spark.sql.execution.columnar.impl.BaseColumnFormatRelation
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.row.{ResultSetDecoder, ResultSetTraversal, UnsafeRowDecoder, UnsafeRowHolder}
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
    relationSchema: StructType,
    allFilters: Seq[Expression],
    schemaAttributes: Seq[AttributeReference],
    isForSampleReservoirAsRegion: Boolean = false)
    extends PartitionedPhysicalScan(output, dataRDD, numBuckets,
      partitionColumns, partitionColumnAliases,
      baseRelation.asInstanceOf[BaseRelation]) with CodegenSupport {

  override val nodeName: String = "ColumnTableScan"

  @transient private val MAX_CURSOR_DECLARATIONS = 30

  override def getMetrics: Map[String, SQLMetric] = {
    if (sqlContext eq null) Map.empty
    else super.getMetrics ++ Map(
      "numRowsBuffer" -> SQLMetrics.createMetric(sparkContext,
        "number of output rows from row buffer"),
      "columnBatchesSeen" -> SQLMetrics.createMetric(sparkContext,
        "column batches seen"),
      "columnBatchesSkipped" -> SQLMetrics.createMetric(sparkContext,
        "column batches skipped by the predicate")) ++ (
        if (otherRDDs.isEmpty) Map.empty
        else Map("numRowsOtherRDDs" -> SQLMetrics.createMetric(sparkContext,
          "number of output rows from other RDDs")))
  }

  override def metricTerm(ctx: CodegenContext, name: String): String =
    if (sqlContext eq null) null else super.metricTerm(ctx, name)

  private def generateStatPredicate(ctx: CodegenContext,
      numRowsTerm: String): String = {

    val numBatchRows = NumBatchRows(numRowsTerm)
    val (columnBatchStatsMap, columnBatchStats) = relation match {
      case _: BaseColumnFormatRelation =>
        val allStats = schemaAttributes.map(a => a ->
            ColumnStatsSchema(a.name, a.dataType))
        (AttributeMap(allStats),
            ColumnStatsSchema.COUNT_ATTRIBUTE +: allStats.flatMap(_._2.schema))
      case _ => (null, Nil)
    }

    def statsFor(a: Attribute) = columnBatchStatsMap(a)

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

      case EqualTo(a: AttributeReference, l: DynamicReplacableConstant) =>
        statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound
      case EqualTo(l: DynamicReplacableConstant, a: AttributeReference) =>
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
      case IsNotNull(a: Attribute) => numBatchRows > statsFor(a).nullCount
    }

    // This code is picked up from InMemoryTableScanExec
    val columnBatchStatFilters: Seq[Expression] = {
      if (relation.isInstanceOf[BaseColumnFormatRelation]) {
        allFilters.flatMap { p =>
          val filter = buildFilter.lift(p)
          val boundFilter = filter.map(BindReferences.bindReference(
            _, columnBatchStats, allowFailures = true))

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

    val predicate = ExpressionCanonicalizer.execute(
      BindReferences.bindReference(columnBatchStatFilters
          .reduceOption(And).getOrElse(Literal(true)), columnBatchStats))
    val statsRow = ctx.freshName("statsRow")
    ctx.INPUT_ROW = statsRow
    ctx.currentVars = null
    val predicateEval = predicate.genCode(ctx)

    val columnBatchesSkipped = metricTerm(ctx, "columnBatchesSkipped")
    // skip filtering if nothing is to be applied
    if (predicateEval.value == "true" && predicateEval.isNull == "false") {
      return ""
    }
    val filterFunction = ctx.freshName("columnBatchFilter")
    ctx.addNewFunction(filterFunction,
      s"""
         |private boolean $filterFunction(UnsafeRow $statsRow) {
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

  def splitMoveNextMethods(ctx: CodegenContext, blocks: ArrayBuffer[String],
      arg: String): String = {
    val functions = blocks.zipWithIndex.map { case (body, i) =>
      val name = ctx.freshName("moveNext")
      val code =
        s"""
           |private void $name(int $arg) {
           |  $body
           |}
         """.stripMargin
      ctx.addNewFunction(name, code)
      name
    }
    functions.map(name => s"$name($arg);").mkString("\n")
  }


  def convertExprToMethodCall(ctx: CodegenContext, expr: ExprCode,
                              attr: Attribute, index: Int, batchOrdinal : String,
                              notNullVar : String): ExprCode = {
    val apply = ctx.freshName("apply")
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
    val isEmbedded = (baseRelation eq null) || (baseRelation.connectionType match {
      case ConnectionType.Embedded => true
      case _ => false
    })
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
    val (weightVarName, weightAssignCode) = if (output.exists(_.name ==
        Utils.WEIGHTAGE_COLUMN_NAME)) {
      val varName = ctx.freshName("weightage")
      ctx.addMutableState("long", varName, s"$varName = 0;")
      (varName, s"$varName = $wrappedRow.weight();")
    } else ("", "")

    val iteratorClass = "scala.collection.Iterator"
    val colIteratorClass = if (isEmbedded) classOf[ColumnBatchIterator].getName
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
        s"$rs = (($rsIterClass)$rowInput).rs();")
    } else {
      ctx.addMutableState("boolean", inputIsOtherRDD,
        s"$inputIsOtherRDD = (partitionIndex >= $otherRDDsPartitionIndex);")
      ctx.addMutableState(iteratorClass, rowInput,
        s"$rowInput = $inputIsOtherRDD ? inputs[0] " +
            s": ($iteratorClass)inputs[0].next();")
      ctx.addMutableState(colIteratorClass, colInput,
        s"$colInput = $inputIsOtherRDD ? null : ($colIteratorClass)inputs[0].next();")
      ctx.addMutableState("java.sql.ResultSet", rs,
        s"$rs = $inputIsOtherRDD ? null : (($rsIterClass)$rowInput).rs();")
      ctx.addMutableState(unsafeHolderClass, unsafeHolder,
        s"$unsafeHolder = new $unsafeHolderClass();")
    }
    ctx.addMutableState(iteratorClass, input,
      if (isForSampleReservoirAsRegion) s"$input = $rowInputSRR;"
      else s"$input = $rowInput;")
    ctx.addMutableState("boolean", inputIsRow, s"$inputIsRow = true;")

    ctx.currentVars = null
    val encodingClass = classOf[ColumnEncoding].getName
    val decoderClass = classOf[ColumnDecoder].getName
    val rsDecoderClass = classOf[ResultSetDecoder].getName
    val rowDecoderClass = classOf[UnsafeRowDecoder].getName
    val batch = ctx.freshName("batch")
    val numBatchRows = s"${batch}NumRows"
    val batchIndex = s"${batch}Index"
    val buffers = s"${batch}Buffers"
    val numRows = ctx.freshName("numRows")
    val batchOrdinal = ctx.freshName("batchOrdinal")

    ctx.addMutableState("java.nio.ByteBuffer", buffers, s"$buffers = null;")
    ctx.addMutableState("int", numBatchRows, s"$numBatchRows = 0;")
    ctx.addMutableState("int", batchIndex, s"$batchIndex = 0;")

    // need DataType and nullable to get decoder in generated code
    // shipping as StructType for efficient serialization
    val planSchema = ctx.addReferenceObj("schema", schema,
      classOf[StructType].getName)
    val columnBufferInitCode = new StringBuilder
    val bufferInitCode = new StringBuilder
    val moveNextMultCode = new StringBuilder
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
    val columnBufferInitCodeBlocks = new ArrayBuffer[String]()
    val bufferInitCodeBlocks = new ArrayBuffer[String]()
    val moveNextCodeBlocks = new ArrayBuffer[String]()

    val isWideSchema = output.length > MAX_CURSOR_DECLARATIONS
    val batchConsumers = getBatchConsumers(parent)
    val columnsInput = output.zipWithIndex.map { case (attr, index) =>
      val decoder = ctx.freshName("decoder")
      val cursor = s"${decoder}Cursor"
      val buffer = ctx.freshName("buffer")
      val cursorVar = s"cursor$index"
      val decoderVar = s"decoder$index"
      val bufferVar = s"buffer$index"
      if(isWideSchema){
        ctx.addMutableState("Object", bufferVar, s"$bufferVar = null;")
      }
      // projections are not pushed in embedded mode for optimized access
      val baseIndex = relationSchema.fieldIndex(attr.name)
      val bufferPosition = if (isEmbedded) baseIndex + 1 else index + 1
      val rsPosition = bufferPosition

      ctx.addMutableState("java.nio.ByteBuffer", buffer, s"$buffer = null;")

      val rowDecoderCode = s"$decoder = new $rsDecoderClass($rs, $rsPosition);"
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
      ctx.addMutableState("long", cursor, s"$cursor = 0L;")


      if (isWideSchema) {
        if (columnBufferInitCode.length > 1024) {
          columnBufferInitCodeBlocks.append(columnBufferInitCode.toString())
          columnBufferInitCode.clear()
        }
      }

      columnBufferInitCode.append(
        s"""
          $buffer = $colInput.getColumnLob($bufferPosition);
          $decoder = $encodingClass$$.MODULE$$.getColumnDecoder($buffer,
            $planSchema.apply($index));
          // initialize the decoder and store the starting cursor position
          $cursor = $decoder.initialize($buffer, $planSchema.apply($index));
        """)


      if (isWideSchema) {
        if (bufferInitCode.length > 1024) {
          bufferInitCodeBlocks.append(bufferInitCode.toString())
          bufferInitCode.clear()
        }

        bufferInitCode.append(
          s"""
          final $decoderClass $decoderVar = $decoder;
          $bufferVar = ($buffer == null || $buffer.isDirect()) ? null
              : $buffer.array();
          long $cursorVar = $cursor;
        """)
      } else {
        bufferInitCode.append(
          s"""
          final $decoderClass $decoderVar = $decoder;
          final Object $bufferVar = ($buffer == null || $buffer.isDirect()) ? null
              : $buffer.array();
          long $cursorVar = $cursor;
        """)
      }

      if (!isWideSchema) {
        cursorUpdateCode.append(s"$cursor = $cursorVar;\n")
      }

      val notNullVar = if (attr.nullable) ctx.freshName("notNull") else null

      if (!isWideSchema) {
        moveNextCode.append(genCodeColumnNext(ctx, decoderVar, bufferVar,
          cursorVar, batchOrdinal, attr.dataType, notNullVar, false)).append('\n')
        val (ev, bufferInit) = genCodeColumnBuffer(ctx, decoderVar, bufferVar,
          cursorVar, attr, notNullVar, weightVarName, false)
        bufferInitCode.append(bufferInit)
        ev
      } else {
        if (isWideSchema) {
          if (moveNextMultCode.length > 1024) {
            moveNextCodeBlocks.append(moveNextMultCode.toString())
            moveNextMultCode.clear()
          }
        }
        val producedCode = genCodeColumnNext(ctx, decoder, bufferVar,
          cursor, batchOrdinal, attr.dataType, notNullVar, true)
        moveNextMultCode.append(producedCode)

        val (ev, bufferInit) = genCodeColumnBuffer(ctx, decoder, bufferVar,
          cursor, attr, notNullVar, weightVarName, true)
        val changedExpr = convertExprToMethodCall(ctx,
          ev, attr, index, batchOrdinal, notNullVar)
        bufferInitCode.append(bufferInit)
        changedExpr
      }
    }

    if (isWideSchema) {
      columnBufferInitCodeBlocks.append(columnBufferInitCode.toString())
      bufferInitCodeBlocks.append(bufferInitCode.toString())
      moveNextCodeBlocks.append(moveNextMultCode.toString())
    }

    val columnBufferInitCodeStr = if (isWideSchema) {
      splitToMethods(ctx, columnBufferInitCodeBlocks)
    } else {
      columnBufferInitCode.toString()
    }

    val bufferInitCodeStr = if (isWideSchema) {
      splitToMethods(ctx, bufferInitCodeBlocks)
    } else {
      bufferInitCode.toString()
    }

    val moveNextCodeStr = if (isWideSchema) {
      splitMoveNextMethods(ctx, moveNextCodeBlocks, batchOrdinal)
    } else {
      moveNextCode.toString()
    }

    // TODO: add filter function for non-embedded mode (using store layer
    //   function that will invoke the above function in independent class)
    val filterFunction = generateStatPredicate(ctx, numBatchRows)
    val unsafeRow = ctx.freshName("unsafeRow")
    val colNextBytes = ctx.freshName("colNextBytes")
    val numColumnsInStatBlob =
      relationSchema.size * ColumnStatsSchema.NUM_STATS_PER_COLUMN + 1

    val incrementBatchOutputRows = if (numOutputRows ne null) {
      s"$numOutputRows.${metricAdd(numBatchRows)};"
    } else ""
    val incrementBufferOutputRows = if (numOutputRows ne null) {
      s"$numOutputRows.${metricAdd(metricValue(numRowsBuffer))};"
    } else ""
    val incrementOtherRows = if (otherRDDs.isEmpty) ""
    else s"$numOutputRows.${metricAdd(metricValue(numRowsOther))};"
    val columnBatchesSeen = metricTerm(ctx, "columnBatchesSeen")
    val incrementBatchCount = if (columnBatchesSeen eq null) ""
    else s"$columnBatchesSeen.${metricAdd("1")};"
    val countIndexInSchema = ColumnStatsSchema.COUNT_INDEX_IN_SCHEMA
    val batchAssign =
      s"""
        final java.nio.ByteBuffer $colNextBytes = (java.nio.ByteBuffer)$colInput.next();
        UnsafeRow $unsafeRow = ${Utils.getClass.getName}.MODULE$$.toUnsafeRow(
          $colNextBytes, $numColumnsInStatBlob);
        $numBatchRows = $unsafeRow.getInt($countIndexInSchema);
        $incrementBatchCount
        $buffers = $colNextBytes;
      """
    val batchInit = if (filterFunction.isEmpty) batchAssign else {
      s"""
        while (true) {
          $batchAssign
          if ($filterFunction($unsafeRow)) {
            break;
          }
          if (!$colInput.hasNext()) return false;
        }"""
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
         |    $incrementBatchOutputRows
         |    // initialize the column buffers and decoders
         |    ${columnBufferInitCodeStr.toString()}
         |  }
         |  $batchIndex = 0;
         |  return true;
         |}
      """.stripMargin)

    val batchConsume = batchConsumers.map(_.batchConsume(ctx, this,
      columnsInput)).mkString("\n")
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
       |    ${bufferInitCodeStr.toString()}
       |    $batchConsume
       |    final int $numRows = $numBatchRows;
       |    for (int $batchOrdinal = $batchIndex; $batchOrdinal < $numRows;
       |         $batchOrdinal++) {
       |      ${moveNextCodeStr}
       |      $consumeCode
       |      if (shouldStop()) {
       |        // increment index for return
       |        $batchIndex = $batchOrdinal + 1;
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

  private def genCodeColumnNext(ctx: CodegenContext, decoder: String,
      buffer: String, cursorVar: String, batchOrdinal: String,
      dataType: DataType, notNullVar: String , isWideSchema: Boolean): String = {
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
      case BinaryType =>
        s"$cursorVar = $decoder.nextBinary($buffer, $cursorVar);"
      case _: ArrayType =>
        s"$cursorVar = $decoder.nextArray($buffer, $cursorVar);"
      case _: MapType =>
        s"$cursorVar = $decoder.nextMap($buffer, $cursorVar);"
      case _: StructType =>
        s"$cursorVar = $decoder.nextStruct($buffer, $cursorVar);"
      case NullType => ""
      case _ =>
        throw new UnsupportedOperationException(s"unknown type $sqlType")
    }
    if (notNullVar != null) {
      if (isWideSchema) {
        ctx.addMutableState("int", notNullVar, "")
        val nullCode =
          s"$notNullVar = $decoder.notNull($buffer, $batchOrdinal);"
        if (moveNext.isEmpty) nullCode
        else s"$nullCode\nif ($notNullVar == 1) $moveNext\n"
      } else {
        val nullCode =
          s"final int $notNullVar = $decoder.notNull($buffer, $batchOrdinal);"
        if (moveNext.isEmpty) nullCode
        else s"$nullCode\nif ($notNullVar == 1) $moveNext"
      }
    } else moveNext
  }

  private def genCodeColumnBuffer(ctx: CodegenContext, decoder: String,
      buffer: String, cursorVar: String, attr: Attribute,
      notNullVar: String, weightVar: String, wideTable : Boolean): (ExprCode, String) = {
    val col = ctx.freshName("col")
    var bufferInit = ""
    var dictionaryAssignCode = ""
    var stringAssignCode = ""
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
        dictionaryLen = ctx.freshName("dictionaryLength")
        dictIndex = ctx.freshName("dictionaryIndex")
        ctx.addMutableState("UTF8String[]", dictionary, "")
        ctx.addMutableState("int", dictionaryLen, "")
        if (wideTable) {
          ctx.addMutableState("int", dictIndex, "")
        }
        // initialize index to dictionaryLength - 1 where null value will
        // reside in case there are nulls in the current batch
        if (wideTable) {
          jtDecl = s"UTF8String $col = null; $dictIndex = $dictionaryLen - 1;"
        } else {
          jtDecl = s"UTF8String $col = null; int $dictIndex = $dictionaryLen - 1;"
        }

        bufferInit =
            s"""
               |$dictionary = $decoder.getStringDictionary();
               |$dictionaryLen = $dictionary != null ? $dictionary.length : -1;
            """.stripMargin
        dictionaryAssignCode =
            s"$dictIndex = $decoder.readDictionaryIndex($buffer, $cursorVar);"
        val nullCheckAddon =
          if (notNullVar != null) s"if ($notNullVar < 0) $nullVar = $col == null;\n"
          else ""
        stringAssignCode =
            s"($dictionary != null ? $dictionary[$dictIndex] " +
                s": $decoder.readUTF8String($buffer, $cursorVar));\n"
        assignCode = stringAssignCode + nullCheckAddon

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
                $col = $stringAssignCode;
                $nullVar = $decoder.wasNull();
              }
            }
          """
        session.foreach(_.addDictionaryCode(ctx, col, DictionaryCode(
          dictionaryCode, assignCode, dictionary, dictIndex, dictionaryLen)))
      }
      (ExprCode(code, nullVar, col), bufferInit)
    } else {
      if (!dictionary.isEmpty) {
        val dictionaryCode = jtDecl + '\n' + dictionaryAssignCode
        session.foreach(_.addDictionaryCode(ctx, col, DictionaryCode(
          dictionaryCode, assignCode, dictionary, dictIndex, dictionaryLen)))
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
