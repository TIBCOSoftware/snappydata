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
import com.pivotal.gemfirexd.internal.engine.store.{OffHeapCompactExecRowWithLobs, ResultWasNull,
RowFormatter}
import org.apache.spark.rdd.{RDD, UnionPartition}
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode,
ExpressionCanonicalizer}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoding
import org.apache.spark.sql.execution.columnar.impl.BaseColumnFormatRelation
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.row.{ResultSetEncodingAdapter, ResultSetTraversal,
UnsafeRowEncodingAdapter, UnsafeRowHolder}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark._

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
    @transient baseRelation: PartitionedDataSourceScan,
    allFilters: Seq[Expression],
    schemaAttributes: Seq[AttributeReference],
    isForSampleReservoirAsRegion: Boolean = false)
    extends PartitionedPhysicalScan(output, dataRDD, numBuckets,
      partitionColumns, baseRelation.asInstanceOf[BaseRelation])
    with CodegenSupport {

  override def getMetrics: Map[String, SQLMetric] = super.getMetrics ++ Map(
    "numRowsBufferOutput" -> SQLMetrics.createMetric(sparkContext,
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

    // Returned filter predicate should return false iff it is impossible for the input expression
    // to evaluate to `true' based on statistics collected about this partition batch.
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
    val filterFunction = ctx.freshName("columnBatchFilter")
    ctx.addNewFunction(filterFunction,
      s"""
         |private boolean $filterFunction(byte[] $statBytes) {
         |  final UnsafeRow $statRow = new UnsafeRow($numStatFields);
         |  $statRow.pointTo($statBytes, $platformClass.BYTE_ARRAY_OFFSET,
         |    $statBytes.length);
         |  $columnBatchesSeen.add(1);
         |  // Skip the column batches based on the predicate
         |  ${predicateEval.code}
         |  if (!${predicateEval.isNull} && ${predicateEval.value}) {
         |    return true;
         |  } else {
         |    $columnBatchesSkipped.add(1);
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

    val weightVarName = this.output.find(_.name == org.apache.spark.sql.collection.Utils
      .WEIGHTAGE_COLUMN_NAME).map(_.toString.replace('#', '_')).getOrElse(null)

    val wrappedRow = ctx.freshName("wrappedRow")
    if(weightVarName != null) {
      ctx.addMutableState("long", weightVarName, s" $weightVarName = 0; ")
    }
    val colItrClass = if (!isEmbedded) classOf[CachedBatchIteratorOnRS].getName
    else if (isOffHeap) classOf[OffHeapLobsIteratorOnScan].getName
    else classOf[ByteArraysIteratorOnScan].getName

    if (otherRDDs.isEmpty) {
      if (isForSampleReservoirAsRegion) {
        ctx.addMutableState("scala.collection.Iterator",
          rowInputSRR, s"$rowInputSRR = (scala.collection.Iterator)inputs[0].next();")
        ctx.addMutableState(unsafeHolderClass, unsafeHolder,
          s"$unsafeHolder = new $unsafeHolderClass();")
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
      if(isForSampleReservoirAsRegion) {
        ctx.addMutableState("scala.collection.Iterator", rowInputSRR,
          s"""
            $rowInputSRR = $inputIsOtherRDD ? null : (scala.collection.Iterator)inputs[0].next();
        """
        )
      }
      ctx.addMutableState("scala.collection.Iterator", rowInput,
          s"""
            $rowInput = $inputIsOtherRDD ? inputs[0]
                : (scala.collection.Iterator)inputs[0].next();
        """
      )
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
    ctx.addMutableState("boolean", inputIsRowSRR, s"$inputIsRowSRR = false;")

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
    // shipping as StructType for efficient serialization
    val planSchema = ctx.addReferenceObj("schema", schema,
      classOf[StructType].getName)
    val columnBufferInitCode = new StringBuilder
    val bufferInitCode = new StringBuilder
    val cursorUpdateCode = new StringBuilder
    val moveNextCode = new StringBuilder
    val reservoirRowFetch = s"""
           $stratumRowClass $wrappedRow = ($stratumRowClass)$rowInputSRR.next();
         """ +
         (
           if (weightVarName != null) {
             s""" $weightVarName = $wrappedRow.weight();"""
           } else {
             ""
           }
         ) +
         s"""$unsafeHolder.setRow((UnsafeRow)$wrappedRow.actualRow());"""

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
        s"""$rowInput.next();"""
      }
    }
    else {
      s"""
        if ($inputIsOtherRDD) {
          $unsafeHolder.setRow((UnsafeRow)$rowInput.next());
        }
        """ +
        (
          if (isForSampleReservoirAsRegion) {
            s"""
              else if( $inputIsRowSRR ) {
                $reservoirRowFetch
              }
            """
          }
      ) +
      s"""
         else {
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
    val variableBuffer = scala.collection.mutable.ArrayBuffer[String]()
    val batchConsumer = getBatchConsumer(parent)
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
          $decoder = $decoderClass.getColumnDecoder(
            $buffer, $planSchema.apply($index));
          // intialize the decoder and store the starting cursor position
          $cursor = $decoder.initializeDecoding(
            $buffer, $planSchema.apply($index));
        """)

      variableBuffer += decoder
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
      s"""
        final $cachedBatchClass $batch = ($cachedBatchClass)$colInput.next();
        $buffers = $batch.buffers();
        $numBatchRows = $batch.numRows();
      """
    } else if (isOffHeap) {
      s"""
        $execRowClass $batch;
        while (true) {
          $batch = ($execRowClass)$colInput.next();
          final byte[] statBytes = $batch.getRowBytes(
            ${PartitionedPhysicalScan.CT_STATROW_POSITION});
          if ($filterFunction(statBytes)) {
            break;
          }
          if (!$colInput.hasNext()) return false;
        }
        $numBatchRows = $batch.getAsInt(
          ${PartitionedPhysicalScan.CT_NUMROWS_POSITION}, ($wasNullClass)null);
      """
    } else {
      s"""
        $rowFormatterClass $rowFormatter;
        while (true) {
          $rowFormatter = $colInput.rowFormatter();
          $buffers = (byte[][])$colInput.next();
          final byte[] statBytes = $rowFormatter.getLob($buffers,
            ${PartitionedPhysicalScan.CT_STATROW_POSITION});
          if ($filterFunction(statBytes)) {
            break;
          }
          if (!$colInput.hasNext()) return false;
        }
        $numBatchRows = $rowFormatter.getAsInt(
          ${PartitionedPhysicalScan.CT_NUMROWS_POSITION}, $buffers[0],
          ($wasNullClass)null);
      """
    }
    val commonSnippet =
      s"""
         $input = $colInput;
         $inputIsRow = false;
         $inputIsRowSRR = false;
         if ($input == null || !$input.hasNext()) {
          return false;
         }
       """.stripMargin
    val nextBatch = ctx.freshName("nextBatch")
    ctx.addNewFunction(nextBatch,
      s"""
         private boolean $nextBatch() throws Exception {
           if ($buffers != null) return true;
           // get next batch or row (latter for non-batch source iteration)
           if ($input == null) return false;
           if (!$input.hasNext()) {
             if ($input == $rowInput) {
      """ +
             (if (isForSampleReservoirAsRegion) {
                 s"""
                     $input = $rowInputSRR;
                     $inputIsRow = false;
                     $inputIsRowSRR = true;
                 """.stripMargin +
                    output.zipWithIndex.map{
                     case (attr, index) =>
                       val baseIndex = baseRelation.schema.fieldIndex(attr.name)
                       s"""${variableBuffer(index)} = new $rowAdapterClass($unsafeHolder,
                          $baseIndex);""".stripMargin
                   }.mkString("\n") +
                 s"""
                   if($input == null  || !$input.hasNext()) {
                     $commonSnippet
                   }
                  """.stripMargin
              } else {
                commonSnippet
              }
             ) +
        s"""}""" +
        (if (isForSampleReservoirAsRegion) {
         s"""
            else if($input == $rowInputSRR) {
              $commonSnippet
            }
          """.stripMargin
        } else {
          ""
        }) +
      s"""
         else {
          return false;
         }
       }
       if ($inputIsRow || $inputIsRowSRR) {
         $nextRowSnippet
         $numBatchRows = 1;
       } else {
         $batchInit
         $numOutputRows.add($numBatchRows);
         // initialize the column buffers and decoders
         ${columnBufferInitCode.toString()}
       }

       $batchIndex = 0;
       return true;
     }
     """.stripMargin)

    val batchConsume = batchConsumer.map(_.batchConsume(ctx,
      columnsInput)).mkString("")

    s"""
       |// Combined iterator for column batches from column table
       |// and ResultSet from row buffer. Also takes care of otherRDDs
       |// case when partition is of otherRDDs by iterating over it
       |// using an UnsafeRow adapter.
       |long $numRowsBuffer = 0L;
       |long $numRowsOther = 0L;
       |try {
       |  while ($nextBatch()) {
       |    ${bufferInitCode.toString()}
       |    $batchConsume
       |    final int numRows = $numBatchRows;
       |    final boolean isRow = $inputIsRow || $inputIsRowSRR;
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

  private def getBatchConsumer(
      parent: CodegenSupport): Option[BatchConsumer] = parent match {
    case null => None
    case b: BatchConsumer => Some(b)
    case _ =>
      // using reflection here since protected parent cannot be accessed
      val m = parent.getClass.getDeclaredMethod("parent")
      m.setAccessible(true)
      getBatchConsumer(m.invoke(parent).asInstanceOf[CodegenSupport])
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
    var bufferInit: String = ""
    var dictionaryCode: String = ""
    var dictionary: String = ""
    var dictionaryIndex: String = ""
    val sqlType = Utils.getSQLDataType(attr.dataType)
    val jt = ctx.javaType(sqlType)
    val colAssign = sqlType match {
      case DateType => s"$col = $decoder.readDate($buffer, $cursorVar);"
      case TimestampType =>
        s"$col = $decoder.readTimestamp($buffer, $cursorVar);"
      case _ if ctx.isPrimitiveType(jt) =>
        val typeName = ctx.primitiveTypeName(jt)
        s"$col = $decoder.read$typeName($buffer, $cursorVar);"
      case StringType =>
        dictionary = ctx.freshName("dictionary")
        dictionaryIndex = ctx.freshName("dictionaryIndex")
        bufferInit =
          s"""
            final UTF8String[] $dictionary = $decoder.getStringDictionary();
            int $dictionaryIndex = -1;
          """
        dictionaryCode =
            s"$dictionaryIndex = $decoder.readDictionaryIndex($buffer, $cursorVar);"
        s"""
          $dictionaryCode
          $col = $dictionary != null ? $dictionary[$dictionaryIndex]
            : $decoder.readUTF8String($buffer, $cursorVar);"""
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
      val nullVar = ctx.freshName("nullVal")
      // For ResultSets wasNull() is always a post-facto operation
      // i.e. works only after get has been invoked. However, for column
      // table buffers as well as UnsafeRow adapter, this is not the case
      // and nonNull() should be invoked before get (and get not invoked
      //   at all if nonNull was false). Hence notNull uses tri-state to
      // indicate (true/false/use wasNull) and code below is a tri-switch.
      val code = s"""
          $jt $col;
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
        """ +
        (if ( weightVar != null && attr.name ==  org.apache.spark.sql.collection.Utils
           .WEIGHTAGE_COLUMN_NAME ) {
           s""" if($col == 0 || $col == 1) {
                  $col = $weightVar;
                }
            """.stripMargin
         } else {
           ""
         })

      if (!dictionary.isEmpty) {
        session.addExCode(ctx, col :: Nil, attr :: Nil,
          ExprCodeEx(None, dictionaryCode, dictionary, dictionaryIndex))
      }
      (ExprCode(code, nullVar, col), bufferInit)
    } else {
      if (!dictionary.isEmpty) {
        session.addExCode(ctx, col :: Nil, attr :: Nil,
          ExprCodeEx(None, dictionaryCode, dictionary, dictionaryIndex))
      }
      (ExprCode(s"final $jt $col;\n$colAssign\n", "false", col), bufferInit)
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
