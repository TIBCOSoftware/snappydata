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

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BoundReference, Expression, Literal}
import org.apache.spark.sql.catalyst.util.{SerializedArray, SerializedMap, SerializedRow}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.encoding.{ColumnEncoder, ColumnEncoding, ColumnStatsSchema}
import org.apache.spark.sql.execution.{SparkPlan, TableInsertExec}
import org.apache.spark.sql.sources.DestroyRelation
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.bitset.BitSetMethods

/**
 * Generated code plan for bulk insertion into a column table.
 */
case class ColumnInsertExec(_child: SparkPlan, partitionColumns: Seq[String],
    _partitionExpressions: Seq[Expression], _numBuckets: Int,
    relation: Option[DestroyRelation], batchParams: (Int, Int, String),
    columnTable: String, onExecutor: Boolean, relationSchema: StructType,
    externalStore: ExternalStore, useMemberVariables: Boolean)
    extends TableInsertExec(_child, partitionColumns, _partitionExpressions,
      _numBuckets, relationSchema, relation, onExecutor) {

  def this(child: SparkPlan, partitionColumns: Seq[String],
      partitionExpressions: Seq[Expression],
      relation: JDBCAppendableRelation, table: String) = {
    // TODO: add compression for binary/complex types
    this(child, partitionColumns, partitionExpressions, relation.numBuckets,
      Some(relation), relation.getColumnBatchParams, table, onExecutor = false,
      relation.schema, relation.externalStore, useMemberVariables = false)
  }

  @transient private var encoderCursorTerms: Seq[(String, String)] = _
  @transient private var maxDeltaRowsTerm: String = _
  @transient private var batchSizeTerm: String = _
  @transient private var defaultBatchSizeTerm: String = _
  @transient private var numInsertions: String = _
  @transient private var schemaTerm: String = _
  @transient private var storeColumnBatch: String = _
  @transient private var storeColumnBatchArgs: String = _
  @transient private var initEncoders: String = _

  @transient private[sql] var batchIdRef = -1

  def columnBatchSize: Int = batchParams._1

  def columnMaxDeltaRows: Int = batchParams._2

  /** Frequency of rows to check for total size exceeding batch size. */
  private val (checkFrequency, checkMask) = {
    val batchSize = columnBatchSize
    if (batchSize >= 16 * 1024 * 1024) ("16", "0x0f")
    else if (batchSize >= 8 * 1024 * 1024)  ("8", "0x07")
    else if (batchSize >= 4 * 1024 * 1024)  ("4", "0x03")
    else if (batchSize >= 2 * 1024 * 1024)  ("2", "0x01")
    else ("1", "0x0")
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    val encodingClass = classOf[ColumnEncoding].getName
    val encoderClass = classOf[ColumnEncoder].getName
    val numInsertedRowsMetric = if (onExecutor) null
    else metricTerm(ctx, "numInsertedRows")
    schemaTerm = ctx.addReferenceObj("schema", relationSchema,
      classOf[StructType].getName)
    encoderCursorTerms = relationSchema.map { _ =>
      (ctx.freshName("encoder"), ctx.freshName("cursor"))
    }
    numInsertions = ctx.freshName("numInsertions")
    ctx.addMutableState("long", numInsertions, s"$numInsertions = -1L;")
    maxDeltaRowsTerm = ctx.freshName("maxDeltaRows")
    batchSizeTerm = ctx.freshName("currentBatchSize")
    val batchSizeDeclaration = if (useMemberVariables) {
      ctx.addMutableState("int", batchSizeTerm, s"$batchSizeTerm = 0;")
      ""
    } else {
      s"int $batchSizeTerm = 0;"
    }
    defaultBatchSizeTerm = ctx.freshName("defaultBatchSize")
    ctx.addMutableState("int", defaultBatchSizeTerm, "")
    val defaultRowSize = ctx.freshName("defaultRowSize")
    val childProduce = doChildProduce(ctx)
    val declarations = encoderCursorTerms.indices.map { i =>
      val (encoder, cursor) = encoderCursorTerms(i)
      ctx.addMutableState(encoderClass, encoder,
        s"""
           |this.$encoder = $encodingClass$$.MODULE$$.getColumnEncoder(
           |  $schemaTerm.fields()[$i]);
        """.stripMargin)
      val cursorDeclaration = if (useMemberVariables) {
        ctx.addMutableState("long", cursor, s"$cursor = 0L;")
        ""
      } else s"long $cursor = 0L;"
      s"""
         |final $encoderClass $encoder = this.$encoder;
         |$defaultRowSize += $encoder.defaultSize($schemaTerm.fields()[$i].dataType());
         |$cursorDeclaration
      """.stripMargin
    }.mkString("\n")
    val checkEnd = if (useMemberVariables) {
      "if (!currentRows.isEmpty()) return"
    } else {
      s"if ($numInsertions >= 0) return"
    }
    // no need to stop in iteration at any point
    ctx.addNewFunction("shouldStop",
      s"""
         |@Override
         |protected final boolean shouldStop() {
         |  return false;
         |}
      """.stripMargin)
    s"""
       |$checkEnd; // already done
       |$numInsertions = 0;
       |int $defaultRowSize = 0;
       |$declarations
       |$defaultBatchSizeTerm = Math.max(
       |  (${math.abs(columnBatchSize)} - 8) / $defaultRowSize, 16);
       |// ceil to nearest multiple of $checkFrequency since size is checked
       |// every $checkFrequency rows
       |$defaultBatchSizeTerm = ((($defaultBatchSizeTerm - 1) / $checkFrequency) + 1)
       |    * $checkFrequency;
       |$initEncoders
       |$batchSizeDeclaration
       |$childProduce
       |if ($batchSizeTerm > 0) {
       |  $storeColumnBatch($columnMaxDeltaRows, $storeColumnBatchArgs);
       |  $batchSizeTerm = 0;
       |}
       |${if (numInsertedRowsMetric eq null) ""
          else s"$numInsertedRowsMetric.${metricAdd(numInsertions)};"}
       |${consume(ctx, Seq(ExprCode("", "false", numInsertions)))}
    """.stripMargin
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode],
      row: ExprCode): String = {
    val schema = relationSchema
    val externalStoreTerm = ctx.addReferenceObj("externalStore", externalStore)

    val buffers = ctx.freshName("buffers")
    val columnBatch = ctx.freshName("columnBatch")
    val sizeTerm = ctx.freshName("size")

    val encoderClass = classOf[ColumnEncoder].getName
    val taskContextClass = classOf[TaskContext].getName
    val buffersCode = new StringBuilder
    val batchFunctionDeclarations = new StringBuilder
    val batchFunctionCall = new StringBuilder
    val calculateSize = new StringBuilder
    val (encodersInit, columnsWrite, columnStats) = schema.indices.map { i =>
      val (encoderTerm, cursorTerm) = encoderCursorTerms(i)
      val field = schema(i)
      val init = s"$cursorTerm = $encoderTerm.initialize(" +
          s"$schemaTerm.fields()[$i], $defaultBatchSizeTerm, true);"
      buffersCode.append(
        s"$buffers[$i] = $encoderTerm.finish($cursorTerm);\n")
      batchFunctionDeclarations.append(
        s"$encoderClass $encoderTerm, long $cursorTerm,\n")
      batchFunctionCall.append(s"$encoderTerm, $cursorTerm,\n")
      calculateSize.append(
        s"$sizeTerm += $encoderTerm.sizeInBytes($cursorTerm);\n")
      (init, genCodeColumnWrite(ctx, field.dataType, field.nullable, encoderTerm,
        cursorTerm, input(i)), genCodeColumnStats(ctx, field, encoderTerm))
    }.unzip3
    initEncoders = encodersInit.mkString("\n")

    batchFunctionDeclarations.setLength(
      batchFunctionDeclarations.length - 2)
    batchFunctionCall.setLength(batchFunctionCall.length - 2)

    val columnBatchClass = classOf[ColumnBatch].getName
    batchIdRef = ctx.references.length
    val batchUUID = ctx.addReferenceObj("batchUUID", None,
      classOf[Option[_]].getName)
    val partitionIdCode = if (partitioned) s"$taskContextClass.getPartitionId()"
    else {
      // check for bucketId variable if available
      ctx.mutableStates.collectFirst {
        case (_, varName, _) if varName ==
            ExternalStoreUtils.COLUMN_BATCH_BUCKETID_VARNAME => varName
      }.getOrElse(
        // add as a reference object which can be updated by caller if required
        s"${ctx.addReferenceObj("partitionId", -1, "Integer")}.intValue()")
    }
    val tableName = ctx.addReferenceObj("columnTable", columnTable,
      "java.lang.String")
    val (statsCode, statsSchema, stats) = columnStats.unzip3
    val statsVars = stats.flatten
    val statsExprs = statsSchema.flatten.zipWithIndex.map { case (a, i) =>
      a.dataType match {
        // some types will always be null so avoid unnecessary generated code
        case _ if statsVars(i).isNull == "true" => Literal(null, NullType)
        case _ => BoundReference(i, a.dataType, a.nullable)
      }
    }
    ctx.INPUT_ROW = null
    ctx.currentVars = statsVars
    val statsEv = GenerateUnsafeProjection.createCode(ctx, statsExprs)
    val statsRow = statsEv.value
    storeColumnBatch = ctx.freshName("storeColumnBatch")
    ctx.addNewFunction(storeColumnBatch,
      s"""
         |private final void $storeColumnBatch(int $maxDeltaRowsTerm,
         |    int $batchSizeTerm, ${batchFunctionDeclarations.toString()}) {
         |  // create statistics row
         |  ${statsCode.mkString("\n")}
         |  ${statsEv.code.trim}
         |  // create ColumnBatch and insert
         |  final java.nio.ByteBuffer[] $buffers =
         |      new java.nio.ByteBuffer[${schema.length}];
         |  ${buffersCode.toString()}
         |  final $columnBatchClass $columnBatch = $columnBatchClass.apply(
         |      $batchSizeTerm, $buffers, $statsRow.getBytes());
         |  $externalStoreTerm.storeColumnBatch($tableName, $columnBatch,
         |      $partitionIdCode, $batchUUID, $maxDeltaRowsTerm);
         |  $numInsertions += $batchSizeTerm;
         |}
      """.stripMargin)
    // no shouldStop check required
    if (!ctx.addedFunctions.contains("shouldStop")) {
      ctx.addNewFunction("shouldStop",
        s"""
          @Override
          protected final boolean shouldStop() {
            return false;
          }
        """)
    }
    storeColumnBatchArgs = s"$batchSizeTerm, ${batchFunctionCall.toString()}"
    s"""
       |if ($columnBatchSize > 0 && ($batchSizeTerm & $checkMask) == 0 &&
       |    $batchSizeTerm > 0) {
       |  // check if batch size has exceeded max allowed
       |  long $sizeTerm = 0L;
       |  ${calculateSize.toString()}
       |  if ($sizeTerm >= $columnBatchSize) {
       |    $storeColumnBatch(-1, $storeColumnBatchArgs);
       |    $batchSizeTerm = 0;
       |    $initEncoders
       |  }
       |}
       |${evaluateVariables(input)}
       |${columnsWrite.mkString("\n")}
       |$batchSizeTerm++;
    """.stripMargin
  }

  private def genCodeColumnWrite(ctx: CodegenContext, dataType: DataType,
      nullable: Boolean, encoder: String, cursorTerm: String,
      ev: ExprCode): String = {
    ColumnWriter.genCodeColumnWrite(ctx, dataType, nullable, encoder,
      cursorTerm, ev, batchSizeTerm)
  }

  private def genCodeColumnStats(ctx: CodegenContext, field: StructField,
      encoder: String): (String, Seq[Attribute], Seq[ExprCode]) = {
    val lower = ctx.freshName("lower")
    val upper = ctx.freshName("upper")
    var lowerIsNull = "false"
    var upperIsNull = "false"
    var canBeNull = false
    val nullCount = ctx.freshName("nullCount")
    val sqlType = Utils.getSQLDataType(field.dataType)
    val jt = ctx.javaType(sqlType)
    val boundsCode = sqlType match {
      case BooleanType =>
        s"""
           |final boolean $lower = $encoder.lowerLong() > 0;
           |final boolean $upper = $encoder.upperLong() > 0;""".stripMargin
      case ByteType | ShortType | IntegerType | LongType |
           DateType | TimestampType =>
        s"""
           |final $jt $lower = ($jt)$encoder.lowerLong();
           |final $jt $upper = ($jt)$encoder.upperLong();""".stripMargin
      case StringType =>
        canBeNull = true
        s"""
           |final UTF8String $lower = $encoder.lowerString();
           |final UTF8String $upper = $encoder.upperString();""".stripMargin
      case FloatType | DoubleType =>
        s"""
           |final $jt $lower = ($jt)$encoder.lowerDouble();
           |final $jt $upper = ($jt)$encoder.upperDouble();""".stripMargin
      case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS =>
        s"""
           |final Decimal $lower = Decimal.createUnsafe($encoder.lowerLong(),
           |  ${d.precision}, ${d.scale});
           |final Decimal $upper = Decimal.createUnsafe($encoder.upperLong(),
           |  ${d.precision}, ${d.scale});""".stripMargin
      case _: DecimalType =>
        canBeNull = true
        s"""
           |final Decimal $lower = $encoder.lowerDecimal();
           |final Decimal $upper = $encoder.upperDecimal();""".stripMargin
      case _ =>
        lowerIsNull = "true"
        upperIsNull = "true"
        canBeNull = false
        s"""
           |final $jt $lower = null;
           |final $jt $upper = null;""".stripMargin
    }
    val nullsCode = if (canBeNull) {
      lowerIsNull = ctx.freshName("lowerIsNull")
      upperIsNull = ctx.freshName("upperIsNull")
      s"""
         |final boolean $lowerIsNull = $lower == null;
         |final boolean $upperIsNull = $upper == null;""".stripMargin
    } else ""
    val code =
      s"""
         |$boundsCode
         |$nullsCode
         |final int $nullCount = $encoder.nullCount();""".stripMargin

    (code, ColumnStatsSchema(field.name, field.dataType).schema, Seq(
      ExprCode("", lowerIsNull, lower),
      ExprCode("", upperIsNull, upper),
      ExprCode("", "false", nullCount)))
  }
}

object ColumnWriter {

  def genCodeColumnWrite(ctx: CodegenContext, dataType: DataType,
      nullable: Boolean, encoder: String, cursorTerm: String, ev: ExprCode,
      batchSizeTerm: String, offsetTerm: String = null,
      baseOffsetTerm: String = null): String = {
    val sqlType = Utils.getSQLDataType(dataType)
    val jt = ctx.javaType(sqlType)
    var isNull = ev.isNull
    val input = ev.value
    val writeValue = sqlType match {
      case _ if ctx.isPrimitiveType(jt) =>
        val typeName = ctx.primitiveTypeName(jt)
        if (offsetTerm eq null) {
          s"$cursorTerm = $encoder.write$typeName($cursorTerm, $input);"
        } else {
          // offsetTerm is non-null for recursive writes of StructType
          s"$encoder.write${typeName}Unchecked($offsetTerm, $input);"
        }
      case StringType =>
        if (offsetTerm eq null) {
          s"$cursorTerm = $encoder.writeUTF8String($cursorTerm, $input);"
        } else {
          s"$cursorTerm = $encoder.writeStructUTF8String($cursorTerm," +
              s" $input, $offsetTerm, $baseOffsetTerm);"
        }
      case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS =>
        if (offsetTerm eq null) {
          s"$cursorTerm = $encoder.writeLongDecimal($cursorTerm, " +
              s"$input, $batchSizeTerm, ${d.precision}, ${d.scale});"
        } else {
          // assume caller has already ensured matching precision+scale
          s"$encoder.writeLongUnchecked($offsetTerm, $input.toUnscaledLong());"
        }
      case d: DecimalType =>
        if (offsetTerm eq null) {
          s"$cursorTerm = $encoder.writeDecimal($cursorTerm, $input, " +
              s"$batchSizeTerm, ${d.precision}, ${d.scale});"
        } else {
          // assume caller has already ensured matching precision+scale
          s"$cursorTerm = $encoder.writeStructDecimal($cursorTerm, " +
              s"$input, $offsetTerm, $baseOffsetTerm);"
        }
      case CalendarIntervalType =>
        if (offsetTerm eq null) {
          s"$cursorTerm = $encoder.writeInterval($cursorTerm, $input);"
        } else {
          s"$cursorTerm = $encoder.writeStructInterval($cursorTerm, " +
              s"$input, $offsetTerm, $baseOffsetTerm);"
        }
      case BinaryType =>
        if (offsetTerm eq null) {
          s"$cursorTerm = $encoder.writeBinary($cursorTerm, $input);"
        } else {
          s"$cursorTerm = $encoder.writeStructBinary($cursorTerm, " +
              s"$input, $offsetTerm, $baseOffsetTerm);"
        }

      // TODO: see if it can be proved that SPARK PR#10725 causes no degradation
      // and get it accepted upstream, then explicit endian checks can be
      // removed completely from ColumnEncoder/ColumnDecoder classes
      // TODO: MapObjects creates new variables every time so leads to new
      // plans being compiled for every Dataset being inserted into the
      // same table. Change it to generate variables using CodegenContext.
      case a: ArrayType =>
        genCodeArrayWrite(ctx, a, encoder, cursorTerm, input,
          batchSizeTerm, offsetTerm, baseOffsetTerm)
      case s: StructType =>
        genCodeStructWrite(ctx, s, encoder, cursorTerm, input,
          batchSizeTerm, offsetTerm, baseOffsetTerm)
      case m: MapType =>
        genCodeMapWrite(ctx, m, encoder, cursorTerm, input,
          batchSizeTerm, offsetTerm, baseOffsetTerm)

      case NullType => isNull = "true"; ""
      case _ =>
        throw new UnsupportedOperationException(s"unknown type $sqlType")
    }
    if (nullable) {
      s"""
         |if ($isNull) {
         |  $encoder.writeIsNull($batchSizeTerm);
         |} else {
         |  $writeValue
         |}""".stripMargin
    } else writeValue
  }

  def genCodeMapWrite(ctx: CodegenContext, m: MapType, encoder: String,
      cursorTerm: String, input: String, batchSizeTerm: String,
      offsetTerm: String = null, baseOffsetTerm: String = null): String = {
    val keys = ctx.freshName("keys")
    val values = ctx.freshName("values")
    val serializedMapClass = classOf[SerializedMap].getName
    val writeOffset = if (offsetTerm eq null) ""
    else s"$encoder.setOffsetAndSize($cursorTerm, $offsetTerm, $baseOffsetTerm, 0);"
    s"""
       |if ($input instanceof $serializedMapClass) {
       |  final $serializedMapClass map = ($serializedMapClass)($input);
       |  $cursorTerm = $encoder.writeUnsafeData($cursorTerm,
       |    map.getBaseObject(), map.getBaseOffset(), map.getSizeInBytes());
       |} else {
       |  final ArrayData $keys = $input.keyArray();
       |  final ArrayData $values = $input.valueArray();
       |
       |  // at least 16 bytes for the size+numElements for keys and values
       |  $cursorTerm = $encoder.ensureCapacity($cursorTerm, 16);
       |  $writeOffset
       |  // write the keys with its size and numElements
       |  ${genCodeArrayWrite(ctx, ArrayType(m.keyType,
            containsNull = false), encoder, cursorTerm, keys, batchSizeTerm,
            offsetTerm = null, baseOffsetTerm = null)}
       |  // write the values with its size and numElements
       |  ${genCodeArrayWrite(ctx, ArrayType(m.valueType,
            m.valueContainsNull), encoder, cursorTerm, values, batchSizeTerm,
            offsetTerm = null, baseOffsetTerm = null)}
       |}
    """.stripMargin
  }

  def genCodeArrayWrite(ctx: CodegenContext, a: ArrayType, encoder: String,
      cursorTerm: String, input: String, batchSizeTerm: String,
      offsetTerm: String = null, baseOffsetTerm: String = null): String = {
    val serializedArrayClass = classOf[SerializedArray].getName
    // this is relative offset since re-allocation could mean that initial
    // cursor position is no longer valid (e.g. for off-heap allocator)
    val baseOffset = ctx.freshName("baseOffset")
    val baseDataOffset = ctx.freshName("baseDataOffset")
    val totalSize = ctx.freshName("totalSize")
    val longSize = ctx.freshName("longSize")
    val data = ctx.freshName("data")
    // skip either both size and numElements or only numElements
    val skipBytes = if (offsetTerm eq null) 8 else 4
    val numElements = ctx.freshName("numElements")
    val index = ctx.freshName("index")

    // check if total size needs to be written at the start or
    // offset+size needs to be written at provided position (for nested types)
    val writeSizeCode = if (offsetTerm eq null) {
      s"$encoder.writeIntUnchecked($cursorTerm - $totalSize, $totalSize);"
    } else {
      s"$encoder.setOffsetAndSize($cursorTerm - $totalSize, $offsetTerm, " +
          s"$baseOffsetTerm, $totalSize);"
    }
    s"""
       |final int $totalSize;
       |final $serializedArrayClass $data;
       |if (($input instanceof $serializedArrayClass) &&
       |    ($data = ($serializedArrayClass)$input).getSkipBytes() == $skipBytes) {
       |  $totalSize = $data.getSizeInBytes();
       |  $cursorTerm = $encoder.writeUnsafeData($cursorTerm,
       |      $data.getBaseObject(), $data.getBaseOffset(), $totalSize);
       |} else {
       |  final int $numElements = $input.numElements();
       |  $cursorTerm = $encoder.initializeComplexType($cursorTerm,
       |      $numElements, $skipBytes, true);
       |  final long $baseOffset = $encoder.getBaseTypeOffset();
       |  final long $baseDataOffset = $encoder.getBaseDataOffset();
       |  for (int $index = 0; $index < $numElements; $index++) {
       |    ${serializeElement(ctx, a.elementType, a.containsNull, index,
              input, encoder, cursorTerm, batchSizeTerm, baseOffset,
              baseDataOffset, skipBytes)}
       |  }
       |  // finally write the total size of data at the start
       |  final long $longSize = $encoder.offset($cursorTerm) - $baseOffset;
       |  if ($longSize > Integer.MAX_VALUE) {
       |    throw new java.nio.BufferOverflowException();
       |  }
       |  $totalSize = (int)$longSize;
       |}
       |$writeSizeCode
    """.stripMargin
  }

  def genCodeStructWrite(ctx: CodegenContext, s: StructType, encoder: String,
      cursorTerm: String, input: String, batchSizeTerm: String,
      offsetTerm: String = null, baseOffsetTerm: String = null): String = {
    val serializedRowClass = classOf[SerializedRow].getName
    // this is relative offset since re-allocation could mean that initial
    // cursor position is no longer valid (e.g. for off-heap allocator)
    val baseOffset = ctx.freshName("baseOffset")
    val baseDataOffset = ctx.freshName("baseDataOffset")
    val totalSize = ctx.freshName("totalSize")
    val longSize = ctx.freshName("longSize")
    val data = ctx.freshName("data")
    // skip 4 bytes for size if required
    val skipBytes = if (offsetTerm eq null) 4 else 0

    val serializeElements = s.indices.map { index =>
      val f = s(index)
      serializeElement(ctx, f.dataType, f.nullable, Integer.toString(index),
        input, encoder, cursorTerm, batchSizeTerm, baseOffset,
        baseDataOffset, skipBytes)
    }.mkString("")
    // check if total size needs to be written at the start or
    // offset+size needs to be written at provided position (for nested types)
    val writeSizeCode = if (offsetTerm eq null) {
      s"$encoder.writeIntUnchecked($cursorTerm - $totalSize, $totalSize);"
    } else {
      s"$encoder.setOffsetAndSize($cursorTerm - $totalSize, $offsetTerm, " +
          s"$baseOffsetTerm, $totalSize);"
    }
    s"""
       |final int $totalSize;
       |final $serializedRowClass $data;
       |if (($input instanceof $serializedRowClass) &&
       |    ($data = ($serializedRowClass)$input).getSkipBytes() == $skipBytes) {
       |  $totalSize = $data.getSizeInBytes();
       |  $cursorTerm = $encoder.writeUnsafeData($cursorTerm,
       |      $data.getBaseObject(), $data.getBaseOffset(), $totalSize);
       |} else {
       |  $cursorTerm = $encoder.initializeComplexType($cursorTerm,
       |      ${s.length}, $skipBytes, false);
       |  final long $baseOffset = $encoder.getBaseTypeOffset();
       |  final long $baseDataOffset = $encoder.getBaseDataOffset();
       |  $serializeElements
       |  // finally write the total size of data at the start
       |  final long $longSize = $encoder.offset($cursorTerm) - $baseOffset;
       |  if ($longSize > Integer.MAX_VALUE) {
       |    throw new java.nio.BufferOverflowException();
       |  }
       |  $totalSize = (int)$longSize;
       |}
       |$writeSizeCode
    """.stripMargin
  }

  // scalastyle:off
  private def serializeElement(ctx: CodegenContext, dt: DataType,
      nullable: Boolean, index: String, input: String, encoder: String,
      cursorTerm: String, batchSizeTerm: String, baseOffset: String,
      baseDataOffset: String, skipBytes: Int): String = {
    // scalastyle:on

    val getter = ctx.getValue(input, dt, index)
    val bitSetMethodsClass = classOf[BitSetMethods].getName
    val fieldCursor = ctx.freshName("fieldCursor")
    val value = ctx.freshName("value")
    var canBeNull = nullable
    val serializeValue =
      s"""
         |final long $fieldCursor = $encoder.baseOffset() + $baseDataOffset +
         |    ($index << 3);
         |${genCodeColumnWrite(ctx, dt, nullable = false, encoder,
            cursorTerm, ExprCode("", "false", value), batchSizeTerm,
            fieldCursor, baseOffset)}
      """.stripMargin
    val (checkNull, assignValue) = dt match {
      case d: DecimalType => val checkNull =
        s"""
           |$input.isNullAt($index) ||
           |  !($value = $getter).changePrecision(${d.precision}, ${d.scale})
          """.stripMargin
        canBeNull = true
        (checkNull, "")
      case _ => (s"$input.isNullAt($index)", s"\n$value = $getter;")
    }
    if (canBeNull) {
      s"""
         |final ${ctx.javaType(dt)} $value;
         |if ($checkNull) {
         |  $bitSetMethodsClass.set($encoder.buffer(),
         |      $encoder.baseOffset() + $baseOffset, $index + ${skipBytes << 3});
         |} else {$assignValue$serializeValue}
        """.stripMargin
    } else {
      s"final ${ctx.javaType(dt)} $value = $getter;$serializeValue"
    }
  }
}
