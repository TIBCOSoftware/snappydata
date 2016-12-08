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

import io.snappydata.Property

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.PartitionedPreferredLocationsRDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BoundReference, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.encoding.{ColumnEncoder, ColumnEncoding, ColumnStatsSchema}
import org.apache.spark.sql.execution.columnar.impl.{BaseColumnFormatRelation, ColumnFormatRelation}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan, UnaryExecNode, WholeStageCodegenExec}
import org.apache.spark.sql.types._

/**
 * Generated code plan for bulk insertion into a column table.
 */
case class ColumnInsertExec(child: SparkPlan, overwrite: Boolean,
    partitionColumns: Seq[Expression], relation: BaseColumnFormatRelation)
    extends UnaryExecNode with CodegenSupport {

  @transient private var encoderCursorTerms: Seq[(String, String)] = _
  @transient private var batchSizeTerm: String = _
  @transient private var defaultBatchSizeTerm: String = _
  @transient private var numInsertedRows: String = _
  @transient private var schemaTerm: String = _
  @transient private var callStoreCachedBatch: String = _
  @transient private var initEncoders: String = _

  private val columnBatchSize = Property.ColumnBatchSize.get(sqlContext.conf)

  @transient private lazy val (metricAdd, metricGet) =
    Utils.metricMethods(sparkContext)

  override lazy val output: Seq[Attribute] =
    AttributeReference("count", LongType, nullable = false)() :: Nil

  private val partitioned = partitionColumns.nonEmpty

  /** Specifies how data is partitioned for the table. */
  override lazy val outputPartitioning: Partitioning = {
    if (partitioned) HashPartitioning(partitionColumns, relation.numBuckets)
    else super.outputPartitioning
  }

  override lazy val metrics = Map(
    "numInsertedRows" -> SQLMetrics.createMetric(sparkContext,
      "number of inserted rows"))

  override protected def doExecute(): RDD[InternalRow] = {
    if (overwrite) {
      relation.truncate()
    }
    // don't expect code generation to fail
    WholeStageCodegenExec(this).execute()
  }

  override def executeCollect(): Array[InternalRow] = {
    if (overwrite) {
      relation.truncate()
    }
    super.executeCollect()
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    val inputRDDs = child.asInstanceOf[CodegenSupport].inputRDDs()
    // wrap shuffle RDDs to set preferred locations as per target table
    if (partitioned) {
      inputRDDs.map(new PartitionedPreferredLocationsRDD(_, relation.region))
    }
    else inputRDDs
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    val encodingClass = classOf[ColumnEncoding].getName
    val encoderClass = classOf[ColumnEncoder].getName
    schemaTerm = ctx.addReferenceObj("schema", relation.schema,
      classOf[StructType].getName)
    encoderCursorTerms = relation.schema.map { _ =>
      (ctx.freshName("encoder"), ctx.freshName("cursor"))
    }
    val result = ctx.freshName("result")
    ctx.addMutableState("long", result, s"$result = -1L;")
    batchSizeTerm = ctx.freshName("currentBatchSize")
    defaultBatchSizeTerm = ctx.freshName("defaultBatchSize")
    val fieldType = ctx.freshName("fieldType")
    val defaultRowSize = ctx.freshName("defaultRowSize")
    val childProduce = child.asInstanceOf[CodegenSupport].produce(ctx, this)
    val declarations = encoderCursorTerms.indices.map { i =>
      val (encoder, cursor) = encoderCursorTerms(i)
      s"""
         |$fieldType = $schemaTerm.fields()[$i];
         |final $encoderClass $encoder = $encodingClass$$.MODULE$$.getColumnEncoder(
         |  $fieldType);
         |$defaultRowSize += $encoder.defaultSize($fieldType.dataType());
         |long $cursor = 0L;
      """.stripMargin
    }.mkString("\n")
    s"""
       |if ($result >= 0) return; // already done
       |${classOf[StructField].getName} $fieldType;
       |int $defaultRowSize = 0;
       |$declarations
       |int $defaultBatchSizeTerm = Math.max(
       |  ($columnBatchSize - 8) / $defaultRowSize, 16);
       |// ceil to nearest multiple of 16 since size is checked every 16 rows
       |$defaultBatchSizeTerm = ((($defaultBatchSizeTerm - 1) / 16) + 1) * 16;
       |$initEncoders
       |int $batchSizeTerm = 0;
       |$childProduce
       |if ($batchSizeTerm > 0) {
       |  $callStoreCachedBatch;
       |  $batchSizeTerm = 0;
       |}
       |$result = ${metricGet(numInsertedRows)};
       |${consume(ctx, Seq(ExprCode("", "false", result)))}
    """.stripMargin
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode],
      row: ExprCode): String = {
    val schema = relation.schema
    numInsertedRows = metricTerm(ctx, "numInsertedRows")
    val externalStore = ctx.addReferenceObj("externalStore",
      relation.externalStore)

    val partitionId = ctx.freshName("partitionId")
    val buffers = ctx.freshName("buffers")
    val cachedBatch = ctx.freshName("cachedBatch")
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
          s"$schemaTerm.fields()[$i], $defaultBatchSizeTerm);"
      buffersCode.append(
        s"$buffers[$i] = (byte[])$encoderTerm.finish($cursorTerm);\n")
      batchFunctionDeclarations.append(
        s"$encoderClass $encoderTerm, long $cursorTerm,\n")
      batchFunctionCall.append(s"$encoderTerm, $cursorTerm,\n")
      calculateSize.append(
        s"$sizeTerm += $encoderTerm.sizeInBytes($cursorTerm);\n")
      (init, genCodeColumnWrite(ctx, field, encoderTerm, cursorTerm, input(i)),
          genCodeColumnStats(ctx, field, encoderTerm))
    }.unzip3
    initEncoders = encodersInit.mkString("\n")

    batchFunctionDeclarations.setLength(
      batchFunctionDeclarations.length - 2)
    batchFunctionCall.setLength(batchFunctionCall.length - 2)

    val cachedBatchClass = classOf[CachedBatch].getName
    val partitionIdCode =
      if (partitioned) s"$taskContextClass.getPartitionId()" else "-1"
    val (statsCode, statsSchema, statsVars) = columnStats.unzip3
    val statsExprs = statsSchema.flatten.zipWithIndex.map { case (a, i) =>
      BoundReference(i, a.dataType, a.nullable)
    }
    ctx.INPUT_ROW = null
    ctx.currentVars = statsVars.flatten
    val statsEv = GenerateUnsafeProjection.createCode(ctx, statsExprs)
    val statsRow = statsEv.value
    val columnTable = ColumnFormatRelation.cachedBatchTableName(relation.table)
    val storeCachedBatch = ctx.freshName("storeCachedBatch")
    ctx.addNewFunction(storeCachedBatch,
      s"""
         |private final void $storeCachedBatch(int $batchSizeTerm,
         |    ${batchFunctionDeclarations.toString()}) {
         |  // create statistics row
         |  ${statsCode.mkString("\n")}
         |  ${statsEv.code.trim}
         |  // create CachedBatch and insert
         |  final byte[][] $buffers = new byte[${schema.length}][];
         |  ${buffersCode.toString()}
         |  final $cachedBatchClass $cachedBatch =
         |    $cachedBatchClass.apply($batchSizeTerm, $buffers, $statsRow);
         |  final int $partitionId = $partitionIdCode;
         |  $externalStore.storeCachedBatch("$columnTable",
         |    $cachedBatch, $partitionId, scala.None$$.MODULE$$);
         |  $numInsertedRows.${metricAdd(batchSizeTerm)};
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
    callStoreCachedBatch =
        s"$storeCachedBatch($batchSizeTerm, ${batchFunctionCall.toString()})"
    s"""
       |if (($batchSizeTerm & 0x0f) == 0 && $batchSizeTerm > 0) {
       |  // check if batch size has exceeded max allowed
       |  long $sizeTerm = 0L;
       |  ${calculateSize.toString()}
       |  if ($sizeTerm >= $columnBatchSize) {
       |    $callStoreCachedBatch;
       |    $batchSizeTerm = 0;
       |    $initEncoders
       |  }
       |}
       |${evaluateVariables(input)}
       |${columnsWrite.mkString("\n")}
       |$batchSizeTerm++;
    """.stripMargin
  }

  private def genCodeColumnWrite(ctx: CodegenContext, field: StructField,
      encoder: String, cursorTerm: String, ev: ExprCode): String = {
    val sqlType = Utils.getSQLDataType(field.dataType)
    val jt = ctx.javaType(sqlType)
    val writeValue = sqlType match {
      case _ if ctx.isPrimitiveType(jt) =>
        val typeName = ctx.primitiveTypeName(jt)
        s"$encoder.write$typeName($cursorTerm, ${ev.value})"
      case StringType =>
        s"$encoder.writeUTF8String($cursorTerm, ${ev.value})"
      case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS =>
        s"$encoder.writeLongDecimal($cursorTerm, " +
            s"${ev.value}, ${d.precision}, ${d.scale})"
      case d: DecimalType =>
        s"$encoder.writeDecimal($cursorTerm, ${ev.value}, " +
            s"${d.precision}, ${d.scale})"
      case CalendarIntervalType =>
        s"$encoder.writeInterval($cursorTerm, ${ev.value})"
      case BinaryType =>
        s"$encoder.writeBinary($cursorTerm, ${ev.value})"
      case _: ArrayType =>
        s"$encoder.writeArray($cursorTerm, (UnsafeArrayData)(${ev.value}))"
      case _: MapType =>
        s"$encoder.writeMap($cursorTerm, (UnsafeMapData)(${ev.value}))"
      case _: StructType =>
        s"$encoder.writeStruct($cursorTerm, (UnsafeRow)(${ev.value}))"
      case NullType => ev.isNull = "true"; ""
      case _ =>
        throw new UnsupportedOperationException(s"unknown type $sqlType")
    }
    if (field.nullable) {
      s"""
         |if (${ev.isNull}) {
         |  $encoder.writeIsNull($batchSizeTerm);
         |} else {
         |  $cursorTerm = $writeValue;
         |}""".stripMargin
    } else s"$cursorTerm = $writeValue;"
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
        canBeNull = true
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
      ExprCode("", "false", nullCount),
      ExprCode("", "false", batchSizeTerm)))
  }
}
