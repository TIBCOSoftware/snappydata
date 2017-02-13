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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BoundReference, Expression, Literal, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, HashPartitioning, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.encoding.{ColumnEncoder, ColumnEncoding, ColumnStatsSchema}
import org.apache.spark.sql.execution.columnar.impl.BaseColumnFormatRelation
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan, UnaryExecNode, WholeStageCodegenExec}
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DelegateRDD, SnappySession}
import org.apache.spark.unsafe.bitset.BitSetMethods

/**
 * Generated code plan for bulk insertion into a column table.
 */
case class ColumnInsertExec(child: SparkPlan, overwrite: Boolean,
    partitionColumns: Seq[String], partitionExpressions: Seq[Expression],
    relation: Option[JDBCAppendableRelation], columnBatchSize: Int,
    numBuckets: Int, columnTable: String, relationSchema: StructType,
    externalStore: ExternalStore, useMemberVariables: Boolean,
    onExecutor: Boolean, private[sql] var nonSerialized: Boolean)
    extends UnaryExecNode with CodegenSupportOnExecutor {

  def this(child: SparkPlan, overwrite: Boolean,
      partitionColumns: Seq[String], partitionExpressions: Seq[Expression],
      relation: JDBCAppendableRelation, table: String,
      nonSerialized: Boolean = false) = {
    // TODO: add compression for binary/complex types
    this(child, overwrite, partitionColumns, partitionExpressions,
      Some(relation), relation.getColumnBatchParams._1, relation.numBuckets,
      table, relation.schema, relation.externalStore,
      useMemberVariables = false, onExecutor = false, nonSerialized)
  }

  @transient private var encoderCursorTerms: Seq[(String, String)] = _
  @transient private var batchSizeTerm: String = _
  @transient private var defaultBatchSizeTerm: String = _
  @transient private var numInsertions: String = _
  @transient private var schemaTerm: String = _
  @transient private var callStoreColumnBatch: String = _
  @transient private var initEncoders: String = _

  @transient private lazy val (metricAdd, _) = Utils.metricMethods

  private[sql] var batchIdRef = -1

  override lazy val output: Seq[Attribute] =
    AttributeReference("count", LongType, nullable = false)() :: Nil

  private val partitioned = partitionExpressions.nonEmpty

  // Enforce default shuffle partitions to match table buckets.
  // Only one insert plan possible in the plan tree, so no clashes.
  if (partitioned) {
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    session.sessionState.conf.setExecutionShufflePartitions(numBuckets)
  }

  /** Specifies how data is partitioned for the table. */
  override lazy val outputPartitioning: Partitioning = {
    if (partitioned) HashPartitioning(partitionExpressions, numBuckets)
    else super.outputPartitioning
  }

  /** Specifies the partition requirements on the child. */
  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitioned) {
      // For partitionColumns find the matching child columns
      val schema = relationSchema
      val childPartitioningAttributes = partitionColumns.map(partColumn =>
        child.output(schema.indexWhere(_.name.equalsIgnoreCase(partColumn))))
      ClusteredDistribution(childPartitioningAttributes) :: Nil
    } else UnspecifiedDistribution :: Nil
  }

  override lazy val metrics: Map[String, SQLMetric] = {
    if (sqlContext eq null) Map.empty
    else Map("numInsertedRows" -> SQLMetrics.createMetric(sparkContext,
      "number of inserted rows"))
  }

  override protected def doExecute(): RDD[InternalRow] = {
    if (overwrite) {
      relation.get.truncate()
    }
    // don't expect code generation to fail
    WholeStageCodegenExec(this).execute()
  }

  override def executeCollect(): Array[InternalRow] = {
    if (overwrite) {
      relation.get.truncate()
    }
    super.executeCollect()
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    val inputRDDs = child.asInstanceOf[CodegenSupport].inputRDDs()
    // wrap shuffle RDDs to set preferred locations as per target table
    if (partitioned) {
      val relation = this.relation.get.asInstanceOf[BaseColumnFormatRelation]
      inputRDDs.map { rdd =>
        val region = relation.region
        assert(region.getTotalNumberOfBuckets == rdd.getNumPartitions)
        new DelegateRDD(sparkContext, rdd,
          Array.tabulate(region.getTotalNumberOfBuckets)(
            StoreUtils.getBucketPreferredLocations(region, _)))
      }
    }
    else inputRDDs
  }

  override protected def doProduce(ctx: CodegenContext): String = {
    val encodingClass = classOf[ColumnEncoding].getName
    val encoderClass = classOf[ColumnEncoder].getName
    val numInsertedRowsMetric = if (sqlContext eq null) null
    else metricTerm(ctx, "numInsertedRows")
    schemaTerm = ctx.addReferenceObj("schema", relationSchema,
      classOf[StructType].getName)
    encoderCursorTerms = relationSchema.map { _ =>
      (ctx.freshName("encoder"), ctx.freshName("cursor"))
    }
    numInsertions = ctx.freshName("numInsertions")
    ctx.addMutableState("long", numInsertions, s"$numInsertions = -1L;")
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
    val childProduce = child match {
      case c: CodegenSupportOnExecutor if onExecutor =>
        c.produceOnExecutor(ctx, this)
      case c: CodegenSupport => c.produce(ctx, this)
      case _ => throw new UnsupportedOperationException(
        s"Expected a child supporting code generation. Got: $child")
    }
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
    s"""
       |$checkEnd; // already done
       |int $defaultRowSize = 0;
       |$declarations
       |$defaultBatchSizeTerm = Math.max(
       |  ($columnBatchSize - 8) / $defaultRowSize, 16);
       |// ceil to nearest multiple of 16 since size is checked every 16 rows
       |$defaultBatchSizeTerm = ((($defaultBatchSizeTerm - 1) / 16) + 1) * 16;
       |$initEncoders
       |$batchSizeDeclaration
       |$childProduce
       |if ($batchSizeTerm > 0) {
       |  $callStoreColumnBatch;
       |  $batchSizeTerm = 0;
       |}
       |${if (sqlContext eq null) "" else s"$numInsertedRowsMetric.${metricAdd(numInsertions)};"}
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
          s"$schemaTerm.fields()[$i], $defaultBatchSizeTerm);"
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
    val storeColumnBatch = ctx.freshName("storeColumnBatch")
    ctx.addNewFunction(storeColumnBatch,
      s"""
         |private final void $storeColumnBatch(int $batchSizeTerm,
         |    ${batchFunctionDeclarations.toString()}) {
         |  // create statistics row
         |  ${statsCode.mkString("\n")}
         |  ${statsEv.code.trim}
         |  // create ColumnBatch and insert
         |  final java.nio.ByteBuffer[] $buffers =
         |      new java.nio.ByteBuffer[${schema.length}];
         |  ${buffersCode.toString()}
         |  final $columnBatchClass $columnBatch = $columnBatchClass.apply(
         |      $batchSizeTerm, $buffers, $statsRow.getBytes());
         |  $externalStoreTerm.storeColumnBatch("$columnTable",
         |    $columnBatch, $partitionIdCode, $batchUUID);
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
    callStoreColumnBatch =
        s"$storeColumnBatch($batchSizeTerm, ${batchFunctionCall.toString()})"
    s"""
       |if (($batchSizeTerm & 0x0f) == 0 && $batchSizeTerm > 0) {
       |  // check if batch size has exceeded max allowed
       |  long $sizeTerm = 0L;
       |  ${calculateSize.toString()}
       |  if ($sizeTerm >= $columnBatchSize) {
       |    $callStoreColumnBatch;
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
      ev: ExprCode, writeArrayTypeSize: Boolean = true,
      aligned: Boolean = false): String = {
    val sqlType = Utils.getSQLDataType(dataType)
    val jt = ctx.javaType(sqlType)
    var isNull = ev.isNull
    val writeValue = sqlType match {
      // aligned = true cases for recursive writes of StructType that creates
      // UnsafeRow format bytes which are 8 byte aligned
      case BooleanType if aligned =>
        s"$encoder.writeBoolean($cursorTerm, ${ev.value});\n$cursorTerm += 8;"
      case ShortType if aligned =>
        s"$encoder.writeShort($cursorTerm, ${ev.value});\n$cursorTerm += 8;"
      case IntegerType if aligned =>
        s"$encoder.writeInt($cursorTerm, ${ev.value});\n$cursorTerm += 8;"
      case FloatType if aligned =>
        s"$encoder.writeFloat($cursorTerm, ${ev.value});\n$cursorTerm += 8;"
      case _ if ctx.isPrimitiveType(jt) =>
        val typeName = ctx.primitiveTypeName(jt)
        s"$cursorTerm = $encoder.write$typeName($cursorTerm, ${ev.value});"
      case StringType =>
        s"$cursorTerm = $encoder.writeUTF8String($cursorTerm, ${ev.value});"
      case d: DecimalType if d.precision <= Decimal.MAX_LONG_DIGITS =>
        s"$cursorTerm = $encoder.writeLongDecimal($cursorTerm, " +
            s"${ev.value}, ${d.precision}, ${d.scale});"
      case d: DecimalType =>
        s"$cursorTerm = $encoder.writeDecimal($cursorTerm, ${ev.value}, " +
            s"${d.precision}, ${d.scale});"
      case CalendarIntervalType =>
        s"$cursorTerm = $encoder.writeInterval($cursorTerm, ${ev.value});"
      case BinaryType =>
        s"$cursorTerm = $encoder.writeBinary($cursorTerm, ${ev.value});"

      // TODO: see what can be done about endian mismatch problem for complex
      // types retrieved from a different endian platform. Only option looks
      // to be new implementations for UnsafeArrayData/UnsafeMapData/UnsafeRow
      // but then storage of raw unsafe data will also require conversion.
      // Better option is to prove that SPARK PR#10725 causes no degradation
      // and get it accepted upstream, then explicit endian checks can be
      // removed completely from ColumnEncoder/ColumnDecoder classes.
      case a: ArrayType =>
        val numElements = ctx.freshName("numElements")
        val index = ctx.freshName("index")
        val input = ev.value
        val eType = Utils.getSQLDataType(a.elementType)
        val ejType = ctx.javaType(eType)
        val getElement = ctx.getValue(input, eType, index)
        // this is relative offset since re-allocation could mean that initial
        // cursor position is no longer valid (e.g. for off-heap allocator)
        val baseOffset = ctx.freshName("baseOffset")
        val eVar = ctx.freshName("element")
        val totalSize = ctx.freshName("totalSize")
        val writeSize = if (writeArrayTypeSize) {
          s"""
             |// 8 additional bytes, 4 for total size and 4 for numElements
             |$cursorTerm = $encoder.ensureCapacity($cursorTerm,
             |  ($numElements + 2) << 2);
             |$cursorTerm += 4; // skip for total size""".stripMargin
        } else {
          s"""
             |// 4 additional bytes for numElements (no size)
             |$cursorTerm = $encoder.ensureCapacity($cursorTerm,
             |  ($numElements + 1) << 2);""".stripMargin
        }
        val writeHeader =
          s"""
             |final int $numElements = $input.numElements();
             |$writeSize
             |final long $baseOffset = $encoder.offset($cursorTerm);
             |$cursorTerm = $encoder.writeIntUnchecked(
             |  $cursorTerm, $numElements);""".stripMargin
        // for non-nullable primitives, optimize to write the offsets upfront
        // else need to rewind and write offset for each element separately
        val serializeFrag = if (ctx.isPrimitiveType(eType) && !a.containsNull) {
          val offset = ctx.freshName("offset")
          s"""
             |$writeHeader
             |// write offsets upfront for fixed-width non-null elements
             |for (int $index = 0, $offset = 4; $index < $numElements;
             |    $index++, $offset += ${eType.defaultSize}) {
             |  $cursorTerm = $encoder.writeIntUnchecked($cursorTerm, $offset);
             |}
             |for (int $index = 0; $index < $numElements; $index++) {
             |  final $ejType $eVar = $getElement;
             |  ${genCodeColumnWrite(ctx, eType, nullable = false, encoder,
                  cursorTerm, ExprCode("", "false", eVar))}
             |}
          """.stripMargin
        } else {
          val baseCursorOffset = ctx.freshName("baseCursorOffset")
          val (eNull, eCode) = if (a.containsNull) {
            val nullVar = ctx.freshName("isNull")
            (nullVar, s"final boolean $nullVar = $input.isNullAt($index);\n")
          } else ("false", "")
          s"""
             |$writeHeader
             |// space for offsets
             |$cursorTerm += ($numElements << 2);
             |for (int $index = 0; $index < $numElements; $index++) {
             |  ${eCode}final $ejType $eVar = $eNull ? ${ctx.defaultValue(ejType)}
             |    : $getElement;
             |  final long $baseCursorOffset = $encoder.baseOffset() + $baseOffset;
             |  // if null, then write negative relative offset (+4 for numElements)
             |  $encoder.writeInt($baseCursorOffset + (($index + 1) << 2), $eNull
             |     ? $baseCursorOffset - $cursorTerm : $cursorTerm - $baseCursorOffset);
             |  // no "isNull" for recursive call since that is specifically
             |  // written as negative offset by code above
             |  ${genCodeColumnWrite(ctx, eType, nullable = false, encoder,
                  cursorTerm, ExprCode("", "false", eVar))}
             |}
          """.stripMargin
        }
        val serializeArray = if (writeArrayTypeSize) {
          s"""
             |$serializeFrag
             |// finally write the total length of serialized array at the start
             |final long $totalSize = $encoder.offset($cursorTerm) - $baseOffset;
             |if ($totalSize > Integer.MAX_VALUE) {
             |  throw new java.nio.BufferOverflowException("Array size " +
             |    $totalSize + " greater than maximum integer size");
             |}
             |// reduce four from totalSize for the integer totalSize itself
             |$encoder.writeIntUnchecked($cursorTerm - $totalSize - 4, (int)$totalSize);
          """.stripMargin
        } else serializeFrag

        if (nonSerialized) serializeArray
        else {
          s"""
             |if ($input instanceof UnsafeArrayData) {
             |  final UnsafeArrayData data = (UnsafeArrayData)($input);
             |  $cursorTerm = $encoder.writeUnsafeData($cursorTerm,
             |    data.getBaseObject(), data.getBaseOffset(), data.getSizeInBytes());
             |} else { $serializeArray }
          """.stripMargin
        }

      case m: MapType =>
        val keys = ctx.freshName("keys")
        val values = ctx.freshName("values")
        val input = ev.value
        // this is relative offset since re-allocation could mean that initial
        // cursor position is no longer valid (e.g. for off-heap allocator)
        val baseOffset = ctx.freshName("baseOffset")
        val totalSize = ctx.freshName("totalSize")
        val serializeMap =
          s"""
             |final ArrayData $keys = $input.keyArray();
             |final ArrayData $values = $input.valueArray();
             |
             |// at least 8 bytes for writing the total size and key array size
             |$cursorTerm = $encoder.ensureCapacity($cursorTerm, 8);
             |// skip 4 bytes to write the total size
             |$cursorTerm += 4;
             |final long $baseOffset = $encoder.offset($cursorTerm);
             |// write the keys with the array size
             |${genCodeColumnWrite(ctx, ArrayType(m.keyType, containsNull = false),
                nullable = false, encoder, cursorTerm, ExprCode("", "false", keys))}
             |// write the values without the value array size
             |${genCodeColumnWrite(ctx, ArrayType(m.valueType, m.valueContainsNull),
                nullable = false, encoder, cursorTerm, ExprCode("", "false", values),
                writeArrayTypeSize = false)}
             |// finally write the total length of serialized map at the start
             |final long $totalSize = $encoder.offset($cursorTerm) - $baseOffset;
             |if ($totalSize > Integer.MAX_VALUE) {
             |  throw new java.nio.BufferOverflowException("Map size " +
             |    $totalSize + " greater than maximum integer size");
             |}
             |// reduce four from totalSize for the integer totalSize itself
             |$encoder.writeIntUnchecked($cursorTerm - $totalSize - 4, (int)$totalSize);
          """.stripMargin
        if (nonSerialized) serializeMap
        else {
          s"""
             |if ($input instanceof UnsafeMapData) {
             |  final UnsafeMapData map = (UnsafeMapData)($input);
             |  $cursorTerm = $encoder.writeUnsafeData($cursorTerm,
             |    map.getBaseObject(), map.getBaseOffset(), map.getSizeInBytes());
             |} else { $serializeMap }
          """.stripMargin
        }

      case s: StructType =>
        val input = ev.value
        val bitSetMethodsClass = classOf[BitSetMethods].getName
        // this is relative offset since re-allocation could mean that initial
        // cursor position is no longer valid (e.g. for off-heap allocator)
        val baseOffset = ctx.freshName("baseOffset")
        val baseCursorOffset = ctx.freshName("baseCursorOffset")
        val totalSize = ctx.freshName("totalSize")
        val numNullBytes = UnsafeRow.calculateBitSetWidthInBytes(s.length)
        val fixedWidth = numNullBytes + (s.length << 3)
        val serializeElements = s.indices.map { index =>
          val f = s(index)
          val dateType = f.dataType
          val getter = ctx.getValue(input, dataType, Integer.toString(index))
          val value = ctx.freshName("value")
          if (f.nullable) {
            s"""
               |if ($input.isNullAt($index)) {
               |  final long $baseCursorOffset = $encoder.baseOffset() + $baseOffset;
               |  $bitSetMethodsClass.set($encoder.buffer(), $baseCursorOffset, $index);
               |} else {
               |  final ${ctx.javaType(dataType)} $value = $getter;
               |  // nullability already taken care of above
               |  ${genCodeColumnWrite(ctx, dateType, nullable = false, encoder,
                    cursorTerm, ExprCode("", "false", value), aligned = true)}
               |}
            """.stripMargin
          } else {
            s"""
               |final ${ctx.javaType(dataType)} $value = $getter;
               |${genCodeColumnWrite(ctx, dateType, nullable = false, encoder,
                  cursorTerm, ExprCode("", "false", value), aligned = true)}
            """.stripMargin
          }
        }.mkString("")
        val serializeStruct =
          s"""
             |// space for nulls and offsets at the start; add 4 for total size
             |$cursorTerm = $encoder.ensureCapacity($cursorTerm, $fixedWidth);
             |// skip total size for base offset
             |final long $baseOffset = $encoder.offset($cursorTerm + 4);
             |// zero out the null bytes for off-heap bytes
             |if ($encoder.isOffHeap()) {
             |  for (int i = 0; i < $numNullBytes; i += 8) {
             |    $encoder.writeLongUnchecked($baseOffset + i, 0L);
             |  }
             |}
             |$cursorTerm += $fixedWidth;
          """.stripMargin
        s"$encoder.writeStruct($cursorTerm, (UnsafeRow)(${ev.value}))"

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
