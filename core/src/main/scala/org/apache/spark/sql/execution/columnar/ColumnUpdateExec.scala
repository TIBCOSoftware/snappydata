/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, ExpressionCanonicalizer}
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.encoding.{ColumnDeltaEncoder, ColumnEncoder, ColumnStatsSchema}
import org.apache.spark.sql.execution.columnar.impl.{BaseColumnFormatRelation, ColumnDelta}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.row.RowExec
import org.apache.spark.sql.sources.JdbcExtendedUtils.quotedName
import org.apache.spark.sql.sources.{ConnectionProperties, DestroyRelation, JdbcExtendedUtils}
import org.apache.spark.sql.store.{CompressionCodecId, StoreUtils}
import org.apache.spark.sql.types.StructType

/**
 * Generated code plan for updates into a column table.
 * This extends [[RowExec]] to generate the combined code for row buffer updates.
 */
case class ColumnUpdateExec(child: SparkPlan, columnTable: String,
    partitionColumns: Seq[String], partitionExpressions: Seq[Expression], numBuckets: Int,
    isPartitioned: Boolean, tableSchema: StructType, externalStore: ExternalStore,
    columnRelation: BaseColumnFormatRelation, updateColumns: Seq[Attribute],
    updateExpressions: Seq[Expression], keyColumns: Seq[Attribute],
    connProps: ConnectionProperties, onExecutor: Boolean) extends ColumnExec {

  assert(updateColumns.length == updateExpressions.length)

  override def relation: Option[DestroyRelation] = Some(columnRelation)

  val compressionCodec: CompressionCodecId.Type = CompressionCodecId.fromName(
    columnRelation.getCompressionCodec)

  private val schemaAttributes = tableSchema.toAttributes

  /**
   * The indexes below are the final ones that go into ColumnFormatKey(columnIndex).
   * For deltas the convention is to use negative values beyond those available for
   * each hierarchy depth. So starting at DELTA_STATROW index of -3, the first column
   * will use indexes -4, -5, -6 for hierarchy depth 3, second column will use
   * indexes -7, -8, -9 and so on. The values below are initialized to the first value
   * in the series while merges with higher hierarchy depth will be done via a
   * CacheListener on the store.
   */
  private val updateIndexes = updateColumns.map(a => ColumnDelta.deltaColumnIndex(
    Utils.fieldIndex(schemaAttributes, a.name,
      sqlContext.conf.caseSensitiveAnalysis), hierarchyDepth = 0)).toArray

  @transient private var _tableToUpdateIndex: IntObjectHashMap[Integer] = _

  /**
   * Map from table column (0 based) to index in updateColumns.
   */
  private def tableToUpdateIndex: IntObjectHashMap[Integer] = {
    if (_tableToUpdateIndex ne null) return _tableToUpdateIndex
    val m = new IntObjectHashMap[Integer](updateIndexes.length)
    for (i <- updateIndexes.indices) {
      m.put(ColumnDelta.tableColumnIndex(updateIndexes(i)) - 1, i)
    }
    _tableToUpdateIndex = m
    _tableToUpdateIndex
  }

  override protected def opType: String = "Update"

  override def nodeName: String = "ColumnUpdate"

  override lazy val metrics: Map[String, SQLMetric] = {
    if (onExecutor) Map.empty
    else Map(
      "numUpdateRows" -> SQLMetrics.createMetric(sparkContext,
        "number of updates to row buffer"),
      "numUpdateColumnBatchRows" -> SQLMetrics.createMetric(sparkContext,
        "number of updates to column batches"))
  }

  override def simpleString: String = s"${super.simpleString} update: columns=$updateColumns " +
      s"expressions=$updateExpressions compression=$compressionCodec"

  @transient private var batchOrdinal: String = _
  @transient private var finishUpdate: String = _
  @transient private var updateMetric: String = _
  @transient protected var txId: String = _

  override protected def delayRollover: Boolean = true

  override protected def doProduce(ctx: CodegenContext): String = {

    val sql = new StringBuilder
    sql.append("UPDATE ").append(quotedName(resolvedName, escapeQuotes = true)).append(" SET ")
    JdbcExtendedUtils.fillColumnsClause(sql, updateColumns.map(_.name),
      escapeQuotes = true, separator = ", ")
    sql.append(" WHERE ")
    // only the ordinalId is required apart from partitioning columns
    if (keyColumns.length > 4) {
      JdbcExtendedUtils.fillColumnsClause(sql, keyColumns.dropRight(4).map(_.name),
        escapeQuotes = true)
      sql.append(" AND ")
    }
    sql.append(StoreUtils.ROWID_COLUMN_NAME).append("=?")

    super.doProduce(ctx, sql.toString(), () =>
      s"""
         |if ($batchOrdinal > 0) {
         |  $finishUpdate($invalidUUID, -1, -1); // force a finish
         |}
         |$taskListener.setSuccess();
      """.stripMargin)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode],
      row: ExprCode): String = {
    // use an array of delta encoders and cursors
    val deltaEncoders = ctx.freshName("deltaEncoders")
    val cursors = ctx.freshName("cursors")
    val index = ctx.freshName("index")
    batchOrdinal = ctx.freshName("batchOrdinal")
    val lastColumnBatchId = ctx.freshName("lastColumnBatchId")
    val lastBucketId = ctx.freshName("lastBucketId")
    val lastNumRows = ctx.freshName("lastNumRows")
    finishUpdate = ctx.freshName("finishUpdate")
    val initializeEncoders = ctx.freshName("initializeEncoders")

    val updateSchema = StructType.fromAttributes(updateColumns)
    val schemaTerm = ctx.addReferenceObj("updateSchema", updateSchema,
      classOf[StructType].getName)
    val deltaIndexes = ctx.addReferenceObj("deltaIndexes", updateIndexes, "int[]")
    val externalStoreTerm = ctx.addReferenceObj("externalStore", externalStore)
    val tableName = ctx.addReferenceObj("columnTable", columnTable, "java.lang.String")
    updateMetric = if (onExecutor) null else metricTerm(ctx, "numUpdateColumnBatchRows")

    val numColumns = updateColumns.length
    val deltaEncoderClass = classOf[ColumnDeltaEncoder].getName
    val encoderClass = classOf[ColumnEncoder].getName
    val columnBatchClass = classOf[ColumnBatch].getName

    ctx.addMutableState(s"$deltaEncoderClass[]", deltaEncoders, "")
    ctx.addMutableState("long[]", cursors,
      s"""
         |$deltaEncoders = new $deltaEncoderClass[$numColumns];
         |$cursors = new long[$numColumns];
         |$initializeEncoders();
      """.stripMargin)
    ctx.addMutableState("int", batchOrdinal, "")
    ctx.addMutableState("long", lastColumnBatchId, s"$lastColumnBatchId = $invalidUUID;")
    ctx.addMutableState("int", lastBucketId, "")
    ctx.addMutableState("int", lastNumRows, "")

    // last three columns in keyColumns should be internal ones
    val keyCols = keyColumns.takeRight(4)
    assert(keyCols.head.name.equalsIgnoreCase(ColumnDelta.mutableKeyNames.head))
    assert(keyCols(1).name.equalsIgnoreCase(ColumnDelta.mutableKeyNames(1)))
    assert(keyCols(2).name.equalsIgnoreCase(ColumnDelta.mutableKeyNames(2)))
    assert(keyCols(3).name.equalsIgnoreCase(ColumnDelta.mutableKeyNames(3)))

    // bind the update expressions
    ctx.INPUT_ROW = null
    ctx.currentVars = input
    val allExpressions = updateExpressions ++ keyColumns
    val boundUpdateExpr = allExpressions.map(
      u => ExpressionCanonicalizer.execute(BindReferences.bindReference(u, child.output)))
    val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(boundUpdateExpr)
    val effectiveCodes = subExprs.codes.mkString("\n")
    val updateInput = ctx.withSubExprEliminationExprs(subExprs.states) {
      boundUpdateExpr.map(_.genCode(ctx))
    }
    ctx.currentVars = null

    val keyVars = updateInput.takeRight(4)
    val ordinalIdVar = keyVars.head.value
    val batchIdVar = keyVars(1).value
    val bucketVar = keyVars(2).value
    val numRowsVar = keyVars(3).value

    val updateVarsCode = evaluateVariables(updateInput)
    // row buffer needs to select the rowId and partitioning columns so drop last three
    val rowConsume = super.doConsume(ctx, updateInput.dropRight(3),
      StructType(getUpdateSchema(allExpressions.dropRight(3))))

    ctx.addNewFunction(initializeEncoders,
      s"""
         |private void $initializeEncoders() {
         |  for (int $index = 0; $index < $numColumns; $index++) {
         |    $deltaEncoders[$index] = new $deltaEncoderClass(0);
         |    $cursors[$index] = $deltaEncoders[$index].initialize($schemaTerm.fields()[$index],
         |        ${classOf[ColumnDelta].getName}.INIT_SIZE(), true);
         |  }
         |}
      """.stripMargin)
    // Creating separate encoder write functions instead of inlining for wide-schemas
    // in updates (especially with support for putInto being added). Performance should
    // be about the same since JVM inlines where it determines will help performance.
    val callEncoders = updateColumns.zipWithIndex.map { case (col, i) =>
      val function = ctx.freshName("encoderFunction")
      val ordinal = ctx.freshName("ordinal")
      val isNull = ctx.freshName("isNull")
      val field = ctx.freshName("field")
      val dataType = col.dataType
      val encoderTerm = ctx.freshName("deltaEncoder")
      val realEncoderTerm = s"${encoderTerm}_realEncoder"
      val cursorTerm = s"$cursors[$i]"
      val ev = updateInput(i)
      ctx.addNewFunction(function,
        s"""
           |private void $function(int $ordinal, int $ordinalIdVar,
           |    boolean $isNull, ${ctx.javaType(dataType)} $field) {
           |  final $deltaEncoderClass $encoderTerm = $deltaEncoders[$i];
           |  final $encoderClass $realEncoderTerm = $encoderTerm.getRealEncoder();
           |  $encoderTerm.setUpdatePosition($ordinalIdVar);
           |  ${ColumnWriter.genCodeColumnWrite(ctx, dataType, col.nullable, realEncoderTerm,
                encoderTerm, cursorTerm, ev.copy(isNull = isNull, value = field), ordinal)}
           |}
        """.stripMargin)
      // code for invoking the function
      s"$function($batchOrdinal, (int)$ordinalIdVar, ${ev.isNull}, ${ev.value});"
    }.mkString("\n")
    // Old code(Keeping the comment for better understanding)
    // Write the delta stats row for all table columns at the end of a batch.
    // Columns that have not been updated will write nulls for all three stats
    // columns so this costs 3 bits per non-updated column (worst case of say
    // 100 column table will be ~38 bytes).

    // New Code : Stats rows are written as UnsafeRow. Unsafe row irrespective
    // of nullability keeps nullbits.
    // So if 100 columns are  there stats row will contain 100 * 3 and UnsafeRow will contain
    // 300 nullbits plus bits required for 8 bytes word allignment. Hence setting unused columns
    // as null does not really saves much.

    // These nullbits are set based on platform endianness. SD ColumnFormatValue
    // assumes LITTLE_ENDIAN bytes. However, Unsafe row itself does not force any endianness
    // and picks up from the platform. Hence a lower byte of the bit set  can be represented as
    // [11111110]. The 0th bit is for batch count which can never be null.
    // This causes SD ColumnFormatValue to understand stats row as a compressed byte
    // array( as first int is 1, hence -1 which after taking a negative
    // equals to 1 i.e LZ4 compression codec id ).
    // Hence setting each 3rd bit( null count stats) with not null flag. This will never cause
    // the word to be read as negative number.
    val allNullsExprs = Seq(ExprCode("", "true", ""),
      ExprCode("", "true", ""), ExprCode("", "false", "-1"))
    val (statsSchema, stats) = tableSchema.indices.map { i =>
      val field = tableSchema(i)
      tableToUpdateIndex.get(i) match {
        case null =>
          // write null for unchanged columns apart from null count field (by this update)
          (ColumnStatsSchema(field.name, field.dataType,
            nullCountNullable = false).schema, allNullsExprs)
        case u => ColumnWriter.genCodeColumnStats(ctx, field,
          s"$deltaEncoders[$u].getRealEncoder()")
      }
    }.unzip
    // GenerateUnsafeProjection will automatically split stats expressions into separate
    // methods if required so no need to add separate functions explicitly.
    // Count is hardcoded as zero which will change for "insert" index deltas.
    val statsEv = ColumnWriter.genStatsRow(ctx, "0", stats, statsSchema)
    ctx.addNewFunction(finishUpdate,
      s"""
         |private void $finishUpdate(long batchId, int bucketId, int numRows) {
         |  if (batchId == $invalidUUID || batchId != $lastColumnBatchId) {
         |    if ($lastColumnBatchId == $invalidUUID) {
         |      // first call
         |      $lastColumnBatchId = batchId;
         |      $lastBucketId = bucketId;
         |      $lastNumRows = numRows;
         |      return;
         |    }
         |    // finish previous encoders, put into table and re-initialize
         |    final java.nio.ByteBuffer[] buffers = new java.nio.ByteBuffer[$numColumns];
         |    for (int $index = 0; $index < $numColumns; $index++) {
         |      buffers[$index] = $deltaEncoders[$index].finish($cursors[$index], $lastNumRows);
         |    }
         |    // create delta statistics row
         |    ${statsEv.code}
         |    // store the delta column batch
         |    final $columnBatchClass columnBatch = $columnBatchClass.apply(
         |        $batchOrdinal, buffers, ${statsEv.value}.getBytes(), $deltaIndexes);
         |    // maxDeltaRows is -1 so that insert into row buffer is never considered
         |    $externalStoreTerm.storeColumnBatch($tableName, columnBatch, $lastBucketId,
         |        $lastColumnBatchId, -1, ${compressionCodec.id}, new scala.Some($connTerm));
         |    $result += $batchOrdinal;
         |    ${if (updateMetric eq null) "" else s"$updateMetric.${metricAdd(batchOrdinal)};"}
         |    $initializeEncoders();
         |    $lastColumnBatchId = batchId;
         |    $lastBucketId = bucketId;
         |    $lastNumRows = numRows;
         |    $batchOrdinal = 0;
         |  }
         |}
      """.stripMargin)

    s"""
       |$effectiveCodes$updateVarsCode
       |if ($batchIdVar != $invalidUUID) {
       |  // finish and apply update if the next column batch ID is seen
       |  if ($batchIdVar != $lastColumnBatchId) {
       |    $finishUpdate($batchIdVar, $bucketVar, $numRowsVar);
       |  }
       |  // write to the encoders
       |  $callEncoders
       |  $batchOrdinal++;
       |} else {
       |  $rowConsume
       |}
    """.stripMargin
  }
}
