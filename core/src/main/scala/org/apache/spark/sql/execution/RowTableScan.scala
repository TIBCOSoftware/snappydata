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
package org.apache.spark.sql.execution

import java.util.GregorianCalendar

import com.pivotal.gemfirexd.internal.engine.store.AbstractCompactExecRow

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.execution.row.{ResultSetTraversal, RowFormatScanRDD}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._

/**
 * Physical plan node for scanning data from a SnappyData row table RDD.
 * If user knows that the data is partitioned or replicated across
 * all nodes, this SparkPlan can be used to avoid expensive shuffle
 * and Broadcast joins. This plan overrides outputPartitioning and
 * makes it inline with the partitioning of the underlying DataSource.
 */
private[sql] final case class RowTableScan(
    output: Seq[Attribute],
    dataRDD: RDD[Any],
    numBuckets: Int,
    partitionColumns: Seq[Expression],
    @transient baseRelation: PartitionedDataSourceScan)
    extends PartitionedPhysicalScan(output, dataRDD, numBuckets,
      partitionColumns, baseRelation.asInstanceOf[BaseRelation]) {

  override def doProduce(ctx: CodegenContext): String = {
    val numOutputRows = metricTerm(ctx, "numOutputRows")
    // PartitionedPhysicalRDD always just has one input
    val input = ctx.freshName("input")
    ctx.addMutableState("scala.collection.Iterator",
      input, s"$input = inputs[0];")
    ctx.currentVars = null

    dataRDD match {
      case rowRdd: RowFormatScanRDD if !rowRdd.pushProjections =>
        doProduceWithoutProjection(ctx, input, numOutputRows,
          output, baseRelation)
      case _ =>
        doProduceWithProjection(ctx, input, numOutputRows,
          output, baseRelation)
    }
  }

  def doProduceWithoutProjection(ctx: CodegenContext, input: String,
      numOutputRows: String, output: Seq[Attribute],
      baseRelation: PartitionedDataSourceScan): String = {
    // case of CompactExecRows
    val numRows = ctx.freshName("numRows")
    val row = ctx.freshName("row")
    val holder = ctx.freshName("nullHolder")
    val holderClass = classOf[ResultNullHolder].getName
    val compactRowClass = classOf[AbstractCompactExecRow].getName
    val baseSchema = baseRelation.schema
    val columnsRowInput = output.map(a => genCodeCompactRowColumn(ctx,
      row, holder, baseSchema.fieldIndex(a.name), a.dataType, a.nullable))
    s"""
       |final $holderClass $holder = new $holderClass();
       |long $numRows = 0L;
       |try {
       |  while ($input.hasNext()) {
       |    final $compactRowClass $row = ($compactRowClass)$input.next();
       |    $numRows++;
       |    ${consume(ctx, columnsRowInput).trim}
       |    if (shouldStop()) return;
       |  }
       |} catch (RuntimeException re) {
       |  throw re;
       |} catch (Exception e) {
       |  throw new RuntimeException(e);
       |} finally {
       |  $numOutputRows.add($numRows);
       |}
    """.stripMargin
  }

  def doProduceWithProjection(ctx: CodegenContext, input: String,
      numOutputRows: String, output: Seq[Attribute],
      baseRelation: PartitionedDataSourceScan): String = {
    // case of ResultSet
    val numRows = ctx.freshName("numRows")
    val iterator = ctx.freshName("iterator")
    val iteratorClass = classOf[ResultSetTraversal].getName
    val rs = ctx.freshName("resultSet")
    val columnsRowInput = output.zipWithIndex.map { case (a, index) =>
      genCodeResultSetColumn(ctx, rs, iterator, index, a.dataType, a.nullable)
    }
    s"""
       |final $iteratorClass $iterator = ($iteratorClass)$input;
       |final java.sql.ResultSet $rs = $iterator.rs();
       |long $numRows = 0L;
       |try {
       |  while ($iterator.hasNext()) {
       |    $iterator.next();
       |    $numRows++;
       |    ${consume(ctx, columnsRowInput).trim}
       |    if (shouldStop()) return;
       |  }
       |} catch (RuntimeException re) {
       |  throw re;
       |} catch (Exception e) {
       |  throw new RuntimeException(e);
       |} finally {
       |  $numOutputRows.add($numRows);
       |}
    """.stripMargin
  }

  private def genCodeCompactRowColumn(ctx: CodegenContext, rowVar: String,
      holder: String, ordinal: Int, dataType: DataType,
      nullable: Boolean): ExprCode = {
    val javaType = ctx.javaType(dataType)
    val col = ctx.freshName("col")
    val pos = ordinal + 1
    val code = dataType match {
      case IntegerType =>
        s"final $javaType $col = $rowVar.getAsInt($pos, $holder);"
      case StringType =>
        // TODO: SW: optimize to store same full UTF8 format in GemXD
        s"""
          final $javaType $col = UTF8String.fromString($rowVar.getAsString(
            $pos, $holder));
        """
      case LongType =>
        s"final $javaType $col = $rowVar.getAsLong($pos, $holder);"
      case BooleanType =>
        s"final $javaType $col = $rowVar.getAsBoolean($pos, $holder);"
      case ShortType =>
        s"final $javaType $col = $rowVar.getAsShort($pos, $holder);"
      case ByteType =>
        s"final $javaType $col = $rowVar.getAsByte($pos, $holder);"
      case FloatType =>
        s"final $javaType $col = $rowVar.getAsFloat($pos, $holder);"
      case DoubleType =>
        s"final $javaType $col = $rowVar.getAsDouble($pos, $holder);"
      case d: DecimalType =>
        val decVar = ctx.freshName("dec")
        s"""
          final java.math.BigDecimal $decVar = $rowVar.getAsBigDecimal(
            $pos, $holder);
          final $javaType $col = $decVar != null ? Decimal.apply($decVar,
            ${d.precision}, ${d.scale}) : null;
        """
      case DateType =>
        // TODO: optimize to avoid Date object and instead get millis
        val cal = ctx.freshName("cal")
        val date = ctx.freshName("date")
        val calClass = classOf[GregorianCalendar].getName
        s"""
          final $calClass $cal = $holder.defaultCal();
          $cal.clear();
          final java.sql.Date $date = $rowVar.getAsDate($pos, $cal, $holder);
          final $javaType $col = $date != null ? org.apache.spark.sql
            .catalyst.util.DateTimeUtils.fromJavaDate($date) : 0;
        """
      case TimestampType =>
        // TODO: optimize to avoid object and instead get nanoseconds
        val cal = ctx.freshName("cal")
        val tsVar = ctx.freshName("ts")
        val calClass = classOf[GregorianCalendar].getName
        s"""
          final $calClass $cal = $holder.defaultCal();
          $cal.clear();
          final java.sql.Timestamp $tsVar = $rowVar.getAsTimestamp($pos,
             $cal, $holder);
          final $javaType $col = $tsVar != null ? org.apache.spark.sql
            .catalyst.util.DateTimeUtils.fromJavaTimestamp($tsVar) : 0L;
        """
      case BinaryType =>
        s"final $javaType $col = $rowVar.getAsBytes($pos, $holder);"
      case _: ArrayType =>
        val bytes = ctx.freshName("bytes")
        s"""
          final byte[] $bytes = $rowVar.getAsBytes($pos, $holder);
          final $javaType $col;
          if ($bytes != null) {
            $col = new UnsafeArrayData();
            $col.pointTo($bytes, Platform.BYTE_ARRAY_OFFSET, $bytes.length);
          } else {
            $col = null;
          }
        """
      case _: MapType =>
        val bytes = ctx.freshName("bytes")
        s"""
          final byte[] $bytes = $rowVar.getAsBytes($pos, $holder);
          final $javaType $col;
          if ($bytes != null) {
            $col = new UnsafeMapData();
            $col.pointTo($bytes, Platform.BYTE_ARRAY_OFFSET, $bytes.length);
          } else {
            $col = null;
          }
        """
      case s: StructType =>
        val bytes = ctx.freshName("bytes")
        s"""
          final byte[] $bytes = $rowVar.getAsBytes($pos, $holder);
          final $javaType $col;
          if ($bytes != null) {
            $col = new UnsafeRow(${s.length});
            $col.pointTo($bytes, Platform.BYTE_ARRAY_OFFSET, $bytes.length);
          } else {
            $col = null;
          }
        """
      case _ =>
        s"""
          $javaType $col = ($javaType)$rowVar.getAsObject($pos, $holder);
        """
    }
    if (nullable) {
      val isNullVar = ctx.freshName("isNull")
      ExprCode(s"$code\nfinal boolean $isNullVar = $holder.wasNullAndClear();",
        isNullVar, col)
    } else {
      ExprCode(code, "false", col)
    }
  }

  private def genCodeResultSetColumn(ctx: CodegenContext, rsVar: String,
      holder: String, ordinal: Int, dataType: DataType,
      nullable: Boolean): ExprCode = {
    val javaType = ctx.javaType(dataType)
    val col = ctx.freshName("col")
    val pos = ordinal + 1
    val code = dataType match {
      case IntegerType =>
        s"final $javaType $col = $rsVar.getInt($pos);"
      case StringType =>
        s"final $javaType $col = UTF8String.fromString($rsVar.getString($pos));"
      case LongType =>
        s"final $javaType $col = $rsVar.getLong($pos);"
      case BooleanType =>
        s"final $javaType $col = $rsVar.getBoolean($pos);"
      case ShortType =>
        s"final $javaType $col = $rsVar.getShort($pos);"
      case ByteType =>
        s"final $javaType $col = $rsVar.getByte($pos);"
      case FloatType =>
        s"final $javaType $col = $rsVar.getFloat($pos);"
      case DoubleType =>
        s"final $javaType $col = $rsVar.getDouble($pos);"
      case d: DecimalType =>
        // When connecting with Oracle DB, the precision and scale of
        // BigDecimal object returned by ResultSet.getBigDecimal is not
        // correctly matched to the table schema reported by
        // ResultSetMetaData.getPrecision and ResultSetMetaData.getScale.
        // If inserting values like 19999 into a column with NUMBER(12, 2)
        // type, you get through a BigDecimal object with scale as 0.
        // But the DataFrame schema has correct type as DecimalType(12, 2).
        // Thus, after saving the DataFrame into parquet file and then
        // retrieve it, you will get wrong result 199.99. So it is needed
        // to set precision and scale for Decimal based on metadata.
        val decVar = ctx.freshName("dec")
        s"""
          final java.math.BigDecimal $decVar = $rsVar.getBigDecimal($pos);
          final $javaType $col = $decVar != null ? Decimal.apply($decVar,
            ${d.precision}, ${d.scale}) : null;
        """
      case DateType =>
        val cal = ctx.freshName("cal")
        val date = ctx.freshName("date")
        val calClass = classOf[GregorianCalendar].getName
        s"""
          final $calClass $cal = $holder.defaultCal();
          $cal.clear();
          final java.sql.Date $date = $rsVar.getDate($pos, $cal);
          final $javaType $col = $date != null ? org.apache.spark.sql
            .catalyst.util.DateTimeUtils.fromJavaDate($date) : 0;
        """
      case TimestampType =>
        val cal = ctx.freshName("cal")
        val tsVar = ctx.freshName("ts")
        val calClass = classOf[GregorianCalendar].getName
        s"""
          final $calClass $cal = $holder.defaultCal();
          $cal.clear();
          final java.sql.Timestamp $tsVar = $rsVar.getTimestamp($pos, $cal);
          final $javaType $col = $tsVar != null ? org.apache.spark.sql
            .catalyst.util.DateTimeUtils.fromJavaTimestamp($tsVar) : 0L;
        """
      case BinaryType =>
        s"final $javaType $col = $rsVar.getBytes($pos);"
      case _: ArrayType =>
        val bytes = ctx.freshName("bytes")
        s"""
          final byte[] $bytes = $rsVar.getBytes($pos);
          final $javaType $col;
          if ($bytes != null) {
            $col = new UnsafeArrayData();
            $col.pointTo($bytes, Platform.BYTE_ARRAY_OFFSET, $bytes.length);
          } else {
            $col = null;
          }
        """
      case _: MapType =>
        val bytes = ctx.freshName("bytes")
        s"""
          final byte[] $bytes = $rsVar.getBytes($pos);
          final $javaType $col;
          if ($bytes != null) {
            $col = new UnsafeMapData();
            $col.pointTo($bytes, Platform.BYTE_ARRAY_OFFSET, $bytes.length);
          } else {
            $col = null;
          }
        """
      case s: StructType =>
        val bytes = ctx.freshName("bytes")
        s"""
          final byte[] $bytes = $rsVar.getBytes($pos);
          final $javaType $col;
          if ($bytes != null) {
            $col = new UnsafeRow(${s.length});
            $col.pointTo($bytes, Platform.BYTE_ARRAY_OFFSET, $bytes.length);
          } else {
            $col = null;
          }
        """
      case _ =>
        s"final $javaType $col = ($javaType)$rsVar.getObject($pos);"
    }
    if (nullable) {
      val isNullVar = ctx.freshName("isNull")
      ExprCode(code + s"\nfinal boolean $isNullVar = $rsVar.wasNull();",
        isNullVar, col)
    } else {
      ExprCode(code, "false", col)
    }
  }
}
