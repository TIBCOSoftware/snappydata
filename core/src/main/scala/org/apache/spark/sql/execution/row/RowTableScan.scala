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
package org.apache.spark.sql.execution.row

import java.util.{GregorianCalendar, TimeZone}

import com.gemstone.gemfire.internal.shared.ClientSharedData
import com.pivotal.gemfirexd.internal.engine.store.{AbstractCompactExecRow, ResultWasNull}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.catalyst.util.{SerializedArray, SerializedMap, SerializedRow}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.{PartitionedDataSourceScan, PartitionedPhysicalScan}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types._

/**
 * Physical plan node for scanning data from a SnappyData row table RDD.
 * If user knows that the data is partitioned or replicated across
 * all nodes, this SparkPlan can be used to avoid expensive shuffle
 * and Broadcast joins. This plan overrides outputPartitioning and
 * makes it inline with the partitioning of the underlying DataSource.
 */
abstract case class RowTableScan(
    output: Seq[Attribute],
    _schema: StructType,
    dataRDD: RDD[Any],
    numBuckets: Int,
    partitionColumns: Seq[Expression],
    partitionColumnAliases: Seq[Seq[Attribute]],
    table: String,
    @transient baseRelation: PartitionedDataSourceScan,
    caseSensitive: Boolean)
    extends PartitionedPhysicalScan(output, dataRDD, numBuckets,
      partitionColumns, partitionColumnAliases,
      baseRelation.asInstanceOf[BaseRelation]) {

  override lazy val schema: StructType = _schema

  override val nodeName: String = "RowTableScan"

  lazy val tableIdentifier: Option[TableIdentifier] = baseRelation match {
    case null => None
    case r => sqlContext match {
      case null => Some(SnappySession.tableIdentifier(r.table, catalog = null, resolve = false))
      case c =>
        Some(c.sparkSession.asInstanceOf[SnappySession].tableIdentifier(r.table, resolve = true))
    }
  }

  override def doProduce(ctx: CodegenContext): String = {
    // a parent plan may set a custom input (e.g. HashJoinExec)
    // for that case no need to add the "shouldStop()" calls
    // PartitionedPhysicalRDD always has one input
    val input = internals.addClassField(ctx, "scala.collection.Iterator", "input",
      v => s"$v = inputs[0];")
    val numOutputRows = if (sqlContext eq null) null
    else metricTerm(ctx, "numOutputRows")
    ctx.currentVars = null

    val code = dataRDD match {
      case null =>
        doProduceWithoutProjection(ctx, input, numOutputRows,
          output, if (baseRelation ne null) baseRelation.schema else schema)
      case rowRdd: RowFormatScanRDD if !rowRdd.pushProjections =>
        doProduceWithoutProjection(ctx, input, numOutputRows,
          output, baseRelation.schema)
      case _ =>
        doProduceWithProjection(ctx, input, numOutputRows, output)
    }
    code
  }

  def doProduceWithoutProjection(ctx: CodegenContext, input: String,
      numOutputRows: String, output: Seq[Attribute],
      baseSchema: StructType): String = {
    // case of CompactExecRows
    val numRows = ctx.freshName("numRows")
    val row = ctx.freshName("row")
    val iterator = ctx.freshName("localIterator")
    val holder = ctx.freshName("nullHolder")
    val holderClass = classOf[ResultSetNullHolder].getName
    val compactRowClass = classOf[AbstractCompactExecRow].getName
    val baseSchemaOutput = baseSchema.toAttributes
    val columnsRowInput = output.map(a => genCodeCompactRowColumn(ctx,
      row, holder, Utils.fieldIndex(baseSchemaOutput, a.name, caseSensitive),
      a.dataType, a.nullable))
    s"""
       |final scala.collection.Iterator $iterator = $input;
       |final $holderClass $holder = new $holderClass();
       |long $numRows = 0L;
       |try {
       |  while ($iterator.hasNext()) {
       |    final $compactRowClass $row = ($compactRowClass)$iterator.next();
       |    $numRows++;
       |    ${consume(ctx, columnsRowInput).trim}
       |    if (shouldStop()) return;
       |  }
       |} catch (RuntimeException re) {
       |  throw re;
       |} catch (Exception e) {
       |  throw new RuntimeException(e);
       |} finally {
       |  ${if (numOutputRows eq null) "" else s"$numOutputRows.${metricAdd(numRows)};"}
       |}
    """.stripMargin
  }

  def doProduceWithProjection(ctx: CodegenContext, input: String,
      numOutputRows: String, output: Seq[Attribute]): String = {
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
       |  ${if (numOutputRows eq null) "" else s"$numOutputRows.${metricAdd(numRows)};"}
       |}
    """.stripMargin
  }

  private def genCodeCompactRowColumn(ctx: CodegenContext, rowVar: String,
      holder: String, ordinal: Int, dataType: DataType,
      nullable: Boolean): ExprCode = {
    val javaType = ctx.javaType(dataType)
    val col = ctx.freshName("col")
    val pos = ordinal + 1
    var useHolder = true
    val code = dataType match {
      case IntegerType =>
        s"final $javaType $col = $rowVar.getAsInt($pos, $holder);"
      case StringType =>
        useHolder = false
        s"final $javaType $col = $rowVar.getAsUTF8String($ordinal);"
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
        useHolder = false
        val decVar = ctx.freshName("dec")
        s"""
          final java.math.BigDecimal $decVar = $rowVar.getAsBigDecimal(
            $pos, null);
          final $javaType $col = $decVar != null ? Decimal.apply($decVar,
            ${d.precision}, ${d.scale}) : null;
        """
      case DateType =>
        val cal = ctx.freshName("cal")
        val dateMs = ctx.freshName("dateMillis")
        val calClass = classOf[GregorianCalendar].getName
        s"""
          final $calClass $cal = $holder.defaultCal();
          $cal.clear();
          final long $dateMs = $rowVar.getAsDateMillis($ordinal, $cal, $holder);
          final $javaType $col = org.apache.spark.sql.collection
              .Utils.millisToDays($dateMs, $holder.defaultTZ());
        """
      case TimestampType =>
        val cal = ctx.freshName("cal")
        val calClass = classOf[GregorianCalendar].getName
        s"""
          final $calClass $cal = $holder.defaultCal();
          $cal.clear();
          final $javaType $col = $rowVar.getAsTimestampMicros(
            $ordinal, $cal, $holder);
        """
      case BinaryType =>
        useHolder = false
        s"final $javaType $col = $rowVar.getAsBytes($pos, null);"
      case _: ArrayType =>
        useHolder = false
        val bytes = ctx.freshName("bytes")
        val arrayClass = classOf[SerializedArray].getName
        s"""
          final byte[] $bytes = $rowVar.getAsBytes($pos, null);
          final $arrayClass $col;
          if ($bytes != null) {
            $col = new $arrayClass(8); // includes size
            $col.pointTo($bytes, Platform.BYTE_ARRAY_OFFSET, $bytes.length);
          } else {
            $col = null;
          }
        """
      case _: MapType =>
        useHolder = false
        val bytes = ctx.freshName("bytes")
        val mapClass = classOf[SerializedMap].getName
        s"""
          final byte[] $bytes = $rowVar.getAsBytes($pos, null);
          final $mapClass $col;
          if ($bytes != null) {
            $col = new $mapClass();
            $col.pointTo($bytes, Platform.BYTE_ARRAY_OFFSET);
          } else {
            $col = null;
          }
        """
      case s: StructType =>
        useHolder = false
        val bytes = ctx.freshName("bytes")
        val structClass = classOf[SerializedRow].getName
        s"""
          final byte[] $bytes = $rowVar.getAsBytes($pos, null);
          final $structClass $col;
          if ($bytes != null) {
            $col = new $structClass(4, ${s.length}); // includes size
            $col.pointTo($bytes, Platform.BYTE_ARRAY_OFFSET, $bytes.length);
          } else {
            $col = null;
          }
        """
      case _ =>
        useHolder = false
        s"$javaType $col = ($javaType)$rowVar.getAsObject($pos, null);"
    }
    if (nullable) {
      val isNullVar = ctx.freshName("isNull")
      if (useHolder) {
        ExprCode(s"$code\nfinal boolean $isNullVar = $holder.wasNullAndClear();",
          isNullVar, col)
      } else {
        ExprCode(s"$code\nfinal boolean $isNullVar = $col == null;",
          isNullVar, col)
      }
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
        val arrayClass = classOf[SerializedArray].getName
        s"""
          final byte[] $bytes = $rsVar.getBytes($pos);
          final $arrayClass $col;
          if ($bytes != null) {
            $col = new $arrayClass(8); // includes size
            $col.pointTo($bytes, Platform.BYTE_ARRAY_OFFSET, $bytes.length);
          } else {
            $col = null;
          }
        """
      case _: MapType =>
        val bytes = ctx.freshName("bytes")
        val mapClass = classOf[SerializedMap].getName
        s"""
          final byte[] $bytes = $rsVar.getBytes($pos);
          final $mapClass $col;
          if ($bytes != null) {
            $col = new $mapClass();
            $col.pointTo($bytes, Platform.BYTE_ARRAY_OFFSET);
          } else {
            $col = null;
          }
        """
      case s: StructType =>
        val bytes = ctx.freshName("bytes")
        val structClass = classOf[SerializedRow].getName
        s"""
          final byte[] $bytes = $rsVar.getBytes($pos);
          final $structClass $col;
          if ($bytes != null) {
            $col = new $structClass(4, ${s.length}); // includes size
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

class ResultSetNullHolder extends ResultWasNull {

  final var wasNull: Boolean = _

  final lazy val defaultCal: GregorianCalendar =
    ClientSharedData.getDefaultCleanCalendar

  final lazy val defaultTZ: TimeZone = defaultCal.getTimeZone

  override final def setWasNull(): Unit = {
    wasNull = true
  }

  final def wasNullAndClear(): Boolean = {
    val result = wasNull
    wasNull = false
    result
  }
}
