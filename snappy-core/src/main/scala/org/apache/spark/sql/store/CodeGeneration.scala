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
package org.apache.spark.sql.store

import java.sql.PreparedStatement
import java.util.Collections

import scala.collection.JavaConverters._

import com.google.common.cache.{CacheBuilder, CacheLoader}
import org.codehaus.janino.CompilerFactory

import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, CodeGenContext, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.collection.WrappedRow
import org.apache.spark.sql.columnar.ExternalStoreUtils
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types._

/**
 * Utilities to generate code for exchanging data from Spark layer
 * (Row, InternalRow) to store (Statement, ExecRow).
 * <p>
 * This is both more efficient and allows better code reuse with the code
 * generation facilities of Spark (esp for complex types like ArrayType).
 */
trait CodeGeneration {

  @throws[java.sql.SQLException]
  def executeStatement(stmt: PreparedStatement, multipleRows: Boolean,
      rows: java.util.Iterator[InternalRow], batchSize: Int,
      schema: Array[StructField], dialect: JdbcDialect): Int
}

private final class ExecuteKey(val name: String,
    val schema: Array[StructField], val dialect: JdbcDialect) {

  override def hashCode(): Int = name.hashCode

  override def equals(other: Any): Boolean = other match {
    case o: ExecuteKey => name == o.name
    case s: String => name == s
    case _ => false
  }
}

object CodeGeneration extends Logging {

  /**
   * A loading cache of generated <code>CodeGeneration</code>s.
   */
  private val execCache = CacheBuilder.newBuilder().maximumSize(100).build(
    new CacheLoader[ExecuteKey, CodeGeneration]() {
      override def load(key: ExecuteKey) = {
        val start = System.nanoTime()
        val result = compilePreparedUpdate(key.schema, key.dialect)
        def elapsed: Double = (System.nanoTime() - start).toDouble / 1000000.0
        logInfo(s"Expression code generated in $elapsed ms")
        result
      }
    })

  private def getColumnSetterFragment(col: Int, dataType: DataType,
      dialect: JdbcDialect, ctx: CodeGenContext,
      buffHolder: String): (String, String) = {
    val nonNullCode: String = dataType match {
      case IntegerType =>
        s"stmt.setInt(${col + 1}, row.getInt($col));"
      case LongType =>
        s"stmt.setLong(${col + 1}, row.getLong($col));"
      case DoubleType =>
        s"stmt.setDouble(${col + 1}, row.getDouble($col));"
      case FloatType =>
        s"stmt.setFloat(${col + 1}, row.getFloat($col));"
      case ShortType =>
        s"stmt.setInt(${col + 1}, row.getShort($col));"
      case ByteType =>
        s"stmt.setInt(${col + 1}, row.getByte($col));"
      case BooleanType =>
        s"stmt.setBoolean(${col + 1}, row.getBoolean($col));"
      case StringType =>
        s"stmt.setString(${col + 1}, row.getString($col));"
      case BinaryType =>
        s"stmt.setBytes(${col + 1}, row.getBinary($col));"
      case TimestampType =>
        s"""
          stmt.setTimestamp(${col + 1},
            DateTimeUtils.toJavaTimestamp(row.getLong($col)));
        """
      case DateType =>
        s"""
          stmt.setDate(${col + 1},
            DateTimeUtils.toJavaDate(row.getInt($col)));
        """
      case d: DecimalType =>
        s"""
          stmt.setBigDecimal(${col + 1},
            row.getDecimal($col, ${d.precision}, ${d.scale}).toJavaBigDecimal);
        """
      case a: ArrayType =>
        s"""
          ArrayData arr = row.getArray($col);
          if ($buffHolder == null) {
            $buffHolder = new BufferHolder();
          } else {
            $buffHolder.reset();
          }
          ${GenerateUnsafeProjection.writeArrayToBuffer(ctx, "arr",
            a.elementType, buffHolder)}
          stmt.setBytes(${col + 1}, java.util.Arrays.copyOf($buffHolder.buffer,
              $buffHolder.totalSize()));
        """
      case m: MapType =>
        s"""
          MapData map = row.getMap($col);
          if ($buffHolder == null) {
            $buffHolder = new BufferHolder();
          } else {
            $buffHolder.reset();
          }
          ${GenerateUnsafeProjection.writeMapToBuffer(ctx, "map",
            m.keyType, m.valueType, buffHolder)}
          stmt.setBytes(${col + 1}, java.util.Arrays.copyOf($buffHolder.buffer,
              $buffHolder.totalSize()));
        """
      case s: StructType =>
        s"""
          InternalRow struct = row.getStruct($col, ${s.length});
          if ($buffHolder == null) {
            $buffHolder = new BufferHolder();
          } else {
            $buffHolder.reset();
          }
          ${GenerateUnsafeProjection.writeStructToBuffer(ctx, "struct",
            s.fields.map(_.dataType), buffHolder)}
          stmt.setBytes(${col + 1}, java.util.Arrays.copyOf($buffHolder.buffer,
              $buffHolder.totalSize()));
        """
      case _ =>
        s"stmt.setObject(${col + 1}, row.get($col, schema[$col].dataType()));"
    }
    (nonNullCode, s"stmt.setNull(${col + 1}, " +
        s"${ExternalStoreUtils.getJDBCType(dialect, dataType)});")
  }

  private def compilePreparedUpdate(schema: Array[StructField],
      dialect: JdbcDialect): CodeGeneration = {
    val ctx = new CodeGenContext
    val bufferHolder = ctx.freshName("bufferHolder")
    val sb = new StringBuilder()
    schema.indices.foreach { col =>
      val (nonNullCode, nullCode) = getColumnSetterFragment(col,
        schema(col).dataType, dialect, ctx, bufferHolder)
      sb.append(
        s"""
          if (!row.isNullAt($col)) {
            $nonNullCode
          } else {
            $nullCode
          }
        """)
    }
    val evaluator = new CompilerFactory().newScriptEvaluator()
    evaluator.setClassName("io.snappydata.execute.GeneratedEvaluation")
    evaluator.setParentClassLoader(getClass.getClassLoader)
    evaluator.setDefaultImports(Array(
      classOf[ArrayData].getName,
      classOf[MapData].getName,
      classOf[InternalRow].getName,
      classOf[BufferHolder].getName))
    val expression =
      s"""
        BufferHolder $bufferHolder = null;
        int rowCount = 0;
        int result = 0;
        while (rows.hasNext()) {
          InternalRow row = (InternalRow)rows.next();
          ${sb.toString()}
          rowCount++;
          if (multipleRows) {
            stmt.addBatch();
            if ((rowCount % batchSize) == 0) {
              result += stmt.executeBatch().length;
              rowCount = 0;
            }
          }
        }
        if (multipleRows) {
          if (rowCount > 0) {
            result += stmt.executeBatch().length;
          }
        } else {
          result += stmt.executeUpdate();
        }
        return result;
      """
    // logInfo(s"DEBUG: Generated code=$expression")
    evaluator.createFastEvaluator(expression, classOf[CodeGeneration],
      Array("stmt", "multipleRows", "rows", "batchSize", "schema",
        "dialect")).asInstanceOf[CodeGeneration]
  }

  def executeUpdate(name: String, stmt: PreparedStatement,
      rows: Iterator[InternalRow], multipleRows: Boolean, batchSize: Int,
      schema: Array[StructField], dialect: JdbcDialect): Int = {
    val result = execCache.get(new ExecuteKey(name, schema, dialect))
    result.executeStatement(stmt, multipleRows, rows.asJava, batchSize,
      schema, dialect)
  }

  def executeUpdate(name: String, stmt: PreparedStatement, rows: Seq[Row],
      multipleRows: Boolean, batchSize: Int, schema: Array[StructField],
      dialect: JdbcDialect): Int = {
    val result = execCache.get(new ExecuteKey(name, schema, dialect))
    val iterator = new java.util.Iterator[InternalRow] {

      private val baseIterator = rows.iterator
      private val internalRow = new WrappedRow(schema)

      override def hasNext: Boolean = baseIterator.hasNext

      override def next(): InternalRow = {
        internalRow.row = baseIterator.next()
        internalRow
      }
    }
    result.executeStatement(stmt, multipleRows, iterator, batchSize,
      schema, dialect)
  }

  def executeUpdate(name: String, stmt: PreparedStatement, row: Row,
      schema: Array[StructField], dialect: JdbcDialect): Int = {
    val result = execCache.get(new ExecuteKey(name, schema, dialect))
    val internalRow = new WrappedRow(schema)
    internalRow.row = row
    result.executeStatement(stmt, multipleRows = false, Collections.singleton(
      internalRow.asInstanceOf[InternalRow]).iterator(), 0, schema, dialect)
  }

  def removeCache(name: String): Unit =
    execCache.invalidate(new ExecuteKey(name, null, null))

  def clearCache(): Unit = execCache.invalidateAll()
}
