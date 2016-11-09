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

import java.lang.reflect.Method
import java.sql.PreparedStatement
import java.util.Collections

import scala.collection.JavaConverters._

import com.gemstone.gemfire.internal.InternalDataSerializer
import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdHeapDataOutputStream
import org.codehaus.janino.CompilerFactory

import org.apache.spark.internal.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.{BufferHolder, CodegenContext, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.{MutableRow, UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils, MapData}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * Utilities to generate code for exchanging data from Spark layer
 * (Row, InternalRow) to store (Statement, ExecRow).
 * <p>
 * This is both more efficient and allows better code reuse with the code
 * generation facilities of Spark (esp for complex types like ArrayType).
 */
object CodeGeneration extends Logging {

  /**
   * A loading cache of generated <code>GeneratedStatement</code>s.
   */
  private[this] val cache = CacheBuilder.newBuilder().maximumSize(100).build(
    new CacheLoader[ExecuteKey, GeneratedStatement]() {
      override def load(key: ExecuteKey) = {
        val start = System.nanoTime()
        val result = compilePreparedUpdate(key.name, key.schema, key.dialect)
        def elapsed: Double = (System.nanoTime() - start).toDouble / 1000000.0
        logInfo(s"PreparedUpdate expression code generated in $elapsed ms")
        result
      }
    })

  private[this] val indexCache = CacheBuilder.newBuilder().maximumSize(100).build(
    new CacheLoader[ExecuteKey, GeneratedIndexStatement]() {
      override def load(key: ExecuteKey) = {
        val start = System.nanoTime()
        val result = compileGeneratedIndexUpdate(key.name, key.schema, key.dialect)
        def elapsed: Double = (System.nanoTime() - start).toDouble / 1000000.0
        logInfo(s"PreparedUpdate expression code generated in $elapsed ms")
        result
      }
    })

  /**
   * A loading cache of generated <code>SerializeComplexType</code>s.
   */
  private[this] val typeCache = CacheBuilder.newBuilder().maximumSize(100).build(
    new CacheLoader[DataType, SerializeComplexType]() {
      override def load(key: DataType) = {
        val start = System.nanoTime()
        val result = compileComplexType(key)
        def elapsed: Double = (System.nanoTime() - start).toDouble / 1000000.0
        logInfo(s"Serializer code generated in $elapsed ms")
        result
      }
    })

  /**
   * Using reflection to invoke these private methods since using the public
   * GenerateUnsafeProjection.createCode method is awkward.
   */
  private[this] val allMethods = GenerateUnsafeProjection.getClass
      .getDeclaredMethods.toIndexedSeq
  private[this] val generateArrayCodeMethod = getComplexTypeCodeMethod(
    "writeArrayToBuffer")
  private[this] val generateMapCodeMethod = getComplexTypeCodeMethod(
    "writeMapToBuffer")
  private[this] val generateStructCodeMethod = getComplexTypeCodeMethod(
    "writeStructToBuffer")

  private[this] def getComplexTypeCodeMethod(methodName: String): Method = {
    val method = allMethods.find(_.getName.endsWith(methodName))
        .getOrElse(sys.error(s"Failed to find method $methodName in " +
            s"GenerateUnsafeProjection (methods=$allMethods)"))
    method.setAccessible(true)
    method
  }

  private[this] def generateComplexTypeCode(method: Method,
      typeArgs: Object*): String =
    method.invoke(GenerateUnsafeProjection, typeArgs: _*).asInstanceOf[String]

  private[this] def getColumnSetterFragment(col: Int, dataType: DataType,
      dialect: JdbcDialect, ctx: CodegenContext): (String, String) = {
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
      case TimestampType => s"""
        stmt.setTimestamp(${col + 1},
          DateTimeUtils.toJavaTimestamp(row.getLong($col)));"""
      case DateType => s"""
        stmt.setDate(${col + 1},
          DateTimeUtils.toJavaDate(row.getInt($col)));"""
      case d: DecimalType => s"""
        stmt.setBigDecimal(${col + 1},
          row.getDecimal($col, ${d.precision}, ${d.scale}).toJavaBigDecimal());"""
      case a: ArrayType => val buffVar = getBufferHolderVar(col)
        // below is added only once while same buffer is used for all rows
        // so it has to be reset everytime in code that sets $.cursor below
        ctx.addMutableState(classOf[BufferHolder].getName, buffVar,
          s"$buffVar = new BufferHolder(new UnsafeRow());"); s"""
        final ArrayData arr = row.getArray($col);
        if (arr instanceof UnsafeArrayData) {
          final UnsafeArrayData unsafeArray = (UnsafeArrayData)arr;
          final byte[] bytes = new byte[unsafeArray.getSizeInBytes()];
          unsafeArray.writeToMemory(bytes, Platform.BYTE_ARRAY_OFFSET);
          stmt.setBytes(${col + 1}, bytes);
        } else {
          $buffVar.cursor = Platform.BYTE_ARRAY_OFFSET;
          ${generateComplexTypeCode(generateArrayCodeMethod, ctx, "arr",
            a.elementType, buffVar)}
          stmt.setBytes(${col + 1}, Arrays.copyOf(
              $buffVar.buffer, $buffVar.totalSize()));
        }"""
      case m: MapType => val buffVar = getBufferHolderVar(col)
        ctx.addMutableState(classOf[BufferHolder].getName, buffVar,
          s"$buffVar = new BufferHolder(new UnsafeRow());"); s"""
        final MapData map = row.getMap($col);
        if (map instanceof UnsafeMapData) {
          final UnsafeMapData unsafeMap = (UnsafeMapData)map;
          final byte[] bytes = new byte[unsafeMap.getSizeInBytes()];
          unsafeMap.writeToMemory(bytes, Platform.BYTE_ARRAY_OFFSET);
          stmt.setBytes(${col + 1}, bytes);
        } else {
          $buffVar.cursor = Platform.BYTE_ARRAY_OFFSET;
          ${generateComplexTypeCode(generateMapCodeMethod, ctx, "map",
            m.keyType, m.valueType, buffVar)}
          stmt.setBytes(${col + 1}, Arrays.copyOf(
             $buffVar.buffer, $buffVar.totalSize()));
        }"""
      case s: StructType => val buffVar = getBufferHolderVar(col)
        ctx.addMutableState(classOf[BufferHolder].getName, buffVar,
          s"$buffVar = new BufferHolder(new UnsafeRow(${s.length}));"); s"""
        final InternalRow struct = row.getStruct($col, ${s.length});
        if (struct instanceof UnsafeRow) {
          final UnsafeRow unsafeRow = (UnsafeRow)struct;
          final byte[] bytes = new byte[unsafeRow.getSizeInBytes()];
          unsafeRow.writeToMemory(bytes, Platform.BYTE_ARRAY_OFFSET);
          stmt.setBytes(${col + 1}, bytes);
        } else {
          $buffVar.cursor = Platform.BYTE_ARRAY_OFFSET;
          ${generateComplexTypeCode(generateStructCodeMethod, ctx, "struct",
            s.fields.map(_.dataType).toSeq, buffVar)}
          stmt.setBytes(${col + 1}, Arrays.copyOf(
              $buffVar.buffer, $buffVar.totalSize()));
        }"""
      case _ =>
        s"stmt.setObject(${col + 1}, row.get($col, schema[$col]));"
    }
    (nonNullCode, s"stmt.setNull(${col + 1}, " +
        s"${ExternalStoreUtils.getJDBCType(dialect, NullType)});")
  }

  private[this] def getBufferHolderVar(numFields: Int): String =
    "bufferHolderForComplexType_" + numFields

  private[this] def defaultImports = Array(
      DateTimeUtils.getClass.getName.replace("$", ""),
      classOf[Platform].getName,
      classOf[InternalRow].getName,
      classOf[UnsafeRow].getName,
      classOf[BufferHolder].getName,
      classOf[UTF8String].getName,
      classOf[Decimal].getName,
      classOf[CalendarInterval].getName,
      classOf[ArrayData].getName,
      classOf[UnsafeArrayData].getName,
      classOf[MapData].getName,
      classOf[UnsafeMapData].getName,
      classOf[java.util.Arrays].getName,
      classOf[MutableRow].getName)

  private[this] def getRowSetterFragment(schema: Array[DataType],
      dialect: JdbcDialect,
      ctx: CodegenContext): StringBuilder = {
    val sb = new StringBuilder()
    schema.indices.foreach { col =>
      val (nonNullCode, nullCode) = getColumnSetterFragment(col,
        schema(col), dialect, ctx)
      sb.append(
        s"""
        if (!row.isNullAt($col)) {
          $nonNullCode
        } else {
          $nullCode
        }""")
    }
    sb
  }

  private[this] def compilePreparedUpdate(table: String,
      schema: Array[DataType], dialect: JdbcDialect): GeneratedStatement = {
    val ctx = new CodegenContext
    val sb = getRowSetterFragment(schema, dialect, ctx)

    val evaluator = new CompilerFactory().newScriptEvaluator()
    evaluator.setClassName("io.snappydata.execute.GeneratedEvaluation")
    evaluator.setParentClassLoader(getClass.getClassLoader)
    evaluator.setDefaultImports(defaultImports)
    val separator = "\n      "
    val varDeclarations = ctx.mutableStates.map { case (javaType, name, init) =>
      s"$javaType $name;$separator${init.replace("this.", "")}"
    }
    val expression = s"""
      ${varDeclarations.mkString(separator)}
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
      return result;"""

    logDebug(s"DEBUG: For update to table=$table, generated code=$expression")
    evaluator.createFastEvaluator(expression, classOf[GeneratedStatement],
      Array("stmt", "multipleRows", "rows", "batchSize", "schema",
        "dialect")).asInstanceOf[GeneratedStatement]
  }


  private[this] def compileGeneratedIndexUpdate(table: String,
      schema: Array[DataType], dialect: JdbcDialect): GeneratedIndexStatement = {
    val ctx = new CodegenContext
    val sb = getRowSetterFragment(schema, dialect, ctx)

    val evaluator = new CompilerFactory().newScriptEvaluator()
    evaluator.setClassName("io.snappydata.execute.GeneratedIndexEvaluation")
    evaluator.setParentClassLoader(getClass.getClassLoader)
    evaluator.setDefaultImports(defaultImports)
    val separator = "\n      "
    val varDeclarations = ctx.mutableStates.map { case (javaType, name, init) =>
      s"$javaType $name;$separator${init.replace("this.", "")}"
    }
    val expression = s"""
      ${varDeclarations.mkString(separator)}
        ${sb.toString()}
        stmt.addBatch();
      return 1;"""

    logDebug(s"DEBUG: For update to index=$table, generated code=$expression")
    evaluator.createFastEvaluator(expression, classOf[GeneratedIndexStatement],
      Array("schema", "dialect", "stmt", "row")).asInstanceOf[GeneratedIndexStatement]
  }


  private[this] def compileComplexType(
      dataType: DataType): SerializeComplexType = {
    val ctx = new CodegenContext
    val inputVar = "value"
    val bufferHolderVar = "bufferHolder"
    val dosVar = "dos"
    val typeConversion = dataType match {
      case a: ArrayType => s"""
        final ArrayData arr = (ArrayData)$inputVar;
        if (arr instanceof UnsafeArrayData) {
          final UnsafeArrayData unsafeArray = (UnsafeArrayData)arr;
          final int size = unsafeArray.getSizeInBytes();
          if ($dosVar != null) {
            InternalDataSerializer.writeArrayLength(size, $dosVar);
            $dosVar.copyMemory(unsafeArray.getBaseObject(),
              unsafeArray.getBaseOffset(), size);
            return null;
          } else {
            final byte[] bytes = new byte[size];
            unsafeArray.writeToMemory(bytes, Platform.BYTE_ARRAY_OFFSET);
            return bytes;
          }
        } else {
          ${generateComplexTypeCode(generateArrayCodeMethod, ctx, "arr",
            a.elementType, bufferHolderVar)}
          if ($dosVar != null) {
            InternalDataSerializer.writeByteArray($bufferHolderVar.buffer,
              $bufferHolderVar.totalSize(), $dosVar);
            return null;
          } else {
            return Arrays.copyOf($bufferHolderVar.buffer,
              $bufferHolderVar.totalSize());
          }
        }"""
      case m: MapType => s"""
        final MapData map = (MapData)$inputVar;
        if (map instanceof UnsafeMapData) {
          final UnsafeMapData unsafeMap = (UnsafeMapData)map;
          final int size = unsafeMap.getSizeInBytes();
          if ($dosVar != null) {
            InternalDataSerializer.writeArrayLength(size, $dosVar);
            $dosVar.copyMemory(unsafeMap.getBaseObject(),
              unsafeMap.getBaseOffset(), size);
            return null;
          } else {
            final byte[] bytes = new byte[size];
            unsafeMap.writeToMemory(bytes, Platform.BYTE_ARRAY_OFFSET);
            return bytes;
          }
        } else {
          ${generateComplexTypeCode(generateMapCodeMethod, ctx, "map",
            m.keyType, m.valueType, bufferHolderVar)}
          if ($dosVar != null) {
            InternalDataSerializer.writeByteArray($bufferHolderVar.buffer,
              $bufferHolderVar.totalSize(), $dosVar);
            return null;
          } else {
            return Arrays.copyOf($bufferHolderVar.buffer,
              $bufferHolderVar.totalSize());
          }
        }"""
      case s: StructType => s"""
        final InternalRow struct = (InternalRow)$inputVar;
        if (struct instanceof UnsafeRow) {
          final UnsafeRow unsafeRow = (UnsafeRow)struct;
          final int size = unsafeRow.getSizeInBytes();
          if ($dosVar != null) {
            InternalDataSerializer.writeArrayLength(size, $dosVar);
            $dosVar.copyMemory(unsafeRow.getBaseObject(),
              unsafeRow.getBaseOffset(), size);
            return null;
          } else {
            final byte[] bytes = new byte[size];
            unsafeRow.writeToMemory(bytes, Platform.BYTE_ARRAY_OFFSET);
            return bytes;
          }
        } else {
          ${generateComplexTypeCode(generateStructCodeMethod, ctx, "struct",
            s.fields.map(_.dataType).toSeq, bufferHolderVar)}
          if ($dosVar != null) {
            InternalDataSerializer.writeByteArray($bufferHolderVar.buffer,
              $bufferHolderVar.totalSize(), $dosVar);
            return null;
          } else {
            return Arrays.copyOf($bufferHolderVar.buffer,
              $bufferHolderVar.totalSize());
          }
        }"""
      case _ => throw Utils.analysisException(
        s"complex type conversion: unexpected type $dataType")
    }

    val evaluator = new CompilerFactory().newScriptEvaluator()
    evaluator.setClassName("io.snappydata.execute.GeneratedSerialization")
    evaluator.setParentClassLoader(getClass.getClassLoader)
    evaluator.setDefaultImports(Array(classOf[Platform].getName,
      classOf[InternalRow].getName,
      classOf[UnsafeRow].getName,
      classOf[UTF8String].getName,
      classOf[Decimal].getName,
      classOf[CalendarInterval].getName,
      classOf[ArrayData].getName,
      classOf[UnsafeArrayData].getName,
      classOf[MapData].getName,
      classOf[UnsafeMapData].getName,
      classOf[java.util.Arrays].getName,
      classOf[InternalDataSerializer].getName,
      classOf[MutableRow].getName))
    val separator = "\n      "
    val varDeclarations = ctx.mutableStates.map { case (javaType, name, init) =>
      s"$javaType $name;$separator${init.replace("this.", "")}"
    }
    val expression = s"""
      ${varDeclarations.mkString(separator)}
      $typeConversion"""

    logDebug(s"DEBUG: For complex type=$dataType, generated code=$expression")
    evaluator.createFastEvaluator(expression, classOf[SerializeComplexType],
      Array(inputVar, bufferHolderVar, dosVar))
        .asInstanceOf[SerializeComplexType]
  }

  private[this] def executeUpdate(name: String, stmt: PreparedStatement,
      rows: java.util.Iterator[InternalRow], multipleRows: Boolean,
      batchSize: Int, schema: Array[DataType], dialect: JdbcDialect): Int = {
    val result = cache.get(new ExecuteKey(name, schema, dialect))
    result.executeStatement(stmt, multipleRows, rows, batchSize,
      schema, dialect)
  }

  def executeUpdate(name: String, stmt: PreparedStatement,
      rows: Iterator[InternalRow], multipleRows: Boolean, batchSize: Int,
      schema: Array[DataType], dialect: JdbcDialect): Int =
    executeUpdate(name, stmt, rows.asJava, multipleRows, batchSize,
      schema, dialect)

  def executeUpdate(name: String, stmt: PreparedStatement, rows: Seq[Row],
      multipleRows: Boolean, batchSize: Int, schema: Array[StructField],
      dialect: JdbcDialect): Int = {
    val iterator = new java.util.Iterator[InternalRow] {

      private val baseIterator = rows.iterator
      private val encoder = RowEncoder(StructType(schema))

      override def hasNext: Boolean = baseIterator.hasNext

      override def next(): InternalRow = {
        encoder.toRow(baseIterator.next())
      }

      override def remove(): Unit =
        throw new UnsupportedOperationException("remove not supported")
    }
    executeUpdate(name, stmt, iterator, multipleRows, batchSize,
      schema.map(_.dataType), dialect)
  }

  def executeUpdate(name: String, stmt: PreparedStatement, row: Row,
      schema: Array[StructField], dialect: JdbcDialect): Int = {
    val encoder = RowEncoder(StructType(schema))
    executeUpdate(name, stmt, Collections.singleton(encoder.toRow(row))
        .iterator(), multipleRows = false, 0, schema.map(_.dataType), dialect)
  }

  def getComplexTypeSerializer(dataType: DataType): SerializeComplexType =
    typeCache.get(dataType)

  def getGeneratedIndexStatement(name: String,
      schema: StructType,
      dialect: JdbcDialect): (PreparedStatement, InternalRow) => Int = {
    val schemaArr = schema.map(_.dataType).toArray
    val result = indexCache.get(new ExecuteKey(name, schemaArr,
      dialect, forIndex = true))
    result.addBatch(schemaArr, dialect) _
  }

  def removeCache(name: String): Unit =
    cache.invalidate(new ExecuteKey(name, null, null))

  def removeCache(dataType: DataType): Unit = cache.invalidate(dataType)

  def removeIndexCache(indexName: String): Unit =
    indexCache.invalidate(new ExecuteKey(indexName, null, null, true))

  def clearCache(): Unit = cache.invalidateAll()

}

trait GeneratedStatement {

  @throws[java.sql.SQLException]
  def executeStatement(stmt: PreparedStatement, multipleRows: Boolean,
      rows: java.util.Iterator[InternalRow], batchSize: Int,
      schema: Array[DataType], dialect: JdbcDialect): Int
}

trait SerializeComplexType {

  @throws[java.io.IOException]
  def serialize(value: Any, bufferHolder: BufferHolder,
      dos: GfxdHeapDataOutputStream): Array[Byte]
}


trait GeneratedIndexStatement {

  @throws[java.sql.SQLException]
  def addBatch(schema: Array[DataType], dialect: JdbcDialect)
      (stmt: PreparedStatement, row: InternalRow): Int
}


private final class ExecuteKey(val name: String,
    val schema: Array[DataType], val dialect: JdbcDialect,
    val forIndex: Boolean = false) {

  override def hashCode(): Int = if (schema != null && !forIndex) {
    scala.util.hashing.MurmurHash3.arrayHash(schema)
  } else name.hashCode

  override def equals(other: Any): Boolean = other match {
    case o: ExecuteKey => if (schema != null && o.schema != null && !forIndex) {
      schema.sameElements(o.schema)
    } else {
      name == o.name
    }
    case s: String => name == s
    case _ => false
  }
}
