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
package org.apache.spark.sql.store

import java.sql.PreparedStatement
import java.util.Collections

import scala.util.hashing.MurmurHash3

import com.gemstone.gemfire.internal.InternalDataSerializer
import com.gemstone.gemfire.internal.shared.ClientSharedUtils
import com.google.common.cache.{CacheBuilder, CacheLoader}
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdHeapDataOutputStream
import org.codehaus.janino.CompilerFactory

import org.apache.spark.metrics.source.CodegenMetrics
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.util.{ArrayData, DateTimeUtils, MapData, SerializedArray, SerializedMap, SerializedRow}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ColumnWriter
import org.apache.spark.sql.execution.columnar.encoding.UncompressedEncoder
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.row.SnappyStoreDialect
import org.apache.spark.sql.sources.JdbcExtendedUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSupport}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.{Logging, SparkEnv}

/**
 * Utilities to generate code for exchanging data from Spark layer
 * (Row, InternalRow) to store (Statement, ExecRow).
 * <p>
 * This extends the Spark code generation facilities to allow lazy
 * generation of code string itself only if not found in cache
 * (and using some other lookup key than the code string)
 */
object CodeGeneration extends Logging with SparkSupport {

  override def logInfo(msg: => String): Unit = super.logInfo(msg)

  override def logDebug(msg: => String): Unit = super.logDebug(msg)

  lazy val (codeCacheSize, cacheSize) = {
    val env = SparkEnv.get
    val size = if (env ne null) {
      env.conf.getInt("spark.sql.codegen.cacheSize", 2000)
    } else 2000
    // don't need as big a cache for other caches
    (size, size >>> 2)
  }

  /**
   * A loading cache of generated <code>GeneratedStatement</code>s.
   */
  private[this] lazy val cache = CacheBuilder.newBuilder().maximumSize(cacheSize).build(
    new CacheLoader[ExecuteKey, GeneratedStatement]() {
      override def load(key: ExecuteKey): GeneratedStatement = {
        val start = System.nanoTime()
        val result = compilePreparedUpdate(key.name, key.schema, key.dialect)
        val elapsed = (System.nanoTime() - start).toDouble / 1000000.0
        logInfo(s"PreparedUpdate expression code generated in $elapsed ms")
        result
      }
    })

  /**
   * Similar to Spark's CodeGenerator.compile cache but allows lookup using
   * a key (name+schema) instead of the code string itself to avoid having
   * to create the code string upfront. Code adapted from CodeGenerator.cache
   */
  private[this] lazy val codeCache = CacheBuilder.newBuilder().maximumSize(codeCacheSize).build(
    new CacheLoader[ExecuteKey, AnyRef]() {
      // invoke CodeGenerator.doCompile by reflection to reduce code duplication
      private val doCompileMethod = {
        val allMethods = CodeGenerator.getClass.getDeclaredMethods.toSeq
        val method = allMethods.find(_.getName.endsWith("doCompile"))
            .getOrElse(sys.error(s"Failed to find method 'doCompile' in " +
                s"CodeGenerator (methods=$allMethods)"))
        method.setAccessible(true)
        method
      }

      override def load(key: ExecuteKey): AnyRef = {
        val (code, references) = key.genCode()
        val startTime = System.nanoTime()
        val result = doCompileMethod.invoke(CodeGenerator, code)
        val endTime = System.nanoTime()
        val timeMs = (endTime - startTime).toDouble / 1000000.0
        CodegenMetrics.METRIC_SOURCE_CODE_SIZE.update(code.body.length)
        CodegenMetrics.METRIC_COMPILATION_TIME.update(timeMs.toLong)
        logInfo(s"Local code for ${key.name} generated in $timeMs ms")
        (result.asInstanceOf[GeneratedClass], references)
      }
    })

  private[this] lazy val indexCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build(
    new CacheLoader[ExecuteKey, GeneratedIndexStatement]() {
      override def load(key: ExecuteKey): GeneratedIndexStatement = {
        val start = System.nanoTime()
        val result = compileGeneratedIndexUpdate(key.name, key.schema, key.dialect)
        val elapsed = (System.nanoTime() - start).toDouble / 1000000.0
        logInfo(s"PreparedUpdate expression code generated in $elapsed ms")
        result
      }
    })

  /**
   * A loading cache of generated <code>SerializeComplexType</code>s.
   */
  private[this] lazy val typeCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build(
    new CacheLoader[DataType, SerializeComplexType]() {
      override def load(key: DataType): SerializeComplexType = {
        val start = System.nanoTime()
        val result = compileComplexType(key)
        val elapsed = (System.nanoTime() - start).toDouble / 1000000.0
        logInfo(s"Serializer code generated in $elapsed ms")
        result
      }
    })

  def getColumnSetterFragment(col: Int, dataType: DataType,
      dialect: JdbcDialect, ev: ExprCode, stmt: String, schema: String,
      ctx: CodegenContext): String = {
    val timeUtilsClass = DateTimeUtils.getClass.getName.replace("$", "")
    val encoderClass = classOf[UncompressedEncoder].getName
    val utilsClass = classOf[ClientSharedUtils].getName
    val serArrayClass = classOf[SerializedArray].getName
    val serMapClass = classOf[SerializedMap].getName
    val serRowClass = classOf[SerializedRow].getName
    val nonNullCode = Utils.getSQLDataType(dataType) match {
      case IntegerType => s"$stmt.setInt(${col + 1}, ${ev.value});"
      case LongType => s"$stmt.setLong(${col + 1}, ${ev.value});"
      case DoubleType => s"$stmt.setDouble(${col + 1}, ${ev.value});"
      case FloatType => s"$stmt.setFloat(${col + 1}, ${ev.value});"
      case ShortType => s"$stmt.setInt(${col + 1}, ${ev.value});"
      case ByteType => s"$stmt.setInt(${col + 1}, ${ev.value});"
      case BooleanType => s"$stmt.setBoolean(${col + 1}, ${ev.value});"
      case StringType => s"$stmt.setString(${col + 1}, ${ev.value}.toString());"
      case BinaryType => s"$stmt.setBytes(${col + 1}, ${ev.value});"
      case TimestampType =>
        s"$stmt.setTimestamp(${col + 1}, $timeUtilsClass.toJavaTimestamp(${ev.value}));"
      case DateType =>
        s"$stmt.setDate(${col + 1}, $timeUtilsClass.toJavaDate(${ev.value}));"
      case _: DecimalType =>
        s"$stmt.setBigDecimal(${col + 1}, ${ev.value}.toJavaBigDecimal());"
      case a: ArrayType =>
        val arr = ctx.freshName("arr")
        val encoder = ctx.freshName("encoder")
        val cursor = ctx.freshName("cursor")
        val encoderVar = internals.addClassField(ctx, encoderClass, "encoderObj",
          v => s"$v = new $encoderClass();", forceInline = true)
        s"""
           |final ArrayData $arr = ${ev.value};
           |if ($arr instanceof $serArrayClass) {
           |  $stmt.setBytes(${col + 1}, (($serArrayClass)$arr).toBytes());
           |} else {
           |  final $encoderClass $encoder = $encoderVar;
           |  long $cursor = $encoder.initialize($schema[$col], 1, false);
           |  ${ColumnWriter.genCodeArrayWrite(ctx, a, encoder, cursor,
                arr, "0")}
           |  // finish and set the bytes into the statement
           |  $stmt.setBytes(${col + 1}, $utilsClass.toBytes($encoder.finish($cursor)));
           |}
        """.stripMargin
      case m: MapType =>
        val map = ctx.freshName("mapValue")
        val encoder = ctx.freshName("encoder")
        val cursor = ctx.freshName("cursor")
        val encoderVar = internals.addClassField(ctx, encoderClass, "encoderObj",
          v => s"$v = new $encoderClass();", forceInline = true)
        s"""
           |final MapData $map = ${ev.value};
           |if ($map instanceof $serMapClass) {
           |  $stmt.setBytes(${col + 1}, (($serMapClass)$map).toBytes());
           |} else {
           |  final $encoderClass $encoder = $encoderVar;
           |  long $cursor = $encoder.initialize($schema[$col], 1, false);
           |  ${ColumnWriter.genCodeMapWrite(ctx, m, encoder, cursor, map, "0")}
           |  // finish and set the bytes into the statement
           |  $stmt.setBytes(${col + 1}, $utilsClass.toBytes($encoder.finish($cursor)));
           |}
        """.stripMargin
      case s: StructType =>
        val struct = ctx.freshName("structValue")
        val encoder = ctx.freshName("encoder")
        val cursor = ctx.freshName("cursor")
        val encoderVar = internals.addClassField(ctx, encoderClass, "encoderObj",
          v => s"$v = new $encoderClass();", forceInline = true)
        s"""
           |final InternalRow $struct = ${ev.value};
           |if ($struct instanceof $serRowClass) {
           |  $stmt.setBytes(${col + 1}, (($serRowClass)$struct).toBytes());
           |} else {
           |  final $encoderClass $encoder = $encoderVar;
           |  long $cursor = $encoder.initialize($schema[$col], 1, false);
           |  ${ColumnWriter.genCodeStructWrite(ctx, s, encoder, cursor,
                struct, "0")}
           |  // finish and set the bytes into the statement
           |  $stmt.setBytes(${col + 1}, $utilsClass.toBytes($encoder.finish($cursor)));
           |}
        """.stripMargin
      case _ =>
        s"$stmt.setObject(${col + 1}, ${ev.value});"
    }
    val code = if (ev.code == "") ""
    else {
      val c = s"${ev.code}\n"
      ev.code = ""
      c
    }
    val jdbcType = JdbcExtendedUtils.getJdbcType(NullType, null, dialect).jdbcNullType
    s"""
       |${code}if (${ev.isNull}) {
       |  $stmt.setNull(${col + 1}, $jdbcType);
       |} else {
       |  $nonNullCode
       |}
    """.stripMargin
  }

  private[this] def defaultImports = Array(
      classOf[Platform].getName,
      classOf[InternalRow].getName,
      classOf[UTF8String].getName,
      classOf[Decimal].getName,
      classOf[CalendarInterval].getName,
      classOf[ArrayData].getName,
      classOf[MapData].getName)

  def getRowSetterFragment(schema: Array[StructField],
      dialect: JdbcDialect, row: String, stmt: String,
      schemaTerm: String, ctx: CodegenContext): String = {
    val rowInput = (col: Int) => ExprCode("", s"$row.isNullAt($col)",
      ctx.getValue(row, schema(col).dataType, Integer.toString(col)))
    genStmtSetters(schema, dialect, rowInput, stmt, schemaTerm, ctx)
  }

  def genStmtSetters(schema: Array[StructField], dialect: JdbcDialect,
      rowInput: Int => ExprCode, stmt: String, schemaTerm: String,
      ctx: CodegenContext): String = {
    schema.indices.map { col =>
      getColumnSetterFragment(col, schema(col).dataType, dialect,
        rowInput(col), stmt, schemaTerm, ctx)
    }.mkString("")
  }

  private[this] def compilePreparedUpdate(table: String,
      schema: Array[StructField], dialect: JdbcDialect): GeneratedStatement = {
    val ctx = new CodegenContext
    val stmt = ctx.freshName("stmt")
    val multipleRows = ctx.freshName("multipleRows")
    val rows = ctx.freshName("rows")
    val batchSize = ctx.freshName("batchSize")
    val schemaTerm = ctx.freshName("schema")
    val row = ctx.freshName("row")
    val rowCount = ctx.freshName("rowCount")
    val result = ctx.freshName("result")
    val code = getRowSetterFragment(schema, dialect, row, stmt, schemaTerm, ctx)

    val evaluator = new CompilerFactory().newScriptEvaluator()
    evaluator.setClassName("io.snappydata.execute.GeneratedEvaluation")
    evaluator.setParentClassLoader(getClass.getClassLoader)
    evaluator.setDefaultImports(defaultImports)
    val separator = "\n      "
    val mutableStates = internals.getInlinedClassFields(ctx)
    val varDeclarations = mutableStates._1.map { case (javaType, name) =>
      s"$javaType $name;"
    }
    val initVars = mutableStates._2.map { init =>
      init.replace("this.", "")
    }
    val expression = s"""
      ${varDeclarations.mkString(separator)}
      ${initVars.mkString(separator)}
      int $rowCount = 0;
      int $result = 0;
      while ($rows.hasNext()) {
        InternalRow $row = (InternalRow)$rows.next();
        $code
        $rowCount++;
        if ($multipleRows) {
          $stmt.addBatch();
          if (($rowCount % $batchSize) == 0) {
            $result += $stmt.executeBatch().length;
            $rowCount = 0;
          }
        }
      }
      if ($multipleRows) {
        if ($rowCount > 0) {
          $result += $stmt.executeBatch().length;
        }
      } else {
        $result += $stmt.executeUpdate();
      }
      return $result;
    """

    logDebug(s"DEBUG: For update to table=$table, generated code=$expression")
    evaluator.createFastEvaluator(expression, classOf[GeneratedStatement],
      Array(stmt, multipleRows, rows, batchSize, schemaTerm))
        .asInstanceOf[GeneratedStatement]
  }

  private[this] def compileGeneratedIndexUpdate(table: String,
      schema: Array[StructField], dialect: JdbcDialect): GeneratedIndexStatement = {
    val ctx = new CodegenContext
    val schemaTerm = ctx.freshName("schema")
    val stmt = ctx.freshName("stmt")
    val row = ctx.freshName("row")
    val code = getRowSetterFragment(schema, dialect, row, stmt, schemaTerm, ctx)

    val evaluator = new CompilerFactory().newScriptEvaluator()
    evaluator.setClassName("io.snappydata.execute.GeneratedIndexEvaluation")
    evaluator.setParentClassLoader(getClass.getClassLoader)
    evaluator.setDefaultImports(defaultImports)
    val separator = "\n      "
    val mutableStates = internals.getInlinedClassFields(ctx)
    val varDeclarations = mutableStates._1.map { case (javaType, name) =>
      s"$javaType $name;"
    }
    val initVars = mutableStates._2.map { init =>
      init.replace("this.", "")
    }
    val expression = s"""
      ${varDeclarations.mkString(separator)}
      ${initVars.mkString(separator)}
        $code
        stmt.addBatch();
      return 1;"""

    logDebug(s"DEBUG: For update to index=$table, generated code=$expression")
    evaluator.createFastEvaluator(expression, classOf[GeneratedIndexStatement],
      Array(schemaTerm, stmt, row)).asInstanceOf[GeneratedIndexStatement]
  }

  private[this] def compileComplexType(
      dataType: DataType): SerializeComplexType = {
    val ctx = new CodegenContext
    val inputVar = ctx.freshName("value")
    val encoderVar = ctx.freshName("encoder")
    val fieldVar = ctx.freshName("field")
    val dosVar = ctx.freshName("dos")
    val utilsClass = classOf[ClientSharedUtils].getName
    val serArrayClass = classOf[SerializedArray].getName
    val serMapClass = classOf[SerializedMap].getName
    val serRowClass = classOf[SerializedRow].getName
    val typeConversion = Utils.getSQLDataType(dataType) match {
      case a: ArrayType =>
        val arr = ctx.freshName("arr")
        val cursor = ctx.freshName("cursor")
        s"""
           |final ArrayData $arr = (ArrayData)$inputVar;
           |if ($arr instanceof $serArrayClass) {
           |  return (($serArrayClass)$arr).toBytes();
           |}
           |long $cursor = $encoderVar.initialize($fieldVar, 1, false);
           |${ColumnWriter.genCodeArrayWrite(ctx, a, encoderVar, cursor,
              arr, "0")}
           |if ($dosVar != null) {
           |  final byte[] b = $utilsClass.toBytes($encoderVar.finish($cursor));
           |  InternalDataSerializer.writeByteArray(b, b.length, $dosVar);
           |  return null;
           |} else {
           |  return $utilsClass.toBytes($encoderVar.finish($cursor));
           |}
        """.stripMargin
      case m: MapType =>
        val map = ctx.freshName("mapValue")
        val cursor = ctx.freshName("cursor")
        s"""
           |final MapData $map = (MapData)$inputVar;
           |if ($map instanceof $serMapClass) {
           |  return (($serMapClass)$map).toBytes();
           |}
           |long $cursor = $encoderVar.initialize($fieldVar, 1, false);
           |${ColumnWriter.genCodeMapWrite(ctx, m, encoderVar, cursor,
              map, "0")}
           |if ($dosVar != null) {
           |  final byte[] b = $utilsClass.toBytes($encoderVar.finish($cursor));
           |  InternalDataSerializer.writeByteArray(b, b.length, $dosVar);
           |  return null;
           |} else {
           |  return $utilsClass.toBytes($encoderVar.finish($cursor));
           |}
        """.stripMargin
      case s: StructType =>
        val struct = ctx.freshName("structValue")
        val cursor = ctx.freshName("cursor")
        s"""
           |final InternalRow $struct = (InternalRow)$inputVar;
           |if ($struct instanceof $serRowClass) {
           |  return (($serRowClass)$struct).toBytes();
           |}
           |long $cursor = $encoderVar.initialize($fieldVar, 1, false);
           |${ColumnWriter.genCodeStructWrite(ctx, s, encoderVar, cursor,
              struct, "0")}
           |if ($dosVar != null) {
           |  final byte[] b = $utilsClass.toBytes($encoderVar.finish($cursor));
           |  InternalDataSerializer.writeByteArray(b, b.length, $dosVar);
           |  return null;
           |} else {
           |  return $utilsClass.toBytes($encoderVar.finish($cursor));
           |}
        """.stripMargin
      case _ => throw Utils.analysisException(
        s"complex type conversion: unexpected type $dataType")
    }

    val evaluator = new CompilerFactory().newScriptEvaluator()
    evaluator.setClassName("io.snappydata.execute.GeneratedSerialization")
    evaluator.setParentClassLoader(getClass.getClassLoader)
    evaluator.setDefaultImports(Array(classOf[Platform].getName,
      classOf[InternalRow].getName,
      classOf[UTF8String].getName,
      classOf[Decimal].getName,
      classOf[CalendarInterval].getName,
      classOf[ArrayData].getName,
      classOf[MapData].getName,
      classOf[InternalDataSerializer].getName))
    val separator = "\n      "
    val mutableStates = internals.getInlinedClassFields(ctx)
    val varDeclarations = mutableStates._1.map { case (javaType, name) =>
      s"$javaType $name;"
    }
    val initVars = mutableStates._2.map { init =>
      init.replace("this.", "")
    }
    val expression = s"""
      ${varDeclarations.mkString(separator)}
      ${initVars.mkString(separator)}
      $typeConversion"""

    logDebug(s"DEBUG: For complex type=$dataType, generated code=$expression")
    evaluator.createFastEvaluator(expression, classOf[SerializeComplexType],
      Array(inputVar, encoderVar, fieldVar, dosVar))
        .asInstanceOf[SerializeComplexType]
  }

  private[this] def executeUpdate(name: String, stmt: PreparedStatement,
      rows: java.util.Iterator[InternalRow], multipleRows: Boolean,
      batchSize: Int, schema: Array[StructField], dialect: JdbcDialect): Int = {
    val result = cache.get(new ExecuteKey(name, schema, dialect))
    result.executeStatement(stmt, multipleRows, rows, batchSize, schema)
  }

  def executeUpdate(name: String, stmt: PreparedStatement, rows: Seq[Row],
      multipleRows: Boolean, batchSize: Int, schema: Array[StructField],
      dialect: JdbcDialect): Int = {
    val iterator: java.util.Iterator[InternalRow] = new java.util.Iterator[InternalRow] {

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
      schema, dialect)
  }

  def executeUpdate(name: String, stmt: PreparedStatement, row: Row,
      schema: Array[StructField], dialect: JdbcDialect): Int = {
    val encoder = RowEncoder(StructType(schema))
    executeUpdate(name, stmt, Collections.singleton(encoder.toRow(row))
        .iterator(), multipleRows = false, 0, schema, dialect)
  }

  def compileCode(name: String, schema: Array[StructField],
      genCode: () => (CodeAndComment, Array[Any])): (GeneratedClass, Array[Any]) = {
    codeCache.get(new ExecuteKey(name, schema, SnappyStoreDialect,
      forIndex = false, genCode = genCode)).asInstanceOf[(GeneratedClass, Array[Any])]
  }

  def getComplexTypeSerializer(dataType: DataType): SerializeComplexType =
    typeCache.get(dataType)

  def getGeneratedIndexStatement(name: String, schema: StructType,
      dialect: JdbcDialect): (PreparedStatement, InternalRow) => Int = {
    val result = indexCache.get(new ExecuteKey(name, schema.fields,
      dialect, forIndex = true))
    result.addBatch(schema.fields)
  }

  def removeCache(name: String): Unit = {
    cache.invalidate(new ExecuteKey(name, null, null))
    indexCache.invalidate(new ExecuteKey(name, null, null, true))
  }

  def clearAllCache(skipTypeCache: Boolean = true): Unit = {
    cache.invalidateAll()
    codeCache.invalidateAll()
    indexCache.invalidateAll()
    if (!skipTypeCache) {
      typeCache.invalidateAll()
    }
  }
}

trait GeneratedStatement {

  @throws[java.sql.SQLException]
  def executeStatement(stmt: PreparedStatement, multipleRows: Boolean,
      rows: java.util.Iterator[InternalRow], batchSize: Int,
      schema: Array[StructField]): Int
}

trait SerializeComplexType {

  @throws[java.io.IOException]
  def serialize(value: Any, encoder: UncompressedEncoder,
      field: StructField, dos: GfxdHeapDataOutputStream): Array[Byte]
}

trait GeneratedIndexStatement {

  @throws[java.sql.SQLException]
  def addBatch(schema: Array[StructField])
      (stmt: PreparedStatement, row: InternalRow): Int
}


final class ExecuteKey(val name: String,
    val schema: Array[StructField], val dialect: JdbcDialect,
    val forIndex: Boolean = false, val genCode: () => (CodeAndComment, Array[Any]) = null) {

  override lazy val hashCode: Int = if ((schema ne null) && !forIndex) {
    MurmurHash3.listHash(name :: schema.toList, MurmurHash3.seqSeed)
  } else name.hashCode

  override def equals(other: Any): Boolean = other match {
    case o: ExecuteKey => if ((schema ne null) && (o.schema ne null) && !forIndex) {
      schema.length == o.schema.length && name == o.name && java.util.Arrays.equals(
        schema.asInstanceOf[Array[AnyRef]], o.schema.asInstanceOf[Array[AnyRef]])
    } else {
      name == o.name
    }
    case s: String => name == s
    case _ => false
  }
}
