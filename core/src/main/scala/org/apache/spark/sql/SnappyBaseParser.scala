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
package org.apache.spark.sql

import scala.collection.mutable

import io.snappydata.Constant
import org.parboiled2._

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SnappyParserConsts => Consts}

/**
 * Base parsing facilities for all SnappyData SQL parsers.
 */
abstract class SnappyBaseParser(session: SnappySession) extends Parser {

  val caseSensitive = session.sessionState.conf.caseSensitiveAnalysis

  private[sql] final val queryHints = new mutable.HashMap[String, String]

  protected def reset(): Unit = queryHints.clear()

  protected final def commentBody: Rule0 = rule {
    "*/" | ANY ~ commentBody
  }

  protected final def commentBodyOrHint: Rule0 = rule {
    '+' ~ (Consts.whitespace.* ~ capture(CharPredicate.Alpha ~
        Consts.identifier.*) ~ Consts.whitespace.* ~
        '(' ~ capture(noneOf(Consts.hintValueEnd).*) ~ ')' ~>
        ((k: String, v: String) => queryHints += (k -> v.trim): Unit)). + ~
        commentBody |
    commentBody
  }

  protected final def lineCommentOrHint: Rule0 = rule {
    '+' ~ (Consts.space.* ~ capture(CharPredicate.Alpha ~
        Consts.identifier.*) ~ Consts.space.* ~
        '(' ~ capture(noneOf(Consts.lineHintEnd).*) ~ ')' ~>
        ((k: String, v: String) => queryHints += (k -> v.trim): Unit)). + ~
        noneOf(Consts.lineCommentEnd).* |
    noneOf(Consts.lineCommentEnd).*
  }

  /** The recognized whitespace characters and comments. */
  protected final def ws: Rule0 = rule {
    quiet(
      Consts.whitespace |
      '-' ~ '-' ~ lineCommentOrHint |
      '/' ~ '*' ~ (commentBodyOrHint | fail("unclosed comment"))
    ).*
  }

  /** All recognized delimiters including whitespace. */
  final def delimiter: Rule0 = rule {
    quiet(&(Consts.delimiters)) ~ ws | EOI
  }

  protected final def commaSep: Rule0 = rule {
    ',' ~ ws
  }

  protected final def digits: Rule1[String] = rule {
    capture(CharPredicate.Digit. +) ~ ws
  }

  protected final def integral: Rule1[String] = rule {
    capture(Consts.plusOrMinus.? ~ CharPredicate.Digit. +) ~ ws
  }

  protected final def scientificNotation: Rule0 = rule {
    Consts.exponent ~ Consts.plusOrMinus.? ~ CharPredicate.Digit. +
  }

  protected final def stringLiteral: Rule1[String] = rule {
    '\'' ~ capture((noneOf("'") | "''").*) ~ '\'' ~ ws ~> ((s: String) =>
      if (s.indexOf("''") >= 0) s.replace("''", "'") else s)
  }

  final def keyword(k: Keyword): Rule0 = rule {
    atomic(ignoreCase(k.lower)) ~ delimiter
  }

  /**
   * Used for DataTypes. Not reserved and otherwise identical to "keyword"
   * apart from the name so as to appear properly in error messages related
   * to incorrect DataType definition.
   */
  protected final def newDataType(t: Keyword): Rule0 = rule {
    atomic(ignoreCase(t.lower)) ~ delimiter
  }

  final def sql: Rule1[LogicalPlan] = rule {
    ws ~ start ~ (';' ~ ws).* ~ EOI
  }

  protected def start: Rule1[LogicalPlan]

  protected final def identifier: Rule1[String] = rule {
    atomic(capture(CharPredicate.Alpha ~ Consts.identifier.*)) ~
        delimiter ~> { (s: String) =>
      val ucase = Utils.toUpperCase(s)
      test(!Consts.reservedKeywords.contains(ucase)) ~
          push(if (caseSensitive) s else ucase)
    } |
    quotedIdentifier
  }

  protected final def quotedIdentifier: Rule1[String] = rule {
    atomic('"' ~ capture((noneOf("\"") | "\"\""). +) ~ '"') ~
        ws ~> { (s: String) =>
      val id = if (s.indexOf("\"\"") >= 0) s.replace("\"\"", "\"") else s
      if (caseSensitive) id else Utils.toUpperCase(id)
    } |
    atomic('`' ~ capture((noneOf("`") | "``"). +) ~ '`') ~ ws ~> { (s: String) =>
      val id = if (s.indexOf("``") >= 0) s.replace("``", "`") else s
      if (caseSensitive) id else Utils.toUpperCase(id)
    }
  }

  /**
   * A strictIdentifier is more restricted than an identifier in that neither
   * any of the SQL reserved keywords nor non-reserved keywords will be
   * interpreted as a strictIdentifier.
   */
  protected final def strictIdentifier: Rule1[String] = rule {
    atomic(capture(CharPredicate.Alpha ~ Consts.identifier.*)) ~
        delimiter ~> { (s: String) =>
      val ucase = Utils.toUpperCase(s)
      test(!Consts.allKeywords.contains(ucase)) ~
          push(if (caseSensitive) s else ucase)
    } |
    quotedIdentifier
  }

  // DataTypes
  // It is not useful to see long list of "expected ARRAY or BIGINT or ..."
  // for parse errors, so not making these separate rules and instead naming
  // the common rule as "datatype" which is otherwise identical to "keyword"
  final def ARRAY: Rule0 = newDataType(Consts.ARRAY)
  final def BIGINT: Rule0 = newDataType(Consts.BIGINT)
  final def BINARY: Rule0 = newDataType(Consts.BINARY)
  final def BLOB: Rule0 = newDataType(Consts.BLOB)
  final def BOOLEAN: Rule0 = newDataType(Consts.BOOLEAN)
  final def BYTE: Rule0 = newDataType(Consts.BYTE)
  final def CHAR: Rule0 = newDataType(Consts.CHAR)
  final def CLOB: Rule0 = newDataType(Consts.CLOB)
  final def DATE: Rule0 = newDataType(Consts.DATE)
  final def DECIMAL: Rule0 = newDataType(Consts.DECIMAL)
  final def DOUBLE: Rule0 = newDataType(Consts.DOUBLE)
  final def FLOAT: Rule0 = newDataType(Consts.FLOAT)
  final def INT: Rule0 = newDataType(Consts.INT)
  final def INTEGER: Rule0 = newDataType(Consts.INTEGER)
  final def LONG: Rule0 = newDataType(Consts.LONG)
  final def MAP: Rule0 = newDataType(Consts.MAP)
  final def NUMERIC: Rule0 = newDataType(Consts.NUMERIC)
  final def REAL: Rule0 = newDataType(Consts.REAL)
  final def SHORT: Rule0 = newDataType(Consts.SHORT)
  final def SMALLINT: Rule0 = newDataType(Consts.SMALLINT)
  final def STRING: Rule0 = newDataType(Consts.STRING)
  final def STRUCT: Rule0 = newDataType(Consts.STRUCT)
  final def TIMESTAMP: Rule0 = newDataType(Consts.TIMESTAMP)
  final def TINYINT: Rule0 = newDataType(Consts.TINYINT)
  final def VARBINARY: Rule0 = newDataType(Consts.VARBINARY)
  final def VARCHAR: Rule0 = newDataType(Consts.VARCHAR)

  protected final def fixedDecimalType: Rule1[DataType] = rule {
    (DECIMAL | NUMERIC) ~ '(' ~ ws ~ digits ~ commaSep ~ digits ~ ')' ~ ws ~>
        ((precision: String, scale: String) =>
          DecimalType(precision.toInt, scale.toInt))
  }

  protected final def primitiveType: Rule1[DataType] = rule {
    STRING ~> (() => StringType) |
    INTEGER ~> (() => IntegerType) |
    INT ~> (() => IntegerType) |
    BIGINT ~> (() => LongType) |
    LONG ~> (() => LongType) |
    DOUBLE ~> (() => DoubleType) |
    fixedDecimalType |
    DECIMAL ~> (() => DecimalType.SYSTEM_DEFAULT) |
    NUMERIC ~> (() => DecimalType.SYSTEM_DEFAULT) |
    DATE ~> (() => DateType) |
    TIMESTAMP ~> (() => TimestampType) |
    FLOAT ~> (() => FloatType) |
    REAL ~> (() => FloatType) |
    BOOLEAN ~> (() => BooleanType) |
    CLOB ~> (() => StringType) |
    BLOB ~> (() => BinaryType) |
    BINARY ~> (() => BinaryType) |
    VARBINARY ~> (() => BinaryType) |
    SMALLINT ~> (() => ShortType) |
    SHORT ~> (() => ShortType) |
    TINYINT ~> (() => ByteType) |
    BYTE ~> (() => ByteType)
  }

  protected final def charType: Rule1[DataType] = rule {
    VARCHAR ~ '(' ~ ws ~ digits ~ ')' ~ ws ~> ((d: String) => StringType) |
    CHAR ~ '(' ~ ws ~ digits ~ ')' ~ ws ~> ((d: String) => StringType)
  }

  final def dataType: Rule1[DataType] = rule {
    charType | primitiveType | arrayType | mapType | structType
  }

  protected final def arrayType: Rule1[DataType] = rule {
    ARRAY ~ '<' ~ ws ~ dataType ~ '>' ~ ws ~>
        ((t: DataType) => ArrayType(t))
  }

  protected final def mapType: Rule1[DataType] = rule {
    MAP ~ '<' ~ ws ~ dataType ~ commaSep ~ dataType ~ '>' ~ ws ~>
        ((t1: DataType, t2: DataType) => MapType(t1, t2))
  }

  protected final def structField: Rule1[StructField] = rule {
    identifier ~ ':' ~ ws ~ dataType ~> ((name: String, t: DataType) =>
      StructField(name, t, nullable = true))
  }

  protected final def structType: Rule1[DataType] = rule {
    STRUCT ~ '<' ~ ws ~ (structField * commaSep) ~ '>' ~ ws ~>
        ((f: Any) => StructType(f.asInstanceOf[Seq[StructField]].toArray))
  }

  protected final def columnCharType: Rule1[DataType] = rule {
    VARCHAR ~ '(' ~ ws ~ digits ~ ')' ~ ws ~> ((d: String) =>
      CharType(d.toInt, baseType = "VARCHAR")) |
    CHAR ~ '(' ~ ws ~ digits ~ ')' ~ ws ~> ((d: String) =>
      CharType(d.toInt, baseType = "CHAR")) |
    STRING ~> (() => CharType(Constant.MAX_VARCHAR_SIZE, baseType = "STRING"))
  }

  final def columnDataType: Rule1[DataType] = rule {
    columnCharType | primitiveType | arrayType | mapType | structType
  }

  final def tableIdentifier: Rule1[TableIdentifier] = rule {
    // case-sensitivity already taken care of properly by "identifier"
    (identifier ~ '.' ~ ws).? ~ identifier ~> ((schema: Any, table: String) =>
      TableIdentifier(table, schema.asInstanceOf[Option[String]]))
  }

  final def functionIdentifier: Rule1[FunctionIdentifier] = rule {
    // case-sensitivity already taken care of properly by "identifier"
    (identifier ~ '.' ~ ws).? ~ identifier ~> ((schema: Any, name: String) =>
      FunctionIdentifier(name, database = schema.asInstanceOf[Option[String]]))
  }
}

final class Keyword private[sql] (s: String) {
  val lower = Utils.toLowerCase(s)
  val upper = Utils.toUpperCase(s)
}

object SnappyParserConsts {
  final val space: CharPredicate = CharPredicate(' ', '\t')
  final val whitespace: CharPredicate = CharPredicate(
    ' ', '\t', '\n', '\r', '\f')
  final val delimiters: CharPredicate = whitespace ++ CharPredicate('@', '*',
    '+', '-', '<', '=', '!', '>', '/', '(', ')', ',', ';', '%', '{', '}', ':',
    '[', ']', '.', '&', '|', '^', '~', '#')
  final val lineCommentEnd = "\n\r\f" + EOI
  final val lineHintEnd = ")\n\r\f" + EOI
  final val hintValueEnd = ")*" + EOI
  final val identifier: CharPredicate = CharPredicate.AlphaNum ++
      CharPredicate('_')
  final val plusOrMinus: CharPredicate = CharPredicate('+', '-')
  final val arithmeticOperator = CharPredicate('*', '/', '%', '&', '|', '^')
  final val exponent: CharPredicate = CharPredicate('e', 'E')
  final val numeric: CharPredicate = CharPredicate.Digit ++
      CharPredicate('.') ++ exponent
  final val numericSuffix: CharPredicate = CharPredicate('D', 'L')
  final val plural: CharPredicate = CharPredicate('s', 'S')

  final val trueFn: () => Boolean = () => true

  final val falseFn: () => Boolean = () => false

  final val reservedKeywords: mutable.Set[String] = mutable.Set[String]()

  final val allKeywords: mutable.Set[String] = mutable.Set[String]()

  /**
   * Registering a Keyword with this method marks it a reserved keyword,
   * i.e. it is interpreted as a keyword wherever it may appear and is never
   * interpreted as an identifier (except if quoted).
   * <p>
   * Use this only for SQL reserved keywords.
   */
  private[sql] def reservedKeyword(s: String): Keyword = {
    val k = new Keyword(s)
    reservedKeywords += k.upper
    allKeywords += k.upper
    k
  }

  /**
   * Registering a Keyword with this method marks it a non-reserved keyword.
   * These can be interpreted as identifiers as per the parsing rules,
   * but never interpreted as a "strictIdentifier". In other words, use
   * "strictIdentifier" in parsing rules where there can be an ambiguity
   * between an identifier and a non-reserved keyword.
   * <p>
   * Use this for all SQL keywords used by grammar that are not reserved.
   */
  private[sql] def nonReservedKeyword(s: String): Keyword = {
    val k = new Keyword(s)
    allKeywords += k.upper
    k
  }

  // reserved keywords
  final val ALL = reservedKeyword("all")
  final val AND = reservedKeyword("and")
  final val AS = reservedKeyword("as")
  final val ASC = reservedKeyword("asc")
  final val BETWEEN = reservedKeyword("between")
  final val BY = reservedKeyword("by")
  final val CASE = reservedKeyword("case")
  final val CAST = reservedKeyword("cast")
  final val CREATE = reservedKeyword("create")
  final val CURRENT = reservedKeyword("current")
  final val CURRENT_DATE = reservedKeyword("current_date")
  final val CURRENT_TIMESTAMP = reservedKeyword("current_timestamp")
  final val DELETE = reservedKeyword("delete")
  final val DESC = reservedKeyword("desc")
  final val DISTINCT = reservedKeyword("distinct")
  final val DROP = reservedKeyword("drop")
  final val ELSE = reservedKeyword("else")
  final val EXCEPT = reservedKeyword("except")
  final val EXISTS = reservedKeyword("exists")
  final val FALSE = reservedKeyword("false")
  final val FROM = reservedKeyword("from")
  final val GROUP = reservedKeyword("group")
  final val HAVING = reservedKeyword("having")
  final val IN = reservedKeyword("in")
  final val INNER = reservedKeyword("inner")
  final val INSERT = reservedKeyword("insert")
  final val INTERSECT = reservedKeyword("intersect")
  final val INTO = reservedKeyword("into")
  final val IS = reservedKeyword("is")
  final val JOIN = reservedKeyword("join")
  final val LEFT = reservedKeyword("left")
  final val LIKE = reservedKeyword("like")
  final val NOT = reservedKeyword("not")
  final val NULL = reservedKeyword("null")
  final val ON = reservedKeyword("on")
  final val OR = reservedKeyword("or")
  final val ORDER = reservedKeyword("order")
  final val OUTER = reservedKeyword("outer")
  final val RIGHT = reservedKeyword("right")
  final val SCHEMA = reservedKeyword("schema")
  final val SELECT = reservedKeyword("select")
  final val SET = reservedKeyword("set")
  final val TABLE = reservedKeyword("table")
  final val THEN = reservedKeyword("then")
  final val TO = reservedKeyword("to")
  final val TRUE = reservedKeyword("true")
  final val UNION = reservedKeyword("union")
  final val UNIQUE = reservedKeyword("unique")
  final val UPDATE = reservedKeyword("update")
  final val WHEN = reservedKeyword("when")
  final val WHERE = reservedKeyword("where")
  final val WITH = reservedKeyword("with")
  final val FUNCTIONS = reservedKeyword("functions")
  final val FUNCTION = reservedKeyword("function")

  // marked as internal keywords to prevent use in SQL
  final val HIVE_METASTORE = reservedKeyword(
    SnappyStoreHiveCatalog.HIVE_METASTORE)

 final val SAMPLER_WEIGHTAGE = nonReservedKeyword(Utils.WEIGHTAGE_COLUMN_NAME)

  // non-reserved keywords
  final val ANTI = nonReservedKeyword("anti")
  final val CACHE = nonReservedKeyword("cache")
  final val CLEAR = nonReservedKeyword("clear")
  final val CLUSTER = nonReservedKeyword("cluster")
  final val COMMENT = nonReservedKeyword("comment")
  final val DESCRIBE = nonReservedKeyword("describe")
  final val DISTRIBUTE = nonReservedKeyword("distribute")
  final val END = nonReservedKeyword("end")
  final val EXTENDED = nonReservedKeyword("extended")
  final val EXTERNAL = nonReservedKeyword("external")
  final val FULL = nonReservedKeyword("full")
  final val GLOBAL = nonReservedKeyword("global")
  final val HASH = nonReservedKeyword("hash")
  final val IF = nonReservedKeyword("if")
  final val INDEX = nonReservedKeyword("index")
  final val INIT = nonReservedKeyword("init")
  final val INTERVAL = nonReservedKeyword("interval")
  final val LAZY = nonReservedKeyword("lazy")
  final val LIMIT = nonReservedKeyword("limit")
  final val NATURAL = nonReservedKeyword("natural")
  final val OPTIONS = nonReservedKeyword("options")
  final val OVERWRITE = nonReservedKeyword("overwrite")
  final val PARTITION = nonReservedKeyword("partition")
  final val PUT = nonReservedKeyword("put")
  final val REFRESH = nonReservedKeyword("refresh")
  final val REGEXP = nonReservedKeyword("regexp")
  final val RLIKE = nonReservedKeyword("rlike")
  final val SEMI = nonReservedKeyword("semi")
  final val SHOW = nonReservedKeyword("show")
  final val SORT = nonReservedKeyword("sort")
  final val START = nonReservedKeyword("start")
  final val STOP = nonReservedKeyword("stop")
  final val STREAM = nonReservedKeyword("stream")
  final val STREAMING = nonReservedKeyword("streaming")
  final val TABLES = nonReservedKeyword("tables")
  final val TEMPORARY = nonReservedKeyword("temporary")
  final val TRUNCATE = nonReservedKeyword("truncate")
  final val UNCACHE = nonReservedKeyword("uncache")
  final val USING = nonReservedKeyword("using")
  final val RETURNS = nonReservedKeyword("returns")

  // Window analytical functions are non-reserved
  final val DURATION = nonReservedKeyword("duration")
  final val FOLLOWING = nonReservedKeyword("following")
  final val OVER = nonReservedKeyword("over")
  final val PRECEDING = nonReservedKeyword("preceding")
  final val RANGE = nonReservedKeyword("range")
  final val ROW = nonReservedKeyword("row")
  final val ROWS = nonReservedKeyword("rows")
  final val SLIDE = nonReservedKeyword("slide")
  final val UNBOUNDED = nonReservedKeyword("unbounded")
  final val WINDOW = nonReservedKeyword("window")

  // interval units are not reserved
  final val DAY = nonReservedKeyword("day")
  final val HOUR = nonReservedKeyword("hour")
  final val MICROSECOND = nonReservedKeyword("microsecond")
  final val MILLISECOND = nonReservedKeyword("millisecond")
  final val MINUTE = nonReservedKeyword("minute")
  final val MONTH = nonReservedKeyword("month")
  final val SECOND = nonReservedKeyword("seconds")
  final val WEEK = nonReservedKeyword("week")
  final val YEAR = nonReservedKeyword("year")

  // cube, rollup, grouping sets are not reserved
  final val CUBE = nonReservedKeyword("cube")
  final val ROLLUP = nonReservedKeyword("rollup")
  final val GROUPING = nonReservedKeyword("grouping")
  final val SETS = nonReservedKeyword("sets")

  // datatypes are not reserved
  final val ARRAY = nonReservedKeyword("array")
  final val BIGINT = nonReservedKeyword("bigint")
  final val BINARY = nonReservedKeyword("binary")
  final val BLOB = nonReservedKeyword("blob")
  final val BOOLEAN = nonReservedKeyword("boolean")
  final val BYTE = nonReservedKeyword("byte")
  final val CHAR = nonReservedKeyword("char")
  final val CLOB = nonReservedKeyword("clob")
  final val DATE = nonReservedKeyword("date")
  final val DECIMAL = nonReservedKeyword("decimal")
  final val DOUBLE = nonReservedKeyword("double")
  final val FLOAT = nonReservedKeyword("float")
  final val INT = nonReservedKeyword("int")
  final val INTEGER = nonReservedKeyword("integer")
  final val LONG = nonReservedKeyword("long")
  final val MAP = nonReservedKeyword("map")
  final val NUMERIC = nonReservedKeyword("numeric")
  final val REAL = nonReservedKeyword("real")
  final val SHORT = nonReservedKeyword("short")
  final val SMALLINT = nonReservedKeyword("smallint")
  final val STRING = nonReservedKeyword("string")
  final val STRUCT = nonReservedKeyword("struct")
  final val TIMESTAMP = nonReservedKeyword("timestamp")
  final val TINYINT = nonReservedKeyword("tinyint")
  final val VARBINARY = nonReservedKeyword("varbinary")
  final val VARCHAR = nonReservedKeyword("varchar")

  // for AQP
  final val ERROR = nonReservedKeyword("error")
  final val ESTIMATE = nonReservedKeyword("estimate")
  final val CONFIDENCE = nonReservedKeyword("confidence")
  final val BEHAVIOR = nonReservedKeyword("behavior")
  final val SAMPLE = nonReservedKeyword("sample")
  final val TOPK = nonReservedKeyword("topk")
}
