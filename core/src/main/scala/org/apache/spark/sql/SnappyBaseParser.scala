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

import org.parboiled2._

import org.apache.spark.sql.SnappyParserConsts.plusOrMinus
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.types._

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
    '+' ~ (SnappyParserConsts.whitespace.* ~ capture(CharPredicate.Alpha ~
        SnappyParserConsts.identifier.*) ~ SnappyParserConsts.whitespace.* ~
        '(' ~ capture(noneOf(SnappyParserConsts.hintValueEnd).*) ~ ')' ~>
        ((k: String, v: String) => queryHints += (k -> v.trim): Unit)). + ~
        commentBody |
    commentBody
  }

  protected final def lineCommentOrHint: Rule0 = rule {
    '+' ~ (SnappyParserConsts.space.* ~ capture(CharPredicate.Alpha ~
        SnappyParserConsts.identifier.*) ~ SnappyParserConsts.space.* ~
        '(' ~ capture(noneOf(SnappyParserConsts.lineHintEnd).*) ~ ')' ~>
        ((k: String, v: String) => queryHints += (k -> v.trim): Unit)). + ~
        noneOf(SnappyParserConsts.lineCommentEnd).* |
    noneOf(SnappyParserConsts.lineCommentEnd).*
  }

  /** The recognized whitespace characters and comments. */
  protected final def ws: Rule0 = rule {
    quiet(
      SnappyParserConsts.whitespace |
      '-' ~ '-' ~ lineCommentOrHint |
      '/' ~ '*' ~ (commentBodyOrHint | fail("unclosed comment"))
    ).*
  }

  /** All recognized delimiters including whitespace. */
  final def delimiter: Rule0 = rule {
    quiet(&(SnappyParserConsts.delimiters)) ~ ws | EOI
  }

  protected final def digits: Rule1[String] = rule {
    capture(CharPredicate.Digit. +) ~ ws
  }

  protected final def integral: Rule1[String] = rule {
    capture(plusOrMinus.? ~ CharPredicate.Digit. +) ~ ws
  }

  protected final def scientificNotation: Rule0 = rule {
    SnappyParserConsts.exponent ~ plusOrMinus.? ~ CharPredicate.Digit. +
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
    atomic(capture(CharPredicate.Alpha ~ SnappyParserConsts.identifier.*)) ~
        delimiter ~> { (s: String) =>
      val ucase = Utils.toUpperCase(s)
      test(!SnappyParserConsts.keywords.contains(ucase)) ~
          push(if (caseSensitive) s else ucase)
    } |
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

  // DataTypes
  // It is not useful to see long list of "expected ARRAY or BIGINT or ..."
  // for parse errors, so not making these separate rules and instead naming
  // the common rule as "datatype" which is otherwise identical to "keyword"
  final def ARRAY: Rule0 = newDataType(SnappyParserConsts.ARRAY)
  final def BIGINT: Rule0 = newDataType(SnappyParserConsts.BIGINT)
  final def BINARY: Rule0 = newDataType(SnappyParserConsts.BINARY)
  final def BLOB: Rule0 = newDataType(SnappyParserConsts.BLOB)
  final def BOOLEAN: Rule0 = newDataType(SnappyParserConsts.BOOLEAN)
  final def BYTE: Rule0 = newDataType(SnappyParserConsts.BYTE)
  final def CHAR: Rule0 = newDataType(SnappyParserConsts.CHAR)
  final def CLOB: Rule0 = newDataType(SnappyParserConsts.CLOB)
  final def DATE: Rule0 = newDataType(SnappyParserConsts.DATE)
  final def DECIMAL: Rule0 = newDataType(SnappyParserConsts.DECIMAL)
  final def DOUBLE: Rule0 = newDataType(SnappyParserConsts.DOUBLE)
  final def FLOAT: Rule0 = newDataType(SnappyParserConsts.FLOAT)
  final def INT: Rule0 = newDataType(SnappyParserConsts.INT)
  final def INTEGER: Rule0 = newDataType(SnappyParserConsts.INTEGER)
  final def LONG: Rule0 = newDataType(SnappyParserConsts.LONG)
  final def MAP: Rule0 = newDataType(SnappyParserConsts.MAP)
  final def NUMERIC: Rule0 = newDataType(SnappyParserConsts.NUMERIC)
  final def REAL: Rule0 = newDataType(SnappyParserConsts.REAL)
  final def SHORT: Rule0 = newDataType(SnappyParserConsts.SHORT)
  final def SMALLINT: Rule0 = newDataType(SnappyParserConsts.SMALLINT)
  final def STRING: Rule0 = newDataType(SnappyParserConsts.STRING)
  final def STRUCT: Rule0 = newDataType(SnappyParserConsts.STRUCT)
  final def TIMESTAMP: Rule0 = newDataType(SnappyParserConsts.TIMESTAMP)
  final def TINYINT: Rule0 = newDataType(SnappyParserConsts.TINYINT)
  final def VARBINARY: Rule0 = newDataType(SnappyParserConsts.VARBINARY)
  final def VARCHAR: Rule0 = newDataType(SnappyParserConsts.VARCHAR)

  protected final def fixedDecimalType: Rule1[DataType] = rule {
    (DECIMAL | NUMERIC) ~ '(' ~ ws ~ digits ~ ',' ~ ws ~ digits ~ ')' ~ ws ~>
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
    MAP ~ '<' ~ ws ~ dataType ~ ',' ~ ws ~ dataType ~ '>' ~ ws ~>
        ((t1: DataType, t2: DataType) => MapType(t1, t2))
  }

  protected final def structField: Rule1[StructField] = rule {
    identifier ~ ':' ~ ws ~ dataType ~> ((name: String, t: DataType) =>
      StructField(name, t, nullable = true))
  }

  protected final def structType: Rule1[DataType] = rule {
    STRUCT ~ '<' ~ ws ~ (structField * (',' ~ ws)) ~ '>' ~ ws ~>
        ((f: Any) => StructType(f.asInstanceOf[Seq[StructField]].toArray))
  }

  protected final def columnCharType: Rule1[DataType] = rule {
    VARCHAR ~ '(' ~ ws ~ digits ~ ')' ~ ws ~> ((d: String) =>
      CharType(d.toInt, isFixedLength = false)) |
    CHAR ~ '(' ~ ws ~ digits ~ ')' ~ ws ~> ((d: String) =>
      CharType(d.toInt, isFixedLength = true))
  }

  final def columnDataType: Rule1[DataType] = rule {
    columnCharType | primitiveType | arrayType | mapType | structType
  }

  final def tableIdentifier: Rule1[TableIdentifier] = rule {
    // case-sensitivity already taken care of properly by "identifier"
    (identifier ~ '.' ~ ws).? ~ identifier ~> ((schema: Any, table: String) =>
      TableIdentifier(table, schema.asInstanceOf[Option[String]]))
  }
}

final class Keyword(s: String) {
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
  final val plural: CharPredicate = CharPredicate('s', 'S')

  final val trueFn: () => Boolean = () => true

  final val falseFn: () => Boolean = () => false

  final val keywords: mutable.Set[String] = mutable.Set[String]()

  private[sql] def keyword(s: String): Keyword = {
    val k = new Keyword(s)
    keywords += k.upper
    k
  }

  // reserved keywords
  final val ALL = keyword("all")
  final val AND = keyword("and")
  final val AS = keyword("as")
  final val ASC = keyword("asc")
  final val BETWEEN = keyword("between")
  final val BY = keyword("by")
  final val CASE = keyword("case")
  final val CREATE = keyword("create")
  final val CURRENT = keyword("current")
  final val DELETE = keyword("delete")
  final val DESC = keyword("desc")
  final val DISTINCT = keyword("distinct")
  final val DROP = keyword("drop")
  final val ELSE = keyword("else")
  final val EXCEPT = keyword("except")
  final val EXISTS = keyword("exists")
  final val FALSE = keyword("false")
  final val FROM = keyword("from")
  final val GROUP = keyword("group")
  final val HAVING = keyword("having")
  final val IN = keyword("in")
  final val INNER = keyword("inner")
  final val INSERT = keyword("insert")
  final val INTERSECT = keyword("intersect")
  final val INTO = keyword("into")
  final val IS = keyword("is")
  final val JOIN = keyword("join")
  final val LEFT = keyword("left")
  final val LIKE = keyword("like")
  final val NOT = keyword("not")
  final val NULL = keyword("null")
  final val ON = keyword("on")
  final val OR = keyword("or")
  final val ORDER = keyword("order")
  final val OUTER = keyword("outer")
  final val RIGHT = keyword("right")
  final val SCHEMA = keyword("schema")
  final val SELECT = keyword("select")
  final val SET = keyword("set")
  final val TABLE = keyword("table")
  final val THEN = keyword("then")
  final val TO = keyword("to")
  final val TRUE = keyword("true")
  final val UNION = keyword("union")
  final val UNIQUE = keyword("unique")
  final val UPDATE = keyword("update")
  final val WHEN = keyword("when")
  final val WHERE = keyword("where")
  final val WITH = keyword("with")

  // non-reserved keywords
  final val ANTI = new Keyword("anti")
  final val CACHE = new Keyword("cache")
  final val CAST = new Keyword("cast")
  final val CLEAR = new Keyword("clear")
  final val CLUSTER = new Keyword("cluster")
  final val COMMENT = new Keyword("comment")
  final val CURRENT_DATE = new Keyword("current_date")
  final val CURRENT_TIMESTAMP = new Keyword("current_timestamp")
  final val DESCRIBE = new Keyword("describe")
  final val DISTRIBUTE = new Keyword("distribute")
  final val END = new Keyword("end")
  final val EXTENDED = new Keyword("extended")
  final val EXTERNAL = new Keyword("external")
  final val FULL = new Keyword("full")
  final val FUNCTION = new Keyword("function")
  final val FUNCTIONS = new Keyword("functions")
  final val GLOBAL = new Keyword("global")
  final val HASH = new Keyword("hash")
  final val IF = new Keyword("if")
  final val INDEX = new Keyword("index")
  final val INIT = new Keyword("init")
  final val INTERVAL = new Keyword("interval")
  final val LAZY = new Keyword("lazy")
  final val LIMIT = new Keyword("limit")
  final val NATURAL = new Keyword("natural")
  final val OPTIONS = new Keyword("options")
  final val OVERWRITE = new Keyword("overwrite")
  final val PARTITION = new Keyword("partition")
  final val PUT = new Keyword("put")
  final val REFRESH = new Keyword("refresh")
  final val REGEXP = new Keyword("regexp")
  final val RLIKE = new Keyword("rlike")
  final val SEMI = new Keyword("semi")
  final val SHOW = new Keyword("show")
  final val SORT = new Keyword("sort")
  final val START = new Keyword("start")
  final val STOP = new Keyword("stop")
  final val STREAM = new Keyword("stream")
  final val STREAMING = new Keyword("streaming")
  final val TABLES = new Keyword("tables")
  final val TEMPORARY = new Keyword("temporary")
  final val TRUNCATE = new Keyword("truncate")
  final val UNCACHE = new Keyword("uncache")
  final val USING = new Keyword("using")

  // Window analytical functions are non-reserved
  final val DURATION = new Keyword("duration")
  final val FOLLOWING = new Keyword("following")
  final val OVER = new Keyword("over")
  final val PRECEDING = new Keyword("preceding")
  final val RANGE = new Keyword("range")
  final val ROW = new Keyword("row")
  final val ROWS = new Keyword("rows")
  final val SLIDE = new Keyword("slide")
  final val UNBOUNDED = new Keyword("unbounded")
  final val WINDOW = new Keyword("window")

  // interval units are not reserved
  final val DAY = new Keyword("day")
  final val HOUR = new Keyword("hour")
  final val MICROSECOND = new Keyword("microsecond")
  final val MILLISECOND = new Keyword("millisecond")
  final val MINUTE = new Keyword("minute")
  final val MONTH = new Keyword("month")
  final val SECOND = new Keyword("seconds")
  final val WEEK = new Keyword("week")
  final val YEAR = new Keyword("year")

  // cube, rollup, grouping sets are not reserved
  final val CUBE = new Keyword("cube")
  final val ROLLUP = new Keyword("rollup")
  final val GROUPING = new Keyword("grouping")
  final val SETS = new Keyword("sets")

  // datatypes are not reserved
  final val ARRAY = new Keyword("array")
  final val BIGINT = new Keyword("bigint")
  final val BINARY = new Keyword("binary")
  final val BLOB = new Keyword("blob")
  final val BOOLEAN = new Keyword("boolean")
  final val BYTE = new Keyword("byte")
  final val CHAR = new Keyword("char")
  final val CLOB = new Keyword("clob")
  final val DATE = new Keyword("date")
  final val DECIMAL = new Keyword("decimal")
  final val DOUBLE = new Keyword("double")
  final val FLOAT = new Keyword("float")
  final val INT = new Keyword("int")
  final val INTEGER = new Keyword("integer")
  final val LONG = new Keyword("long")
  final val MAP = new Keyword("map")
  final val NUMERIC = new Keyword("numeric")
  final val REAL = new Keyword("real")
  final val SHORT = new Keyword("short")
  final val SMALLINT = new Keyword("smallint")
  final val STRING = new Keyword("string")
  final val STRUCT = new Keyword("struct")
  final val TIMESTAMP = new Keyword("timestamp")
  final val TINYINT = new Keyword("tinyint")
  final val VARBINARY = new Keyword("varbinary")
  final val VARCHAR = new Keyword("varchar")
}
