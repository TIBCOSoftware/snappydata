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
import scala.language.implicitConversions
import scala.util.{Failure, Success}

import org.parboiled2._

import org.apache.spark.sql.SnappyParserConsts.plusOrMinus
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.types._

/**
 * Base parsing facilities for all SnappyData SQL parsers.
 */
abstract class SnappyBaseParser(context: SnappyContext) extends Parser {

  val caseSensitive = context.conf.caseSensitiveAnalysis

  private[sql] final val queryHints = new mutable.HashMap[String, String]

  protected def reset(): Unit = queryHints.clear()

  protected final def commentBody: Rule0 = rule {
    "*/" | ANY ~ commentBody
  }

  protected final def commentBodyOrHint: Rule0 = rule {
    '+' ~ (SnappyParserConsts.whitespace.* ~ capture(CharPredicate.Alpha ~
        SnappyParserConsts.identifier.*) ~ SnappyParserConsts.whitespace.* ~
        '(' ~ capture(noneOf(SnappyParserConsts.hintValueEnd).*) ~ ')' ~>
        ((k: String, v: String) => queryHints += (k -> v.trim): Unit)).+ ~
        commentBody | commentBody
  }

  protected final def lineCommentOrHint: Rule0 = rule {
    '+' ~ (SnappyParserConsts.space.* ~ capture(CharPredicate.Alpha ~
        SnappyParserConsts.identifier.*) ~ SnappyParserConsts.space.* ~
        '(' ~ capture(noneOf(SnappyParserConsts.lineHintEnd).*) ~ ')' ~>
        ((k: String, v: String) => queryHints += (k -> v.trim): Unit)).+ ~
        noneOf(SnappyParserConsts.lineCommentEnd).* |
    noneOf(SnappyParserConsts.lineCommentEnd).*
  }

  /** The recognized whitespace characters and comments. */
  protected final def ws: Rule0 = rule {
    quiet(
      SnappyParserConsts.whitespace |
      '-' ~ '-' ~ lineCommentOrHint |
      '/' ~ (
          '/' ~ lineCommentOrHint |
          '*' ~ (commentBodyOrHint | fail("unclosed comment"))
      ) |
      '#' ~ lineCommentOrHint
    ).*
  }

  /** All recognized delimiters including whitespace. */
  final def delimiter: Rule0 = rule {
    quiet(&(SnappyParserConsts.delimiters)) ~ ws | EOI
  }

  protected final def digits: Rule1[String] = rule {
    capture(CharPredicate.Digit.+) ~ ws
  }

  protected final def integral: Rule1[String] = rule {
    capture(plusOrMinus.? ~ CharPredicate.Digit.+) ~ ws
  }

  protected final def scientificNotation: Rule0 = rule {
    SnappyParserConsts.exponent ~ plusOrMinus.? ~ CharPredicate.Digit.+
  }

  protected final def stringLiteral: Rule1[String] = rule {
    (('\'' ~ capture((SnappyParserConsts.singleQuotedString | "''").*) ~ '\'') ~>
        ((s: String) => if (s.indexOf("''") >= 0) s.replace("''", "'") else s) |
    ('"' ~ capture((SnappyParserConsts.doubleQuotedString | "\"\"").*) ~ '"') ~>
        ((s: String) => if (s.indexOf("\"\"") >= 0) s.replace("\"\"", "\"")
        else s)) ~ ws
  }

  final def keyword(k: Keyword): Rule0 = rule {
    atomic(ignoreCase(k.lower)) ~ delimiter
  }

  /**
   * Used for DataTypes. Not reserved and otherwise identical to "keyword"
   * apart from the name so as to appear properly in error messages related
   * to incorrect DataType definition.
   */
  final def dataType(t: Keyword): Rule0 = rule {
    atomic(ignoreCase(t.lower)) ~ delimiter
  }

  protected final def sql: Rule1[LogicalPlan] = rule {
    ws ~ start ~ (';' ~ ws).* ~ EOI
  }

  def parse(): LogicalPlan = {
    sql.run() match {
      case Success(plan) => plan
      case Failure(e: ParseError) =>
        throw Utils.analysisException(formatError(e))
      case Failure(e) =>
        val ae = Utils.analysisException(e.toString)
        ae.initCause(e)
        throw ae
    }
  }

  protected def start: Rule1[LogicalPlan]

  protected final def identifier: Rule1[String] = rule {
    atomic(capture(CharPredicate.Alpha ~ SnappyParserConsts.identifier.*)) ~
        delimiter ~> { (s: String) =>
        val ucase = Utils.toUpperCase(s)
        test(!SnappyParserConsts.keywords.contains(ucase)) ~
            push(if (caseSensitive) s else ucase) } |
    atomic('`' ~ capture(SnappyParserConsts.quotedIdentifier.+) ~ '`') ~ ws
  }

  // DataTypes
  // It is not useful to see long list of "expected ARRAY or BIGINT or ..."
  // for parse errors, so not making these separate rules and instead naming
  // the common rule as "datatype" which is otherwise identical to "keyword"
  final def ARRAY = dataType(SnappyParserConsts.ARRAY)
  final def BIGINT = dataType(SnappyParserConsts.BIGINT)
  final def BINARY = dataType(SnappyParserConsts.BINARY)
  final def BLOB = dataType(SnappyParserConsts.BLOB)
  final def BOOLEAN = dataType(SnappyParserConsts.BOOLEAN)
  final def BYTE = dataType(SnappyParserConsts.BYTE)
  final def CHAR = dataType(SnappyParserConsts.CHAR)
  final def CLOB = dataType(SnappyParserConsts.CLOB)
  final def DATE = dataType(SnappyParserConsts.DATE)
  final def DECIMAL = dataType(SnappyParserConsts.DECIMAL)
  final def DOUBLE = dataType(SnappyParserConsts.DOUBLE)
  final def FLOAT = dataType(SnappyParserConsts.FLOAT)
  final def INT = dataType(SnappyParserConsts.INT)
  final def INTEGER = dataType(SnappyParserConsts.INTEGER)
  final def LONG = dataType(SnappyParserConsts.LONG)
  final def MAP = dataType(SnappyParserConsts.MAP)
  final def NUMERIC = dataType(SnappyParserConsts.NUMERIC)
  final def REAL = dataType(SnappyParserConsts.REAL)
  final def SHORT = dataType(SnappyParserConsts.SHORT)
  final def SMALLINT = dataType(SnappyParserConsts.SMALLINT)
  final def STRING = dataType(SnappyParserConsts.STRING)
  final def STRUCT = dataType(SnappyParserConsts.STRUCT)
  final def TIMESTAMP = dataType(SnappyParserConsts.TIMESTAMP)
  final def TINYINT = dataType(SnappyParserConsts.TINYINT)
  final def VARBINARY = dataType(SnappyParserConsts.VARBINARY)
  final def VARCHAR = dataType(SnappyParserConsts.VARCHAR)

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
    VARCHAR ~ '(' ~ ws ~ digits ~ ')' ~ ws ~> ((d: String) => StringType) |
    CHAR ~ '(' ~ ws ~ digits ~ ')' ~ ws ~> ((d: String) => StringType) |
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

  protected final def dataType: Rule1[DataType] = rule {
    primitiveType | arrayType | mapType | structType
  }

  protected final def arrayType: Rule1[DataType] = rule {
    ARRAY ~ '<' ~ ws ~ dataType ~ '>' ~ ws ~>
        ((tpe: DataType) => ArrayType(tpe))
  }

  protected final def mapType: Rule1[DataType] = rule {
    MAP ~ '<' ~ ws ~ dataType ~ ',' ~ ws ~ dataType ~ '>' ~ ws ~>
        ((tpe1: DataType, tpe2: DataType) => MapType(tpe1, tpe2))
  }

  protected final def structField: Rule1[StructField] = rule {
    identifier ~ ':' ~ ws ~ dataType ~> ((name: String, tpe: DataType) =>
      StructField(name, tpe, nullable = true))
  }

  protected final def structType: Rule1[DataType] = rule {
    STRUCT ~ '<' ~ ws ~ (structField * (',' ~ ws)) ~ '>' ~ ws ~>
        ((fields: Seq[StructField]) => StructType(fields.toArray))
  }

  protected final def tableIdentifier: Rule1[TableIdentifier] = rule {
    // case-sensitivity already taken care of properly by "identifier"
    (identifier ~ '.' ~ ws).? ~ identifier ~> ((schemaOpt: Option[String],
        table: String) => TableIdentifier(table, schemaOpt))
  }

  /** Returns the rest of the input string that are not parsed yet */
  protected final def restInput: String =
    input.sliceString(cursor, input.length)
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
  final val singleQuotedString: CharPredicate = CharPredicate('\'').negated
  final val doubleQuotedString: CharPredicate = CharPredicate('"').negated
  final val identifier: CharPredicate = CharPredicate.AlphaNum ++
      CharPredicate('_')
  final val quotedIdentifier: CharPredicate = CharPredicate(
    '`', '\n', '\r', '\f').negated
  final val plusOrMinus: CharPredicate = CharPredicate('+', '-')
  final val arithmeticOperator = CharPredicate('*', '/', '%', '&', '|', '^')
  final val exponent: CharPredicate = CharPredicate('e', 'E')
  final val numeric: CharPredicate = CharPredicate.Digit ++
      CharPredicate('.') ++ exponent
  final val plural: CharPredicate = CharPredicate('s', 'S')

  final val trueFn: () => Boolean = () => true

  final val keywords: mutable.Set[String] = mutable.Set[String]()

  private[sql] def keyword(s: String): Keyword = {
    val k = new Keyword(s)
    keywords += k.upper
    k
  }

  final val ALL = keyword("all")
  final val AND = keyword("and")
  final val APPROXIMATE = keyword("approximate")
  final val AS = keyword("as")
  final val ASC = keyword("asc")
  final val BETWEEN = keyword("between")
  final val BY = keyword("by")
  final val CASE = keyword("case")
  final val CAST = keyword("cast")
  final val DELETE = keyword("delete")
  final val DESC = keyword("desc")
  final val DISTINCT = keyword("distinct")
  final val ELSE = keyword("else")
  final val END = keyword("end")
  final val EXCEPT = keyword("except")
  final val EXISTS = keyword("exists")
  final val FALSE = keyword("false")
  final val FROM = keyword("from")
  final val FULL = keyword("full")
  final val GROUP = keyword("group")
  final val HAVING = keyword("having")
  final val IN = keyword("in")
  final val INNER = keyword("inner")
  final val INSERT = keyword("insert")
  final val INTERSECT = keyword("intersect")
  final val INTERVAL = keyword("interval")
  final val INTO = keyword("into")
  final val IS = keyword("is")
  final val JOIN = keyword("join")
  final val LEFT = keyword("left")
  final val LIKE = keyword("like")
  final val LIMIT = keyword("limit")
  final val NOT = keyword("not")
  final val NULL = keyword("null")
  final val ON = keyword("on")
  final val OR = keyword("or")
  final val ORDER = keyword("order")
  final val PUT = keyword("put")
  final val SORT = keyword("sort")
  final val OUTER = keyword("outer")
  final val OVERWRITE = keyword("overwrite")
  final val REGEXP = keyword("regexp")
  final val RIGHT = keyword("right")
  final val RLIKE = keyword("rlike")
  final val SELECT = keyword("select")
  final val SEMI = keyword("semi")
  final val TABLE = keyword("table")
  final val THEN = keyword("then")
  final val TO = keyword("to")
  final val TRUE = keyword("true")
  final val UNION = keyword("union")
  final val UPDATE = keyword("update")
  final val WHEN = keyword("when")
  final val WHERE = keyword("where")
  final val WITH = keyword("with")

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

  // Added for streaming window CQs
  final val DURATION = keyword("duration")
  final val SLIDE = keyword("slide")
  final val WINDOW = keyword("window")

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
