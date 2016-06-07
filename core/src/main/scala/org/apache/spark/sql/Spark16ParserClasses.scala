/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.spark.sql

import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTableUsingAsSelect, CreateTableUsing, RefreshTable}

import scala.language.implicitConversions

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import scala.language.implicitConversions
import scala.util.matching.Regex
import scala.util.parsing.combinator.PackratParsers
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import org.apache.spark.sql.types._
import scala.util.parsing.combinator.RegexParsers

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.StringType

import scala.util.parsing.input.CharArrayReader._


abstract class ParserDialect {
  // this is the main function that will be implemented by sql parser.
  def parse(sqlText: String): LogicalPlan
}


/**
 * A parser for foreign DDL commands.
 */
class DDLParser(parseQuery: String => LogicalPlan)
  extends AbstractSparkSQLParser with DataTypeParser with Logging {

  def parse(input: String, exceptionOnError: Boolean): LogicalPlan = {
    try {
      parse(input)
    } catch {
      case ddlException: DDLException => throw ddlException
      case _ if !exceptionOnError => parseQuery(input)
      case x: Throwable => throw x
    }
  }

  // Keyword is a convention with AbstractSparkSQLParser, which will scan all of the `Keyword`
  // properties via reflection the class in runtime for constructing the SqlLexical object
  protected val CREATE = Keyword("CREATE")
  protected val TEMPORARY = Keyword("TEMPORARY")
  protected val TABLE = Keyword("TABLE")
  protected val IF = Keyword("IF")
  protected val NOT = Keyword("NOT")
  protected val EXISTS = Keyword("EXISTS")
  protected val USING = Keyword("USING")
  protected val OPTIONS = Keyword("OPTIONS")
  protected val DESCRIBE = Keyword("DESCRIBE")
  protected val EXTENDED = Keyword("EXTENDED")
  protected val AS = Keyword("AS")
  protected val COMMENT = Keyword("COMMENT")
  protected val REFRESH = Keyword("REFRESH")

  protected lazy val ddl: Parser[LogicalPlan] = createTable | describeTable | refreshTable

  protected def start: Parser[LogicalPlan] = ddl

  /**
   * `CREATE [TEMPORARY] TABLE [IF NOT EXISTS] avroTable
   * USING org.apache.spark.sql.avro
   * OPTIONS (path "../hive/src/test/resources/data/files/episodes.avro")`
   * or
   * `CREATE [TEMPORARY] TABLE [IF NOT EXISTS] avroTable(intField int, stringField string...)
   * USING org.apache.spark.sql.avro
   * OPTIONS (path "../hive/src/test/resources/data/files/episodes.avro")`
   * or
   * `CREATE [TEMPORARY] TABLE [IF NOT EXISTS] avroTable
   * USING org.apache.spark.sql.avro
   * OPTIONS (path "../hive/src/test/resources/data/files/episodes.avro")`
   * AS SELECT ...
   */
  protected lazy val createTable: Parser[LogicalPlan] = {
    // TODO: Support database.table.
    (CREATE ~> TEMPORARY.? <~ TABLE) ~ (IF ~> NOT <~ EXISTS).? ~ tableIdentifier ~
      tableCols.? ~ (USING ~> className) ~ (OPTIONS ~> options).? ~ (AS ~> restInput).? ^^ {
      case temp ~ allowExisting ~ tableIdent ~ columns ~ provider ~ opts ~ query =>
        if (temp.isDefined && allowExisting.isDefined) {
          throw new DDLException(
            "a CREATE TEMPORARY TABLE statement does not allow IF NOT EXISTS clause.")
        }

        val options = opts.getOrElse(Map.empty[String, String])
        if (query.isDefined) {
          if (columns.isDefined) {
            throw new DDLException(
              "a CREATE TABLE AS SELECT statement does not allow column definitions.")
          }
          // When IF NOT EXISTS clause appears in the query, the save mode will be ignore.
          val mode = if (allowExisting.isDefined) {
            SaveMode.Ignore
          } else if (temp.isDefined) {
            SaveMode.Overwrite
          } else {
            SaveMode.ErrorIfExists
          }

          val queryPlan = parseQuery(query.get)
          CreateTableUsingAsSelect(tableIdent,
            provider,
            temp.isDefined,
            Array.empty[String],
            None,
            mode,
            options,
            queryPlan)
        } else {
          val userSpecifiedSchema = columns.flatMap(fields => Some(StructType(fields)))
          CreateTableUsing(
            tableIdent,
            userSpecifiedSchema,
            provider,
            temp.isDefined,
            options,
            Array.empty[String],
            None,
            allowExisting.isDefined,
            managedIfNoPath = false)
        }
    }
  }

  // This is the same as tableIdentifier in SqlParser.
  protected lazy val tableIdentifier: Parser[TableIdentifier] =
    (ident <~ ".").? ~ ident ^^ {
      case maybeDbName ~ tableName => TableIdentifier(tableName, maybeDbName)
    }

  protected lazy val tableCols: Parser[Seq[StructField]] = "(" ~> repsep(column, ",") <~ ")"

  /*
   * describe [extended] table avroTable
   * This will display all columns of table `avroTable` includes column_name,column_type,comment
   */
  protected lazy val describeTable: Parser[LogicalPlan] =
    (DESCRIBE ~> opt(EXTENDED)) ~ tableIdentifier ^^ {
      case e ~ tableIdent =>
        DescribeTableCommand(tableIdent, e.isDefined, false)
    }

  protected lazy val refreshTable: Parser[LogicalPlan] =
    REFRESH ~> TABLE ~> tableIdentifier ^^ {
      case tableIndet =>
        RefreshTable(tableIndet)
    }

  protected lazy val options: Parser[Map[String, String]] =
    "(" ~> repsep(pair, ",") <~ ")" ^^ { case s: Seq[(String, String)] => s.toMap }

  protected lazy val className: Parser[String] = repsep(ident, ".") ^^ { case s => s.mkString(".")}

  override implicit def regexToParser(regex: Regex): Parser[String] = acceptMatch(
    s"identifier matching regex $regex", {
      case lexical.Identifier(str) if regex.unapplySeq(str).isDefined => str
      case lexical.Keyword(str) if regex.unapplySeq(str).isDefined => str
    }
  )

  protected lazy val optionPart: Parser[String] = "[_a-zA-Z][_a-zA-Z0-9]*".r ^^ {
    case name => name
  }

  protected lazy val optionName: Parser[String] = repsep(optionPart, ".") ^^ {
    case parts => parts.mkString(".")
  }

  protected lazy val pair: Parser[(String, String)] =
    optionName ~ stringLit ^^ { case k ~ v => (k, v) }

  protected lazy val column: Parser[StructField] =
    ident ~ dataType ~ (COMMENT ~> stringLit).?  ^^ { case columnName ~ typ ~ cm =>
      val meta = cm match {
        case Some(comment) =>
          new MetadataBuilder().putString(COMMENT.str.toLowerCase, comment).build()
        case None => Metadata.empty
      }

      StructField(columnName, typ, nullable = true, meta)
    }
}

/**
  * This is a data type parser that can be used to parse string representations of data types
  * provided in SQL queries. This parser is mixed in with DDLParser and SqlParser.
  */
private[sql] trait DataTypeParser extends StandardTokenParsers {

  // This is used to create a parser from a regex. We are using regexes for data type strings
  // since these strings can be also used as column names or field names.
  import lexical.Identifier
  implicit def regexToParser(regex: Regex): Parser[String] = acceptMatch(
    s"identifier matching regex ${regex}",
    { case Identifier(str) if regex.unapplySeq(str).isDefined => str }
  )

  protected lazy val primitiveType: Parser[DataType] =
    "(?i)string".r ^^^ StringType |
      "(?i)float".r ^^^ FloatType |
      "(?i)(?:int|integer)".r ^^^ IntegerType |
      "(?i)tinyint".r ^^^ ByteType |
      "(?i)smallint".r ^^^ ShortType |
      "(?i)double".r ^^^ DoubleType |
      "(?i)(?:bigint|long)".r ^^^ LongType |
      "(?i)binary".r ^^^ BinaryType |
      "(?i)boolean".r ^^^ BooleanType |
      fixedDecimalType |
      "(?i)decimal".r ^^^ DecimalType.USER_DEFAULT |
      "(?i)date".r ^^^ DateType |
      "(?i)timestamp".r ^^^ TimestampType |
      varchar |
      char

  protected lazy val fixedDecimalType: Parser[DataType] =
    ("(?i)decimal".r ~> "(" ~> numericLit) ~ ("," ~> numericLit <~ ")") ^^ {
      case precision ~ scale =>
        DecimalType(precision.toInt, scale.toInt)
    }

  protected lazy val char: Parser[DataType] =
    "(?i)char".r ~> "(" ~> (numericLit <~ ")") ^^^ StringType

  protected lazy val varchar: Parser[DataType] =
    "(?i)varchar".r ~> "(" ~> (numericLit <~ ")") ^^^ StringType

  protected lazy val arrayType: Parser[DataType] =
    "(?i)array".r ~> "<" ~> dataType <~ ">" ^^ {
      case tpe => ArrayType(tpe)
    }

  protected lazy val mapType: Parser[DataType] =
    "(?i)map".r ~> "<" ~> dataType ~ "," ~ dataType <~ ">" ^^ {
      case t1 ~ _ ~ t2 => MapType(t1, t2)
    }

  protected lazy val structField: Parser[StructField] =
    ident ~ ":" ~ dataType ^^ {
      case name ~ _ ~ tpe => StructField(name, tpe, nullable = true)
    }

  protected lazy val structType: Parser[DataType] =
    ("(?i)struct".r ~> "<" ~> repsep(structField, ",") <~ ">"  ^^ {
      case fields => new StructType(fields.toArray)
    }) |
      ("(?i)struct".r ~ "<>" ^^^ StructType(Nil))

  protected lazy val dataType: Parser[DataType] =
    arrayType |
      mapType |
      structType |
      primitiveType

  def toDataType(dataTypeString: String): DataType = synchronized {
    phrase(dataType)(new lexical.Scanner(dataTypeString)) match {
      case Success(result, _) => result
      case failure: NoSuccess => throw new DataTypeException(failMessage(dataTypeString))
    }
  }

  private def failMessage(dataTypeString: String): String = {
    s"Unsupported dataType: $dataTypeString. If you have a struct and a field name of it has " +
      "any special characters, please use backticks (`) to quote that field name, e.g. `x+y`. " +
      "Please note that backtick itself is not supported in a field name."
  }
}

private[sql] object DataTypeParser {
  lazy val dataTypeParser = new DataTypeParser {
    override val lexical = new SqlLexical
  }

  def parse(dataTypeString: String): DataType = dataTypeParser.toDataType(dataTypeString)
}

/** The exception thrown from the [[DataTypeParser]]. */
private[sql] class DataTypeException(message: String) extends Exception(message)


private[sql] abstract class AbstractSparkSQLParser
  extends StandardTokenParsers with PackratParsers {

  def parse(input: String): LogicalPlan = synchronized {
    // Initialize the Keywords.
    initLexical
    phrase(start)(new lexical.Scanner(input)) match {
      case Success(plan, _) => plan
      case failureOrError => sys.error(failureOrError.toString)
    }
  }
  /* One time initialization of lexical.This avoid reinitialization of  lexical in parse method */
  protected lazy val initLexical: Unit = lexical.initialize(reservedWords)

  protected case class Keyword(str: String) {
    def normalize: String = lexical.normalizeKeyword(str)
    def parser: Parser[String] = normalize
  }

  protected implicit def asParser(k: Keyword): Parser[String] = k.parser

  // By default, use Reflection to find the reserved words defined in the sub class.
  // NOTICE, Since the Keyword properties defined by sub class, we couldn't call this
  // method during the parent class instantiation, because the sub class instance
  // isn't created yet.
  protected lazy val reservedWords: Seq[String] =
    this
      .getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword].normalize)

  // Set the keywords as empty by default, will change that later.
  override val lexical = new SqlLexical

  protected def start: Parser[LogicalPlan]

  // Returns the whole input string
  protected lazy val wholeInput: Parser[String] = new Parser[String] {
    def apply(in: Input): ParseResult[String] =
      Success(in.source.toString, in.drop(in.source.length()))
  }

  // Returns the rest of the input string that are not parsed yet
  protected lazy val restInput: Parser[String] = new Parser[String] {
    def apply(in: Input): ParseResult[String] =
      Success(
        in.source.subSequence(in.offset, in.source.length()).toString,
        in.drop(in.source.length()))
  }
}

class SqlLexical extends StdLexical {
  case class DecimalLit(chars: String) extends Token {
    override def toString: String = chars
  }

  /* This is a work around to support the lazy setting */
  def initialize(keywords: Seq[String]): Unit = {
    reserved.clear()
    reserved ++= keywords
  }

  /* Normal the keyword string */
  def normalizeKeyword(str: String): String = str.toLowerCase

  delimiters += (
    "@", "*", "+", "-", "<", "=", "<>", "!=", "<=", ">=", ">", "/", "(", ")",
    ",", ";", "%", "{", "}", ":", "[", "]", ".", "&", "|", "^", "~", "<=>"
    )

  protected override def processIdent(name: String) = {
    val token = normalizeKeyword(name)
    if (reserved contains token) Keyword(token) else Identifier(name)
  }

  override lazy val token: Parser[Token] =
    ( rep1(digit) ~ scientificNotation ^^ { case i ~ s => DecimalLit(i.mkString + s) }
      | '.' ~> (rep1(digit) ~ scientificNotation) ^^
      { case i ~ s => DecimalLit("0." + i.mkString + s) }
      | rep1(digit) ~ ('.' ~> digit.*) ~ scientificNotation ^^
      { case i1 ~ i2 ~ s => DecimalLit(i1.mkString + "." + i2.mkString + s) }
      | digit.* ~ identChar ~ (identChar | digit).* ^^
      { case first ~ middle ~ rest => processIdent((first ++ (middle :: rest)).mkString) }
      | rep1(digit) ~ ('.' ~> digit.*).? ^^ {
      case i ~ None => NumericLit(i.mkString)
      case i ~ Some(d) => DecimalLit(i.mkString + "." + d.mkString)
    }
      | '\'' ~> chrExcept('\'', '\n', EofCh).* <~ '\'' ^^
      { case chars => StringLit(chars mkString "") }
      | '"' ~> chrExcept('"', '\n', EofCh).* <~ '"' ^^
      { case chars => StringLit(chars mkString "") }
      | '`' ~> chrExcept('`', '\n', EofCh).* <~ '`' ^^
      { case chars => Identifier(chars mkString "") }
      | EofCh ^^^ EOF
      | '\'' ~> failure("unclosed string literal")
      | '"' ~> failure("unclosed string literal")
      | delim
      | failure("illegal character")
      )

  override def identChar: Parser[Elem] = letter | elem('_')

  private lazy val scientificNotation: Parser[String] =
    (elem('e') | elem('E')) ~> (elem('+') | elem('-')).? ~ rep1(digit) ^^ {
      case s ~ rest => "e" + s.mkString + rest.mkString
    }

  override def whitespace: Parser[Any] =
    ( whitespaceChar
      | '/' ~ '*' ~ comment
      | '/' ~ '/' ~ chrExcept(EofCh, '\n').*
      | '#' ~ chrExcept(EofCh, '\n').*
      | '-' ~ '-' ~ chrExcept(EofCh, '\n').*
      | '/' ~ '*' ~ failure("unclosed comment")
      ).*
}

class DDLException(message: String) extends RuntimeException(message)


/**
  * The top level Spark SQL parser. This parser recognizes syntaxes that are available for all SQL
  * dialects supported by Spark SQL, and delegates all the other syntaxes to the `fallback` parser.
  *
  * @param fallback A function that parses an input string to a logical plan
  */
class SparkSQLParser(fallback: String => LogicalPlan) extends AbstractSparkSQLParser {

  // A parser for the key-value part of the "SET [key = [value ]]" syntax
  private object SetCommandParser extends RegexParsers {
    private val key: Parser[String] = "(?m)[^=]+".r

    private val value: Parser[String] = "(?m).*$".r

    private val output: Seq[Attribute] = Seq(AttributeReference("", StringType, nullable = false)())

    private val pair: Parser[LogicalPlan] =
      (key ~ ("=".r ~> value).?).? ^^ {
        case None => SetCommand(None)
        case Some(k ~ v) => SetCommand(Some(k.trim -> v.map(_.trim)))
      }

    def apply(input: String): LogicalPlan = parseAll(pair, input) match {
      case Success(plan, _) => plan
      case x => sys.error(x.toString)
    }
  }

  protected val AS = Keyword("AS")
  protected val CACHE = Keyword("CACHE")
  protected val CLEAR = Keyword("CLEAR")
  protected val DESCRIBE = Keyword("DESCRIBE")
  protected val EXTENDED = Keyword("EXTENDED")
  protected val FUNCTION = Keyword("FUNCTION")
  protected val FUNCTIONS = Keyword("FUNCTIONS")
  protected val IN = Keyword("IN")
  protected val LAZY = Keyword("LAZY")
  protected val SET = Keyword("SET")
  protected val SHOW = Keyword("SHOW")
  protected val TABLE = Keyword("TABLE")
  protected val TABLES = Keyword("TABLES")
  protected val UNCACHE = Keyword("UNCACHE")

  override protected lazy val start: Parser[LogicalPlan] =
    cache | uncache | set | show | desc | others

  private lazy val cache: Parser[LogicalPlan] =
    CACHE ~> LAZY.? ~ (TABLE ~> ident) ~ (AS ~> restInput).? ^^ {
      case isLazy ~ tableName ~ plan =>
        CacheTableCommand(tableName, plan.map(fallback), isLazy.isDefined)
    }

  private lazy val uncache: Parser[LogicalPlan] =
    ( UNCACHE ~ TABLE ~> ident ^^ {
      case tableName => UncacheTableCommand(tableName)
    }
      | CLEAR ~ CACHE ^^^ ClearCacheCommand
      )

  private lazy val set: Parser[LogicalPlan] =
    SET ~> restInput ^^ {
      case input => SetCommandParser(input)
    }

  // It can be the following patterns:
  // SHOW FUNCTIONS;
  // SHOW FUNCTIONS mydb.func1;
  // SHOW FUNCTIONS func1;
  // SHOW FUNCTIONS `mydb.a`.`func1.aa`;
  private lazy val show: Parser[LogicalPlan] =
    ( SHOW ~> TABLES ~ (IN ~> ident).? ^^ {
      case _ ~ dbName => ShowTablesCommand(dbName, None)
    }
      | SHOW ~ FUNCTIONS ~> ((ident <~ ".").? ~ (ident | stringLit)).? ^^ {
      case Some(f) => ShowFunctionsCommand(f._1, Some(f._2))
      case None => ShowFunctionsCommand(None, None)
    }
      )

  private lazy val desc: Parser[LogicalPlan] =
    DESCRIBE ~ FUNCTION ~> EXTENDED.? ~ (ident | stringLit) ^^ {
      case isExtended ~ functionName =>
        DescribeFunctionCommand(FunctionIdentifier(functionName), isExtended.isDefined)
    }

  private lazy val others: Parser[LogicalPlan] =
    wholeInput ^^ {
      case input => fallback(input)
    }

}