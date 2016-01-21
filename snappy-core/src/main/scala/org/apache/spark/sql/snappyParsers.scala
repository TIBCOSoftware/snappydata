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

import java.sql.SQLException
import java.util.regex.Pattern

import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.{DefaultParserDialect, SqlLexical, SqlParserBase, TableIdentifier}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{DataType, StringType, StructType}
import org.apache.spark.streaming.{Duration, Milliseconds, Minutes, Seconds}


class SnappyParserBase(caseSensitive: Boolean) extends SqlParserBase {

  protected val PUT = Keyword("PUT")
  protected val DELETE = Keyword("DELETE")
  protected val UPDATE = Keyword("UPDATE")
  // Added for streaming window CQs
  protected val WINDOW = Keyword("WINDOW")
  protected val DURATION = Keyword("DURATION")
  protected val SLIDE = Keyword("SLIDE")
  protected val MILLISECONDS = Keyword("MILLISECONDS")
  protected val SECONDS = Keyword("SECONDS")
  protected val MINUTES = Keyword("MINUTES")

  override val lexical = new SnappyLexical(caseSensitive)

  override protected lazy val start: Parser[LogicalPlan] = start1 | insert |
      put | cte | dmlForExternalTable

  protected lazy val put: Parser[LogicalPlan] =
    PUT ~> (OVERWRITE ^^^ true | INTO ^^^ false) ~ (TABLE ~> relation) ~
        select ^^ {
      case o ~ r ~ s => InsertIntoTable(r, Map.empty[String, Option[String]], s, o, false)
    }

  protected lazy val dmlForExternalTable: Parser[LogicalPlan] =
    (INSERT ~> INTO | PUT ~> INTO | DELETE ~> FROM | UPDATE) ~> tableIdentifier ~
        wholeInput ^^ {
      case r ~ s => DMLExternalTable(r, UnresolvedRelation(r), s)
    }

  protected lazy val unit: Parser[Duration] =
    (
        stringLit <~ MILLISECONDS ^^ { case str => Milliseconds(str.toInt) }
      | stringLit <~ SECONDS ^^ { case str => Seconds(str.toInt) }
      | stringLit <~ MINUTES ^^ { case str => Minutes(str.toInt) }
    )

  protected lazy val windowOptions: Parser[(Duration, Option[Duration])] =
    WINDOW ~ "(" ~> (DURATION ~> unit) ~
      ("," ~ SLIDE ~> unit).? <~ ")" ^^ {
      case duration ~ slide => (duration, slide)
    }

  protected override lazy val relationFactor: Parser[LogicalPlan] =
    (tableIdentifier ~ windowOptions.? ~ (opt(AS) ~> opt(ident)) ^^ {
      case tableIdent ~ window ~ alias => window.map { win =>
        WindowLogicalPlan(
          win._1,
          win._2,
          UnresolvedRelation(tableIdent, alias))
      }.getOrElse(UnresolvedRelation(tableIdent, alias))
    }
      |
      ("(" ~> start <~ ")") ~ windowOptions.? ~ (AS.? ~> ident) ^^ {
      case child ~ window ~ alias => window.map { win =>
        WindowLogicalPlan(
          win._1,
          win._2,
          Subquery(alias, child))
      }.getOrElse(Subquery(alias, child))
    })

  override def parseExpression(input: String): Expression = synchronized {
    // Initialize the Keywords.
    initLexical
    phrase(projection)(new lexical.Scanner(input)) match {
      case Success(plan, _) => plan
      // case failureOrError => sys.error(failureOrError.toString)
      case failureOrError =>
        throw new SQLException(failureOrError.toString, "42X01")
    }
  }

  override def parse(input: String): LogicalPlan = synchronized {
    // Initialize the Keywords.
    initLexical
    phrase(start)(new lexical.Scanner(input)) match {
      case Success(plan, _) => plan
      case failureOrError =>
        throw new SQLException(failureOrError.toString, "42X01")
    }
  }
}

final class SnappyLexical(caseSensitive: Boolean) extends SqlLexical {

  protected override def processIdent(name: String) = {
    val token = normalizeKeyword(name)
    if (reserved contains token) Keyword(token)
    else if (caseSensitive) {
      Identifier(name)
    } else {
      Identifier(Utils.toUpperCase(name))
    }
  }
}

object SnappyParser extends SnappyParserBase(false)

object SnappyParserCaseSensitive extends SnappyParserBase(true)

/** Snappy dialect adds SnappyParser additions to the standard "sql" dialect */
private[sql] final class SnappyParserDialect(caseSensitive: Boolean)
    extends DefaultParserDialect {

  @transient protected override val sqlParser =
    if (caseSensitive) SnappyParserCaseSensitive else SnappyParser
}

/**
 * Snappy DDL extensions for streaming and sampling.
 */
private[sql] class SnappyDDLParser(caseSensitive: Boolean,
    parseQuery: String => LogicalPlan) extends DDLParser(parseQuery) {

  override val lexical = new SnappyLexical(caseSensitive)

  override def parse(input: String): LogicalPlan = synchronized {
    // Initialize the Keywords.
    initLexical
    phrase(start)(new lexical.Scanner(input)) match {
      case Success(plan, _) => plan
      case failureOrError =>
        throw new SQLException(failureOrError.toString, "42X01")
    }
  }

  override def parse(input: String, exceptionOnError: Boolean): LogicalPlan = {
    try {
      parse(input)
    } catch {
      case ddlException: DDLException => throw ddlException
      case t: SQLException if !exceptionOnError =>
        parseQuery(input)
    }
  }

  override protected lazy val ddl: Parser[LogicalPlan] =
    createTable | describeTable | refreshTable | dropTable |
        createStream  | streamContext | truncateTable | createIndex | dropIndex

  protected val STREAM = Keyword("STREAM")
  protected val STREAMING = Keyword("STREAMING")
  protected val CONTEXT = Keyword("CONTEXT")
  protected val START = Keyword("START")
  protected val STOP = Keyword("STOP")
  protected val INIT = Keyword("INIT")
  protected val DROP = Keyword("DROP")
  protected val TRUNCATE = Keyword("TRUNCATE")
  protected val INDEX = Keyword("INDEX")
  protected val ON = Keyword("ON")

  protected override lazy val className: Parser[String] =
    repsep(ident, ".") ^^ { case s =>
      // A workaround to address lack of case information at this point.
      // If value is all CAPS then convert to lowercase else preserve case.
      if (s.exists(Utils.hasLowerCase)) s.mkString(".")
      else s.map(Utils.toLowerCase).mkString(".")
    }

  private val DDLEnd = Pattern.compile(USING.str + "\\s+[a-zA-Z_0-9\\.]+\\s*" +
      s"(\\s${OPTIONS.str}|\\s${AS.str}|$$)", Pattern.CASE_INSENSITIVE)

  protected override lazy val createTable: Parser[LogicalPlan] =
    (CREATE ~> TEMPORARY.? <~ TABLE) ~ (IF ~> NOT <~ EXISTS).? ~
        tableIdentifier ~ externalTableInput ~ (USING ~> className).? ~
        (OPTIONS ~> options).? ~ (AS ~> restInput).? ^^ {
      case temporary ~ allowExisting ~ tableIdent ~ schemaString ~
          providerName ~ opts ~ query =>

        val options = opts.getOrElse(Map.empty[String, String])
        val provider = SnappyContext.getProvider(providerName.getOrElse(SnappyContext.DEFAULT_SOURCE))
        if (query.isDefined) {
          if (schemaString.length > 0) {
            throw new DDLException("CREATE TABLE AS SELECT statement " +
                "does not allow column definitions.")
          }
          // When IF NOT EXISTS clause appears in the query,
          // the save mode will be ignore.
          val mode = if (allowExisting.isDefined) SaveMode.Ignore
          else SaveMode.ErrorIfExists
          val queryPlan = parseQuery(query.get)

          if (temporary.isDefined) {
            CreateTableUsingAsSelect(tableIdent, provider, temporary = true,
              Array.empty[String], mode, options, queryPlan)
          } else {
            CreateExternalTableUsingSelect(tableIdent, provider,
              Array.empty[String], mode, options, queryPlan)
          }
        } else {
          val hasExternalSchema = if (temporary.isDefined) false
          else {
            // check if provider class implements ExternalSchemaRelationProvider
            try {
              val clazz: Class[_] = ResolvedDataSource.lookupDataSource(provider)
              classOf[ExternalSchemaRelationProvider].isAssignableFrom(clazz)
            } catch {
              case cnfe: ClassNotFoundException => throw new DDLException(cnfe.toString)
              case t: Throwable => throw t
            }
          }
          val userSpecifiedSchema = if (hasExternalSchema) None
          else {
            phrase(tableCols.?)(new lexical.Scanner(schemaString)) match {
              case Success(columns, _) =>
                columns.flatMap(fields => Some(StructType(fields)))
              case failure =>
                throw new DDLException(failure.toString)
            }
          }
          val schemaDDL = if (hasExternalSchema) Some(schemaString) else None

          if (temporary.isDefined) {
            CreateTableUsing(tableIdent, userSpecifiedSchema, provider,
              temporary = true, options, allowExisting.isDefined,
              managedIfNoPath = false)
          } else {
            CreateExternalTableUsing(tableIdent, userSpecifiedSchema,
              schemaDDL, provider, allowExisting.isDefined, options)
          }
        }
    }

  protected override lazy val varchar: Parser[DataType] =
    (("(?i)varchar".r ~> ("(" ~> numericLit <~ ")").?) |
     "(?i)clob".r) ^^^ StringType

  protected lazy val createIndex: Parser[LogicalPlan] =
    (CREATE ~> INDEX ~> tableIdentifier) ~ (ON ~> tableIdentifier) ~ wholeInput ^^ {
      case indexName ~ tableName ~ sql =>
        CreateIndex(tableName, sql)
    }

  protected lazy val dropIndex: Parser[LogicalPlan] =
    (DROP ~> INDEX ~> tableIdentifier) ~ wholeInput ^^ {
      case indexName ~ sql =>
        DropIndex(sql)
    }

  protected lazy val dropTable: Parser[LogicalPlan] =
    (DROP ~> TEMPORARY.? <~ TABLE) ~ (IF ~> EXISTS).? ~ tableIdentifier ^^ {
      case temporary ~ allowExisting ~ tableName =>
        DropTable(tableName, temporary.isDefined, allowExisting.isDefined)
    }

  protected lazy val truncateTable: Parser[LogicalPlan] =
    (TRUNCATE ~> TEMPORARY.? <~ TABLE) ~ tableIdentifier ^^ {
      case temporary ~ tableName =>
        TruncateTable(tableName, temporary.isDefined)
    }

  protected lazy val createStream: Parser[LogicalPlan] =
    (CREATE ~> STREAM ~> TABLE ~> tableIdentifier) ~ (IF ~> NOT <~ EXISTS).? ~
        tableCols.? ~ (USING ~> className) ~ (OPTIONS ~> options) ^^ {
      case streamName ~ allowExisting ~ cols ~ providerName ~ opts =>
        val specifiedSchema = cols.flatMap(fields => Some(StructType(fields)))
        val provider = SnappyContext.getProvider(providerName)
        val userOpts = opts.updated("tableName", streamName.unquotedString)
        CreateExternalTableUsing(streamName, specifiedSchema, None,
          provider, allowExisting.isDefined, userOpts)
    }

  protected lazy val streamContext: Parser[LogicalPlan] =
    (STREAMING ~>
        (INIT ^^^ 0 | START ^^^ 1 | STOP ^^^ 2) ~ numericLit.?) ^^ {
      case action ~ batchInterval =>
        if (batchInterval.isDefined) {
          StreamOperationsLogicalPlan(action, Some(batchInterval.get.toInt))
        } else {
          StreamOperationsLogicalPlan(action, None)
        }
    }


  protected lazy val externalTableInput: Parser[String] = new Parser[String] {
    def apply(in: Input): ParseResult[String] = {
      val source = in.source
      val remaining = source.subSequence(in.offset, source.length).toString
      val m = DDLEnd.matcher(remaining)
      if (m.find) {
        val index = m.start()
        val externalTableDefinition = remaining.substring(0, index).trim
        val others = remaining.substring(index)
        val reader = new PackratReader(new lexical.Scanner(others))
        Success(externalTableDefinition, reader)
      } else {
        Success(
          in.source.subSequence(in.offset, in.source.length()).toString,
          in.drop(in.source.length()))
      }
    }
  }
}

private[sql] case class CreateExternalTableUsing(
    tableIdent: TableIdentifier,
    userSpecifiedSchema: Option[StructType],
    schemaDDL: Option[String],
    provider: String,
    allowExisting: Boolean,
    options: Map[String, String]) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = sqlContext.asInstanceOf[SnappyContext]
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
    snc.createTable(snc.catalog.newQualifiedTableName(tableIdent), provider,
      userSpecifiedSchema, schemaDDL, mode, options)
    Seq.empty
  }
}

private[sql] case class CreateExternalTableUsingSelect(
    tableIdent: TableIdentifier,
    provider: String,
    partitionColumns: Array[String],
    mode: SaveMode,
    options: Map[String, String],
    query: LogicalPlan) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = sqlContext.asInstanceOf[SnappyContext]
    val catalog = snc.catalog
    snc.createTable(catalog.newQualifiedTableName(tableIdent), provider,
      partitionColumns, mode, options, query)
    // refresh cache of the table in catalog
    catalog.invalidateTable(tableIdent)
    Seq.empty
  }
}

private[sql] case class DropTable(
    tableIdent: TableIdentifier,
    temporary: Boolean,
    ifExists: Boolean) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = sqlContext.asInstanceOf[SnappyContext]
    val qualifiedTable = snc.catalog.newQualifiedTableName(tableIdent)
    snc.dropTable(qualifiedTable, ifExists)
    Seq.empty
  }
}

private[sql] case class TruncateTable(
    tableIdent: TableIdentifier,
    temporary: Boolean) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = sqlContext.asInstanceOf[SnappyContext]
    val qualifiedTable = snc.catalog.newQualifiedTableName(tableIdent)
    snc.truncateTable(qualifiedTable)
    Seq.empty
  }
}

private[sql] case class CreateIndex(
    tableIdent: TableIdentifier,
    sql: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = sqlContext.asInstanceOf[SnappyContext]
    snc.createIndexOnTable(snc.catalog.newQualifiedTableName(tableIdent), sql)
    Seq.empty
  }
}

private[sql] case class DropIndex(
    sql: String) extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    val snc = sqlContext.asInstanceOf[SnappyContext]
    snc.dropIndexOfTable(sql)
    Seq.empty
  }
}

case class DMLExternalTable(
    tableName: TableIdentifier,
    child: LogicalPlan,
    command: String)
    extends LogicalPlan with Command {

  override def children: Seq[LogicalPlan] = child :: Nil

  override def output: Seq[Attribute] = child.output
}

private[sql] case class StreamOperationsLogicalPlan(action: Int,
    batchInterval: Option[Int])
    extends LogicalPlan with Command {

  override def output: Seq[Attribute] = Seq.empty

  /** Returns a Seq of the children of this node */
  override def children: Seq[LogicalPlan] = Seq.empty
}

private[sql] case class SnappyStreamingActionsCommand(action: Int,
    batchInterval: Option[Int])
    extends RunnableCommand {

  override def run(sqlContext: SQLContext): Seq[Row] = {
    action match {
      case 0 =>
        SnappyStreamingContext(sqlContext.asInstanceOf[SnappyContext],
          Seconds(batchInterval.get))
      case 1 => SnappyStreamingContext.start()
      case 2 => SnappyStreamingContext.stop()
    }
    Seq.empty[Row]
  }
}
