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

import scala.util.Try

import io.snappydata.Constant
import org.parboiled2._
import shapeless.{::, HNil}

import org.apache.spark.sql.SnappyParserConsts.{falseFn, trueFn}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.ParserUtils
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTableUsing, DataSource, RefreshTable}
import org.apache.spark.sql.sources.ExternalSchemaRelationProvider
import org.apache.spark.sql.streaming.StreamPlanProvider
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SnappyParserConsts => Consts}
import org.apache.spark.streaming.{Duration, Milliseconds, Minutes, Seconds, SnappyStreamingContext}

abstract class SnappyDDLParser(session: SnappySession)
    extends SnappyBaseParser(session) {
  final def ALL: Rule0 = rule { keyword(Consts.ALL) }
  final def AND: Rule0 = rule { keyword(Consts.AND) }
  final def AS: Rule0 = rule { keyword(Consts.AS) }
  final def ASC: Rule0 = rule { keyword(Consts.ASC) }
  final def BETWEEN: Rule0 = rule { keyword(Consts.BETWEEN) }
  final def BY: Rule0 = rule { keyword(Consts.BY) }
  final def CASE: Rule0 = rule { keyword(Consts.CASE) }
  final def CAST: Rule0 = rule { keyword(Consts.CAST) }
  final def CREATE: Rule0 = rule { keyword(Consts.CREATE) }
  final def CURRENT: Rule0 = rule { keyword(Consts.CURRENT) }
  final def CURRENT_DATE: Rule0 = rule { keyword(Consts.CURRENT_DATE) }
  final def CURRENT_TIMESTAMP: Rule0 = rule { keyword(Consts.CURRENT_TIMESTAMP) }
  final def DELETE: Rule0 = rule { keyword(Consts.DELETE) }
  final def DESC: Rule0 = rule { keyword(Consts.DESC) }
  final def DISTINCT: Rule0 = rule { keyword(Consts.DISTINCT) }
  final def DROP: Rule0 = rule { keyword(Consts.DROP) }
  final def ELSE: Rule0 = rule { keyword(Consts.ELSE) }
  final def EXCEPT: Rule0 = rule { keyword(Consts.EXCEPT) }
  final def EXISTS: Rule0 = rule { keyword(Consts.EXISTS) }
  final def FALSE: Rule0 = rule { keyword(Consts.FALSE) }
  final def FROM: Rule0 = rule { keyword(Consts.FROM) }
  final def GROUP: Rule0 = rule { keyword(Consts.GROUP) }
  final def HAVING: Rule0 = rule { keyword(Consts.HAVING) }
  final def IN: Rule0 = rule { keyword(Consts.IN) }
  final def INNER: Rule0 = rule { keyword(Consts.INNER) }
  final def INSERT: Rule0 = rule { keyword(Consts.INSERT) }
  final def INTERSECT: Rule0 = rule { keyword(Consts.INTERSECT) }
  final def INTO: Rule0 = rule { keyword(Consts.INTO) }
  final def IS: Rule0 = rule { keyword(Consts.IS) }
  final def JOIN: Rule0 = rule { keyword(Consts.JOIN) }
  final def LEFT: Rule0 = rule { keyword(Consts.LEFT) }
  final def LIKE: Rule0 = rule { keyword(Consts.LIKE) }
  final def NOT: Rule0 = rule { keyword(Consts.NOT) }
  final def NULL: Rule0 = rule { keyword(Consts.NULL) }
  final def ON: Rule0 = rule { keyword(Consts.ON) }
  final def OR: Rule0 = rule { keyword(Consts.OR) }
  final def ORDER: Rule0 = rule { keyword(Consts.ORDER) }
  final def OUTER: Rule0 = rule { keyword(Consts.OUTER) }
  final def RIGHT: Rule0 = rule { keyword(Consts.RIGHT) }
  final def SCHEMA: Rule0 = rule { keyword(Consts.SCHEMA) }
  final def SELECT: Rule0 = rule { keyword(Consts.SELECT) }
  final def SET: Rule0 = rule { keyword(Consts.SET) }
  final def TABLE: Rule0 = rule { keyword(Consts.TABLE) }
  final def THEN: Rule0 = rule { keyword(Consts.THEN) }
  final def TO: Rule0 = rule { keyword(Consts.TO) }
  final def TRUE: Rule0 = rule { keyword(Consts.TRUE) }
  final def UNION: Rule0 = rule { keyword(Consts.UNION) }
  final def UNIQUE: Rule0 = rule { keyword(Consts.UNIQUE) }
  final def UPDATE: Rule0 = rule { keyword(Consts.UPDATE) }
  final def WHEN: Rule0 = rule { keyword(Consts.WHEN) }
  final def WHERE: Rule0 = rule { keyword(Consts.WHERE) }
  final def WITH: Rule0 = rule { keyword(Consts.WITH) }

  // non-reserved keywords
  final def ANTI: Rule0 = rule { keyword(Consts.ANTI) }
  final def CACHE: Rule0 = rule { keyword(Consts.CACHE) }
  final def CLEAR: Rule0 = rule { keyword(Consts.CLEAR) }
  final def CLUSTER: Rule0 = rule { keyword(Consts.CLUSTER) }
  final def COMMENT: Rule0 = rule { keyword(Consts.COMMENT) }
  final def DESCRIBE: Rule0 = rule { keyword(Consts.DESCRIBE) }
  final def DISTRIBUTE: Rule0 = rule { keyword(Consts.DISTRIBUTE) }
  final def END: Rule0 = rule { keyword(Consts.END) }
  final def EXTENDED: Rule0 = rule { keyword(Consts.EXTENDED) }
  final def EXTERNAL: Rule0 = rule { keyword(Consts.EXTERNAL) }
  final def FULL: Rule0 = rule { keyword(Consts.FULL) }
  final def FUNCTION: Rule0 = rule { keyword(Consts.FUNCTION) }
  final def FUNCTIONS: Rule0 = rule { keyword(Consts.FUNCTIONS) }
  final def GLOBAL: Rule0 = rule { keyword(Consts.GLOBAL) }
  final def HASH: Rule0 = rule { keyword(Consts.HASH) }
  final def IF: Rule0 = rule { keyword(Consts.IF) }
  final def INDEX: Rule0 = rule { keyword(Consts.INDEX) }
  final def INIT: Rule0 = rule { keyword(Consts.INIT) }
  final def INTERVAL: Rule0 = rule { keyword(Consts.INTERVAL) }
  final def LAZY: Rule0 = rule { keyword(Consts.LAZY) }
  final def LIMIT: Rule0 = rule { keyword(Consts.LIMIT) }
  final def NATURAL: Rule0 = rule { keyword(Consts.NATURAL) }
  final def OPTIONS: Rule0 = rule { keyword(Consts.OPTIONS) }
  final def OVERWRITE: Rule0 = rule { keyword(Consts.OVERWRITE) }
  final def PARTITION: Rule0 = rule { keyword(Consts.PARTITION) }
  final def PUT: Rule0 = rule { keyword(Consts.PUT) }
  final def REFRESH: Rule0 = rule { keyword(Consts.REFRESH) }
  final def REGEXP: Rule0 = rule { keyword(Consts.REGEXP) }
  final def RLIKE: Rule0 = rule { keyword(Consts.RLIKE) }
  final def SEMI: Rule0 = rule { keyword(Consts.SEMI) }
  final def SHOW: Rule0 = rule { keyword(Consts.SHOW) }
  final def SORT: Rule0 = rule { keyword(Consts.SORT) }
  final def START: Rule0 = rule { keyword(Consts.START) }
  final def STOP: Rule0 = rule { keyword(Consts.STOP) }
  final def STREAM: Rule0 = rule { keyword(Consts.STREAM) }
  final def STREAMING: Rule0 = rule { keyword(Consts.STREAMING) }
  final def TABLES: Rule0 = rule { keyword(Consts.TABLES) }
  final def TEMPORARY: Rule0 = rule { keyword(Consts.TEMPORARY) }
  final def TRUNCATE: Rule0 = rule { keyword(Consts.TRUNCATE) }
  final def UNCACHE: Rule0 = rule { keyword(Consts.UNCACHE) }
  final def USING: Rule0 = rule { keyword(Consts.USING) }

  // Window analytical functions (non-reserved)
  final def DURATION: Rule0 = rule { keyword(Consts.DURATION) }
  final def FOLLOWING: Rule0 = rule { keyword(Consts.FOLLOWING) }
  final def OVER: Rule0 = rule { keyword(Consts.OVER) }
  final def PRECEDING: Rule0 = rule { keyword(Consts.PRECEDING) }
  final def RANGE: Rule0 = rule { keyword(Consts.RANGE) }
  final def ROW: Rule0 = rule { keyword(Consts.ROW) }
  final def ROWS: Rule0 = rule { keyword(Consts.ROWS) }
  final def SLIDE: Rule0 = rule { keyword(Consts.SLIDE) }
  final def UNBOUNDED: Rule0 = rule { keyword(Consts.UNBOUNDED) }
  final def WINDOW: Rule0 = rule { keyword(Consts.WINDOW) }

  // interval units (non-reserved)
  final def DAY: Rule0 = rule { intervalUnit(Consts.DAY) }
  final def HOUR: Rule0 = rule { intervalUnit(Consts.HOUR) }
  final def MICROS: Rule0 = rule { intervalUnit("micro") }
  final def MICROSECOND: Rule0 = rule { intervalUnit(Consts.MICROSECOND) }
  final def MILLIS: Rule0 = rule { intervalUnit("milli") }
  final def MILLISECOND: Rule0 = rule { intervalUnit(Consts.MILLISECOND) }
  final def MINS: Rule0 = rule { intervalUnit("min") }
  final def MINUTE: Rule0 = rule { intervalUnit(Consts.MINUTE) }
  final def MONTH: Rule0 = rule { intervalUnit(Consts.MONTH) }
  final def SECS: Rule0 = rule { intervalUnit("sec") }
  final def SECOND: Rule0 = rule { intervalUnit(Consts.SECOND) }
  final def WEEK: Rule0 = rule { intervalUnit(Consts.WEEK) }
  final def YEAR: Rule0 = rule { intervalUnit(Consts.YEAR) }

  // cube, rollup, grouping sets
  final def CUBE: Rule0 = rule { keyword(Consts.CUBE) }
  final def ROLLUP: Rule0 = rule { keyword(Consts.ROLLUP) }
  final def GROUPING: Rule0 = rule { keyword(Consts.GROUPING) }
  final def SETS: Rule0 = rule { keyword(Consts.SETS) }

  // DDLs, SET, SHOW etc

  final type TableEnd = (Option[String], Option[Map[String, String]],
      Option[LogicalPlan])

  protected def createTable: Rule1[LogicalPlan] = rule {
    CREATE ~ (EXTERNAL ~> trueFn | TEMPORARY ~> falseFn).? ~ TABLE ~
        (IF ~ NOT ~ EXISTS ~> trueFn).? ~ tableIdentifier ~
        tableEnd ~> { (te: Any, notExists: Any, tableIdent: TableIdentifier,
        schemaStr: StringBuilder, remaining: TableEnd) =>

      val tempOrExternal = te.asInstanceOf[Option[Boolean]]
      val ifNotExists = notExists.asInstanceOf[Option[Boolean]]
      val options = remaining._2.getOrElse(Map.empty[String, String])
      val provider = remaining._1.getOrElse(SnappyContext.DEFAULT_SOURCE)
      val allowExisting = ifNotExists.isDefined
      val schemaString = schemaStr.toString().trim

      val hasExternalSchema = if (tempOrExternal.isDefined) false
      else {
        // check if provider class implements ExternalSchemaRelationProvider
        try {
          val clazz: Class[_] = DataSource(session, SnappyContext
              .getProvider(provider, onlyBuiltIn = false)).providingClass
          classOf[ExternalSchemaRelationProvider].isAssignableFrom(clazz)
        } catch {
          case ce: ClassNotFoundException =>
            throw Utils.analysisException(ce.toString)
          case t: Throwable => throw t
        }
      }
      val userSpecifiedSchema = if (hasExternalSchema) None
      else synchronized {
        // parse the schema string expecting Spark SQL format
        val colParser = newInstance()
        colParser.parseSQL(schemaString, colParser.tableSchemaOpt.run())
            .map(StructType(_))
      }
      val schemaDDL = if (hasExternalSchema && schemaString.length > 0) {
        Some(schemaString)
      } else None

      remaining._3 match {
        case Some(queryPlan) =>
          // When IF NOT EXISTS clause appears in the query,
          // the save mode will be ignore.
          val mode = if (allowExisting) SaveMode.Ignore
          else SaveMode.ErrorIfExists
          tempOrExternal match {
            case None =>
              CreateMetastoreTableUsingSelect(tableIdent, None,
                userSpecifiedSchema, schemaDDL, provider, temporary = false,
                Array.empty[String], mode, options, queryPlan, isBuiltIn = true)
            case Some(true) =>
              CreateMetastoreTableUsingSelect(tableIdent, None,
                userSpecifiedSchema, schemaDDL, provider, temporary = false,
                Array.empty[String], mode, options, queryPlan, isBuiltIn = false)
            case Some(false) if remaining._1.isEmpty && remaining._2.isEmpty =>
              CreateMetastoreTableUsingSelect(tableIdent, None,
                userSpecifiedSchema, schemaDDL, provider, temporary = true,
                Array.empty[String], mode, options, queryPlan, isBuiltIn = false)
            case Some(_) => throw Utils.analysisException(
              "CREATE TEMPORARY TABLE ... USING ... does not allow AS query")
          }
        case None =>
          tempOrExternal match {
            case None =>
              CreateMetastoreTableUsing(tableIdent, None, userSpecifiedSchema,
                schemaDDL, provider, allowExisting, options, isBuiltIn = true)
            case Some(true) =>
              CreateMetastoreTableUsing(tableIdent, None, userSpecifiedSchema,
                schemaDDL, provider, allowExisting, options, isBuiltIn = false)
            case Some(false) =>
              CreateTableUsing(tableIdent, userSpecifiedSchema, provider,
                temporary = true, options, Array.empty[String], None,
                allowExisting, managedIfNoPath = false)
          }
      }
    }
  }

  protected final def beforeDDLEnd: Rule0 = rule {
    noneOf("uUoOaA-/")
  }

  protected final def ddlEnd: Rule1[TableEnd] = rule {
    ws ~ (USING ~ qualifiedName).? ~ (OPTIONS ~ options).? ~ (AS ~ query).? ~
        ws ~ &((';' ~ ws).* ~ EOI) ~> ((provider: Any, options: Any,
        asQuery: Any) => (provider, options, asQuery).asInstanceOf[TableEnd])
  }

  protected final def tableEnd1: Rule[StringBuilder :: HNil,
      StringBuilder :: TableEnd :: HNil] = rule {
    ddlEnd.asInstanceOf[Rule[StringBuilder :: HNil,
        StringBuilder :: TableEnd :: HNil]] |
    (capture(ANY ~ beforeDDLEnd.*) ~> ((s: StringBuilder, n: String) =>
      s.append(n))) ~ tableEnd1
  }

  protected final def tableEnd: Rule2[StringBuilder, TableEnd] = rule {
    (capture(beforeDDLEnd.*) ~> ((s: String) =>
      new StringBuilder().append(s))) ~ tableEnd1
  }

  protected def createIndex: Rule1[LogicalPlan] = rule {
    (CREATE ~ (GLOBAL ~ HASH ~> falseFn | UNIQUE ~> trueFn).? ~ INDEX) ~
        tableIdentifier ~ ON ~ tableIdentifier ~
        colsWithDirection ~ (OPTIONS ~ options).? ~> {
      (indexType: Any, indexName: TableIdentifier, tableName: TableIdentifier,
          cols: Map[String, Option[SortDirection]], opts: Any) =>
        val parameters = opts.asInstanceOf[Option[Map[String, String]]]
            .getOrElse(Map.empty[String, String])
        val options = indexType.asInstanceOf[Option[Boolean]] match {
          case Some(false) =>
            parameters + (ExternalStoreUtils.INDEX_TYPE -> "unique")
          case Some(true) =>
            parameters + (ExternalStoreUtils.INDEX_TYPE -> "global hash")
          case None => parameters
        }
        CreateIndex(indexName, tableName, cols, options)
    }
  }

  protected def dropIndex: Rule1[LogicalPlan] = rule {
    DROP ~ INDEX ~ (IF ~ EXISTS ~> trueFn).? ~ tableIdentifier ~>
        ((ifExists: Any, indexName: TableIdentifier) => DropIndex(indexName,
          ifExists.asInstanceOf[Option[Boolean]].isDefined))
  }

  protected def dropTable: Rule1[LogicalPlan] = rule {
    DROP ~ TABLE ~ (IF ~ EXISTS ~> trueFn).? ~ tableIdentifier ~>
        ((ifExists: Any, tableIdent: TableIdentifier) => DropTable(tableIdent,
          ifExists.asInstanceOf[Option[Boolean]].isDefined))
  }

  protected def truncateTable: Rule1[LogicalPlan] = rule {
    TRUNCATE ~ TABLE ~ tableIdentifier ~> TruncateTable
  }

  protected def createStream: Rule1[LogicalPlan] = rule {
    CREATE ~ STREAM ~ TABLE ~ (IF ~ NOT ~ EXISTS ~> trueFn).? ~
        tableIdentifier ~ tableSchema.? ~ USING ~ qualifiedName ~
        OPTIONS ~ options ~> {
      (ifNotExists: Any, streamIdent: TableIdentifier, schema: Any,
          pname: String, opts: Map[String, String]) =>
        val specifiedSchema = schema.asInstanceOf[Option[Seq[StructField]]]
            .map(fields => StructType(fields))
        val provider = SnappyContext.getProvider(pname, onlyBuiltIn = false)
        // check that the provider is a stream relation
        val clazz = DataSource(session, provider).providingClass
        if (!classOf[StreamPlanProvider].isAssignableFrom(clazz)) {
          throw Utils.analysisException(s"CREATE STREAM provider $pname" +
              " does not implement StreamPlanProvider")
        }
        // provider has already been resolved, so isBuiltIn==false allows
        // for both builtin as well as external implementations
        CreateMetastoreTableUsing(streamIdent, None, specifiedSchema, None,
          provider, ifNotExists.asInstanceOf[Option[Boolean]].isDefined,
          opts, isBuiltIn = false)
    }
  }

  protected def streamContext: Rule1[LogicalPlan] = rule {
    STREAMING ~ (
        INIT ~ durationUnit ~> ((batchInterval: Duration) =>
          SnappyStreamingActionsCommand(0, Some(batchInterval))) |
        START ~> (() => SnappyStreamingActionsCommand(1, None)) |
        STOP ~> (() => SnappyStreamingActionsCommand(2, None))
    )
  }

  /*
   * describe [extended] table avroTable
   * This will display all columns of table `avroTable` includes column_name,
   *   column_type,comment
   */
  protected def describeTable: Rule1[LogicalPlan] = rule {
    DESCRIBE ~ (EXTENDED ~> trueFn).? ~ tableIdentifier ~>
        ((extended: Any, tableIdent: TableIdentifier) =>
          DescribeTableCommand(tableIdent, extended
              .asInstanceOf[Option[Boolean]].isDefined, isFormatted = false))
  }

  protected def refreshTable: Rule1[LogicalPlan] = rule {
    REFRESH ~ TABLE ~ tableIdentifier ~> RefreshTable
  }

  protected def cache: Rule1[LogicalPlan] = rule {
    CACHE ~ (LAZY ~> trueFn).? ~ TABLE ~ tableIdentifier ~
        (AS ~ query).? ~> ((isLazy: Any, tableIdent: TableIdentifier,
        plan: Any) => CacheTableCommand(tableIdent,
      plan.asInstanceOf[Option[LogicalPlan]],
      isLazy.asInstanceOf[Option[Boolean]].isDefined))
  }

  protected def uncache: Rule1[LogicalPlan] = rule {
    UNCACHE ~ TABLE ~ tableIdentifier ~> UncacheTableCommand |
    CLEAR ~ CACHE ~> (() => ClearCacheCommand)
  }

  protected def set: Rule1[LogicalPlan] = rule {
    SET ~ (
        CURRENT.? ~ SCHEMA ~ '='.? ~ ws ~ identifier ~>
            ((schemaName: String) => SetSchema(schemaName)) |
        capture(ANY.*) ~> { (rest: String) =>
          val separatorIndex = rest.indexOf('=')
          if (separatorIndex >= 0) {
            val key = rest.substring(0, separatorIndex).trim
            val value = rest.substring(separatorIndex + 1).trim
            SetCommand(Some(key -> Option(value)))
          } else if (rest.nonEmpty) {
            SetCommand(Some(rest.trim -> None))
          } else {
            SetCommand(None)
          }
        }
    )
  }

  // It can be the following patterns:
  // SHOW FUNCTIONS;
  // SHOW FUNCTIONS mydb.func1;
  // SHOW FUNCTIONS func1;
  // SHOW FUNCTIONS `mydb.a`.`func1.aa`;
  protected def show: Rule1[LogicalPlan] = rule {
    SHOW ~ TABLES ~ ((FROM | IN) ~ identifier).? ~> ((ident: Any) =>
      ShowTablesCommand(ident.asInstanceOf[Option[String]], None)) |
    SHOW ~ identifier.? ~ FUNCTIONS ~ LIKE.? ~
        (tableIdentifier | stringLiteral).? ~> { (id: Any, nameOrPat: Any) =>
      val (user, system) = id.asInstanceOf[Option[String]]
          .map(_.toLowerCase) match {
        case None | Some("all") => (true, true)
        case Some("system") => (false, true)
        case Some("user") => (true, false)
        case Some(x) =>
          throw Utils.analysisException(s"SHOW $x FUNCTIONS not supported")
      }
      nameOrPat match {
        case Some(name: TableIdentifier) => ShowFunctionsCommand(
          name.database, Some(name.table), user, system)
        case Some(pat: String) => ShowFunctionsCommand(
          None, Some(ParserUtils.unescapeSQLString(pat)), user, system)
        case None => ShowFunctionsCommand(None, None, user, system)
        case _ => throw Utils.analysisException(
          s"SHOW FUNCTIONS $nameOrPat unexpected")
      }
    }
  }

  protected def desc: Rule1[LogicalPlan] = rule {
    DESCRIBE ~ FUNCTION ~ (EXTENDED ~> trueFn).? ~
        (identifier | stringLiteral) ~> ((extended: Any, name: String) =>
      DescribeFunctionCommand(FunctionIdentifier(name),
        extended.asInstanceOf[Option[Boolean]].isDefined))
  }

  // helper non-terminals

  protected final def sortDirection: Rule1[SortDirection] = rule {
    ASC ~> (() => Ascending) | DESC ~> (() => Descending)
  }

  protected final def colsWithDirection: Rule1[Map[String,
      Option[SortDirection]]] = rule {
    '(' ~ ws ~ (identifier ~ sortDirection.? ~> ((id: Any, direction: Any) =>
      (id, direction))).*(commaSep) ~ ')' ~ ws ~> ((cols: Any) =>
      cols.asInstanceOf[Seq[(String, Option[SortDirection])]].toMap)
  }

  protected final def durationUnit: Rule1[Duration] = rule {
    integral ~ (
        (MILLIS | MILLISECOND) ~> ((s: String) => Milliseconds(s.toInt)) |
        (SECS | SECOND) ~> ((s: String) => Seconds(s.toInt)) |
        (MINS | MINUTE) ~> ((s: String) => Minutes(s.toInt))
    )
  }

  /** the string passed in *SHOULD* be lower case */
  protected final def intervalUnit(k: String): Rule0 = rule {
    atomic(ignoreCase(k) ~ Consts.plural.?) ~ delimiter
  }

  protected final def intervalUnit(k: Keyword): Rule0 = rule {
    atomic(ignoreCase(k.lower) ~ Consts.plural.?) ~ delimiter
  }

  protected final def qualifiedName: Rule1[String] = rule {
    capture((Consts.identifier | '.').*) ~ delimiter
  }

  protected def column: Rule1[StructField] = rule {
    identifier ~ columnDataType ~ ((NOT ~> trueFn).? ~ NULL).? ~
        (COMMENT ~ stringLiteral).? ~> { (columnName: String,
        t: DataType, notNull: Any, cm: Any) =>
      val builder = new MetadataBuilder()
      val (dataType, empty) = t match {
        case CharType(size, baseType) =>
          builder.putLong(Constant.CHAR_TYPE_SIZE_PROP, size)
              .putString(Constant.CHAR_TYPE_BASE_PROP, baseType)
          (StringType, false)
        case _ => (t, true)
      }
      val metadata = cm.asInstanceOf[Option[String]] match {
        case Some(comment) => builder.putString(
          Consts.COMMENT.lower, comment).build()
        case None => if (empty) Metadata.empty else builder.build()
      }
      val notNullOpt = notNull.asInstanceOf[Option[Option[Boolean]]]
      StructField(columnName, dataType, notNullOpt.isEmpty ||
          notNullOpt.get.isEmpty, metadata)
    }
  }

  protected final def tableSchema: Rule1[Seq[StructField]] = rule {
    '(' ~ ws ~ (column + commaSep) ~ ')' ~ ws
  }

  protected final def tableSchemaOpt: Rule1[Option[Seq[StructField]]] = rule {
    (tableSchema ~> (Some(_)) | ws ~> (() => None)).named("tableSchema") ~ EOI
  }

  protected final def pair: Rule1[(String, String)] = rule {
    qualifiedName ~ stringLiteral ~ ws ~> ((k: String, v: String) => k -> v)
  }

  protected final def options: Rule1[Map[String, String]] = rule {
    '(' ~ ws ~ (pair * commaSep) ~ ')' ~ ws ~>
        ((pairs: Any) => pairs.asInstanceOf[Seq[(String, String)]].toMap)
  }

  protected def ddl: Rule1[LogicalPlan] = rule {
    createTable | describeTable | refreshTable | dropTable | truncateTable |
    createStream | streamContext | createIndex | dropIndex
  }

  protected def query: Rule1[LogicalPlan]

  protected def parseSQL[T](sqlText: String, parseRule: => Try[T]): T

  protected def newInstance(): SnappyDDLParser
}

private[sql] case class CreateMetastoreTableUsing(
    tableIdent: TableIdentifier,
    baseTable: Option[TableIdentifier],
    userSpecifiedSchema: Option[StructType],
    schemaDDL: Option[String],
    provider: String,
    allowExisting: Boolean,
    options: Map[String, String],
    isBuiltIn: Boolean) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
    snc.createTable(snc.sessionState.catalog
        .newQualifiedTableName(tableIdent), provider, userSpecifiedSchema,
      schemaDDL, mode, snc.addBaseTableOption(baseTable, options), isBuiltIn)
    Seq.empty
  }
}

private[sql] case class CreateMetastoreTableUsingSelect(
    tableIdent: TableIdentifier,
    baseTable: Option[TableIdentifier],
    userSpecifiedSchema: Option[StructType],
    schemaDDL: Option[String],
    provider: String,
    temporary: Boolean,
    partitionColumns: Array[String],
    mode: SaveMode,
    options: Map[String, String],
    query: LogicalPlan,
    isBuiltIn: Boolean) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.catalog
    if (temporary) {
      // the equivalent of a registerTempTable of a DataFrame
      if (tableIdent.database.isDefined) {
        throw Utils.analysisException(
          s"Temporary table '$tableIdent' should not have specified a database")
      }
      Dataset.ofRows(session, query).createTempView(tableIdent.table)
    } else {
      snc.createTable(catalog.newQualifiedTableName(tableIdent), provider,
        userSpecifiedSchema, schemaDDL, partitionColumns, mode,
        snc.addBaseTableOption(baseTable, options), query, isBuiltIn)
    }
    Seq.empty
  }
}

private[sql] case class DropTable(
    tableIdent: TableIdentifier,
    ifExists: Boolean) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.catalog
    snc.dropTable(catalog.newQualifiedTableName(tableIdent), ifExists)
    Seq.empty
  }
}

private[sql] case class TruncateTable(
    tableIdent: TableIdentifier) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.catalog
    snc.truncateTable(catalog.newQualifiedTableName(tableIdent))
    Seq.empty
  }
}

private[sql] case class CreateIndex(indexName: TableIdentifier,
    baseTable: TableIdentifier,
    indexColumns: Map[String, Option[SortDirection]],
    options: Map[String, String]) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.catalog
    val tableIdent = catalog.newQualifiedTableName(baseTable)
    val indexIdent = catalog.newQualifiedTableName(indexName)
    snc.createIndex(indexIdent, tableIdent, indexColumns, options)
    Seq.empty
  }
}

private[sql] case class DropIndex(
    indexName: TableIdentifier,
    ifExists: Boolean) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {
    val snc = session.asInstanceOf[SnappySession]
    val catalog = snc.sessionState.catalog
    val indexIdent = catalog.newQualifiedTableName(indexName)
    snc.dropIndex(indexIdent, ifExists)
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

private[sql] case class SetSchema(schemaName: String) extends RunnableCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.asInstanceOf[SnappySession].setSchema(schemaName)
    Seq.empty[Row]
  }
}

private[sql] case class SnappyStreamingActionsCommand(action: Int,
    batchInterval: Option[Duration]) extends RunnableCommand {

  override def run(session: SparkSession): Seq[Row] = {

    def creatingFunc(): SnappyStreamingContext = {
      // batchInterval will always be defined when action == 0
      new SnappyStreamingContext(session.sparkContext, batchInterval.get)
    }

    action match {
      case 0 =>
        val ssc = SnappyStreamingContext.getInstance()
        ssc match {
          case Some(x) => // TODO .We should create a named Streaming
          // Context and check if the configurations match
          case None => SnappyStreamingContext.getActiveOrCreate(creatingFunc)
        }
      case 1 =>
        val ssc = SnappyStreamingContext.getInstance()
        ssc match {
          case Some(x) => x.start()
          case None => throw Utils.analysisException(
            "Streaming Context has not been initialized")
        }
      case 2 =>
        val ssc = SnappyStreamingContext.getActive
        ssc match {
          case Some(strCtx) => strCtx.stop(stopSparkContext = false,
            stopGracefully = true)
          case None => // throw Utils.analysisException(
          // "There is no running Streaming Context to be stopped")
        }
    }
    Seq.empty[Row]
  }
}
