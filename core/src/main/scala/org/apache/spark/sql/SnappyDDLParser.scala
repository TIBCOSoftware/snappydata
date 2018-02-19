/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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


import java.io.File

import scala.util.Try

import io.snappydata.Constant
import org.parboiled2._
import shapeless.{::, HNil}

import org.apache.spark.sql.catalyst.catalog.{FunctionResource, FunctionResourceType}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.ParserUtils
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTempViewUsing, DataSource, LogicalRelation, RefreshTable}
import org.apache.spark.sql.sources.{ExternalSchemaRelationProvider, JdbcExtendedUtils}
import org.apache.spark.sql.streaming.StreamPlanProvider
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SnappyParserConsts => Consts}
import org.apache.spark.streaming._

abstract class SnappyDDLParser(session: SparkSession)
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
  final def GRANT: Rule0 = rule { keyword(Consts.GRANT) }
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
  final def REVOKE: Rule0 = rule { keyword(Consts.REVOKE) }
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
  final def ADD: Rule0 = rule { keyword(Consts.ADD) }
  final def ALTER: Rule0 = rule { keyword(Consts.ALTER) }
  final def ANTI: Rule0 = rule { keyword(Consts.ANTI) }
  final def CACHE: Rule0 = rule { keyword(Consts.CACHE) }
  final def CLEAR: Rule0 = rule { keyword(Consts.CLEAR) }
  final def CLUSTER: Rule0 = rule { keyword(Consts.CLUSTER) }
  final def COLUMN: Rule0 = rule { keyword(Consts.COLUMN) }
  final def COMMENT: Rule0 = rule { keyword(Consts.COMMENT) }
  final def DESCRIBE: Rule0 = rule { keyword(Consts.DESCRIBE) }
  final def DISTRIBUTE: Rule0 = rule { keyword(Consts.DISTRIBUTE) }
  final def END: Rule0 = rule { keyword(Consts.END) }
  final def EXTENDED: Rule0 = rule { keyword(Consts.EXTENDED) }
  final def EXTERNAL: Rule0 = rule { keyword(Consts.EXTERNAL) }
  final def FETCH: Rule0 = rule { keyword(Consts.FETCH) }
  final def FIRST: Rule0 = rule { keyword(Consts.FIRST) }
  final def FN: Rule0 = rule { keyword(Consts.FN) }
  final def FULL: Rule0 = rule { keyword(Consts.FULL) }
  final def FUNCTION: Rule0 = rule { keyword(Consts.FUNCTION) }
  final def FUNCTIONS: Rule0 = rule { keyword(Consts.FUNCTIONS) }
  final def GLOBAL: Rule0 = rule { keyword(Consts.GLOBAL) }
  final def HASH: Rule0 = rule { keyword(Consts.HASH) }
  final def IF: Rule0 = rule { keyword(Consts.IF) }
  final def INDEX: Rule0 = rule { keyword(Consts.INDEX) }
  final def INIT: Rule0 = rule { keyword(Consts.INIT) }
  final def INTERVAL: Rule0 = rule { keyword(Consts.INTERVAL) }
  final def LAST: Rule0 = rule { keyword(Consts.LAST) }
  final def LAZY: Rule0 = rule { keyword(Consts.LAZY) }
  final def LIMIT: Rule0 = rule { keyword(Consts.LIMIT) }
  final def NATURAL: Rule0 = rule { keyword(Consts.NATURAL) }
  final def NULLS: Rule0 = rule { keyword(Consts.NULLS) }
  final def ONLY: Rule0 = rule { keyword(Consts.ONLY) }
  final def OPTIONS: Rule0 = rule { keyword(Consts.OPTIONS) }
  final def OVERWRITE: Rule0 = rule { keyword(Consts.OVERWRITE) }
  final def PARTITION: Rule0 = rule { keyword(Consts.PARTITION) }
  final def PUT: Rule0 = rule { keyword(Consts.PUT) }
  final def REFRESH: Rule0 = rule { keyword(Consts.REFRESH) }
  final def REGEXP: Rule0 = rule { keyword(Consts.REGEXP) }
  final def REPLACE: Rule0 = rule { keyword(Consts.REPLACE) }
  final def RETURNS: Rule0 = rule { keyword(Consts.RETURNS) }
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
  final def VALUES: Rule0 = rule { keyword(Consts.VALUES) }
  final def VIEW: Rule0 = rule { keyword(Consts.VIEW) }

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

  // cube, rollup, grouping sets etc
  final def CUBE: Rule0 = rule { keyword(Consts.CUBE) }
  final def ROLLUP: Rule0 = rule { keyword(Consts.ROLLUP) }
  final def GROUPING: Rule0 = rule { keyword(Consts.GROUPING) }
  final def SETS: Rule0 = rule { keyword(Consts.SETS) }
  final def LATERAL: Rule0 = rule { keyword(Consts.LATERAL) }

  // DDLs, SET, SHOW etc

  final type TableEnd = (Option[String], Option[Map[String, String]],
      Option[LogicalPlan])

  protected final def ifNotExists: Rule1[Boolean] = rule {
    (IF ~ NOT ~ EXISTS ~ push(true)).? ~> ((o: Any) => o != None)
  }

  protected final def ifExists: Rule1[Boolean] = rule {
    (IF ~ EXISTS ~ push(true)).? ~> ((o: Any) => o != None)
  }

  protected final def identifierWithComment: Rule1[(String, Option[String])] = rule {
    identifier ~ (COMMENT ~ stringLiteral).? ~>
        ((id: String, cm: Any) => id -> cm.asInstanceOf[Option[String]])
  }

  protected def createTable: Rule1[LogicalPlan] = rule {
    CREATE ~ (EXTERNAL ~ push(true)).? ~ TABLE ~ ifNotExists ~
        tableIdentifier ~ tableEnd ~> { (external: Any, allowExisting: Boolean,
        tableIdent: TableIdentifier, schemaStr: StringBuilder, remaining: TableEnd) =>

      val options = remaining._2.getOrElse(Map.empty[String, String])
      val provider = remaining._1.getOrElse(Consts.DEFAULT_SOURCE)
      val schemaString = schemaStr.toString().trim

      val hasExternalSchema = if (external != None) false
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
          val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
          external match {
            case Some(true) =>
              CreateTableUsingSelect(tableIdent, None,
                userSpecifiedSchema, schemaDDL, provider,
                Array.empty[String], mode, options, queryPlan, isBuiltIn = false)
            case _ =>
              CreateTableUsingSelect(tableIdent, None,
                userSpecifiedSchema, schemaDDL, provider,
                Array.empty[String], mode, options, queryPlan, isBuiltIn = true)
          }
        case None =>
          external match {
            case Some(true) =>
              CreateTableUsing(tableIdent, None, userSpecifiedSchema,
                schemaDDL, provider, allowExisting, options, isBuiltIn = false)
            case _ =>
              CreateTableUsing(tableIdent, None, userSpecifiedSchema,
                schemaDDL, provider, allowExisting, options, isBuiltIn = true)
          }
      }
    }
  }

  protected final def beforeDDLEnd: Rule0 = rule {
    noneOf("uUoOaA-;/")
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
    (CREATE ~ (GLOBAL ~ HASH ~ push(false) | UNIQUE ~ push(true)).? ~ INDEX) ~
        tableIdentifier ~ ON ~ tableIdentifier ~
        colsWithDirection ~ (OPTIONS ~ options).? ~> {
      (indexType: Any, indexName: TableIdentifier, tableName: TableIdentifier,
          cols: Map[String, Option[SortDirection]], opts: Any) =>
        val parameters = opts.asInstanceOf[Option[Map[String, String]]]
            .getOrElse(Map.empty[String, String])
        val options = indexType.asInstanceOf[Option[Boolean]] match {
          case Some(false) =>
            parameters + (ExternalStoreUtils.INDEX_TYPE -> "global hash")
          case Some(true) =>
            parameters + (ExternalStoreUtils.INDEX_TYPE -> "unique")
          case None => parameters
        }
        CreateIndex(indexName, tableName, cols, options)
    }
  }

  protected final def globalOrTemporary: Rule1[Boolean] = rule {
    (GLOBAL ~ push(true)).? ~ TEMPORARY ~> ((g: Any) => g != None)
  }

  protected def createView: Rule1[LogicalPlan] = rule {
    CREATE ~ (OR ~ REPLACE ~ push(true)).? ~ (globalOrTemporary.? ~ VIEW |
        globalOrTemporary ~ TABLE) ~ ifNotExists ~ tableIdentifier ~
        ('(' ~ ws ~ (identifierWithComment + commaSep) ~ ')' ~ ws).? ~
        (COMMENT ~ stringLiteral).? ~ AS ~ capture(query) ~> { (replace: Any, gt: Any,
        allowExisting: Boolean, table: TableIdentifier, cols: Any, comment: Any,
        plan: LogicalPlan, queryStr: String) =>

      val viewType = gt match {
        case Some(true) | true => GlobalTempView
        case Some(false) | false => LocalTempView
        case _ => PersistedView
      }
      val userCols = cols.asInstanceOf[Option[Seq[(String, Option[String])]]] match {
        case Some(seq) => seq
        case None => Nil
      }
      CreateViewCommand(
        name = table,
        userSpecifiedColumns = userCols,
        comment = comment.asInstanceOf[Option[String]],
        properties = Map.empty,
        originalText = Option(queryStr),
        child = plan,
        allowExisting = allowExisting,
        replace = replace != None,
        viewType = viewType)
    }
  }

  protected def createTempViewUsing: Rule1[LogicalPlan] = rule {
    CREATE ~ (OR ~ REPLACE ~ push(true)).? ~ globalOrTemporary ~ (VIEW | TABLE) ~
        tableIdentifier ~ tableSchema.? ~ USING ~ qualifiedName ~
        (OPTIONS ~ options).? ~> ((replace: Any, global: Boolean, tableIdent: TableIdentifier,
        schema: Any, provider: String, options: Any) => CreateTempViewUsing(
      tableIdent = tableIdent,
      userSpecifiedSchema = schema.asInstanceOf[Option[Seq[StructField]]].map(StructType(_)),
      replace = replace != None,
      global = global,
      provider = provider,
      options = options.asInstanceOf[Option[Map[String, String]]].getOrElse(Map.empty)))
  }

  protected def dropIndex: Rule1[LogicalPlan] = rule {
    DROP ~ INDEX ~ ifExists ~ tableIdentifier ~> DropIndex
  }

  protected def dropTable: Rule1[LogicalPlan] = rule {
    DROP ~ (TABLE ~ push(false)) ~ ifExists ~ tableIdentifier ~> DropTableOrView
  }

  protected def dropView: Rule1[LogicalPlan] = rule {
    DROP ~ (VIEW ~ push(true)) ~ ifExists ~ tableIdentifier ~> DropTableOrView
  }

  protected def truncateTable: Rule1[LogicalPlan] = rule {
    TRUNCATE ~ TABLE ~ ifExists ~ tableIdentifier ~> TruncateManagedTable
  }

  protected def alterTableAddColumn: Rule1[LogicalPlan] = rule {
    ALTER ~ TABLE ~ tableIdentifier ~ ADD  ~ COLUMN.? ~ column  ~> AlterTableAddColumn
  }

  protected def alterTableDropColumn: Rule1[LogicalPlan] = rule {
    ALTER ~ TABLE ~ tableIdentifier ~ DROP  ~ COLUMN.? ~ qualifiedName ~> AlterTableDropColumn
  }

  protected def createStream: Rule1[LogicalPlan] = rule {
    CREATE ~ STREAM ~ TABLE ~ ifNotExists ~ tableIdentifier ~ tableSchema.? ~
        USING ~ qualifiedName ~ OPTIONS ~ options ~> {
      (allowExisting: Boolean, streamIdent: TableIdentifier, schema: Any,
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
        CreateTableUsing(streamIdent, None, specifiedSchema, None,
          provider, allowExisting, opts, isBuiltIn = false)
    }
  }

  protected final def resourceType: Rule1[FunctionResource] = rule {
    identifier ~ stringLiteral ~> { (rType: String, path: String) =>
      val resourceType = rType.toLowerCase
      resourceType match {
        case "jar" =>
          FunctionResource(FunctionResourceType.fromString(resourceType), path)
        case _ =>
          throw Utils.analysisException(s"CREATE FUNCTION with resource type '$resourceType'")
      }
    }
  }

  def checkExists(resource: FunctionResource): Unit = {
    if (!new File(resource.uri).exists()) {
      throw new AnalysisException(s"No file named ${resource.uri} exists")
    }
  }

  /**
   * Create a [[CreateFunctionCommand]] command.
   *
   * For example:
   * {{{
   *   CREATE [TEMPORARY] FUNCTION [db_name.]function_name AS class_name RETURNS ReturnType
   *    USING JAR 'file_uri';
   * }}}
   */
  protected def createFunction: Rule1[LogicalPlan] = rule {
    CREATE ~ (TEMPORARY ~ push(true)).? ~ FUNCTION ~ functionIdentifier ~ AS ~
        qualifiedName ~ RETURNS ~ columnDataType ~ USING ~ resourceType ~>
        { (te: Any, functionIdent: FunctionIdentifier, className: String,
            t: DataType, funcResource : FunctionResource) =>

          val isTemp = te.asInstanceOf[Option[Boolean]].isDefined
          val funcResources = Seq(funcResource)
          funcResources.foreach(checkExists)
          val classNameWithType = className + "__" + t.catalogString
          CreateFunctionCommand(
            functionIdent.database,
            functionIdent.funcName,
            classNameWithType,
            funcResources,
            isTemp)
        }
  }


  /**
   * Create a [[DropFunctionCommand]] command.
   *
   * For example:
   * {{{
   *   DROP [TEMPORARY] FUNCTION [IF EXISTS] function;
   * }}}
   */
  protected def dropFunction: Rule1[LogicalPlan] = rule {
    DROP ~ (TEMPORARY ~ push(true)).? ~ FUNCTION ~ ifExists ~ functionIdentifier ~>
        ((te: Any, ifExists: Boolean, functionIdent: FunctionIdentifier) => DropFunctionCommand(
          functionIdent.database,
          functionIdent.funcName,
          ifExists = ifExists,
          isTemp = te.asInstanceOf[Option[Boolean]].isDefined))
  }

  /**
   * GRANT/REVOKE on a table (only for column and row tables).
   *
   * Example:
   * {{{
   *   GRANT SELECT ON table TO user1, user2;
   *   GRANT INSERT ON table TO ldapGroup: group1;
   * }}}
   */
  protected def grantRevoke: Rule1[LogicalPlan] = rule {
    (GRANT | REVOKE) ~ ANY.* ~> (() => DMLExternalTable(
      TableIdentifier("SYSDUMMY1", Some("SYSIBM")) /* dummy table */ ,
      LogicalRelation(new execution.row.DefaultSource().createRelation(session.sqlContext,
        SaveMode.Ignore, Map(JdbcExtendedUtils.DBTABLE_PROPERTY -> "SYSIBM.SYSDUMMY1"),
        "", None)), input.sliceString(0, input.length)))
  }

  protected def streamContext: Rule1[LogicalPlan] = rule {
    STREAMING ~ (
        INIT ~ durationUnit ~> ((batchInterval: Duration) =>
          SnappyStreamingActions(0, Some(batchInterval))) |
        START ~> (() => SnappyStreamingActions(1, None)) |
        STOP ~> (() => SnappyStreamingActions(2, None))
    )
  }

  /*
   * describe [extended] table avroTable
   * This will display all columns of table `avroTable` includes column_name,
   *   column_type,comment
   */
  protected def describeTable: Rule1[LogicalPlan] = rule {
    DESCRIBE ~ (EXTENDED ~ push(true)).? ~ tableIdentifier ~>
        ((extended: Any, tableIdent: TableIdentifier) =>
          DescribeTableCommand(tableIdent, Map.empty[String, String], extended
              .asInstanceOf[Option[Boolean]].isDefined, isFormatted = false))
  }

  protected def refreshTable: Rule1[LogicalPlan] = rule {
    REFRESH ~ TABLE ~ tableIdentifier ~> RefreshTable
  }

  protected def cache: Rule1[LogicalPlan] = rule {
    CACHE ~ (LAZY ~ push(true)).? ~ TABLE ~ tableIdentifier ~
        (AS ~ query).? ~> ((isLazy: Any, tableIdent: TableIdentifier,
        plan: Any) => CacheTableCommand(tableIdent,
      plan.asInstanceOf[Option[LogicalPlan]],
      isLazy.asInstanceOf[Option[Boolean]].isDefined))
  }

  protected def uncache: Rule1[LogicalPlan] = rule {
    UNCACHE ~ TABLE ~ ifExists ~ tableIdentifier ~>
        ((ifExists: Boolean, tableIdent: TableIdentifier) =>
          UncacheTableCommand(tableIdent, ifExists)) |
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
        (functionIdentifier | stringLiteral).? ~> { (id: Any, nameOrPat: Any) =>
      val (user, system) = id.asInstanceOf[Option[String]]
          .map(_.toLowerCase) match {
        case None | Some("all") => (true, true)
        case Some("system") => (false, true)
        case Some("user") => (true, false)
        case Some(x) =>
          throw Utils.analysisException(s"SHOW $x FUNCTIONS not supported")
      }
      nameOrPat match {
        case Some(name: FunctionIdentifier) => ShowFunctionsCommand(
          name.database, Some(name.funcName), user, system)
        case Some(pat: String) => ShowFunctionsCommand(
          None, Some(ParserUtils.unescapeSQLString(pat)), user, system)
        case None => ShowFunctionsCommand(None, None, user, system)
        case _ => throw Utils.analysisException(
          s"SHOW FUNCTIONS $nameOrPat unexpected")
      }
    }
  }

  protected def desc: Rule1[LogicalPlan] = rule {
    DESCRIBE ~ FUNCTION ~ (EXTENDED ~ push(true)).? ~
        functionIdentifier ~> ((extended: Any, name: FunctionIdentifier) =>
      DescribeFunctionCommand(name,
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
    (unquotedIdentifier + ('.' ~ ws)) ~>
        ((ids: Seq[String]) => ids.mkString(".")) |
    quotedIdentifier
  }

  protected def column: Rule1[StructField] = rule {
    identifier ~ columnDataType ~ ((NOT ~ push(true)).? ~ NULL).? ~
        (COMMENT ~ stringLiteral).? ~> { (columnName: String,
        t: DataType, notNull: Any, cm: Any) =>
      val builder = new MetadataBuilder()
      val (dataType, empty) = t match {
        case CharStringType(size, baseType) =>
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
    createView | createTempViewUsing | dropView |
    alterTableAddColumn | alterTableDropColumn | createStream | streamContext |
    createIndex | dropIndex | createFunction | dropFunction | grantRevoke | show
  }

  protected def query: Rule1[LogicalPlan]

  protected def parseSQL[T](sqlText: String, parseRule: => Try[T]): T

  protected def newInstance(): SnappyDDLParser
}

case class CreateTableUsing(
    tableIdent: TableIdentifier,
    baseTable: Option[TableIdentifier],
    userSpecifiedSchema: Option[StructType],
    schemaDDL: Option[String],
    provider: String,
    allowExisting: Boolean,
    options: Map[String, String],
    isBuiltIn: Boolean) extends Command

case class CreateTableUsingSelect(
    tableIdent: TableIdentifier,
    baseTable: Option[TableIdentifier],
    userSpecifiedSchema: Option[StructType],
    schemaDDL: Option[String],
    provider: String,
    partitionColumns: Array[String],
    mode: SaveMode,
    options: Map[String, String],
    query: LogicalPlan,
    isBuiltIn: Boolean) extends Command

case class DropTableOrView(isView: Boolean, ifExists: Boolean,
    tableIdent: TableIdentifier) extends Command

case class TruncateManagedTable(ifExists: Boolean, tableIdent: TableIdentifier) extends Command

case class AlterTableAddColumn(tableIdent: TableIdentifier, addColumn: StructField)
    extends Command

case class AlterTableDropColumn(tableIdent: TableIdentifier, column: String) extends Command

case class CreateIndex(indexName: TableIdentifier,
    baseTable: TableIdentifier,
    indexColumns: Map[String, Option[SortDirection]],
    options: Map[String, String]) extends Command

case class DropIndex(ifExists: Boolean, indexName: TableIdentifier) extends Command

case class DMLExternalTable(
    tableName: TableIdentifier,
    query: LogicalPlan,
    command: String)
    extends LeafNode with Command {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override lazy val resolved: Boolean = query.resolved
  override lazy val output: Seq[Attribute] = AttributeReference("count", IntegerType)() :: Nil
}

case class SetSchema(schemaName: String) extends Command

case class SnappyStreamingActions(action: Int, batchInterval: Option[Duration]) extends Command
