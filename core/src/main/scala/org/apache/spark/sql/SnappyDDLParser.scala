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
import java.lang
import java.nio.file.{Files, Paths}
import java.util.Map.Entry
import java.util.function.Consumer

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import com.gemstone.gemfire.SystemFailure
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.iapi.util.IdUtil
import io.snappydata.Constant
import org.parboiled2._
import shapeless.{::, HNil}

import org.apache.spark.deploy.SparkSubmitUtils
import org.apache.spark.sql.catalyst.catalog.{FunctionResource, FunctionResourceType}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.{FunctionIdentifier, SQLBuilder, TableIdentifier}
import org.apache.spark.sql.collection.{ToolsCallbackInit, Utils}
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTempViewUsing, DataSource, LogicalRelation, RefreshTable}
import org.apache.spark.sql.hive.QualifiedTableName
import org.apache.spark.sql.internal.{BypassRowLevelSecurity, MarkerForCreateTableAsSelect}
import org.apache.spark.sql.policy.PolicyProperties
import org.apache.spark.sql.sources.{ExternalSchemaRelationProvider, JdbcExtendedUtils}
import org.apache.spark.sql.streaming.StreamPlanProvider
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SnappyParserConsts => Consts}
import org.apache.spark.streaming._

import scala.util.control.NonFatal

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
  final def POLICY: Rule0 = rule { keyword(Consts.POLICY) }
  final def CURRENT: Rule0 = rule { keyword(Consts.CURRENT) }
  final def CURRENT_DATE: Rule0 = rule { keyword(Consts.CURRENT_DATE) }
  final def CURRENT_TIMESTAMP: Rule0 = rule { keyword(Consts.CURRENT_TIMESTAMP) }
  final def DELETE: Rule0 = rule { keyword(Consts.DELETE) }
  final def DESC: Rule0 = rule { keyword(Consts.DESC) }
  final def DEPLOY: Rule0 = rule { keyword(Consts.DEPLOY) }
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
  final def PACKAGE: Rule0 = rule { keyword(Consts.PACKAGE) }
  final def PATH: Rule0 = rule { keyword(Consts.PATH) }
  final def REPOS: Rule0 = rule { keyword(Consts.REPOS) }
  final def REVOKE: Rule0 = rule { keyword(Consts.REVOKE) }
  final def RIGHT: Rule0 = rule { keyword(Consts.RIGHT) }
  final def SCHEMA: Rule0 = rule { keyword(Consts.SCHEMA) }
  final def SELECT: Rule0 = rule { keyword(Consts.SELECT) }
  final def SET: Rule0 = rule { keyword(Consts.SET) }
  final def TABLE: Rule0 = rule { keyword(Consts.TABLE) }
  final def THEN: Rule0 = rule { keyword(Consts.THEN) }
  final def TO: Rule0 = rule { keyword(Consts.TO) }
  final def TRUE: Rule0 = rule { keyword(Consts.TRUE) }
  final def UNDEPLOY: Rule0 = rule { keyword(Consts.UNDEPLOY) }
  final def UNION: Rule0 = rule { keyword(Consts.UNION) }
  final def UNIQUE: Rule0 = rule { keyword(Consts.UNIQUE) }
  final def UPDATE: Rule0 = rule { keyword(Consts.UPDATE) }
  final def WHEN: Rule0 = rule { keyword(Consts.WHEN) }
  final def WHERE: Rule0 = rule { keyword(Consts.WHERE) }
  final def WITH: Rule0 = rule { keyword(Consts.WITH) }
  final def USER: Rule0 = rule { keyword(Consts.USER) }

  // non-reserved keywords
  final def MINUS: Rule0 = rule { keyword(Consts.MINUS) }
  final def RESET: Rule0 = rule { keyword(Consts.RESET) }
  final def ADD: Rule0 = rule { keyword(Consts.ADD) }
  final def ALTER: Rule0 = rule { keyword(Consts.ALTER) }
  final def ANTI: Rule0 = rule { keyword(Consts.ANTI) }
  final def CACHE: Rule0 = rule { keyword(Consts.CACHE) }
  final def CALL: Rule0 = rule{ keyword(Consts.CALL) }
  final def CLEAR: Rule0 = rule { keyword(Consts.CLEAR) }
  final def CLUSTER: Rule0 = rule { keyword(Consts.CLUSTER) }
  final def COLUMN: Rule0 = rule { keyword(Consts.COLUMN) }
  final def COMMENT: Rule0 = rule { keyword(Consts.COMMENT) }
  final def DESCRIBE: Rule0 = rule { keyword(Consts.DESCRIBE) }
  final def DISTRIBUTE: Rule0 = rule { keyword(Consts.DISTRIBUTE) }
  final def DISK_STORE: Rule0 = rule { keyword(Consts.DISK_STORE) }
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
  final def JAR: Rule0 = rule { keyword(Consts.JAR) }
  final def JARS: Rule0 = rule { keyword(Consts.JARS) }
  final def LAST: Rule0 = rule { keyword(Consts.LAST) }
  final def LAZY: Rule0 = rule { keyword(Consts.LAZY) }
  final def LIMIT: Rule0 = rule { keyword(Consts.LIMIT) }
  final def LIST: Rule0 = rule { keyword(Consts.LIST) }
  final def NATURAL: Rule0 = rule { keyword(Consts.NATURAL) }
  final def NULLS: Rule0 = rule { keyword(Consts.NULLS) }
  final def ONLY: Rule0 = rule { keyword(Consts.ONLY) }
  final def OPTIONS: Rule0 = rule { keyword(Consts.OPTIONS) }
  final def OVERWRITE: Rule0 = rule { keyword(Consts.OVERWRITE) }
  final def PACKAGES: Rule0 = rule { keyword(Consts.PACKAGES) }
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
  final def FOR: Rule0 = rule { keyword(Consts.FOR) }
  final def ENABLE: Rule0 = rule { keyword(Consts.ENABLE) }
  final def DISABLE: Rule0 = rule { keyword(Consts.DISABLE) }
  final def LEVEL: Rule0 = rule { keyword(Consts.LEVEL) }
  final def SECURITY: Rule0 = rule { keyword(Consts.SECURITY) }
  final def LDAPGROUP: Rule0 = rule { keyword(Consts.LDAPGROUP) }

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

  private val CURRENT_USER = "CURRENT_USER"

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
            throw Utils.analysisException(ce.toString, Some(ce))
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
                Array.empty[String], mode, options, MarkerForCreateTableAsSelect(queryPlan),
                isBuiltIn = false)
            case _ =>
              CreateTableUsingSelect(tableIdent, None,
                userSpecifiedSchema, schemaDDL, provider,
                Array.empty[String], mode, options, MarkerForCreateTableAsSelect(queryPlan),
                isBuiltIn = true)
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

  protected final def policyFor: Rule1[String] = rule {
    (FOR ~ capture(ALL | SELECT | UPDATE | INSERT | DELETE)).? ~> ((forOpt: Any) =>
      forOpt match {
        case Some(v) => v.asInstanceOf[String].trim
        case None => SnappyParserConsts.SELECT.upper
      })
  }

  protected final def policyTo: Rule1[Seq[String]] = rule {
    (TO ~
        (CURRENT ~ '_' ~ USER ~ push(CURRENT_USER)  |
            (LDAPGROUP ~ ws ~ ':' ~ ws ~
                push(SnappyParserConsts.LDAPGROUP.upper + ':')).? ~
                identifier ~ ws ~> {(ldapOpt: Any, x) =>
              ldapOpt.asInstanceOf[Option[String]].map(_ + x).getOrElse(x)}
        ).+ (commaSep) ~> {
        (policyTo: Any) => policyTo.asInstanceOf[Seq[String]].map(_.trim)
          }).? ~> { (toOpt: Any) =>
      toOpt match {
        case Some(x) => x.asInstanceOf[Seq[String]]
        case _ => Seq(CURRENT_USER)
      }
    }

  }

  protected def createPolicy: Rule1[LogicalPlan] = rule {
    (CREATE ~ POLICY) ~ tableIdentifier ~ ON ~ tableIdentifier ~ policyFor ~
        policyTo ~ USING ~ capture(expression) ~> { (policyName: TableIdentifier,
        tableName: TableIdentifier, policyFor: String,
        applyTo: Seq[String], filterExp: Expression, filterStr: String) => {
      val snappySession = session.asInstanceOf[SnappySession]
      val tableIdent = snappySession.sessionState.catalog.
          newQualifiedTableName(tableName)
      val applyToAll = applyTo.exists(_.equalsIgnoreCase(CURRENT_USER))
      val expandedApplyTo = if (applyToAll) {
        Seq.empty[String]
      } else {
        import scala.collection.JavaConverters._
        ExternalStoreUtils.getExpandedGranteesIterator(applyTo).toSeq
      }

      var currentUser = this.session.conf.get(com.pivotal.gemfirexd.Attribute.USERNAME_ATTR, "")

      currentUser = IdUtil.getUserAuthorizationId(
        if (currentUser.isEmpty) Constant.DEFAULT_SCHEMA
        else snappySession.sessionState.catalog.formatDatabaseName(currentUser))

      val policyIdent = snappySession.sessionState.catalog
          .newQualifiedTableName(policyName)
      val filter = PolicyProperties.createFilterPlan(filterExp, tableIdent, currentUser,
        expandedApplyTo)
      CreatePolicy(policyIdent, tableIdent, policyFor, applyTo, expandedApplyTo, currentUser,
        filterStr, filter)
    }
    }
  }

  protected def dropPolicy: Rule1[LogicalPlan] = rule {
    DROP ~ POLICY ~ ifExists ~ tableIdentifier ~> DropPolicy
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
      CreateSnappyViewCommand(
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

  protected def alterTableToggleRowLevelSecurity: Rule1[LogicalPlan] = rule {
    ALTER ~ TABLE ~ tableIdentifier ~ ((ENABLE ~ push(true)) | (DISABLE ~ push(false))) ~
        ROW ~ LEVEL ~ SECURITY ~> {
      (tableName: TableIdentifier, enbableRLS: Boolean) =>
        AlterTableToggleRowLevelSecurity(tableName, enbableRLS)
    }
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
      throw Utils.analysisException(s"No file named ${resource.uri} exists")
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
   * GRANT/REVOKE/CREATE DISKSTORE/CALL on a table (only for column and row tables).
   *
   * Example:
   * {{{
   *   GRANT SELECT ON table TO user1, user2;
   *   GRANT INSERT ON table TO ldapGroup: group1;
   *   CREATE DISKSTORE diskstore_name ('dir1' 10240)
   *   DROP DISKSTORE diskstore_name
   *   CALL SYSCS_UTIL.SYSCS_SET_RUNTIMESTATISTICS(1)
   * }}}
   */
  protected def grantRevoke: Rule1[LogicalPlan] = rule {
    (GRANT | REVOKE | (CREATE | DROP) ~ DISK_STORE | ("{".? ~ CALL)) ~ ANY.* ~>
        /* dummy table because we will pass sql to gemfire layer so we only need to have sql */
        (() => DMLExternalTable(TableIdentifier("SYSDUMMY1", Some("SYSIBM")),
          LogicalRelation(new execution.row.DefaultSource().createRelation(session.sqlContext,
            SaveMode.Ignore, Map(JdbcExtendedUtils.DBTABLE_PROPERTY -> "SYSIBM.SYSDUMMY1"),
            "", None)), input.sliceString(0, input.length)))
  }

  protected def deployPackages: Rule1[LogicalPlan] = rule {
    DEPLOY ~ ((PACKAGE ~ tableIdentifier ~ stringLiteral ~
        (REPOS ~ stringLiteral).? ~ (PATH ~ stringLiteral).? ~>
        ((alias: TableIdentifier, packages: String, repos: Any, path: Any) => DeployCommand(
          packages, alias.identifier, repos.asInstanceOf[Option[String]],
          path.asInstanceOf[Option[String]], restart = false))) |
      JAR ~ tableIdentifier ~ stringLiteral ~>
          ((alias: TableIdentifier, commaSepPaths: String) => DeployJarCommand(
        alias.identifier, commaSepPaths, restart = false))) |
    UNDEPLOY ~ tableIdentifier ~> ((alias: TableIdentifier) => UnDeployCommand(alias.identifier)) |
    LIST ~ (
      PACKAGES ~> (() => ListPackageJarsCommand(true)) |
      JARS ~> (() => ListPackageJarsCommand(false))
    )
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

  protected def reset: Rule1[LogicalPlan] = rule {
    RESET ~> { () => ResetCommand }
  }

  // It can be the following patterns:
  // SHOW FUNCTIONS;
  // SHOW FUNCTIONS mydb.func1;
  // SHOW FUNCTIONS func1;
  // SHOW FUNCTIONS `mydb.a`.`func1.aa`;
  protected def show: Rule1[LogicalPlan] = rule {
   SHOW ~ TABLES ~ ((FROM | IN) ~ identifier).? ~> ((ident: Any) =>
      ShowTablesCommand(ident.asInstanceOf[Option[String]], None)) |
       SHOW ~ strictIdentifier.? ~ FUNCTIONS ~ LIKE.? ~
        (functionIdentifier | stringLiteral).? ~> { (id: Any, nameOrPat: Any) =>
      val (user, system) = id.asInstanceOf[Option[String]]
          .map(_.toLowerCase) match {
        case None | Some("all") => (true, true)
        case Some("system") => (false, true)
        case Some("user") => (true, false)
        case Some(x) =>
          throw new ParseException(s"SHOW $x FUNCTIONS not supported")
      }
      nameOrPat match {
        case Some(name: FunctionIdentifier) => ShowFunctionsCommand(
          name.database, Some(name.funcName), user, system)
        case Some(pat: String) => ShowFunctionsCommand(
          None, Some(pat), user, system)
        case None => ShowFunctionsCommand(None, None, user, system)
        case _ => throw new ParseException(
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
    ((unquotedIdentifier | quotedIdentifier) + ('.' ~ ws)) ~>
        ((ids: Seq[String]) => ids.mkString("."))
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
    alterTableToggleRowLevelSecurity | alterTableAddColumn | alterTableDropColumn |
    createStream | streamContext |
    createIndex | dropIndex | createFunction | dropFunction | grantRevoke | show |
    createPolicy | dropPolicy
  }

  protected def query: Rule1[LogicalPlan]
  protected def expression: Rule1[Expression]
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

case class CreatePolicy(policyName: QualifiedTableName, tableName: QualifiedTableName,
    policyFor: String, applyTo: Seq[String], expandedPolicyApplyTo: Seq[String],
    currentUser: String, filterStr: String, filter: BypassRowLevelSecurity) extends Command

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

case class DropPolicy(ifExists: Boolean,
    policyIdentifier: TableIdentifier) extends Command

case class TruncateManagedTable(ifExists: Boolean, tableIdent: TableIdentifier) extends Command

case class AlterTableAddColumn(tableIdent: TableIdentifier, addColumn: StructField)
    extends Command

case class AlterTableToggleRowLevelSecurity(tableIdent: TableIdentifier, enable: Boolean)
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

case class DeployCommand(
  coordinates: String,
  alias: String,
  repos: Option[String],
  jarCache: Option[String],
  restart: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    try {
      val jarsstr = SparkSubmitUtils.resolveMavenCoordinates(coordinates, repos, jarCache)
      if (jarsstr.nonEmpty) {
        val jars = jarsstr.split(",")
        val sc = sparkSession.sparkContext
        val uris = jars.map(j => sc.env.rpcEnv.fileServer.addFile(new File(j)))
        SnappySession.addJarURIs(uris)
        Utils.mapExecutors[Unit](sparkSession.sparkContext, () => {
          ToolsCallbackInit.toolsCallback.addURIsToExecutorClassLoader(uris)
          Iterator.empty
        })
        val deployCmd = s"$coordinates|${repos.getOrElse("")}|${jarCache.getOrElse("")}"
        ToolsCallbackInit.toolsCallback.addURIs(alias, jars, deployCmd)
      }
      Seq.empty[Row]
    } catch {
      case ex: Throwable =>
        ex match {
          case err: Error =>
            if (SystemFailure.isJVMFailureError(err)) {
              SystemFailure.initiateFailure(err)
              // If this ever returns, rethrow the error. We're poisoned
              // now, so don't let this thread continue.
              throw err
            }
          case _ =>
        }
        Misc.checkIfCacheClosing(ex)
        if (restart) {
          logWarning(s"Following mvn coordinate" +
              s" could not be resolved during restart: ${coordinates}", ex)
          if (lang.Boolean.parseBoolean(System.getProperty("FAIL_ON_JAR_UNAVAILABILITY", "true"))) {
            throw ex
          }
          Seq.empty[Row]
        } else {
          throw ex
        }
    }
  }
}

case class DeployJarCommand(
  alias: String,
  paths: String,
  restart: Boolean) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (paths.nonEmpty) {
      val jars = paths.split(",")
      val (availableUris, unavailableUris) = jars.partition(f => Files.isReadable(Paths.get(f)))
      if (unavailableUris.nonEmpty) {
        logWarning(s"Following jars are unavailable" +
            s" for deployment during restart: ${unavailableUris.deep.mkString(",")}")
        if (restart && lang.Boolean.parseBoolean(
          System.getProperty("FAIL_ON_JAR_UNAVAILABILITY", "true"))) {
          throw new IllegalStateException(
            s"Could not find deployed jars: ${unavailableUris.mkString(",")}")
        }
        if (!restart) {
          throw new IllegalArgumentException(s"jars not readable: ${unavailableUris.mkString(",")}")
        }
      }
      val sc = sparkSession.sparkContext
      val uris = availableUris.map(j => sc.env.rpcEnv.fileServer.addFile(new File(j)))
      SnappySession.addJarURIs(uris)
      Utils.mapExecutors[Unit](sparkSession.sparkContext, () => {
        ToolsCallbackInit.toolsCallback.addURIsToExecutorClassLoader(uris)
        Iterator.empty
      })
      ToolsCallbackInit.toolsCallback.addURIs(alias, jars, paths, isPackage = false)
    }
    Seq.empty[Row]
  }
}

case class ListPackageJarsCommand(isJar: Boolean) extends RunnableCommand {
  override val output: Seq[Attribute] = {
    AttributeReference("alias", StringType, nullable = false)() ::
    AttributeReference("coordinate", StringType, nullable = false)() ::
    AttributeReference("isPackage", BooleanType, nullable = false)() :: Nil
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val commands = ToolsCallbackInit.toolsCallback.getGlobalCmndsSet()
    val rows = new ArrayBuffer[Row]
    commands.forEach(new Consumer[Entry[String, String]] {
      override def accept(t: Entry[String, String]): Unit = {
        val alias = t.getKey
        val value = t.getValue
        val indexOf = value.indexOf('|')
        if (indexOf > 0) {
          // It is a package
          val pkg = value.substring(0, indexOf)
          rows += Row(alias, pkg, true)
        }
        else {
          // It is a jar
          val jars = value.split(',')
          val jarfiles = jars.map(f => {
            val lastIndexOf = f.lastIndexOf('/')
            val length = f.length
            if (lastIndexOf > 0) f.substring(lastIndexOf + 1, length)
            else {
              f
            }
          })
          rows += Row(alias, jarfiles.mkString(","), false)
        }
      }
    })
    rows
  }
}

case class UnDeployCommand(alias: String) extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    ToolsCallbackInit.toolsCallback.removePackage(alias)
    Seq.empty[Row]
  }
}

case class CreateSnappyViewCommand(name: TableIdentifier,
    userSpecifiedColumns: Seq[(String, Option[String])],
    comment: Option[String],
    properties: Map[String, String],
    originalText: Option[String],
    child: LogicalPlan,
    allowExisting: Boolean,
    replace: Boolean,
    viewType: ViewType)
    extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // If the plan cannot be analyzed, throw an exception and don't proceed.
    val qe = sparkSession.sessionState.executePlan(child)
    qe.assertAnalyzed()
    val analyzedPlan = qe.analyzed

    if (userSpecifiedColumns.nonEmpty &&
        userSpecifiedColumns.length != analyzedPlan.output.length) {
      throw new AnalysisException(s"The number of columns produced by the SELECT clause " +
          s"(num: `${analyzedPlan.output.length}`) does not match the number of column names " +
          s"specified by CREATE VIEW (num: `${userSpecifiedColumns.length}`).")
    }

    val aliasedPlan = if (userSpecifiedColumns.isEmpty) {
      analyzedPlan
    } else {
      val projectList = analyzedPlan.output.zip(userSpecifiedColumns).map {
        case (attr, (colName, None)) => Alias(attr, colName)()
        case (attr, (colName, Some(colComment))) =>
          val meta = new MetadataBuilder().putString("comment", colComment).build()
          Alias(attr, colName)(explicitMetadata = Some(meta))
      }
      sparkSession.sessionState.executePlan(Project(projectList, analyzedPlan)).analyzed
    }
    val viewSQL: String = new SQLBuilder(aliasedPlan).toSQL

    // Validate the view SQL - make sure we can parse it and analyze it.
    // If we cannot analyze the generated query, there is probably a bug in SQL generation.
    try {
      sparkSession.sql(viewSQL).queryExecution.assertAnalyzed()
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException(s"Failed to analyze the canonicalized SQL: $viewSQL", e)
    }
    var opts = JdbcExtendedUtils.addSplitProperty(viewSQL, Constant.SPLIT_VIEW_TEXT_PROPERTY,
      properties)
    opts = JdbcExtendedUtils.addSplitProperty(viewSQL,
      Constant.SPLIT_VIEW_ORIGINAL_TEXT_PROPERTY, opts)
    val dummyText = "select 1"
    val dummyPlan = sparkSession.sessionState.sqlParser.parsePlan(dummyText)
    val cmd = CreateViewCommand(name, Nil, comment, opts.toMap, Some(dummyText),
      dummyPlan, allowExisting, replace, viewType)
    cmd.run(sparkSession)
  }
}
