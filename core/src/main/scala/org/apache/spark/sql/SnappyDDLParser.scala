/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

import com.pivotal.gemfirexd.internal.iapi.util.IdUtil
import io.snappydata.sql.catalog.{CatalogObjectType, SnappyExternalCatalog}
import io.snappydata.{Constant, Property, QueryHint}
import org.parboiled2._
import shapeless.{::, HNil}

import org.apache.spark.sql.SnappyParserConsts.plusOrMinus
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTableType, FunctionResource, FunctionResourceType}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{CreateTempViewUsing, DataSource, LogicalRelation, RefreshTable}
import org.apache.spark.sql.policy.PolicyProperties
import org.apache.spark.sql.sources.JdbcExtendedUtils
import org.apache.spark.sql.streaming.StreamPlanProvider
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SnappyParserConsts => Consts}
import org.apache.spark.streaming._

abstract class SnappyDDLParser(session: SnappySession)
    extends SnappyBaseParser(session) {

  // reserved keywords
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
  final def UPDATE: Rule0 = rule { keyword(Consts.UPDATE) }
  final def WHEN: Rule0 = rule { keyword(Consts.WHEN) }
  final def WHERE: Rule0 = rule { keyword(Consts.WHERE) }
  final def WITH: Rule0 = rule { keyword(Consts.WITH) }
  final def USER: Rule0 = rule { keyword(Consts.USER) }

  // non-reserved keywords
  final def ADD: Rule0 = rule { keyword(Consts.ADD) }
  final def ALTER: Rule0 = rule { keyword(Consts.ALTER) }
  final def ANALYZE: Rule0 = rule { keyword(Consts.ANALYZE) }
  final def ANTI: Rule0 = rule { keyword(Consts.ANTI) }
  final def AUTHORIZATION: Rule0 = rule { keyword(Consts.AUTHORIZATION) }
  final def BUCKETS: Rule0 = rule { keyword(Consts.BUCKETS) }
  final def CACHE: Rule0 = rule { keyword(Consts.CACHE) }
  final def CALL: Rule0 = rule{ keyword(Consts.CALL) }
  final def CASCADE: Rule0 = rule { keyword(Consts.CASCADE) }
  final def CHECK: Rule0 = rule { keyword(Consts.CHECK) }
  final def CLEAR: Rule0 = rule { keyword(Consts.CLEAR) }
  final def CLUSTER: Rule0 = rule { keyword(Consts.CLUSTER) }
  final def CLUSTERED: Rule0 = rule { keyword(Consts.CLUSTERED) }
  final def CODEGEN: Rule0 = rule { keyword(Consts.CODEGEN) }
  final def COLUMN: Rule0 = rule { keyword(Consts.COLUMN) }
  final def COLUMNS: Rule0 = rule { keyword(Consts.COLUMNS) }
  final def COMMENT: Rule0 = rule { keyword(Consts.COMMENT) }
  final def COMPUTE: Rule0 = rule { keyword(Consts.COMPUTE) }
  final def CONSTRAINT: Rule0 = rule { keyword(Consts.CONSTRAINT) }
  final def CROSS: Rule0 = rule { keyword(Consts.CROSS) }
  final def CURRENT_USER: Rule0 = rule { keyword(Consts.CURRENT_USER) }
  final def DEFAULT: Rule0 = rule { keyword(Consts.DEFAULT) }
  final def DEPLOY: Rule0 = rule { keyword(Consts.DEPLOY) }
  final def DATABASE: Rule0 = rule { keyword(Consts.DATABASE) }
  final def DATABASES: Rule0 = rule { keyword(Consts.DATABASES) }
  final def DESCRIBE: Rule0 = rule { keyword(Consts.DESCRIBE) }
  final def DISABLE: Rule0 = rule { keyword(Consts.DISABLE) }
  final def DISTRIBUTE: Rule0 = rule { keyword(Consts.DISTRIBUTE) }
  final def DISKSTORE: Rule0 = rule { keyword(Consts.DISKSTORE) }
  final def ENABLE: Rule0 = rule { keyword(Consts.ENABLE) }
  final def END: Rule0 = rule { keyword(Consts.END) }
  final def EXECUTE: Rule0 = rule { keyword(Consts.EXECUTE) }
  final def EXPLAIN: Rule0 = rule { keyword(Consts.EXPLAIN) }
  final def EXTENDED: Rule0 = rule { keyword(Consts.EXTENDED) }
  final def EXTERNAL: Rule0 = rule { keyword(Consts.EXTERNAL) }
  final def FETCH: Rule0 = rule { keyword(Consts.FETCH) }
  final def FIRST: Rule0 = rule { keyword(Consts.FIRST) }
  final def FN: Rule0 = rule { keyword(Consts.FN) }
  final def FOR: Rule0 = rule { keyword(Consts.FOR) }
  final def FOREIGN: Rule0 = rule { keyword(Consts.FOREIGN) }
  final def FORMATTED: Rule0 = rule { keyword(Consts.FORMATTED) }
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
  final def LDAPGROUP: Rule0 = rule { keyword(Consts.LDAPGROUP) }
  final def LEVEL: Rule0 = rule { keyword(Consts.LEVEL) }
  final def LIMIT: Rule0 = rule { keyword(Consts.LIMIT) }
  final def LIST: Rule0 = rule { keyword(Consts.LIST) }
  final def LOAD: Rule0 = rule { keyword(Consts.LOAD) }
  final def LOCATION: Rule0 = rule { keyword(Consts.LOCATION) }
  final def MEMBERS: Rule0 = rule { keyword(Consts.MEMBERS) }
  final def MINUS: Rule0 = rule { keyword(Consts.MINUS) }
  final def MSCK: Rule0 = rule { keyword(Consts.MSCK) }
  final def NATURAL: Rule0 = rule { keyword(Consts.NATURAL) }
  final def NULLS: Rule0 = rule { keyword(Consts.NULLS) }
  final def OF: Rule0 = rule { keyword(Consts.OF) }
  final def ONLY: Rule0 = rule { keyword(Consts.ONLY) }
  final def OPTIONS: Rule0 = rule { keyword(Consts.OPTIONS) }
  final def OUT: Rule0 = rule { keyword(Consts.OUT) }
  final def OVERWRITE: Rule0 = rule { keyword(Consts.OVERWRITE) }
  final def PACKAGE: Rule0 = rule { keyword(Consts.PACKAGE) }
  final def PACKAGES: Rule0 = rule { keyword(Consts.PACKAGES) }
  final def PARTITION: Rule0 = rule { keyword(Consts.PARTITION) }
  final def PARTITIONED: Rule0 = rule { keyword(Consts.PARTITIONED) }
  final def PATH: Rule0 = rule { keyword(Consts.PATH) }
  final def PERCENT: Rule0 = rule { keyword(Consts.PERCENT) }
  final def POLICY: Rule0 = rule { keyword(Consts.POLICY) }
  final def PURGE: Rule0 = rule { keyword(Consts.PURGE) }
  final def PUT: Rule0 = rule { keyword(Consts.PUT) }
  final def REFRESH: Rule0 = rule { keyword(Consts.REFRESH) }
  final def REGEXP: Rule0 = rule { keyword(Consts.REGEXP) }
  final def RENAME: Rule0 = rule { keyword(Consts.RENAME) }
  final def REPLACE: Rule0 = rule { keyword(Consts.REPLACE) }
  final def REPOS: Rule0 = rule { keyword(Consts.REPOS) }
  final def RESET: Rule0 = rule { keyword(Consts.RESET) }
  final def RESTRICT: Rule0 = rule { keyword(Consts.RESTRICT) }
  final def RETURNS: Rule0 = rule { keyword(Consts.RETURNS) }
  final def RLIKE: Rule0 = rule { keyword(Consts.RLIKE) }
  final def SCHEMAS: Rule0 = rule { keyword(Consts.SCHEMAS) }
  final def SECURITY: Rule0 = rule { keyword(Consts.SECURITY) }
  final def SEMI: Rule0 = rule { keyword(Consts.SEMI) }
  final def SERDE: Rule0 = rule { keyword(Consts.SERDE) }
  final def SERDEPROPERTIES: Rule0 = rule { keyword(Consts.SERDEPROPERTIES) }
  final def SHOW: Rule0 = rule { keyword(Consts.SHOW) }
  final def SORT: Rule0 = rule { keyword(Consts.SORT) }
  final def SORTED: Rule0 = rule { keyword(Consts.SORTED) }
  final def START: Rule0 = rule { keyword(Consts.START) }
  final def STATISTICS: Rule0 = rule { keyword(Consts.STATISTICS) }
  final def STOP: Rule0 = rule { keyword(Consts.STOP) }
  final def STREAM: Rule0 = rule { keyword(Consts.STREAM) }
  final def STREAMING: Rule0 = rule { keyword(Consts.STREAMING) }
  final def TABLES: Rule0 = rule { keyword(Consts.TABLES) }
  final def TABLESAMPLE: Rule0 = rule { keyword(Consts.TABLESAMPLE) }
  final def TBLPROPERTIES: Rule0 = rule { keyword(Consts.TBLPROPERTIES) }
  final def TEMPORARY: Rule0 = rule { keyword(Consts.TEMPORARY) | keyword(Consts.TEMP) }
  final def TRIGGER: Rule0 = rule { keyword(Consts.TRIGGER) }
  final def TRUNCATE: Rule0 = rule { keyword(Consts.TRUNCATE) }
  final def UNCACHE: Rule0 = rule { keyword(Consts.UNCACHE) }
  final def UNDEPLOY: Rule0 = rule { keyword(Consts.UNDEPLOY) }
  final def UNIQUE: Rule0 = rule { keyword(Consts.UNIQUE) }
  final def UNSET: Rule0 = rule { keyword(Consts.UNSET) }
  final def USE: Rule0 = rule { keyword(Consts.USE) }
  final def USING: Rule0 = rule { keyword(Consts.USING) }
  final def VALUES: Rule0 = rule { keyword(Consts.VALUES) }
  final def VIEW: Rule0 = rule { keyword(Consts.VIEW) }
  final def VIEWS: Rule0 = rule { keyword(Consts.VIEWS) }

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

  /** spark parser used for hive DDLs that are not relevant to SnappyData's builtin sources */
  protected final lazy val sparkParser: SparkSqlParser =
    new SparkSqlParser(session.snappySessionState.conf)

  // DDLs, SET etc

  final type ColumnDirectionMap = Seq[(String, Option[SortDirection])]
  final type TableEnd = (Option[String], Option[Map[String, String]],
      Array[String], Option[BucketSpec], Option[LogicalPlan])

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

      val options = remaining._2 match {
        case None => Map.empty[String, String]
        case Some(m) => m
      }
      val provider = remaining._1 match {
        case None =>
          // behave like SparkSession in case hive support has been enabled
          if (session.enableHiveSupport) DDLUtils.HIVE_PROVIDER else Consts.DEFAULT_SOURCE
        case Some(p) => p
      }
      // check if hive provider is being used
      if (provider.equalsIgnoreCase(DDLUtils.HIVE_PROVIDER)) {
        if (session.enableHiveSupport) {
          sparkParser.parsePlan(input.sliceString(0, input.length))
        } else {
          throw Utils.analysisException(s"Hive support (${Property.EnableHiveSupport.name}) " +
              "is required to create hive tables")
        }
      } else {
        val schemaString = schemaStr.toString().trim
        // check if a relation supporting free-form schema has been used that supports
        // syntax beyond Spark support
        val (userSpecifiedSchema, schemaDDL) = if (schemaString.length > 0) {
          if (ExternalStoreUtils.isExternalSchemaRelationProvider(provider)) {
            None -> Some(schemaString)
          } else synchronized {
            // parse the schema string expecting Spark SQL format
            val colParser = newInstance()
            colParser.parseSQL(schemaString, colParser.tableSchemaOpt.run())
                .map(StructType(_)) -> None
          }
        } else None -> None

        // When IF NOT EXISTS clause appears in the query,
        // the save mode will be ignore.
        val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
        CreateTableUsingCommand(tableIdent, None, userSpecifiedSchema, schemaDDL,
          provider, mode, options, remaining._3, remaining._4, remaining._5, external == None)
      }
    }
  }

  protected def createTableLike: Rule1[LogicalPlan] = rule {
    CREATE ~ TABLE ~ ifNotExists ~ tableIdentifier ~ LIKE ~ tableIdentifier ~>
        ((allowExisting: Boolean, targetIdent: TableIdentifier, sourceIdent: TableIdentifier) =>
          CreateTableLikeCommand(targetIdent, sourceIdent, allowExisting))
  }

  protected final def booleanLiteral: Rule1[Boolean] = rule {
    TRUE ~> (() => true) | FALSE ~> (() => false)
  }

  protected final def numericLiteral: Rule1[String] = rule {
    capture(plusOrMinus.? ~ Consts.numeric. + ~ (Consts.exponent ~
        plusOrMinus.? ~ CharPredicate.Digit. +).? ~ Consts.numericSuffix.? ~
        Consts.numericSuffix.?) ~ delimiter ~> ((s: String) => s)
  }

  protected final def defaultLiteral: Rule1[Option[String]] = rule {
    stringLiteral ~> ((s: String) => Option(s)) |
    numericLiteral ~> ((s: String) => Option(s)) |
    booleanLiteral ~> ((b: Boolean) => Option(b.toString)) |
    NULL ~> (() => None)
  }

  protected final def defaultVal: Rule1[Option[String]] = rule {
    (DEFAULT ~ defaultLiteral ~ ws).?  ~> ((value: Any) => value match {
        case Some(v) => v.asInstanceOf[Option[String]]
        case None => None
      })
  }

  protected final def policyFor: Rule1[String] = rule {
    (FOR ~ capture(ALL | SELECT | UPDATE | INSERT | DELETE)).? ~> ((forOpt: Any) =>
      forOpt match {
        case Some(v) => v.asInstanceOf[String].trim
        case None => SnappyParserConsts.SELECT.lower
      })
  }

  protected final def policyTo: Rule1[Seq[String]] = rule {
    (TO ~
        (capture(CURRENT_USER) |
            (LDAPGROUP ~ ':' ~ ws ~ push(true)).? ~
                identifier ~ ws ~> ((ldapOpt: Any, id) => ldapOpt match {
              case None => IdUtil.getUserAuthorizationId(id)
              case _ => IdUtil.getUserAuthorizationId(SnappyParserConsts.LDAPGROUP.lower) +
                  ':' + IdUtil.getUserAuthorizationId(id)
            })
        ). + (commaSep) ~> {
        (policyTo: Any) => policyTo.asInstanceOf[Seq[String]].map(_.trim)
          }).? ~> { (toOpt: Any) =>
      toOpt match {
        case Some(x) => x.asInstanceOf[Seq[String]]
        case _ => SnappyParserConsts.CURRENT_USER.lower :: Nil
      }
    }
  }

  protected def createPolicy: Rule1[LogicalPlan] = rule {
    (CREATE ~ POLICY) ~ tableIdentifier ~ ON ~ tableIdentifier ~ policyFor ~
        policyTo ~ USING ~ capture(expression) ~> { (policyName: TableIdentifier,
        tableName: TableIdentifier, policyFor: String,
        applyTo: Seq[String], filterExp: Expression, filterStr: String) => {
      val applyToAll = applyTo.exists(_.equalsIgnoreCase(
        SnappyParserConsts.CURRENT_USER.lower))
      val expandedApplyTo = if (applyToAll) Nil
      else ExternalStoreUtils.getExpandedGranteesIterator(applyTo).toSeq
      /*
      val targetRelation = snappySession.sessionState.catalog.lookupRelation(tableIdent)
      val isTargetExternalRelation =  targetRelation.find(x => x match {
        case _: ExternalRelation => true
        case _ => false
      }).isDefined
      */
      // use normalized value for string comparison in filters
      val currentUser = IdUtil.getUserAuthorizationId(this.session.conf.get(
        com.pivotal.gemfirexd.Attribute.USERNAME_ATTR, Constant.DEFAULT_SCHEMA))
      val filter = PolicyProperties.createFilterPlan(filterExp, tableName,
        currentUser, expandedApplyTo)

      CreatePolicyCommand(policyName, tableName, policyFor, applyTo, expandedApplyTo,
        currentUser, filterStr, filter)
    }
    }
  }

  protected def dropPolicy: Rule1[LogicalPlan] = rule {
    DROP ~ POLICY ~ ifExists ~ tableIdentifier ~> DropPolicyCommand
  }

  protected final def beforeDDLEnd: Rule0 = rule {
    noneOf("uUoOaA-;/")
  }

  protected final def identifierList: Rule1[Seq[String]] = rule {
    '(' ~ ws ~ (identifier + commaSep) ~ ')' ~ ws
  }

  protected final def bucketSpec: Rule1[BucketSpec] = rule {
    CLUSTERED ~ BY ~ identifierList ~ (SORTED ~ BY ~ colsWithDirection).? ~
        INTO ~ integral ~ BUCKETS ~> ((cols: Seq[String], sort: Any, buckets: String) =>
      sort match {
        case None => BucketSpec(buckets.toInt, cols, Nil)
        case Some(m) =>
          val sortColumns = m.asInstanceOf[ColumnDirectionMap].map {
            case (_, Some(Descending)) => throw Utils.analysisException(
              s"Column ordering for buckets must be ASC but was DESC")
            case (c, _) => c
          }
          BucketSpec(buckets.toInt, cols, sortColumns.toSeq)
      })
  }

  protected final def ddlEnd: Rule1[TableEnd] = rule {
    ws ~ (USING ~ qualifiedName).? ~ (OPTIONS ~ options).? ~
        (PARTITIONED ~ BY ~ identifierList).? ~
        bucketSpec.? ~ (AS ~ query).? ~ ws ~ &((';' ~ ws).* ~ EOI) ~>
        ((provider: Any, options: Any, parts: Any, buckets: Any, asQuery: Any) => {
          val partitions = parts match {
            case None => Utils.EMPTY_STRING_ARRAY
            case Some(p) => p.asInstanceOf[Seq[String]].toArray
          }
          (provider, options, partitions, buckets, asQuery).asInstanceOf[TableEnd]
        })
  }

  protected final def tableEnd1: Rule[StringBuilder :: HNil,
      StringBuilder :: TableEnd :: HNil] = rule {
    ddlEnd.asInstanceOf[Rule[StringBuilder :: HNil,
        StringBuilder :: TableEnd :: HNil]] |
    // no free form pass through to store/spark-hive layer if USING has been provided
    // to detect genuine syntax errors correctly rather than store throwing
    // some irrelevant error
    (!(ws ~ (USING ~ qualifiedName | OPTIONS ~ options)) ~ capture(ANY ~ beforeDDLEnd.*) ~>
        ((s: StringBuilder, n: String) => s.append(n))) ~ tableEnd1
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
          cols: ColumnDirectionMap, opts: Any) =>
        val parameters = opts.asInstanceOf[Option[Map[String, String]]]
            .getOrElse(Map.empty[String, String])
        val options = indexType.asInstanceOf[Option[Boolean]] match {
          case Some(false) =>
            parameters + (ExternalStoreUtils.INDEX_TYPE -> "global hash")
          case Some(true) =>
            parameters + (ExternalStoreUtils.INDEX_TYPE -> "unique")
          case None => parameters
        }
        CreateIndexCommand(indexName, tableName, cols, options)
    }
  }

  protected final def globalTemporary: Rule1[Boolean] = rule {
    (GLOBAL ~ push(true)).? ~ TEMPORARY ~> ((g: Any) => g != None)
  }

  protected def createView: Rule1[LogicalPlan] = rule {
    CREATE ~ (OR ~ REPLACE ~ push(true)).? ~ (globalTemporary.? ~ VIEW |
        globalTemporary ~ TABLE) ~ ifNotExists ~ tableIdentifier ~
        ('(' ~ ws ~ (identifierWithComment + commaSep) ~ ')' ~ ws).? ~
        (COMMENT ~ stringLiteral).? ~ (TBLPROPERTIES ~ options).? ~
        AS ~ capture(query) ~> { (replace: Any, gt: Any,
        allowExisting: Boolean, table: TableIdentifier, cols: Any, comment: Any,
        opts: Any, plan: LogicalPlan, queryStr: String) =>

      val viewType = gt match {
        case Some(true) | true => GlobalTempView
        case Some(false) | false => LocalTempView
        case _ => PersistedView
      }
      val userCols = cols.asInstanceOf[Option[Seq[(String, Option[String])]]] match {
        case Some(seq) => seq
        case None => Nil
      }
      val viewOpts = opts.asInstanceOf[Option[Map[String, String]]] match {
        case Some(m) => m
        case None => Map.empty[String, String]
      }
      CreateViewCommand(
        name = table,
        userSpecifiedColumns = userCols,
        comment = comment.asInstanceOf[Option[String]],
        properties = viewOpts,
        originalText = Option(queryStr),
        child = plan,
        allowExisting = allowExisting,
        replace = replace != None,
        viewType = viewType)
    }
  }

  protected def createTempViewUsing: Rule1[LogicalPlan] = rule {
    CREATE ~ (OR ~ REPLACE ~ push(true)).? ~ globalTemporary ~ (VIEW ~ push(false) |
        TABLE ~ push(true)) ~ tableIdentifier ~ tableSchema.? ~ USING ~ qualifiedName ~
        (OPTIONS ~ options).? ~> ((replace: Any, global: Boolean, isTable: Boolean,
        table: TableIdentifier, schema: Any, provider: String, opts: Any) => CreateTempViewUsing(
      tableIdent = table,
      userSpecifiedSchema = schema.asInstanceOf[Option[Seq[StructField]]].map(StructType(_)),
      // in Spark replace is always true for CREATE TEMPORARY TABLE
      replace = replace != None || (!global && isTable),
      global = global,
      provider = provider,
      options = opts.asInstanceOf[Option[Map[String, String]]].getOrElse(Map.empty)))
  }

  protected def dropIndex: Rule1[LogicalPlan] = rule {
    DROP ~ INDEX ~ ifExists ~ tableIdentifier ~> DropIndexCommand
  }

  protected def dropTable: Rule1[LogicalPlan] = rule {
    DROP ~ TABLE ~ ifExists ~ tableIdentifier ~ (PURGE ~ push(true)).? ~>
        ((exists: Boolean, table: TableIdentifier, purge: Any) =>
          DropTableOrViewCommand(table, exists, isView = false, purge = purge != None))
  }

  protected def dropView: Rule1[LogicalPlan] = rule {
    DROP ~ VIEW ~ ifExists ~ tableIdentifier ~> ((exists: Boolean, table: TableIdentifier) =>
      DropTableOrViewCommand(table, exists, isView = true, purge = false))
  }

  protected def alterView: Rule1[LogicalPlan] = rule {
    ALTER ~ VIEW ~ tableIdentifier ~ AS.? ~ capture(query) ~> ((name: TableIdentifier,
        plan: LogicalPlan, queryStr: String) => AlterViewAsCommand(name, queryStr, plan))
  }

  protected def createSchema: Rule1[LogicalPlan] = rule {
    CREATE ~ SCHEMA ~ ifNotExists ~ identifier ~ (
        AUTHORIZATION ~ (
            LDAPGROUP ~ ':' ~ ws ~ identifier ~> ((group: String) => group -> true) |
            identifier ~> ((id: String) => id -> false)
        )
    ).? ~> ((notExists: Boolean, schemaName: String, authId: Any) =>
      CreateSchemaCommand(notExists, schemaName, authId.asInstanceOf[Option[(String, Boolean)]]))
  }

  protected def dropSchema: Rule1[LogicalPlan] = rule {
    DROP ~ (SCHEMA ~ push(false) | DATABASE ~ push(true)) ~ ifExists ~ identifier ~
        (RESTRICT ~ push(false) | CASCADE ~ push(true)).? ~> ((isDb: Boolean,
        exists: Boolean, schemaName: String, cascade: Any) => DropSchemaOrDbCommand(
      schemaName, exists, cascade.asInstanceOf[Option[Boolean]].contains(true), isDb))
  }

  protected def truncateTable: Rule1[LogicalPlan] = rule {
    TRUNCATE ~ TABLE ~ ifExists ~ tableIdentifier ~> TruncateManagedTableCommand
  }

  protected def alterTableToggleRowLevelSecurity: Rule1[LogicalPlan] = rule {
    ALTER ~ TABLE ~ tableIdentifier ~ ((ENABLE ~ push(true)) | (DISABLE ~ push(false))) ~
        ROW ~ LEVEL ~ SECURITY ~> {
      (tableName: TableIdentifier, enbableRLS: Boolean) =>
        AlterTableToggleRowLevelSecurityCommand(tableName, enbableRLS)
    }
  }

  protected final def canAlter(id: TableIdentifier, op: String,
      allBuiltins: Boolean = false): TableIdentifier = {
    val catalogTable = session.sessionState.catalog.getTempViewOrPermanentTableMetadata(id)
    if (catalogTable.tableType != CatalogTableType.VIEW) {
      val objectType = CatalogObjectType.getTableType(catalogTable)
      // many alter commands are not supported for tables backed by snappy-store and topK
      if ((allBuiltins && !(objectType == CatalogObjectType.External ||
          objectType == CatalogObjectType.Hive)) ||
          (!allBuiltins && (CatalogObjectType.isTableBackedByRegion(objectType) ||
              objectType == CatalogObjectType.TopK))) {
        throw Utils.analysisException(
          s"ALTER TABLE... $op for table $id not supported by provider ${catalogTable.provider}")
      }
    }
    id
  }

  private def toPartSpec(partSpec: Any): Option[Map[String, String]] = {
    partSpec.asInstanceOf[Option[Map[String, Option[String]]]] match {
      case None => None
      case Some(spec) => Some(spec.mapValues {
        case None => null
        case Some(v) => v
      })
    }
  }

  private final def alterTableProps: Rule1[LogicalPlan] = rule {
    ALTER ~ TABLE ~ tableIdentifier ~ partitionSpec.? ~ SET ~ (
        SERDEPROPERTIES ~ options ~> ((id: TableIdentifier, partSpec: Any,
            opts: Map[String, String]) => AlterTableSerDePropertiesCommand(canAlter(
          id, "SET SERDEPROPERTIES"), None, Some(opts), toPartSpec(partSpec))) |
        SERDE ~ stringLiteral ~ (WITH ~ SERDEPROPERTIES ~ options).? ~>
            ((id: TableIdentifier, partSpec: Any, name: String, opts: Any) =>
              AlterTableSerDePropertiesCommand(canAlter(id, "SET SERDE", allBuiltins = true),
                Some(name), opts.asInstanceOf[Option[Map[String, String]]],
                toPartSpec(partSpec))) |
        LOCATION ~ stringLiteral ~> ((id: TableIdentifier, partSpec: Any,
            path: String) => AlterTableSetLocationCommand(canAlter(
          id, "SET LOCATION", allBuiltins = true), toPartSpec(partSpec), path))
    )
  }

  protected def alterTableOrView: Rule1[LogicalPlan] = rule {
    ALTER ~ (TABLE ~ push(false) | VIEW ~ push(true)) ~ tableIdentifier ~ (
        RENAME ~ TO ~ tableIdentifier ~> ((view: Boolean, from: TableIdentifier,
            to: TableIdentifier) => AlterTableRenameCommand(canAlter(from, "RENAME"), to, view)) |
        SET ~ TBLPROPERTIES ~ options ~> ((view: Boolean, id: TableIdentifier,
            opts: Map[String, String]) => AlterTableSetPropertiesCommand(canAlter(
          id, "SET TBLPROPERTIES"), opts, view)) |
        UNSET ~ TBLPROPERTIES ~ (IF ~ EXISTS ~ push(true)).? ~
            '(' ~ ws ~ (optionKey + commaSep) ~ ')' ~ ws ~> ((view: Boolean,
            id: TableIdentifier, exists: Any, keys: Seq[String]) =>
          AlterTableUnsetPropertiesCommand(canAlter(id, "UNSET TBLPROPERTIES"), keys,
            exists.asInstanceOf[Option[Boolean]].isDefined, view))
    )
  }

  protected def alterTable: Rule1[LogicalPlan] = rule {
    ALTER ~ TABLE ~ tableIdentifier ~ (
        (ADD ~ push(true) | DROP ~ push(false)) ~ (
            // other store ALTER statements which don't effect the snappydata catalog
            capture((CONSTRAINT | CHECK | FOREIGN | UNIQUE) ~ ANY. +) ~ EOI ~>
                ((table: TableIdentifier, isAdd: Boolean, s: String) =>
                  AlterTableMiscCommand(table, s"ALTER TABLE ${quotedUppercaseId(table)} " +
                    s"${if (isAdd) "ADD" else "DROP"} $s")) |
            COLUMN.? ~ (column | identifier) ~ defaultVal ~> {
              (t: TableIdentifier, isAdd: Boolean, c: Any, defVal: Option[String]) =>
                val col = c match {
                  case s: String => if (isAdd) throw new ParseException(
                    s"ALTER TABLE ADD COLUMN needs column definition but got '$c'")
                    StructField(s, NullType)
                  case f: StructField => if (!isAdd) throw new ParseException(
                    s"ALTER TABLE DROP COLUMN needs column name but got '$c'")
                    f
                }
                AlterTableAddDropColumnCommand(t, isAdd, col, defVal)
            }
        ) |
        // other store ALTER statements which don't effect the snappydata catalog
        ALTER ~ capture(ANY. +) ~ EOI ~> ((table: TableIdentifier, s: String) =>
          AlterTableMiscCommand(table, s"ALTER TABLE ${quotedUppercaseId(table)} ALTER $s"))
    )
  }

  protected def createStream: Rule1[LogicalPlan] = rule {
    CREATE ~ STREAM ~ TABLE ~ ifNotExists ~ tableIdentifier ~ tableSchema.? ~
        USING ~ qualifiedName ~ OPTIONS ~ options ~> {
      (allowExisting: Boolean, streamIdent: TableIdentifier, schema: Any,
          provider: String, opts: Map[String, String]) =>
        val specifiedSchema = schema.asInstanceOf[Option[Seq[StructField]]]
            .map(fields => StructType(fields))
        // check that the provider is a stream relation
        val clazz = DataSource.lookupDataSource(provider)
        if (!classOf[StreamPlanProvider].isAssignableFrom(clazz)) {
          throw Utils.analysisException(s"CREATE STREAM provider $provider" +
              " does not implement StreamPlanProvider")
        }
        // provider has already been resolved, so isBuiltIn==false allows
        // for both builtin as well as external implementations
        val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
        CreateTableUsingCommand(streamIdent, None, specifiedSchema, None,
          provider, mode, opts, partitionColumns = Utils.EMPTY_STRING_ARRAY,
          bucketSpec = None, query = None, isBuiltIn = true)
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

  protected def checkExists(resource: FunctionResource): Unit = {
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
          val catalogString = t match {
            case VarcharType(Int.MaxValue) => "string"
            case _ => t.catalogString
          }
          val classNameWithType = className + "__" + catalogString
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
   * Commands like GRANT/REVOKE/CREATE DISKSTORE/CALL on a table that are passed through
   * as is to the SnappyData store layer (only for column and row tables).
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
  protected def passThrough: Rule1[LogicalPlan] = rule {
    (GRANT | REVOKE | (CREATE | DROP) ~ (DISKSTORE | TRIGGER) |
        (ch('{').? ~ ws ~ (CALL | EXECUTE))) ~ ANY.* ~>
        /* dummy table because we will pass sql to gemfire layer so we only need to have sql */
        (() => DMLExternalTable(TableIdentifier(JdbcExtendedUtils.DUMMY_TABLE_NAME,
          Some(JdbcExtendedUtils.SYSIBM_SCHEMA)),
          LogicalRelation(new execution.row.DefaultSource().createRelation(session.sqlContext,
            Map(SnappyExternalCatalog.DBTABLE_PROPERTY -> JdbcExtendedUtils
                .DUMMY_TABLE_QUALIFIED_NAME))), input.sliceString(0, input.length)))
  }

  /**
   * Handle other statements not appropriate for SnappyData's builtin sources but used by hive/file
   * based sources in Spark. This rule should always be at the end of the "start" rule so that
   * this is used as the last fallback and not before any of the SnappyData customizations.
   */
  protected def delegateToSpark: Rule1[LogicalPlan] = rule {
    (
        ADD | ANALYZE | ALTER ~ (DATABASE | TABLE | VIEW) | CREATE ~ DATABASE |
        DESCRIBE | DESC | LIST | LOAD | MSCK | REFRESH | SHOW | TRUNCATE
    ) ~ ANY.* ~ EOI ~>
        (() => sparkParser.parsePlan(input.sliceString(0, input.length)))
  }

  protected def deployPackages: Rule1[LogicalPlan] = rule {
    DEPLOY ~ ((PACKAGE ~ packageIdentifier ~ stringLiteral ~
        (REPOS ~ stringLiteral).? ~ (PATH ~ stringLiteral).? ~>
        ((alias: TableIdentifier, packages: String, repos: Any, path: Any) => DeployCommand(
          packages, alias.identifier, repos.asInstanceOf[Option[String]],
          path.asInstanceOf[Option[String]], restart = false))) |
      JAR ~ packageIdentifier ~ stringLiteral ~>
          ((alias: TableIdentifier, commaSepPaths: String) => DeployJarCommand(
        alias.identifier, commaSepPaths, restart = false))) |
    UNDEPLOY ~ packageIdentifier ~> ((alias: TableIdentifier) =>
      UnDeployCommand(alias.identifier)) |
    LIST ~ (
      PACKAGES ~> (() => ListPackageJarsCommand(true)) |
      JARS ~> (() => ListPackageJarsCommand(false))
    )
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
  protected def describe: Rule1[LogicalPlan] = rule {
    (DESCRIBE | DESC) ~ (
        FUNCTION ~ (EXTENDED ~ push(true)).? ~
            functionIdentifier ~> ((extended: Any, name: FunctionIdentifier) =>
          DescribeFunctionCommand(name,
            extended.asInstanceOf[Option[Boolean]].isDefined)) |
        (SCHEMA | DATABASE) ~ (EXTENDED ~ push(true)).? ~ identifier ~>
            ((extended: Any, name: String) =>
              DescribeDatabaseCommand(name, extended.asInstanceOf[Option[Boolean]].isDefined)) |
        (EXTENDED ~ push(true) | FORMATTED ~ push(false)).? ~ tableIdentifier ~>
            ((extendedOrFormatted: Any, tableIdent: TableIdentifier) => {
              // ensure columns are sent back as CLOB for large results with EXTENDED
              queryHints.put(QueryHint.ColumnsAsClob.toString, "data_type,comment")
              val (isExtended, isFormatted) = extendedOrFormatted match {
                case None => (false, false)
                case Some(true) => (true, false)
                case Some(false) => (false, true)
              }
              new DescribeSnappyTableCommand(tableIdent, Map.empty[String, String],
                isExtended, isFormatted)
            })
    )
  }

  protected def refreshTable: Rule1[LogicalPlan] = rule {
    REFRESH ~ TABLE ~ tableIdentifier ~> RefreshTable
  }

  protected def cache: Rule1[LogicalPlan] = rule {
    CACHE ~ (LAZY ~ push(true)).? ~ TABLE ~ tableIdentifier ~
        (AS ~ query).? ~> ((isLazy: Any, tableIdent: TableIdentifier,
        plan: Any) => SnappyCacheTableCommand(tableIdent,
      input.sliceString(0, input.length), plan.asInstanceOf[Option[LogicalPlan]],
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
        CURRENT.? ~ (SCHEMA | DATABASE) ~ '='.? ~ ws ~ identifier ~>
            ((schemaName: String) => SetSchemaCommand(schemaName)) |
        capture(ANY.*) ~> { (rest: String) =>
          val separatorIndex = rest.indexOf('=')
          if (separatorIndex >= 0) {
            val key = rest.substring(0, separatorIndex).trim
            val value = rest.substring(separatorIndex + 1).trim
            if (key.startsWith("spark.sql.aqp.")) {
              new SetCommand(Some(key -> Option(value))) with InvalidateCachedPlans
            } else {
              SetCommand(Some(key -> Option(value)))
            }
          } else if (rest.nonEmpty) {
            SetCommand(Some(rest.trim -> None))
          } else {
            SetCommand(None)
          }
        }
    ) |
    USE ~ identifier ~> SetSchemaCommand
  }

  protected def reset: Rule1[LogicalPlan] = rule {
    RESET ~> { () => ResetCommand }
  }

  // helper non-terminals

  protected final def sortDirection: Rule1[SortDirection] = rule {
    ASC ~> (() => Ascending) | DESC ~> (() => Descending)
  }

  protected final def colsWithDirection: Rule1[ColumnDirectionMap] = rule {
    '(' ~ ws ~ (identifier ~ sortDirection.? ~> ((id: Any, direction: Any) =>
      (id, direction))).*(commaSep) ~ ')' ~ ws ~> ((cols: Any) =>
      cols.asInstanceOf[Seq[(String, Option[SortDirection])]])
  }

  protected final def durationUnit: Rule1[Duration] = rule {
    integral ~ (
        (MILLISECOND | MILLIS) ~> ((s: String) => Milliseconds(s.toInt)) |
        (SECOND | SECS) ~> ((s: String) => Seconds(s.toInt)) |
        (MINUTE | MINS) ~> ((s: String) => Minutes(s.toInt))
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
        case CharType(size) =>
          builder.putLong(Constant.CHAR_TYPE_SIZE_PROP, size)
              .putString(Constant.CHAR_TYPE_BASE_PROP, "CHAR")
          (StringType, false)
        case VarcharType(Int.MaxValue) => // indicates CLOB type
          builder.putString(Constant.CHAR_TYPE_BASE_PROP, "CLOB")
          (StringType, false)
        case VarcharType(size) =>
          builder.putLong(Constant.CHAR_TYPE_SIZE_PROP, size)
              .putString(Constant.CHAR_TYPE_BASE_PROP, "VARCHAR")
          (StringType, false)
        case StringType =>
          builder.putString(Constant.CHAR_TYPE_BASE_PROP, "STRING")
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

  final def tableSchemaOpt: Rule1[Option[Seq[StructField]]] = rule {
    (tableSchema ~> (Some(_)) | ws ~> (() => None)).named("tableSchema") ~ EOI
  }

  protected final def optionKey: Rule1[String] = rule {
    qualifiedName | stringLiteral
  }

  protected final def option: Rule1[(String, String)] = rule {
    optionKey ~ ('=' ~ '='.? ~ ws).? ~ stringLiteral ~ ws ~> ((k: String, v: String) => k -> v)
  }

  protected final def options: Rule1[Map[String, String]] = rule {
    '(' ~ ws ~ (option * commaSep) ~ ')' ~ ws ~>
        ((pairs: Any) => pairs.asInstanceOf[Seq[(String, String)]].toMap)
  }

  protected def ddl: Rule1[LogicalPlan] = rule {
    createTableLike | createTable | describe | refreshTable | dropTable | truncateTable |
    createView | createTempViewUsing | dropView | alterView | createSchema | dropSchema |
    alterTableToggleRowLevelSecurity | createPolicy | dropPolicy |
    alterTableProps | alterTableOrView | alterTable | createStream | streamContext |
    createIndex | dropIndex | createFunction | dropFunction | passThrough
  }

  protected def partitionSpec: Rule1[Map[String, Option[String]]]
  protected def query: Rule1[LogicalPlan]
  protected def literal: Rule1[Expression]
  protected def expression: Rule1[Expression]
  protected def parseSQL[T](sqlText: String, parseRule: => Try[T]): T

  protected def newInstance(): SnappyDDLParser
}

case class DMLExternalTable(
    tableName: TableIdentifier,
    query: LogicalPlan,
    command: String)
    extends LeafNode with Command {
  
  override def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override lazy val resolved: Boolean = query.resolved
  override lazy val output: Seq[Attribute] = AttributeReference("count", IntegerType)() :: Nil
}
