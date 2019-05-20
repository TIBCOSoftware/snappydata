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

import java.util.regex.Pattern

import com.gemstone.gemfire.internal.shared.ClientSharedUtils
import io.snappydata.Constant
import io.snappydata.jdbc.TomcatConnectionPool

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.sources.JdbcExtendedUtils
import org.apache.spark.sql.types._

/**
 * Default dialect for SnappyData using pooled client Driver.
 */
@DeveloperApi
case object SnappyDataPoolDialect extends SnappyDataBaseDialect with Logging {

  // register the dialect
  JdbcDialects.registerDialect(SnappyDataPoolDialect)

  private val URL_PATTERN = Pattern.compile("^" + Constant.POOLED_THIN_CLIENT_URL,
    Pattern.CASE_INSENSITIVE)
  private val COMPLEX_TYPE_AS_JSON_HINT_PATTERN = Pattern.compile(
    s"${Constant.COMPLEX_TYPE_AS_JSON_HINT}\\s*\\(([^)]*)\\)", Pattern.CASE_INSENSITIVE)

  def register(): Unit = {
    // no-op, all registration is done in the object constructor
  }

  def canHandle(url: String): Boolean = URL_PATTERN.matcher(url).find()

  /**
   * Parse the table plan and check all unresolved relations using metadata from connection.
   */
  private def parsePlanAndResolve(session: SparkSession, sqlText: String): QueryExecution = {
    val sessionState = session.sessionState
    val plan = sessionState.sqlParser.parsePlan(sqlText)
    sessionState.executePlan(plan.transformUp {
      case u: UnresolvedRelation =>
        // use the current connection, if any, to check in meta-data
        TomcatConnectionPool.CURRENT_CONNECTION.get() match {
          case null => u
          case conn =>
            val tableName = u.tableIdentifier.table
            val schemaName = u.tableIdentifier.database match {
              case None => conn.getSchema
              case Some(s) => s
            }
            val metadata = JdbcExtendedUtils.getTableSchema(schemaName, tableName,
              conn, Some(session))
            if (metadata.isEmpty) u
            else {
              val output = metadata.toAttributes
              // create SubqueryAlias by reflection to account for differences
              // in Spark versions (2 arguments vs 3)
              try {
                val cons = classOf[SubqueryAlias].getConstructor(classOf[String],
                  classOf[LogicalPlan], classOf[Option[_]])
                // use LocalRelation.apply to allow same code to work against newer Spark
                // releases where LocalRelation class primary constructor has changed signature
                cons.newInstance(tableName, LocalRelation.apply(output: _*), None)
              } catch {
                case _: Exception => // fallback to two argument constructor
                  val cons = classOf[SubqueryAlias].getConstructor(classOf[String],
                    classOf[LogicalPlan])
                  cons.newInstance(tableName, LocalRelation.apply(output: _*))
              }
            }
        }
    })
  }

  override def getTableExistsQuery(query: String): String = {
    // parse the query locally, resolve relations and look them up individually
    val session = SparkSession.builder().getOrCreate()
    // parse the table plan and check all unresolved relations
    try {
      // Spark parser understands LIMIT
      val qe = parsePlanAndResolve(session, s"SELECT 1 FROM $query LIMIT 1")
      qe.assertAnalyzed()
      // at this point we know all tables in the sub-query exist so return a dummy
      s"SELECT 1 FROM ${JdbcExtendedUtils.DUMMY_TABLE_QUALIFIED_NAME}"
    } catch {
      case ae: AnalysisException => throw ae
      case t: Throwable =>
        // fallback to full query on connection
        logWarning(s"Unexpected exception in getTableExistsQuery: $t")
        // snappy-store parser understand FETCH FIRST ROW ONLY
        s"SELECT 1 FROM $query FETCH FIRST ROW ONLY"
    }
  }

  override def getSchemaQuery(query: String): String = {
    // parse the query locally, resolve relations, find the final output schema
    // and return a dummy query with matching schema
    val session = SparkSession.builder().getOrCreate()
    // parse the table plan and check all unresolved relations
    try {
      // Spark parser understands LIMIT
      val qe = parsePlanAndResolve(session, s"SELECT * FROM $query LIMIT 1")
      qe.assertAnalyzed()
      // at this point we know all tables in the sub-query exist so return a dummy query
      // with matching schema
      val schema = qe.analyzed.output
      // top-level complex types will be returned in JSON format by default
      val hasComplexType = schema.exists(a => JdbcExtendedUtils.getSQLDataType(a.dataType) match {
        case _: ArrayType | _: MapType | _: StructType => true
        case _ => false
      })
      // check for complexTypeAsJson hint
      val useJson = if (hasComplexType) {
        val matcher = COMPLEX_TYPE_AS_JSON_HINT_PATTERN.matcher(query)
        if (matcher.find()) {
          ClientSharedUtils.parseBoolean(matcher.group(1))
        } else Constant.COMPLEX_TYPE_AS_JSON_DEFAULT
      } else true
      // ignoring nullability for now
      schema.map { attr =>
        val dataType = if (hasComplexType) attr.dataType match {
          case _: ArrayType | _: MapType | _: StructType => if (useJson) StringType else BinaryType
          case d => d
        } else attr.dataType
        val typeName = JdbcExtendedUtils.getJdbcType(dataType, attr.metadata,
          this).databaseTypeDefinition
        val columnName = JdbcExtendedUtils.toUpperCase(attr.name)
        s"""CAST (${defaultDataTypeValue(attr)} AS $typeName) AS "$columnName""""
      }.mkString("SELECT ", ", ", s" FROM ${JdbcExtendedUtils.DUMMY_TABLE_QUALIFIED_NAME}")
    } catch {
      case ae: AnalysisException => throw ae
      case t: Throwable =>
        // fallback to full query on connection
        logWarning(s"Unexpected exception in getSchemaQuery: $t")
        // snappy-store parser understand FETCH FIRST ROW ONLY
        s"SELECT * FROM $query FETCH FIRST ROW ONLY"
    }
  }

  private def defaultDataTypeValue(attribute: Attribute): String = attribute.dataType match {
    case _ if attribute.nullable => "NULL"
    case IntegerType | LongType | ShortType | FloatType | DoubleType | ByteType |
         BooleanType | _: DecimalType => "0"
    case StringType => "''"
    case DateType => "'1976-01-01'"
    case TimestampType => "'1976-01-01 00:00:00.0'"
    case BinaryType => "X'00'"
    // don't have non-null values for complex types that will run on store side
    // so just mark as null which will cause negligible overhead for complex types
    case _ => "NULL"
  }
}
