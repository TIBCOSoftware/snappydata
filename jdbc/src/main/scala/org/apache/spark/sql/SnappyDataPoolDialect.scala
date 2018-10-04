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

import io.snappydata.jdbc.ClientPoolDriver

import org.apache.spark.Logging
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, SubqueryAlias}
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

  def register(): Unit = {
    // no-op, all registration is done in the object constructor
  }

  def canHandle(url: String): Boolean = url.startsWith("jdbc:snappydata:pool://")

  /**
   * Parse the table plan and check all unresolved relations using metadata from connection.
   */
  private def parsePlanAndResolve(session: SparkSession, sqlText: String): QueryExecution = {
    val sessionState = session.sessionState
    val plan = sessionState.sqlParser.parsePlan(sqlText)
    sessionState.executePlan(plan.resolveOperators {
      case u: UnresolvedRelation =>
        // use the current connection, if any, to check in meta-data
        ClientPoolDriver.CURRENT_CONNECTION.get() match {
          case null => u
          case conn =>
            // convert to upper case since hive catalog is case-insensitive
            val tableName = JdbcExtendedUtils.toUpperCase(u.tableIdentifier.table)
            val schemaName = u.tableIdentifier.database match {
              case None =>
                val defaultSchema = sessionState.catalog.getCurrentDatabase
                if (defaultSchema == "default") "APP"
                else JdbcExtendedUtils.toUpperCase(defaultSchema)
              case Some(s) => s
            }
            val metadata = JdbcExtendedUtils.getTableSchema(schemaName, tableName, conn)
            if (metadata.isEmpty) u
            else {
              val output = StructType(metadata).toAttributes
              SubqueryAlias(tableName, LocalRelation(output), None)
            }
        }
    })
  }

  override def getTableExistsQuery(query: String): String = {
    // parse the query locally, resolve relations and look them up individually
    SparkSession.getActiveSession match {
      case None => // fallback to default
      case Some(session) =>
        try {
          // parse the table plan and check all unresolved relations
          val qe = parsePlanAndResolve(session, s"SELECT 1 FROM $query LIMIT 1")
          qe.assertAnalyzed()
          // at this point we know all tables in the sub-query exist so return a dummy
          return s"SELECT 1 FROM ${JdbcExtendedUtils.DUMMY_TABLE_QUALIFIED_NAME}"
        } catch {
          case ae: AnalysisException => throw ae
          case t: Throwable =>
            // fallback to full query on connection
            logWarning(s"Unexpected exception in getTableExistsQuery: $t")
        }
    }
    s"SELECT 1 FROM $query LIMIT 1"
  }

  override def getSchemaQuery(query: String): String = {
    // parse the query locally, resolve relations, find the final output schema
    // and return a dummy query with matching schema
    SparkSession.getActiveSession match {
      case None => // fallback to default
      case Some(session) =>
        // parse the table plan and check all unresolved relations
        try {
          val qe = parsePlanAndResolve(session, s"SELECT * FROM $query LIMIT 1")
          qe.assertAnalyzed()
          // at this point we know all tables in the sub-query exist so return a dummy query
          // with matching schema
          val schema = qe.analyzed.output
          val namesAndTypes = JdbcExtendedUtils.getSQLNamesAndTypes(schema, this)
          // ignoring nullability for now
          return namesAndTypes.indices.map { i =>
            val (name, typeName) = namesAndTypes(i)
            s"""CAST (${defaultNonNullValue(schema(i))} AS $typeName) AS "$name""""
          }.mkString("SELECT ", ", ", s" FROM ${JdbcExtendedUtils.DUMMY_TABLE_QUALIFIED_NAME}")
        } catch {
          case ae: AnalysisException => throw ae
          case t: Throwable =>
            // fallback to full query on connection
            logWarning(s"Unexpected exception in getSchemaQuery: $t")
        }
    }
    s"SELECT * FROM $query LIMIT 1"
  }

  private def defaultNonNullValue(attribute: Attribute): String = attribute.dataType match {
    case _ if attribute.nullable => "NULL"
    case IntegerType | LongType | ShortType | FloatType | DoubleType | ByteType |
         BooleanType | _: DecimalType => "0"
    case StringType => "''"
    case DateType => "'1976-01-01'"
    case TimestampType => "'1976-01-01 00:00:00.0'"
    case BinaryType => "X'00'"
    case _ => "NULL"
  }
}
