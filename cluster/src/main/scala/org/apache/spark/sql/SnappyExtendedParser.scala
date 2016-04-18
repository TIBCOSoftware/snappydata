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

import scala.util.{Failure, Success}

import org.parboiled2._
import shapeless.{::, HNil}

import org.apache.spark.sql.catalyst.ParserDialect
import org.apache.spark.sql.catalyst.analysis.{Exists, InSubquery}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.hive.QualifiedTableName

/**
 * SnappyData extensions to SQL grammar. Currently subquery in WHERE support.
 */
class SnappyExtendedParser(caseSensitive: Boolean)
    extends SnappyParser(caseSensitive) {

  override protected def extraComparisonExpression: Rule[Expression :: HNil,
      Expression :: HNil] = rule {
    super.extraComparisonExpression |
    IN ~ query ~> ((e: Expression, subQuery: LogicalPlan) =>
      InSubquery(e, subQuery, positive = true)) |
    NOT ~ IN ~ query ~> ((e: Expression, subQuery: LogicalPlan) =>
      InSubquery(e, subQuery, positive = false)) |
    '=' ~ query ~> ((e: Expression, subQuery: LogicalPlan) =>
      InSubquery(e, subQuery, positive = true))
  }

  override protected def primary: Rule1[Expression] = rule {
    super.primary |
    EXISTS ~ query ~> ((subQuery: LogicalPlan) =>
      Exists(subQuery, positive = true)) |
    NOT ~ EXISTS ~ query ~> ((subQuery: LogicalPlan) =>
      Exists(subQuery, positive = false))
  }

  def expr: Rule1[Expression] = rule {
    ws ~ projection ~ EOI
  }

  def tableIdent: Rule1[QualifiedTableName] = rule {
    ws ~ tableIdentifier ~ EOI
  }
}

/**
 * Snappy extended dialect additions to the standard "sql" dialect.
 */
private[sql] final class SnappyExtendedParserDialect(caseSensitive: Boolean)
    extends ParserDialect {

  private[sql] val sqlParser = new SnappyExtendedParser(caseSensitive)

  override def parse(sqlText: String): LogicalPlan = synchronized {
    sqlParser.input = sqlText
    sqlParser.parse()
  }

  /** TODO: What to do these methods?
  // parse expressions in a projection
  override def parseExpression(sqlText: String): Expression = synchronized {
    sqlParser.input = sqlText
    sqlParser.expr.run() match {
      case Success(plan) => plan
      case Failure(e: ParseError) =>
        throw Utils.analysisException(sqlParser.formatError(e))
      case Failure(e) =>
        val ae = Utils.analysisException(e.toString)
        ae.initCause(e)
        throw ae
    }
  }

  // parse table identifiers
  override def parseTableIdentifier(
      sqlText: String): QualifiedTableName = synchronized {
    sqlParser.input = sqlText
    sqlParser.tableIdent.run() match {
      case Success(plan) => plan
      case Failure(e: ParseError) =>
        throw Utils.analysisException(sqlParser.formatError(e))
      case Failure(e) =>
        val ae = Utils.analysisException(e.toString)
        ae.initCause(e)
        throw ae
    }
  }
    */
}
