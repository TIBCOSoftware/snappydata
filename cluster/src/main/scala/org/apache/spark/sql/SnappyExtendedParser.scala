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

import org.parboiled2._
import shapeless.{::, HNil}

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{Exists, InSubquery}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * SnappyData extensions to SQL grammar. Currently subquery in WHERE support.
 */
class SnappyExtendedParser(context: SnappyContext)
    extends SnappyParser(context) {

  override protected def comparisonExpression1: Rule[Expression :: HNil,
      Expression :: HNil] = rule {
    super.comparisonExpression1 |
    IN ~ query ~> ((e: Expression, subQuery: LogicalPlan) =>
      InSubquery(e, subQuery, positive = true)) |
    NOT ~ IN ~ query ~> ((e: Expression, subQuery: LogicalPlan) =>
      InSubquery(e, subQuery, positive = false)) |
    '=' ~ ws ~ query ~> ((e: Expression, subQuery: LogicalPlan) =>
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

  def tableIdent: Rule1[TableIdentifier] = rule {
    ws ~ tableIdentifier ~ EOI
  }
}

/**
 * Snappy extended dialect additions to the standard "sql" dialect.
 */
private[sql] final class SnappyExtendedParserDialect(context: SnappyContext)
    extends SnappyParserDialect(context) {

  @transient private[sql] override val sqlParser =
    new SnappyExtendedParser(context)
}
