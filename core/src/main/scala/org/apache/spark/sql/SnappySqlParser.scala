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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.AbstractSqlParser
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.DataType

class SnappySqlParser(session: SnappySession) extends AbstractSqlParser {

  protected def astBuilder = throw new UnsupportedOperationException(
    "SnappyData parser does not use AST")

  @transient protected[sql] val sqlParser: SnappyParser =
    new SnappyParser(session)

  /** Creates/Resolves DataType for a given SQL string. */
  override def parseDataType(sqlText: String): DataType = {
    sqlParser.parse(sqlText, sqlParser.parseDataType.run())
  }

  /** Creates Expression for a given SQL string. */
  override def parseExpression(sqlText: String): Expression = {
    sqlParser.parse(sqlText, sqlParser.parseExpression.run())
  }

  /** Creates TableIdentifier for a given SQL string. */
  override def parseTableIdentifier(sqlText: String): TableIdentifier = {
    sqlParser.parse(sqlText, sqlParser.parseTableIdentifier.run())
  }

  override def parsePlan(sqlText: String): LogicalPlan = {
    sqlParser.parse(sqlText, sqlParser.sql.run())
  }

  def parsePlan(sqlText: String, clearExecutionData: Boolean): LogicalPlan = {
    sqlParser.parse(sqlText, sqlParser.sql.run(), clearExecutionData)
  }
}
