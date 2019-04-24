/*
 * Copyright (c) 2019 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.internal

import io.snappydata.SnappyFunSuite
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.sql.types.{DataType, DecimalType, FloatType, IntegerType, LongType, StringType}
import org.apache.spark.sql.{AnalysisException, Row}

class UpdateStatementTypeCastingSuite extends SnappyFunSuite with BeforeAndAfterAll
    with BeforeAndAfter {

  override def beforeAll(): Unit = {
    // creating table with COLUMN_MAX_DELTA_ROWS = 1 to flush the records immediately on
    // column table because if all records will be in row buffer then spark's null safe type
    // casting doesn't kick in
    snc.sql("create table testTable (id long, int_col int, long_col long, dec_col decimal(5,2)) " +
        "using column options(COLUMN_MAX_DELTA_ROWS '1')")
  }

  override def afterAll(): Unit = {
    snc.sql("drop table testTable")
  }

  before {
    snc.sql("truncate table testTable")
    snc.sql("insert into testTable values (1, 1, 1, 1.2)")
    snc.sql("insert into testTable values (2, 2, 2, 1.2)")
  }

  test("Arithmetic operator, first operand is string type and is not a numeric literal") {
    assertForAnalysisException("update testTable set int_col = 'some_string' + 1")
  }

  test("Arithmetic operator, first operand is string type and is a numeric literal") {
    assertForAnalysisException("update testTable set int_col = '1' + 1")
  }

  test("Arithmetic operator, second operand is string type and is not a numeric literal") {
    assertForAnalysisException("update testTable set int_col = 1 + 'some_string'")
  }

  test("Arithmetic operator, second operand is string type and is a numeric literal") {
    assertForAnalysisException("update testTable set int_col = 1 + '1'")
  }

  test("Arithmetic operator, second operand is string type and is a numeric literal" +
      " casted as int ") {
    snc.sql("update testTable set int_col = 1 + cast('200' as int)").collect()
    val rows = snc.sql("select * from testTable order by id").collect()
    assertResult(Array(Row(1, 201, 1, new java.math.BigDecimal("1.20")),
      Row(2, 201, 2, new java.math.BigDecimal("1.20"))))(rows)
  }

  test("Arithmetic operator, both operands are numeric") {
    snc.sql("update testTable set int_col = 1 + 500")
    val rows = snc.sql("select * from testTable order by id").collect()
    print(rows(0)(3).getClass.getCanonicalName)
    assertResult(Array(Row(1, 501, 1, new java.math.BigDecimal("1.20")),
      Row(2, 501, 2, new java.math.BigDecimal("1.20"))))(rows)
  }

  test("Plain assignment, assigning string typed non numeric literal") {
    assertForAnalysisException("update testTable set int_col = 'some_string'",
      "INT_COL", IntegerType, StringType)
  }

  test("Plain assignment, assigning string typed numeric literal") {
    assertForAnalysisException("update testTable set int_col = '1'",
      "INT_COL", IntegerType, StringType)
  }

  test("Plain assignment, assigning wider integral type to narrower integral type") {
    assertForAnalysisException("update testTable set int_col = 10000000000000000",
      "INT_COL", IntegerType, LongType)
  }

  test("Plain assignment, assigning wider decimal to a narrower decimal") {
    assertForAnalysisException("update testTable set dec_col = 104.123",
      "DEC_COL", DecimalType(5, 2), DecimalType(6, 3))
  }

  test("Plain assignment, assigning narrower decimal to a wider decimal") {
    snc.sql("update testTable set dec_col = 10.1")
    val rows = snc.sql("select * from testTable order by id").collect()
    assertResult(Array(Row(1, 1, 1, new java.math.BigDecimal("10.1")),
      Row(2, 2, 2, new java.math.BigDecimal("10.1"))))(rows)
  }

  test("Plain assignment, assigning null") {
    snc.sql("update testTable set dec_col = null")
    val rows = snc.sql("select * from testTable order by id").collect()
    assertResult(Array(Row(1, 1, 1, null),
      Row(2, 2, 2, null)))(rows)
  }

  test("Plain assignment, assigning narrow integral type to wider integral type") {
    snc.sql("update testTable set long_col = 100")
    val rows = snc.sql("select * from testTable order by id").collect()
    assertResult(Array(Row(1, 1, 100, new java.math.BigDecimal("1.20")),
      Row(2, 2, 100, new java.math.BigDecimal("1.20"))))(rows)
  }

  test("Plain assignment, assigning float to a decimal") {
    assertForAnalysisException("update testTable set dec_col = CAST(104.123 as float)",
      "DEC_COL", DecimalType(5, 2), FloatType)
  }

  test("Plain assignment, assigning string typed numeric literal casted as int") {
    snc.sql("update testTable set int_col = cast('300' as int)")
    val rows = snc.sql("select * from testTable order by id").collect()
    assertResult(Array(Row(1, 300, 1, new java.math.BigDecimal("1.20")),
      Row(2, 300, 2, new java.math.BigDecimal("1.20"))))(rows)
  }

  test("Plain assignment, assigning number") {
    snc.sql("update testTable set int_col = 400").collect()
    val rows = snc.sql("select * from testTable order by id").collect()
    assertResult(Array(Row(1, 400, 1, new java.math.BigDecimal("1.20")),
      Row(2, 400, 2, new java.math.BigDecimal("1.20"))))(rows)
  }

  def assertForAnalysisException(sql: String, attrName: String, attrDt: DataType,
      exprDt: DataType): Unit = {
    try {
      snc.sql(sql)
      fail("AnalysisException was expected here.")
    } catch {
      case e: AnalysisException =>
        val expectedMessage = s"Data type of expression ($exprDt) is not compatible" +
            s" with the data type of attribute '$attrName' ($attrDt)"
        assert(e.message.equals(expectedMessage))
    }
  }


  def assertForAnalysisException(sql: String): Unit = {
    try {
      snc.sql(sql)
      fail("AnalysisException was expected here.")
    } catch {
      case e: AnalysisException =>
        val expectedMessage = s"Implicit type casting is not performed for update statements"
        assert(e.message.equals(expectedMessage))
    }
  }
}
