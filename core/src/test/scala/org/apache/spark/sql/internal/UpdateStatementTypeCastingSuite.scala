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
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.{AnalysisException, Row}

class UpdateStatementTypeCastingSuite extends SnappyFunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    // creating table with COLUMN_MAX_DELTA_ROWS = 1 to flush the records immediately on
    // column table as if all records will be in row buffer then spark's null safe type
    // casting doesn't kick in
    snc.sql("create table test (id long, int_col int) " +
        "using column options(COLUMN_MAX_DELTA_ROWS '1')")
    snc.sql("insert into test values (1, 1)")
    snc.sql("insert into test values (2, 2)")
  }

  override def afterAll() : Unit = {
    snc.sql("drop table test")
  }

  test("Arithmetic operator, first operand is string type and is not a numeric literal") {
    assertForAnalysisException("update test set int_col = 'some_string' + 1")
  }

  test("Arithmetic operator, first operand is string type and is a numeric literal") {
    assertForAnalysisException("update test set int_col = '1' + 1")

  }

  test("Arithmetic operator, second operand is string type and is not a numeric literal") {
    assertForAnalysisException("update test set int_col = 1 + 'some_string'")
  }

  test("Arithmetic operator, second operand is string type and is a numeric literal") {
    assertForAnalysisException("update test set int_col = 1 + '1'")
  }

  test("Arithmetic operator, second operand is string type and is a numeric literal" +
      " casted as int ") {
    snc.sql("update test set int_col = 1 + cast('200' as int)").collect()
    val rows = snc.sql("select * from test order by id").collect()
    assertResult(Array(Row(1, 201), Row(2, 201)))(rows)
  }

  test("Arithmetic operator, both operands are numeric") {
    snc.sql("update test set int_col = 1 + 500")
    val rows = snc.sql("select * from test order by id").collect()
    assertResult(Array(Row(1, 501), Row(2, 501)))(rows)
  }

  test("Plain assignment, assigning string typed non numeric literal") {
    assertForAnalysisException("update test set int_col = 'some_string'")
  }

  test("Plain assignment, assigning string typed numeric literal") {
    assertForAnalysisException("update test set int_col = '1'")
  }

  test("Plain assignment, assigning string typed numeric literal casted as int") {
    snc.sql("update test set int_col = cast('300' as int)")
    val rows = snc.sql("select * from test order by id").collect()
    assertResult(Array(Row(1, 300), Row(2, 300)))(rows)
  }

  test("Plain assignment, assigning number") {
    snc.sql("update test set int_col = 400").collect()
    val rows = snc.sql("select * from test order by id").collect()
    assertResult(Array(Row(1, 400), Row(2, 400)))(rows)
  }

  def assertForAnalysisException(sql: String): Unit = {
    try {
      snc.sql(sql)
      fail("AnalysisException was expected here.")
    } catch {
      case e: AnalysisException =>
        val expectedMessage = "Implicit type casting is not performed for update statements"
        assert(e.message.equals(expectedMessage))
    }
  }
}
