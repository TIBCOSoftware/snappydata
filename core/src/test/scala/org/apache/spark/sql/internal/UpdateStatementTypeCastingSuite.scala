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
import org.junit.Assert._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.hive.SnappyAnalyzer
import org.apache.spark.sql.types.{DataType, DecimalType, FloatType, IntegerType, LongType, StringType}
import org.apache.spark.sql.{AnalysisException, Row}

class UpdateStatementTypeCastingSuite extends SnappyFunSuite with BeforeAndAfterAll
    with BeforeAndAfter {

  override def beforeAll(): Unit = {
    // creating table with COLUMN_MAX_DELTA_ROWS = 1 to flush the records immediately on
    // column table because if all records will be in row buffer then spark's fail safe type
    // casting doesn't kick in
    snc.sql(
      """create table testTable (id long, int_col int, long_col long, dec_col decimal(15,7),
        | string_col varchar(20)) using column options(COLUMN_MAX_DELTA_ROWS '1')""".stripMargin)
  }

  override def afterAll(): Unit = {
    snc.sql("drop table testTable")
  }

  before {
    snc.sql("truncate table testTable")
    snc.sql("insert into testTable values (1, 1, 1, 1.2, 'abc')")
    snc.sql("insert into testTable values (2, 2, 2, 1.2, 'xyz')")
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

  test("Arithmetic operator, one operand is some column") {
    snc.sql("update testTable set int_col = int_col + 1").collect()
    val expectedResult = Seq(Row(1, 2, 1, new java.math.BigDecimal("1.20"), "abc"),
      Row(2, 3, 2, new java.math.BigDecimal("1.20"), "xyz"))
    checkAnswer(snc.sql("select * from testTable order by id"), expectedResult)
  }

  test("Arithmetic operator, second operand is string type and is a numeric literal" +
      " casted as int ") {
    snc.sql("update testTable set int_col = 1 + cast('200' as int)").collect()
    val expectedResult = Seq(Row(1, 201, 1, new java.math.BigDecimal("1.20"), "abc"),
      Row(2, 201, 2, new java.math.BigDecimal("1.20"), "xyz"))
    checkAnswer(snc.sql("select * from testTable order by id"), expectedResult)
  }

  test("Arithmetic operator, both operands are numeric") {
    snc.sql("update testTable set int_col = 1 + 500")
    val expectedResult = Seq(Row(1, 501, 1, new java.math.BigDecimal("1.20"), "abc"),
      Row(2, 501, 2, new java.math.BigDecimal("1.20"), "xyz"))
    checkAnswer(snc.sql("select * from testTable order by id"), expectedResult)
  }

  test("Arithmetic operator, assigning to string with one string operand cast to int") {
    snc.sql("update testTable set string_col = 1000")
    snc.sql("update testTable set string_col = cast( string_col as int )+ 500")
    val expectedResult = Seq(Row(1, 1, 1, new java.math.BigDecimal("1.20"), "1500"),
      Row(2, 2, 2, new java.math.BigDecimal("1.20"), "1500"))
    checkAnswer(snc.sql("select * from testTable order by id"), expectedResult)
  }

  test("Arithmetic operator in condition part, string promotion is supported") {
    snc.sql("update testTable set int_col = 100 where id = (1 + '1')")
    val expectedResult = Seq(Row(1, 1, 1, new java.math.BigDecimal("1.20"), "abc"),
      Row(2, 100, 2, new java.math.BigDecimal("1.20"), "xyz"))
    checkAnswer(snc.sql("select * from testTable order by id"), expectedResult)
  }

  test("Arithmetic operator in condition part, string typed operand is not a number") {
    snc.sql("update testTable set int_col = 100 where id = (1 + 'abc')")
    val expectedResult = Seq(Row(1, 1, 1, new java.math.BigDecimal("1.20"), "abc"),
      Row(2, 2, 2, new java.math.BigDecimal("1.20"), "xyz"))
    checkAnswer(snc.sql("select * from testTable order by id"), expectedResult)
  }

  test("Plain assignment, assigning string typed non numeric literal") {
    assertForAnalysisException("update testTable set int_col = 'some_string'",
      "int_col", IntegerType, StringType)
  }

  test("Plain assignment, assigning string typed numeric literal") {
    assertForAnalysisException("update testTable set int_col = '1'",
      "int_col", IntegerType, StringType)
  }

  test("Plain assignment, assigning wider integral type to narrower integral type") {
    assertForAnalysisException("update testTable set int_col = 10000000000000000",
      "int_col", IntegerType, LongType)
  }

  test("Plain assignment, assigning wider decimal to a narrower decimal") {
    assertForAnalysisException("update testTable set dec_col = 104.12356756887",
      "dec_col", DecimalType(15, 7), DecimalType(14, 11))
  }

  test("Plain assignment, assigning narrower decimal to a wider decimal") {
    snc.sql("update testTable set dec_col = 10.1")
    val expectedResult = Seq(Row(1, 1, 1, new java.math.BigDecimal("10.1"), "abc"),
      Row(2, 2, 2, new java.math.BigDecimal("10.1"), "xyz"))
    checkAnswer(snc.sql("select * from testTable order by id"), expectedResult)
  }

  test("Plain assignment, assigning null") {
    snc.sql("update testTable set dec_col = null")
    val expectedResult = Seq(Row(1, 1, 1, null, "abc"), Row(2, 2, 2, null, "xyz"))
    checkAnswer(snc.sql("select * from testTable order by id"), expectedResult)
  }

  test("Plain assignment, assigning narrow integral type to wider integral type") {
    snc.sql("update testTable set long_col = 100")
    val expectedResult = Seq(Row(1, 1, 100, new java.math.BigDecimal("1.20"), "abc"),
      Row(2, 2, 100, new java.math.BigDecimal("1.20"), "xyz"))
    checkAnswer(snc.sql("select * from testTable order by id"), expectedResult)
  }

  test("Plain assignment, assigning float to a decimal") {
    assertForAnalysisException("update testTable set dec_col = CAST(104.123 as float)",
      "dec_col", DecimalType(15, 7), FloatType)
  }

  test("Plain assignment, assigning string typed numeric literal cast as int") {
    snc.sql("update testTable set int_col = cast('300' as int)")
    val expectedResult = Seq(Row(1, 300, 1, new java.math.BigDecimal("1.20"), "abc"),
      Row(2, 300, 2, new java.math.BigDecimal("1.20"), "xyz"))
    checkAnswer(snc.sql("select * from testTable order by id"), expectedResult)
  }

  test("Plain assignment, assigning number") {
    snc.sql("update testTable set int_col = 400").collect()
    val expectedResult = Seq(Row(1, 400, 1, new java.math.BigDecimal("1.20"), "abc"),
      Row(2, 400, 2, new java.math.BigDecimal("1.20"), "xyz"))
    checkAnswer(snc.sql("select * from testTable order by id"), expectedResult)
  }

  test("Plain assignment, assigning tighter numeric type to a decimal type") {
    snc.sql("update testTable set dec_col = cast(1 as short)")
    val expectedResult = Seq(Row(1, 1, 1, new java.math.BigDecimal("1"), "abc"),
      Row(2, 2, 2, new java.math.BigDecimal("1"), "xyz"))
    checkAnswer(snc.sql("select * from testTable order by id"), expectedResult)
  }

  test("Plain assignment, assigning numeric type to a string type") {
    snc.sql("update testTable set string_col = 100.20")
    val expectedResult = Seq(Row(1, 1, 1, new java.math.BigDecimal("1.20"), "100.20"),
      Row(2, 2, 2, new java.math.BigDecimal("1.20"), "100.20"))
    checkAnswer(snc.sql("select * from testTable order by id"), expectedResult)
  }

  test("Plain assignment, assigning wider numeric type to a decimal type") {
    assertForAnalysisException("update testTable set dec_col = cast(1 as int)",
      "dec_col", DecimalType(15, 7), IntegerType)
  }

  test("SnappyAnalyzer rules matches the rules from upstream Analyzer") {
    val analyzer = new Analyzer(snc.sessionState.catalog, snc.sessionState.conf)
    val snappyAnalyzer = new SnappyAnalyzer(snc.snappySessionState)
    assertEquals(analyzer.batches.size, snappyAnalyzer.batches.size)
    for ((expBatch, actBatch) <- analyzer.batches zip snappyAnalyzer.ruleBatches) {
      assertEquals(expBatch.name, actBatch.name)
      assertEquals(expBatch.strategy.toString, actBatch.strategy.toString)
      for ((exp, act) <- expBatch.rules zip actBatch.rules) {
        assertEquals(exp.ruleName, act.ruleName)
      }
    }
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
        assertEquals(expectedMessage, e.message)
    }
  }

  def assertForAnalysisException(sql: String): Unit = {
    try {
      snc.sql(sql)
      fail("AnalysisException was expected here.")
    } catch {
      case e: AnalysisException =>
        val expectedMessage = s"Implicit type casting of string type to numeric type is not" +
            s" performed for update statements."
        assertEquals(expectedMessage, e.message)
    }
  }
}
