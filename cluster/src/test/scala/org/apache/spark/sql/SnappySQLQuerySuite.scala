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

import io.snappydata.SnappyFunSuite

import org.apache.spark.sql.catalyst.util.stackTraceToString
import org.apache.spark.sql.test.SQLTestData.TestData2

class SnappySQLQuerySuite extends SnappyFunSuite {

  // Ported test from Spark
  test("SNAP-1885 : left semi greater than predicate and equal operator") {
    val df = snc.createDataFrame(snc.sparkContext.parallelize(
          TestData2(1, 1) ::
          TestData2(1, 2) ::
          TestData2(2, 1) ::
          TestData2(2, 2) ::
          TestData2(3, 1) ::
          TestData2(3, 2) :: Nil, 2))
    df.write.format("row").saveAsTable("testData2")

    checkAnswer(
      snc.sql("SELECT * FROM testData2 x LEFT SEMI JOIN testData2 y " +
          "ON x.b = y.b and x.a >= y.a + 2"),
      Seq(Row(3, 1), Row(3, 2))
    )

    checkAnswer(
      snc.sql("SELECT * FROM testData2 x LEFT SEMI JOIN testData2 y " +
          "ON x.b = y.a and x.a >= y.b + 1"),
      Seq(Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2))
    )
  }

  protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    val analyzedDF = try df catch {
      case ae: AnalysisException =>
        if (ae.plan.isDefined) {
          fail(
            s"""
               |Failed to analyze query: $ae
               |${ae.plan.get}
               |
               |${stackTraceToString(ae)}
               |""".stripMargin)
        } else {
          throw ae
        }
    }

    assertEmptyMissingInput(analyzedDF)

    QueryTest.checkAnswer(analyzedDF, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  test("SNAP-1884 Join with temporary table not returning rows") {
    val df = snc.createDataFrame(snc.sparkContext.parallelize(
      LowerCaseData(1, "a") ::
        LowerCaseData(2, "b") ::
        LowerCaseData(3, "c") ::
        LowerCaseData(4, "d") :: Nil))
    df.write.format("row").saveAsTable("lowerCaseData")
    snc.sql("SELECT DISTINCT n FROM lowerCaseData ORDER BY n DESC")
      .limit(2)
      .createOrReplaceTempView("subset1")
    snc.sql("SELECT DISTINCT n FROM lowerCaseData ORDER BY n ASC")
      .limit(2)
      .createOrReplaceTempView("subset2")
    checkAnswer(
      snc.sql("SELECT * FROM lowerCaseData INNER JOIN subset1 ON " +
        "subset1.n = lowerCaseData.n ORDER BY lowerCaseData.n"),
      Row(3, "c", 3) ::
        Row(4, "d", 4) :: Nil)

    checkAnswer(
      snc.sql("SELECT * FROM lowerCaseData INNER JOIN subset2 " +
        "ON subset2.n = lowerCaseData.n ORDER BY lowerCaseData.n"),
      Row(1, "a", 1) ::
        Row(2, "b", 2) :: Nil)

    snc.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    df.write.format("column").saveAsTable("collowerCaseData")

    snc.sql("SELECT DISTINCT n FROM collowerCaseData ORDER BY n DESC")
      .limit(2)
      .createOrReplaceTempView("colsubset1")
    snc.sql("SELECT DISTINCT n FROM collowerCaseData ORDER BY n ASC")
      .limit(2)
      .createOrReplaceTempView("colsubset2")
    checkAnswer(
      snc.sql("SELECT * FROM collowerCaseData INNER JOIN colsubset1 ON " +
        "colsubset1.n = collowerCaseData.n ORDER BY collowerCaseData.n"),
      Row(3, "c", 3) ::
        Row(4, "d", 4) :: Nil)

    checkAnswer(
      snc.sql("SELECT * FROM collowerCaseData INNER JOIN colsubset2 " +
        "ON colsubset2.n = collowerCaseData.n ORDER BY collowerCaseData.n"),
      Row(1, "a", 1) ::
        Row(2, "b", 2) :: Nil)
  }
  /**
    * Asserts that a given [[Dataset]] does not have missing inputs in all the analyzed plans.
    */
  def assertEmptyMissingInput(query: Dataset[_]): Unit = {
    assert(query.queryExecution.analyzed.missingInput.isEmpty,
      s"The analyzed logical plan has missing inputs:\n${query.queryExecution.analyzed}")
    assert(query.queryExecution.optimizedPlan.missingInput.isEmpty,
      s"The optimized logical plan has missing inputs:\n${query.queryExecution.optimizedPlan}")
    assert(query.queryExecution.executedPlan.missingInput.isEmpty,
      s"The physical plan has missing inputs:\n${query.queryExecution.executedPlan}")
  }

}

case class LowerCaseData(n: Int, l: String)