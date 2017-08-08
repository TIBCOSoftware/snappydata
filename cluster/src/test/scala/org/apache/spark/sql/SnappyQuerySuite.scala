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

import io.snappydata.SnappyFunSuite
import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.util.stackTraceToString
import org.scalatest.BeforeAndAfterAll

class SnappyQuerySuite extends SnappyFunSuite
  with Logging
  with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    snc.conf.clear()
    super.afterAll()
  }

  val snap = snc

  import snap.implicits._

  test("SNAP-1840 -> uncorrelated scalar subquery") {

    val df = Seq((1, "one"), (2, "two"), (3, "three")).toDF("key", "value")
    df.write.format("row").saveAsTable("subqueryData")

    checkAnswer(
      snc.sql("select -(select max(key) from subqueryData)"),
      Array(Row(-3))
    )

    checkAnswer(
      snc.sql("select (select key from subqueryData where key > 2 order by key limit 1) + 1"),
      Array(Row(4))
    )

    checkAnswer(
      snc.sql("select (select value from subqueryData limit 0)"),
      Array(Row(null))
    )

    checkAnswer(
      snc.sql("select (select min(value) from subqueryData" +
        " where key = (select max(key) from subqueryData) - 1)"),
      Array(Row("two"))
    )
    snc.dropTable("subqueryData", true)
  }

  test("NOT EXISTS predicate subquery") {
    val row = identity[(java.lang.Integer, java.lang.Double)](_)

    lazy val l = Seq(
      row(1, 2.0),
      row(1, 2.0),
      row(2, 1.0),
      row(2, 1.0),
      row(3, 3.0),
      row(null, null),
      row(null, 5.0),
      row(6, null)).toDF("a", "b")

    lazy val r = Seq(
      row(2, 3.0),
      row(2, 3.0),
      row(3, 2.0),
      row(4, 1.0),
      row(null, null),
      row(null, 5.0),
      row(6, null)).toDF("c", "d")

    l.write.format("row").saveAsTable("l")
    r.write.format("row").saveAsTable("r")

    checkAnswer(
      snc.sql("select * from l where not exists (select * from r where l.a = r.c)"),
      Row(1, 2.0) :: Row(1, 2.0) :: Row(null, null) :: Row(null, 5.0) :: Nil)
    checkAnswer(
      snc.sql("select * from l where not exists (select * from r where l.a = r.c and l.b < r.d)"),
      Row(1, 2.0) :: Row(1, 2.0) :: Row(3, 3.0) ::
        Row(null, null) :: Row(null, 5.0) :: Row(6, null) :: Nil)

    snc.dropTable("l", true)
    snc.dropTable("r", true)
  }

  /**
    * Runs the plan and makes sure the answer matches the expected result.
    *
    * @param df             the [[DataFrame]] to be executed
    * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
    */
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
