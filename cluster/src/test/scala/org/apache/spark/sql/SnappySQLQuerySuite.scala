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

import org.apache.spark.sql.test.SQLTestData.TestData2

class SnappySQLQuerySuite extends SnappyFunSuite {

  private lazy val session: SnappySession = snc.snappySession

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

  import session.implicits._

  test("SNAP-1840 -> uncorrelated scalar subquery") {

    val df = Seq((1, "one"), (2, "two"), (3, "three")).toDF("key", "value")
    df.write.format("row").saveAsTable("subqueryData")

    checkAnswer(
      session.sql("select -(select max(key) from subqueryData)"),
      Array(Row(-3))
    )

    checkAnswer(
      session.sql("select (select key from subqueryData where key > 2 order by key limit 1) + 1"),
      Array(Row(4))
    )

    checkAnswer(
      session.sql("select (select value from subqueryData limit 0)"),
      Array(Row(null))
    )

    checkAnswer(
      session.sql("select (select min(value) from subqueryData" +
          " where key = (select max(key) from subqueryData) - 1)"),
      Array(Row("two"))
    )
    session.dropTable("subqueryData", ifExists = true)
  }

  test("NOT EXISTS predicate subquery") {
    val row = identity[(java.lang.Integer, java.lang.Double)] _

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
      session.sql("select * from l where not exists (select * from r where l.a = r.c)"),
      Row(1, 2.0) :: Row(1, 2.0) :: Row(null, null) :: Row(null, 5.0) :: Nil)
    checkAnswer(
      session.sql("select * from l where not exists " +
          "(select * from r where l.a = r.c and l.b < r.d)"),
      Row(1, 2.0) :: Row(1, 2.0) :: Row(3, 3.0) ::
          Row(null, null) :: Row(null, 5.0) :: Row(6, null) :: Nil)

    session.dropTable("l", ifExists = true)
    session.dropTable("r", ifExists = true)
  }
}

case class LowerCaseData(n: Int, l: String)
