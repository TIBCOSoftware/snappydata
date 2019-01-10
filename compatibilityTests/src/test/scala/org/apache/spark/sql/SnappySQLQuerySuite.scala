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
/*
 * Test for SPARK-10316 taken from Spark's DataFrameSuite having license as below.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import java.io.File

import org.apache.spark.sql.execution.aggregate
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSnappySessionContext, SnappySparkTestUtil}

/**
  * Some of the tests from SQLQuerySuite is re-written here to suite SnappyData requirements.
  * Those tests are excluded from being executed from SnappySession.
  */
class SnappySQLQuerySuite extends SQLQuerySuite with
    SharedSnappySessionContext with SnappySparkTestUtil {

  import testImplicits._

  override def excluded: Seq[String] = Seq("inner join ON, one match per row",
    "data source table created in InMemoryCatalog should be able to read/write",
    // Explicit parquet table creation. Needs path option as parquet is EXTERNAL table in SD
    "SPARK-16644: Aggregate should not put aggregate expressions to constraints",
    // Explicit parquet table creation. Needs path option as parquet is EXTERNAL table in SD
    "SPARK-16975: Column-partition path starting '_' should be handled correctly",
    // SD create table fails even if a temp table with the same name exists.
    "CREATE TABLE USING should not fail if a same-name temp view exists",
    // SD does not support TABLESAMPLE operator
     "negative in LIMIT or TABLESAMPLE",
    // double quote in leteral value
     "date row"
     )

  override def ignored: Seq[String] = Seq(
    "describe functions",
    "inner join where, one match per row",
    "inner join, where, multiple matches",
    "left outer join",
    "right outer join",
    "SPARK-17863: SELECT distinct does not work correctly if order by missing attribute",
    "metadata is propagated correctly",
    "SPARK-4322 Grouping field with struct field as sub expression",
    "aggregation with codegen updates peak execution memory",
    "SPARK-9511: error with table starting with number",
    "run sql directly on files",
    "Star Expansion - table with zero column",
    "Common subexpression elimination",
    "Eliminate noop ordinal ORDER BY *** FAILED",
    "check code injection is prevented",
    "Eliminate noop ordinal ORDER BY"
  )

  test("SD:inner join ON, one match per row") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      checkAnswer(
        sql("SELECT * FROM UPPERCASEDATA U JOIN lowercasedata L ON U.N = L.n"),
        Seq(
          Row(1, "A", 1, "a"),
          Row(2, "B", 2, "b"),
          Row(3, "C", 3, "c"),
          Row(4, "D", 4, "d")))
    }
  }
  // Changed the code as PARQUET is an external table in SD
  test("SD:data source table created in InMemoryCatalog should be able to read/write") {
    withDir("tbl") {
      withTable("tbl") {
        sql("CREATE EXTERNAL TABLE tbl(i INT, j STRING) USING parquet options(path 'tbl')")
        checkAnswer(sql("SELECT i, j FROM tbl"), Nil)

        Seq(1 -> "a", 2 -> "b").toDF("i", "j").write.mode("overwrite").insertInto("tbl")
        checkAnswer(sql("SELECT i, j FROM tbl"), Row(1, "a") :: Row(2, "b") :: Nil)

        Seq(3 -> "c", 4 -> "d").toDF("i", "j").write.mode("append").saveAsTable("tbl")
        checkAnswer(
          sql("SELECT i, j FROM tbl"),
          Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Row(4, "d") :: Nil)
      }
    }
  }

  // Changed the code as PARQUET is an external table in SD
  test("SD:SPARK-16644: Aggregate should not put aggregate expressions to constraints") {
    withDir("tbl") {
      withTable("tbl") {
        sql("CREATE EXTERNAL TABLE tbl(a INT, b INT) USING parquet options(path 'tbl')")
        checkAnswer(sql(
          """
            |SELECT
            |  a,
            |  MAX(b) AS c1,
            |  b AS c2
            |FROM tbl
            |WHERE a = b
            |GROUP BY a, b
            |HAVING c1 = 1
          """.stripMargin), Nil)
      }
    }
  }

  test("SD:SPARK-16975: Column-partition path starting '_' should be handled correctly") {
    withTempDir { dir =>
      val parquetDir = new File(dir, "parquet").getCanonicalPath
      spark.range(10).withColumn("_col", $"id").write.partitionBy("_col").parquet(parquetDir)
      spark.read.parquet(parquetDir)
    }
  }
  // Different behaviour than Spark.
  test("SD:CREATE TABLE USING should fail even if a same-name temp view exists") {
    withTable("same_name") {
      withTempView("same_name") {
        spark.range(10).createTempView("same_name")
        intercept[AnalysisException] {
          sql("CREATE TABLE same_name(i int) USING json")
        }
      }
    }
  }
  // We don't support double quotes around literals
  test("SD: date row") {
    checkAnswer(sql(
      """select cast('2015-01-28' as date) from testData limit 1"""),
      Row(java.sql.Date.valueOf("2015-01-28"))
    )
  }

  override protected def testCodeGen(sqlText: String, expectedResults: Seq[Row]): Unit = {
    val df = sql(sqlText)
    // First, check if we have GeneratedAggregate.
    val hasGeneratedAgg = df.queryExecution.sparkPlan
        .collect { case _: aggregate.SnappyHashAggregateExec => true }
        .nonEmpty
    if (!hasGeneratedAgg) {
      fail(
        s"""
           |Codegen is enabled, but query $sqlText does not have SnappyHashAggregateExec in the plan.
           |${df.queryExecution.simpleString}
         """.stripMargin)
    }
    // Then, check results.
    checkAnswer(df, expectedResults)
  }
}
