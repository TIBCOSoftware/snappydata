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

import org.apache.spark.sql.execution.GlobalTempViewSuite
import org.apache.spark.sql.test.{SharedSnappySessionContext, SnappySparkTestUtil}

/**
 * SnappyData allows global temporary views to be addressed without the "global_temp" schema
 * prefix, so couple of tests have been overridden to expect the same.
 */
class SnappyGlobalTempViewSuite extends GlobalTempViewSuite
    with SharedSnappySessionContext with SnappySparkTestUtil {

  import testImplicits._

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    globalTempDB = spark.sharedState.globalTempViewManager.database
  }

  private var globalTempDB: String = _

  override def excluded: Seq[String] = Seq(
    "basic semantic",
    "should lookup global temp view if and only if global temp db is specified"
  )

  test("basic snappy semantic") {
    sql("CREATE GLOBAL TEMP VIEW src AS SELECT 1, 'a'")

    // Snappy can query global temporary table without global_temp prefix
    spark.table("src")

    // Use qualified name to refer to the global temp view explicitly.
    checkAnswer(spark.table(s"$globalTempDB.src"), Row(1, "a"))

    // Table name without database can also refer to a global temp view.
    sql(s"DROP VIEW src")

    // The global temp view should be dropped successfully.
    intercept[TableNotFoundException](spark.table(s"$globalTempDB.src"))

    // We can also use Dataset API to create global temp view
    Seq(1 -> "a").toDF("i", "j").createGlobalTempView("src")
    checkAnswer(spark.table(s"$globalTempDB.src"), Row(1, "a"))

    // Use qualified name to rename a global temp view.
    sql(s"ALTER VIEW $globalTempDB.src RENAME TO src2")
    intercept[TableNotFoundException](spark.table(s"$globalTempDB.src"))
    checkAnswer(spark.table(s"$globalTempDB.src2"), Row(1, "a"))

    // Use qualified name to alter a global temp view.
    sql(s"ALTER VIEW $globalTempDB.src2 AS SELECT 2, 'b'")
    checkAnswer(spark.table(s"$globalTempDB.src2"), Row(2, "b"))

    // We can also use Catalog API to drop global temp view
    spark.catalog.dropGlobalTempView("src2")
    intercept[TableNotFoundException](spark.table(s"$globalTempDB.src2"))
  }

  test("should lookup global temp view if possible") {
    try {
      sql("CREATE GLOBAL TEMP VIEW same_name AS SELECT 3, 4")
      sql("CREATE TEMP VIEW same_name AS SELECT 1, 2")

      checkAnswer(sql("SELECT * FROM same_name"), Row(1, 2))

      // we never lookup global temp views if database is not specified in table name
      spark.catalog.dropTempView("same_name")
      checkAnswer(sql("SELECT * FROM same_name"), Row(3, 4))

      // Use qualified name to lookup a global temp view.
      checkAnswer(sql(s"SELECT * FROM $globalTempDB.same_name"), Row(3, 4))
    } finally {
      spark.catalog.dropTempView("same_name")
      spark.catalog.dropGlobalTempView("same_name")
    }
  }
}
