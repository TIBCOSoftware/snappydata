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

import org.scalatest.Tag

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSnappySessionContext

class SnappySQLQuerySuite extends SQLQuerySuite with SharedSnappySessionContext {

  import testImplicits._

  def excluded: Seq[String] = Seq("inner join ON, one match per row",
  "data source table created in InMemoryCatalog should be able to read/write")

  override protected def test(testName: String, testTags: Tag*)(testFun: => Unit) = {
    if (!excluded.contains(testName)) {
      super.test(testName, testTags: _*)(testFun)
    }
  }


  test("snappy : inner join ON, one match per row") {
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

  test("snappy: data source table created in InMemoryCatalog should be able to read/write") {
    withTable("tbl") {
      sql("CREATE EXTERNAL TABLE tbl(i INT, j STRING) USING parquet")
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
