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
package org.apache.spark.sql.execution

import io.snappydata.{Property, SnappyFunSuite}

import org.apache.spark.sql.{DataFrame, QueryTest, Row, SnappySession, SparkSession}

class AggregationSuite extends SnappyFunSuite {

  private def checkAnswer(result: DataFrame, expected: Seq[Row]): Unit = {
    QueryTest.checkAnswer(result, expected) match {
      case Some(errMessage) => throw new RuntimeException(errMessage)
      case None => // all good
    }
  }

  test("AVG plan failure for nullables") {
    val spark = new SparkSession(sc)
    val snappy = new SnappySession(sc)
    snappy.sql(s"set ${Property.ColumnBatchSize.name}=5000")

    val checkDF = spark.range(10000).selectExpr("id", "(id * 12) as k",
      "concat('val', cast((id % 100) as string)) as s")
    val insertDF = snappy.range(10000).selectExpr("id", "(id * 12) as k",
      "concat('val', cast((id % 100) as string)) as s")
    val symDF = spark.range(20).selectExpr("concat('val', cast(id as string)) as s")
    val sDF = snappy.range(20).selectExpr("concat('val', cast(id as string)) as s")

    snappy.sql("drop table if exists test")
    snappy.sql("create table test (id bigint, k bigint, s varchar(10)) " +
        "using column options(buckets '3')")
    insertDF.write.insertInto("test")
    snappy.sql("drop table if exists sym")
    snappy.sql("create table sym (s varchar(10))")
    sDF.write.insertInto("sym")

    // simple aggregation
    var query = "select avg(k) from test"
    var expectedAnswer = checkDF.selectExpr("avg(k)").collect().toSeq
    var result = snappy.sql(query)
    checkAnswer(result, expectedAnswer)

    // group by aggregation
    query = "select s, avg(k) from test group by s"
    expectedAnswer = checkDF.groupBy("s").agg("k" -> "avg").collect().toSeq
    result = snappy.sql(query)
    checkAnswer(result, expectedAnswer)

    // join aggregation
    query = "select avg(k) from test natural join sym"
    expectedAnswer = checkDF.join(symDF, "s").selectExpr("avg(k)").collect().toSeq
    result = snappy.sql(query)
    checkAnswer(result, expectedAnswer)

    // join then group by aggregation
    query = "select avg(k) from test natural join sym"
    expectedAnswer = checkDF.join(symDF, "s").selectExpr("avg(k)").collect().toSeq
    result = snappy.sql(query)
    checkAnswer(result, expectedAnswer)
  }
}
