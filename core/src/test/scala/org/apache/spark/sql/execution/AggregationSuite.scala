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
package org.apache.spark.sql.execution

import java.sql.Timestamp

import io.snappydata.{Property, SnappyFunSuite}

import org.apache.spark.sql.{Row, SparkSession}

class AggregationSuite extends SnappyFunSuite {

  test("AVG plan failure for nullables") {
    val spark = new SparkSession(sc)
    val snappy = snc.snappySession
    snappy.sql(s"set ${Property.ColumnBatchSize.name}=5000")

    val checkDF = spark.range(10000).selectExpr("id", "(id * 12) as k",
      "concat('val', cast((id % 100) as string)) as s")
    val insertDF = snappy.range(10000).selectExpr("id", "(id * 12) as k",
      "concat('val', cast((id % 100) as string)) as s")
    val symDF = spark.range(20).selectExpr("concat('val', cast(id as string)) as s")
    val sDF = snappy.range(20).selectExpr("concat('val', cast(id as string)) as s")

    snappy.sql("drop table if exists test")
    snappy.sql("create table test (id bigint, k bigint, s varchar(10)) " +
        "using column options(buckets '8')")
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

  test("support for LATERAL VIEW in SnappyParser") {
    val snappy = snc.snappySession
    val json =
      """{ "id": 1, "name": "A green door", "price": 12.50, "tags": ["home", "green"],
        | "parts" : [ { "lock" : "One lock", "key" : "single key" },
        | { "lock" : "2 lock", "key" : "2 key" } ] }""".stripMargin
    val rdd = sc.makeRDD(Seq(json))
    val ds = snappy.read.json(rdd)
    ds.createOrReplaceTempView("json")
    val res = snappy.sql("SELECT id, part.lock, part.key FROM json " +
        "LATERAL VIEW explode(parts) partTable AS part")
    checkAnswer(res, Seq(Row(1, "One lock", "single key"), Row(1, "2 lock", "2 key")))
  }

  test("interval expression and literals") {
    val snappy = snc.snappySession
    val ts = "2019-05-20 02:04:01"
    val tsTime = Timestamp.valueOf(ts).getTime

    var expr = s"select timestamp('$ts') + interval 4 hours"
    assert(snappy.sql(expr).collect() ===
        Array(Row(new Timestamp(tsTime + 4L * 3600000L))))

    expr = s"select timestamp('$ts') + interval '4' hours 3 mins"
    assert(snappy.sql(expr).collect() ===
        Array(Row(new Timestamp(tsTime + 4L * 3600000L + 3L * 60000L))))

    expr = s"select timestamp('$ts') + interval hour('$ts') hours"
    assert(snappy.sql(expr).collect() ===
        Array(Row(new Timestamp(tsTime + 2L * 3600000L))))

    expr = s"select timestamp('$ts') + interval hour('$ts') hours 3 minutes"
    assert(snappy.sql(expr).collect() ===
        Array(Row(new Timestamp(tsTime + 2L * 3600000L + 3L * 60000L))))

    expr = s"select timestamp('$ts') + interval minute('$ts') minutes '3' secs"
    assert(snappy.sql(expr).collect() ===
        Array(Row(new Timestamp(tsTime + 4L * 60000L + 3L * 1000L))))

    expr = s"select timestamp('$ts') + interval 4 hours second('$ts') seconds"
    assert(snappy.sql(expr).collect() ===
        Array(Row(new Timestamp(tsTime + 4L * 3600000L + 1L * 1000L))))
  }
}
