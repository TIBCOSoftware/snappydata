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

import org.apache.spark.sql.types.{DecimalType, IntegerType, StructField, StructType}
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

  test("support for LATERAL VIEW in SQL parser") {
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

  test("support for PIVOT in SQL parser") {
    val snappy = snc.snappySession
    snappy.sql("create table dayAvgTemp (day date, temp int)")
    val data = getClass.getResource("/dayAvgTemp.txt").getPath
    snappy.read.csv(data).createOrReplaceTempView("dayAvgTemp_staging")
    snappy.sql("insert into dayAvgTemp select * from dayAvgTemp_staging")

    val decType = new DecimalType(5, 2)
    val expectedSchema = StructType(StructField("year", IntegerType) ::
        (1 to 12).map(i => StructField(i.toString, decType)).toList)
    val expectedResult =
      """
        |[2019,10.93,16.04,23.36,31.32,36.39,34.79,27.57,27.36,28.50,31.18,25.07,17.46]
        |[2018,12.07,15.14,24.18,31.89,37.46,37.79,28.14,27.61,26.79,30.50,24.25,17.07]
        |[2017,10.14,16.68,24.21,31.11,35.43,35.32,27.54,28.75,26.82,29.75,23.82,16.43]
        |[2016,12.86,17.14,24.68,32.36,37.11,33.79,26.46,28.43,27.29,29.75,23.79,17.11]
        |[2015,11.96,17.61,24.04,31.82,34.68,36.57,27.61,27.82,27.43,29.46,24.29,16.00]
      """.stripMargin

    // PIVOT should work with implicit group by columns
    val res1 = snappy.sql(
      """
        |select * from (
        |  select year(day) year, month(day) month, temp
        |  from dayAvgTemp
        |)
        |PIVOT (
        |  CAST(avg(temp) AS DECIMAL(5, 2))
        |  FOR month IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        |)
        |ORDER BY year DESC
      """.stripMargin
    )
    assert(res1.schema === expectedSchema)
    assert(res1.collect().mkString("\n") === expectedResult.trim)

    // also check PIVOT with explicit group by columns
    val res2 = snappy.sql(
      """
        |select * from (
        |  select year(day) year, month(day) month, temp
        |  from dayAvgTemp
        |)
        |PIVOT (
        |  CAST(avg(temp) AS DECIMAL(5, 2))
        |  FOR month IN (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)
        |)
        |GROUP BY year
        |ORDER BY year DESC
      """.stripMargin
    )
    assert(res2.schema === expectedSchema)
    assert(res2.collect().mkString("\n") === expectedResult.trim)
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
