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
package org.apache.spark.sql.execution

import io.snappydata.SnappyFunSuite

import org.apache.spark.sql.{QueryTest, SnappySession, SparkSession}

class AggregationSuite extends SnappyFunSuite {

  test("AVG plan failure") {
    val spark = new SparkSession(sc)
    val snappy = new SnappySession(sc)

    val checkDF = spark.range(1000)
        .selectExpr("id", "(rand() * 1000.0) as k")
    checkDF.cache()

    val expectedAnswer = checkDF.selectExpr("avg(k)").collect().toSeq

    val query = "select avg(k) from test"
    snappy.sql("drop table if exists test")
    snappy.sql("create table test (id bigint, k double) using column")
    snappy.insert("test", checkDF.collect(): _*)
    val result = snappy.sql(query)

    QueryTest.checkAnswer(result, expectedAnswer) match {
      case Some(errMessage) => throw new RuntimeException(errMessage)
      case None => // all good
    }
  }
}
