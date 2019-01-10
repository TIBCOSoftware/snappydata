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

package io.snappydata

import io.snappydata.datasource.v2.SnappyDataSource
import org.scalatest.ConfigMap

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession

class BasicDataSourceV2Suite extends SparkFunSuite {

  var spark: SparkSession = _

  override def beforeAll(configMap: ConfigMap): Unit = {
    spark = SparkSession.builder()
        .appName("BasicDataSourceV2Suite")
        .master("local[*]")
        .getOrCreate()
  }

  override def afterAll(configMap: ConfigMap): Unit = {
    spark.stop()
  }

  test("initialize a datasource") {
    //    val df = spark.read.format("snappydata")
    //        .option("snappydata.connection", "localhost:1527" )
    //        .option("table", "app.t1")

    val df = spark.read.format(classOf[SnappyDataSource].getName)
        .option("snappydata.connection", "localhost:1527" )
        .option("table", "APP.TEST_TABLE")
        .option("user", "APP")
        .option("password", "APP")
    //    df.load().select("COL1").collect()
    //    df.load().select("COL1").collect().foreach(println)
    //    df.load().count()

    df.load().createOrReplaceTempView("v1")
    //    val df2 =spark.sql("select avg(COL1) from v1 group by COL1")
    //    df2.explain()
    //    df2.collect().foreach(println)


    val df3 = spark.sql("select id, rank, designation from v1 ")/* where id is null */
    df3.explain(true)
    // scalastyle:off
    println("numrows = " + df3.count())
    df3.collect().foreach(println)

  }

}
