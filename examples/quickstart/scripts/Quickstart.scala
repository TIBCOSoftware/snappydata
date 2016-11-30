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
/**
 * This script demonstrate the performance difference between Spark and Snappydata.
 * To execute this script on spark you can use below command:
 * ./spark-shell --driver-memory 4g --master local[1] --packages "SnappyDataInc:snappydata:0.6
 * .2-s_2.11" -i Quickstart
 * .scala
 *
 * To execute this script on spark you can use same command as above without specifying packages
 * as follows:
 * ./spark-shell --driver-memory 4g --master local[1] -i Quickstart.scala
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf


//Benchmark function that will execute a function and returns time taken to execute that function
def benchmark(name: String)(f: => Unit): Double = {
  val startTime = System.nanoTime
  f
  val endTime = System.nanoTime
  val timeTaken = (endTime - startTime).toDouble / 1000000000
  println(s"Time taken in $name: " + timeTaken + " seconds")
  timeTaken
}

//Create spark session
val spark = SparkSession.builder.master("local[1]").appName("spark, " +
    "Snappy Perf test").getOrCreate()

//Create Dataframe can register temp table
spark.conf.set(SQLConf.COMPRESS_CACHED.key,false)
var sparkDataFrame = spark.range(100000000).selectExpr("id", "floor(rand() * 10000) as k")
sparkDataFrame.cache
sparkDataFrame.createOrReplaceTempView("sparkCacheTable")


benchmark("spark.sql(\"select avg(k), avg(id) from sparkCacheTable\")") {
  spark.sql("select avg(k), avg(id) from sparkCacheTable").show
}

benchmark("spark.sql(\"select avg(k), avg(id) from sparkCacheTable\").show") {
  spark.sql("select avg(k), avg(id) from sparkCacheTable").show
}

val timeRequiredForQ1Spark = benchmark("spark.sql(\"select avg(k), avg(id) from " +
    "sparkCacheTable\").show") {
  spark.sql("select avg(k), avg(id) from sparkCacheTable").show
}

val timeRequiredForQ2Spark = benchmark("spark.sql(\"select avg(k), avg(id) from sparkCacheTable " +
    "group by " +
    "(id%100)\").show") {
  spark.sql("select avg(k), avg(id) from sparkCacheTable group by (id%100)").show
}

sparkDataFrame.unpersist()
System.gc()
System.runFinalization()


//Create SnappySession to execute queries from spark
val snappy = org.apache.spark.sql.SnappyContext.apply().snappySession
snappy.conf.set(SQLConf.COMPRESS_CACHED.key,false)
var testDF = snappy.range(100000000).selectExpr("id", "floor(rand() * 10000) as k")

snappy.sql("drop table if exists snappyTable")
snappy.sql("create table snappyTable (id bigint not null, k bigint not null) using column")
testDF.write.insertInto("snappyTable")


benchmark("snappy.sql(\"select avg(k), avg(id) from snappyTable\")") {
  snappy.sql("select avg(k), avg(id) from snappyTable").show
}


benchmark("snappy.sql(\"select avg(k), avg(id) from snappyTable\").show") {
  snappy.sql("select avg(k), avg(id) from snappyTable").show
}

val timeRequiredForQ1Snappy = benchmark("snappy.sql(\"select avg(k), avg(id) from snappyTable\")" +
    ".show") {
  snappy.sql("select avg(k), avg(id) from snappyTable").show
}
val q1Diff = (timeRequiredForQ1Spark / timeRequiredForQ1Snappy)
println(s"\n\nSnappy is $q1Diff times faster than spark \n\n")
if (!(q1Diff >= 10)) {
  println("Cannot meet the required performance")
}



val timeRequiredForQ2Snappy = benchmark("snappy.sql(\"select avg(k), avg(id) from \" + " +
    "\"snappyTable group by " +
    "(id%100)\").show") {
  snappy.sql("select avg(k), avg(id) from " + "snappyTable group by (id%100)").show
}

val q2Diff = (timeRequiredForQ2Spark / timeRequiredForQ2Snappy)
println(s"\n\nSnappy is $q2Diff times faster than spark \n\n")
if (!(q2Diff >= 3)) {
  println("Cannot meet the required performance")
}



System.exit(0)