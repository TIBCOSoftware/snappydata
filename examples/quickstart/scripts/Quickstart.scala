/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
 * ./spark-shell --driver-memory 4g --master local[*] --packages "SnappyDataInc:snappydata:0.7
 * .2-s_2.11" -i Quickstart
 * .scala
 *
 * To execute this script on spark you can use same command as above without specifying packages
 * as follows:
 * ./bin/spark-shell --driver-memory=4g --driver-java-options="-XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:MaxNewSize=1g -i Quickstart.scala
 */

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

//Benchmark function that will execute a function and returns time taken to execute that function
def benchmark(name: String, times: Int = 5, warmups: Int = 3)(f: => Unit) : Double = {
  for (i <- 1 to warmups) {
    f
  }
  val startTime = System.nanoTime
  for (i <- 1 to times) {
    f
  }
  val endTime = System.nanoTime
  val timeTaken = (endTime - startTime).toDouble / (times * 1000000.0)
  println(s"Average time taken in $name for $times runs: $timeTaken millis")
  timeTaken
}

//Create Dataframe can register temp table
var testDF = spark.range(100000000).selectExpr("id", "concat('sym', cast((id % 100) as STRING)) as sym")
testDF.cache
testDF.createOrReplaceTempView("sparkCacheTable")


val timeTakenSpark = benchmark("Spark perf") {spark.sql("select sym, avg(id) from sparkCacheTable group by sym").collect()}


testDF.unpersist()
System.gc()
System.runFinalization()


//Create SnappySession to execute queries from spark
val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext)
testDF = snappy.range(100000000).selectExpr("id", "concat('sym', cast((id % 100) as varchar(10))) as sym")

snappy.sql("drop table if exists snappyTable")
snappy.sql("create table snappyTable (id bigint not null, sym varchar(10) not null) using column")
benchmark("Snappy insert perf", 1, 0) {testDF.write.insertInto("snappyTable") }

val timeTakenSnappy = benchmark("Snappy perf") {snappy.sql("select sym, avg(id) from snappyTable group by sym").collect()}
System.exit(0)
