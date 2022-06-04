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
 *
 * ./spark-shell --driver-memory 4g --master local[*] \
 * --packages "io.snappydata:snappydata-spark-connector_2.11:1.3.1" -i Quickstart.scala
 *
 * Or you can execute this script on SnappyData's spark distribution with the same command as above
 * without requiring packages as follows:
 *
 * ./bin/spark-shell --driver-memory=4g --driver-java-options="-XX:+UseConcMarkSweepGC \
 * -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:MaxNewSize=1g" -i Quickstart.scala
 */

// Benchmark function that will execute a function and returns time taken to execute that function
def benchmark(name: String, times: Int = 5, warmups: Int = 3)(f: => Unit): Double = {
  for (_ <- 1 to warmups) {
    f
  }
  val startTime = System.nanoTime
  for (_ <- 1 to times) {
    f
  }
  val endTime = System.nanoTime
  val timeTaken = (endTime - startTime).toDouble / (times * 1000000.0)
  // scalastyle:off println
  println()
  println(s"Average time taken in $name for $times runs: $timeTaken millis")
  println()
  // scalastyle:on println
  timeTaken
}

sc.setLogLevel("ERROR")
// Create Dataframe can register temp table
var testDF = spark.range(100000000).selectExpr("id",
  "concat('sym', cast((id % 100) as STRING)) as sym")
testDF.cache()
testDF.createOrReplaceTempView("sparkCacheTable")

val timeTakenSpark = benchmark("Spark perf") {
  spark.sql("select sym, avg(id) from sparkCacheTable group by sym").collect()
}


testDF.unpersist()
System.gc()
System.runFinalization()


// Create SnappySession to execute queries from spark
val snappy = new org.apache.spark.sql.SnappySession(spark.sparkContext)
testDF = snappy.range(100000000).selectExpr("id",
  "concat('sym', cast((id % 100) as varchar(10))) as sym")

snappy.sql("drop table if exists snappyTable")
snappy.sql("create table snappyTable (id bigint not null, sym varchar(10) not null) using column")
benchmark("Snappy insert perf", 1, 0) {
  testDF.write.insertInto("snappyTable")
}

// GROUP BY on single key when number of results is not large is faster with older implementation
snappy.conf.set("snappydata.sql.useOptimizedHashAggregateForSingleKey", "false")
// Direct collect for GROUP BY at driver avoiding an EXCHANGE when number of results is not large
snappy.conf.set("snappydata.sql.useDriverCollectForGroupBy", "true")

val timeTakenSnappy = benchmark("Snappy perf") {
  snappy.sql("select sym, avg(id) from snappyTable group by sym").collect()
}

System.exit(0)
