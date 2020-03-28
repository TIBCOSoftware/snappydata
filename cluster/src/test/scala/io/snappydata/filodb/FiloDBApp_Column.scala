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

package io.snappydata.filodb

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.util.Random


import org.apache.spark.sql.{DataFrame, SaveMode, SnappyContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * This application depicts how a Spark cluster can
 * connect to a Snappy cluster to fetch and query the tables
 * using Scala APIs in a Spark App.
 */
object FiloDBApp_Column {

  def main(args: Array[String]) {
    // scalastyle:off println

    val taxiCsvFile = args(0)
    val numRuns = 50

    // Queries
    val medallions = Array("23A89BC906FBB8BD110677FBB0B0A6C5",
      "3F8D8853F7EF89A7630565DDA38E9526",
      "3FE07421C46041F4801C7711F5971C63",
      "789B8DC7F3CB06A645B0CDC24591B832",
      "18E80475A4E491022BC2EF8559DABFD8",
      "761033F2C6F96EBFA9F578E968FDEDE5",
      "E4C72E0EE95C31D6B1FEFCF3F876EF90",
      "AF1421FCAA4AE912BDFC996F8A9B5675",
      "FB085B55ABF581ADBAD3E16283C78C01",
      "29CBE2B638D6C9B7239D2CA7A72A70E9")

    // trip info for a single driver within a given time range
    val singleDriverQueries = (1 to 20).map { i =>
      val medallion = medallions(Random.nextInt(medallions.size))
      s"SELECT avg(trip_distance), avg(passenger_count) from nyctaxi where medallion = '$medallion'" +
          s" AND pickup_datetime > '2013-01-15T00Z' AND pickup_datetime < '2013-01-22T00Z'"
    }

    // average trip distance by day for several days

    val allQueries = singleDriverQueries


    val conf = (new SparkConf).setMaster("local[8]")
        .setAppName("test")
        .set("spark.scheduler.mode", "FAIR")
        .set("spark.ui.enabled", "false") // No need for UI when doing perf stuff

    val sc = new SparkContext(conf)
    val snc = SnappyContext(sc)
    snc.sql("set spark.sql.shuffle.partitions=4")
    snc.dropTable("NYCTAXI", ifExists = true)

    // Ingest file - note, this will take several minutes
    puts("Starting ingestion...")
    val csvDF = snc.read.format("com.databricks.spark.csv").
        option("header", "true").option("inferSchema", "true").load(taxiCsvFile)

    val p1 = Map(("PARTITION_BY" -> "medallion"), ("BUCKETS" -> "5"))
    snc.createTable("NYCTAXI", "column", csvDF.schema, p1)
    csvDF.write.format("column").mode(SaveMode.Append).options(p1).saveAsTable("NYCTAXI")
    puts("Ingestion done.")

    // run queries

    val cnts = snc.sql("select count(*) from NYCTAXI").collect()
    for (s <- cnts) {
      var output = s.toString()
      puts(s"Total count : $output")
    }

    import scala.concurrent.ExecutionContext.Implicits.global

    val cachedDF = new collection.mutable.HashMap[String, DataFrame]

    def getCachedDF(query: String): DataFrame =
      cachedDF.getOrElseUpdate(query, snc.sql(query))

    def runQueries(queries: Array[String], numQueries: Int = 1000): Unit = {
      val startMillis = System.currentTimeMillis
      val futures = (0 until numQueries).map(i => getCachedDF(queries(Random.nextInt(queries.size))).rdd.collectAsync)
      val fut = Future.sequence(futures.asInstanceOf[Seq[Future[Array[_]]]])
      Await.result(fut, Duration.Inf)
      val endMillis = System.currentTimeMillis
      val qps = numQueries / ((endMillis - startMillis) / 1000.0)
      puts(s"Ran $numQueries queries in ${endMillis - startMillis} millis.  QPS = $qps")
    }

    puts("Warming up...")
    runQueries(allQueries.toArray, 100)
    Thread sleep 2000
    puts("Now running queries for real...")
    (0 until numRuns).foreach { i => runQueries(allQueries.toArray) }

    sc.stop()

  }

  def puts(s: String): Unit = {
    //scalastyle:off
    println(s)
    //scalastyle:on
  }
}
