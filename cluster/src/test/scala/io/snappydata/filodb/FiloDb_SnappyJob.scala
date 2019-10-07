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
import scala.concurrent.ExecutionContext.Implicits.global

import com.typesafe.config.Config

import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, DataFrame, SaveMode, SnappySession, SnappySQLJob}



object FiloDb_SnappyJob extends SnappySQLJob {

  var nycTaxiDataPath: String = _
  var sqlSparkProperties: Array[String] = _

  val cachedDF = new collection.mutable.HashMap[String, DataFrame]

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val sc = snSession.sqlContext
    val taxiCsvFile: String = nycTaxiDataPath
    val numRuns = 50 // Make this higher when doing performance profiling

    for (prop <- sqlSparkProperties) {
      sc.sql(s"set $prop")
    }

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
      s"SELECT avg(trip_distance), avg(passenger_count) " +
          s"from nyctaxi where medallion = '$medallion'" +
          s" AND pickup_datetime > '2013-01-15T00Z' AND pickup_datetime < '2013-01-22T00Z'"
    }

    // average trip distance by day for several days

    val allQueries = singleDriverQueries

    // Ingest file - note, this will take several minutes
    puts("Starting ingestion...")

    val csvDF = sc.read.format("com.databricks.spark.csv").
        option("header", "true").option("inferSchema", "true").load(taxiCsvFile)

    csvDF.printSchema()

    val p1 = Map(("PARTITION_BY" -> "medallion") /* ,("BUCKETS"-> "5") */)
    sc.createTable("NYCTAXI", "column", csvDF.schema, p1)
    csvDF.write.format("column").mode(SaveMode.Append).options(p1).saveAsTable("NYCTAXI")
    puts("Ingestion done.")

    val cnts = sc.sql("select count(*) from NYCTAXI").collect()
    puts(s"Total data inserted ${cnts.length}")

    //
    //    val taxiDF = sql.filoDataset("nyc_taxi")
    //    taxiDF.registerTempTable("nyc_taxi")
    //    val numRecords = taxiDF.count()
    //    puts(s"Ingested $numRecords records")
    //
    //    // run queries
    //

    def getCachedDF(query: String): DataFrame =
      cachedDF.getOrElseUpdate(query, sc.sql(query))

    def runQueries(queries: Array[String], numQueries: Int = 1000): Unit = {
      val startMillis = System.currentTimeMillis
      val futures = (0 until numQueries).map(
        i => getCachedDF(queries(Random.nextInt(queries.size))).rdd.collectAsync)
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

    // clean up!
    //FiloSetup.shutdown()
    //sc.stop()
  }


  override def isValidJob(snSession: SnappySession, config: Config): SnappyJobValidation = {
    nycTaxiDataPath = if (config.hasPath("dataLocation")) {
      config.getString("dataLocation")
    } else {
      "/QASNAPPY/TPCH/DATA/1"
    }

    val sqlSparkProps = if (config.hasPath("sparkSqlProps")) {
      config.getString("sparkSqlProps")
    }
    else " "

    sqlSparkProperties = sqlSparkProps.split(" ")

    SnappyJobValid()
  }

  def puts(s: String): Unit = {
    //scalastyle:off
    println(s)
    //scalastyle:on
  }
}
