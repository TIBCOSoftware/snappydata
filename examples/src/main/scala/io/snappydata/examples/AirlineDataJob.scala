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

package io.snappydata.examples

import java.io.PrintWriter

import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config

import org.apache.spark.sql.snappydata._
import org.apache.spark.sql.{SnappySession, SnappyJobValid, DataFrame, SnappyContext, SnappyJobValidation, SnappySQLJob}

/**
 * Fetches already created tables. Airline table is already persisted in
 * Snappy store. Cache the airline table in Spark cache as well for
 * comparison. Sample airline table and persist it in Snappy store.
 * Run a aggregate query on all the three tables and return the results in
 * a Map.This Map will be sent over REST.
 *
 * Run this on your local machine:
 * <p/>
 * `$ sbin/snappy-start-all.sh`
 * <p/>
 * Create tables
 *
 * `$ ./bin/snappy-job.sh submit --lead localhost:8090 \
 * --app-name CreateAndLoadAirlineDataJob --class io.snappydata.examples.CreateAndLoadAirlineDataJob \
 * --app-jar $SNAPPY_HOME/examples/jars/quickstart.jar`
 *
 *
 * `$ ./bin/snappy-job.sh submit --lead localhost:8090 \
 * --app-name AirlineDataJob --class io.snappydata.examples.AirlineDataJob \
 * --app-jar $SNAPPY_HOME/examples/jars/quickstart.jar`
 *
 */

object AirlineDataJob extends SnappySQLJob {

  override def runSnappyJob(snc: SnappySession, jobConfig: Config): Any = {
    val colTable = "AIRLINE"
    val parquetTable = "STAGING_AIRLINE"
    val rowTable = "AIRLINEREF"
    val sampleTable = "AIRLINE_SAMPLE"

    def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath
    // scalastyle:off println
    val pw = new PrintWriter("AirlineDataJob.out")
    Try {
      snc.sql("set spark.sql.shuffle.partitions=6")
      // Get the already created tables
      val airlineDF: DataFrame = snc.table(colTable)
      val airlineCodeDF: DataFrame = snc.table(rowTable)
      val airlineParquetDF: DataFrame = snc.table(parquetTable)
      val sampleDF: DataFrame = snc.table(sampleTable)

      // Cache the airline data in a Spark table as well.
      // We will use this to compare against the Snappy Store table
      airlineParquetDF.cache()
      airlineParquetDF.count()

      // Query Snappy Store's Airline table :Which Airlines Arrive
      // On Schedule? JOIN with reference table
      val actualResult = airlineDF.join(airlineCodeDF,
        airlineDF.col("UniqueCarrier").equalTo(airlineCodeDF("CODE"))).
          groupBy(airlineDF("UniqueCarrier"), airlineCodeDF("DESCRIPTION")).
          agg("ArrDelay" -> "avg").orderBy("avg(ArrDelay)")

      val start = System.currentTimeMillis
      val result = actualResult.collect()
      val totalTime = (System.currentTimeMillis - start)
      pw.println(s"****** Query Execution on Airline Snappy " +
          s"table took ${totalTime}ms ******")
      result.foreach(rs => {
        pw.println(rs.toString)
      })

      // Query Spark cached DataFrame : Which Airlines
      // Arrive On Schedule? JOIN with reference table
      val parquetResult = airlineParquetDF.join(airlineCodeDF,
        airlineParquetDF.col("UniqueCarrier").equalTo(airlineCodeDF("CODE"))).
          groupBy(airlineParquetDF("UniqueCarrier"), airlineCodeDF("DESCRIPTION")).
          agg("ArrDelay" -> "avg").orderBy("avg(ArrDelay)")

      val startP = System.currentTimeMillis
      val resultP = parquetResult.collect()
      val totalTimeP = (System.currentTimeMillis - startP)
      pw.println(s"\n****** Query Execution on Airline Spark " +
          s"table took ${totalTimeP}ms******")
      resultP.foreach(rs => {
        pw.println(rs.toString)
      })

      // Query Snappy Store's Sample table :Which Airlines
      // Arrive On Schedule? JOIN with reference table
      val sampleResult = sampleDF.join(airlineCodeDF,
        sampleDF.col("UniqueCarrier").equalTo(airlineCodeDF("CODE"))).
          groupBy(sampleDF("UniqueCarrier"), airlineCodeDF("DESCRIPTION")).
          agg("ArrDelay" -> "avg").orderBy("avg(ArrDelay)")

      val startS = System.currentTimeMillis
      val resultS = sampleResult.collect()
      val totalTimeS = (System.currentTimeMillis - startS)
      pw.println(s"\n****** Query Execution on Airline Sample " +
          s"table took ${totalTimeS}ms******")
      resultS.foreach(rs => {
        pw.println(rs.toString)
      })

      // Query Snappy Store's Airline table with error clause.
      // This fetches the results directly from the sample table
      // if the error is within the specified limit
      val sampleBaseTableResult = airlineDF.groupBy(airlineDF("Year_"))
          .agg("ArrDelay" -> "avg")
          .orderBy("Year_").withError(0.20, 0.80)
      val startSB = System.currentTimeMillis
      val resultSB = sampleBaseTableResult.collect()
      val totalTimeSB = (System.currentTimeMillis - startSB)
      pw.println(s"\n****** Query Execution with sampling on " +
          s"Airline table took ${totalTimeSB}ms******")
      resultSB.foreach(rs => {
        pw.println(rs.toString)
      })
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/AirlineDataJob.out"
      case Failure(e) => pw.close();
        throw e;
    }
    // scalastyle:on println
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
