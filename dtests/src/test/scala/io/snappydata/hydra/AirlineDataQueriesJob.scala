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
package io.snappydata.hydra

import java.io.{File, FileOutputStream, PrintWriter}

import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config

import org.apache.spark.sql._

/**
  * Fetches already created tables. Airline table is already persisted in
  * Snappy store. Cache the airline table in Spark cache as well for
  * comparison. Sample airline table and persist it in Snappy store.
  * Run a aggregate query on column and row table and return the results.
  * This Map will be sent over REST.
  */

object AirlineDataQueriesJob extends SnappySQLJob {
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val colTable = "AIRLINE"
    val parquetTable = "STAGING_AIRLINE"
    val rowTable = "AIRLINEREF"
    //    val sampleTable = "AIRLINE_SAMPLE"
    val snc = snSession.sqlContext

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    // scalastyle:off println
    val pw = new PrintWriter(new FileOutputStream(new File(jobConfig.getString("logFileName")),
      true));
    Try {
      snc.sql("set spark.sql.shuffle.partitions=6")
      // Get the already created tables
      val airlineDF: DataFrame = snc.table(colTable)
      val airlineCodeDF: DataFrame = snc.table(rowTable)
      val airlineParquetDF: DataFrame = snc.table(parquetTable)
      //      val sampleDF: DataFrame = snc.table(sampleTable)

      // Cache the airline data in a Spark table as well.
      // We will use this to compare against the Snappy Store table
      airlineParquetDF.cache()
      airlineParquetDF.count()

      runQueries(pw, snc)

    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/${jobConfig.getString("logFileName")}"
      case Failure(e) => pw.close();
        throw e;
    }

  }

  // Method for running all olap and oltp queries and calculating total elapsed time
  // for each query collectively at the end along with the number of times query Executed.

  def runQueries(pw: PrintWriter, snc: SnappyContext): Unit = {
    var query1ExecutionCount, query2ExecutionCount,
    query3ExecutionCount, query4ExecutionCount, query5ExecutionCount = 0
    var totalTimeQuery1, totalTimeQuery2, totalTimeQuery3,
    totalTimeQuery4, totalTimeQuery5: Long = 0
    val startTime = System.currentTimeMillis
    val EndTime: Long = startTime + 600000

    while (EndTime > System.currentTimeMillis()) {
      //    while (startTime < EndTime) {
      // This Query retrives which airline had the most flights each year.
      val query1: String = "select  count(*) flightRecCount, description AirlineName,  " +
          "UniqueCarrier carrierCode ,Year_ \n   " +
          "from airline , airlineref\n   " +
          "where airline.UniqueCarrier = airlineref.code\n   " +
          "group by UniqueCarrier,description, Year_ \n   " +
          "order by flightRecCount desc limit 10 "
      val query1Result = snc.sql(query1)
      val startTimeQuery1 = System.currentTimeMillis
      val result1 = query1Result.collect()
      totalTimeQuery1 += (System.currentTimeMillis - startTimeQuery1)
      query1ExecutionCount += 1

      // This query retrives which Airlines Arrive On Schedule
      val query2: String = "select AVG(ArrDelay) arrivalDelay, UniqueCarrier carrier from  " +
          "airline   \n" +
          " group by UniqueCarrier\n" +
          "order by arrivalDelay "
      val query2Result = snc.sql(query2)
      val startTimeQuery2 = System.currentTimeMillis
      val result2 = query2Result.collect()
      totalTimeQuery2 += (System.currentTimeMillis - startTimeQuery2)
      query2ExecutionCount += 1

      // This method retrives which Airlines Arrive On Schedule. JOIN with reference table.
      val query3: String = "select AVG(ArrDelay) arrivalDelay, description AirlineName,  " +
          "UniqueCarrier carrier \n" +
          "from airline, airlineref \n" +
          "where airline.UniqueCarrier = airlineref.Code \n  " +
          "group by UniqueCarrier, description \n  " +
          "order by arrivalDelay "

      val Query3Result = snc.sql(query3)
      val startTimeQuery3 = System.currentTimeMillis
      val result3 = Query3Result.collect()
      totalTimeQuery3 += (System.currentTimeMillis - startTimeQuery3)
      query3ExecutionCount += 1

      // This query retrives the trend in arrival delays across all airlines in the US
      val query4: String = "select AVG(ArrDelay) ArrivalDelay, Year_\n  " +
          "from airline \n  " +
          "group by Year_ \n  " +
          "order by Year_ "
      val Query4Result = snc.sql(query4)
      val startTimeQuery4 = System.currentTimeMillis
      val result4 = Query4Result.collect()
      totalTimeQuery4 += (System.currentTimeMillis - startTimeQuery4)
      query4ExecutionCount += 1


      // This query retrives Which airline out of SanFrancisco had most delays due to weather
      val query5: String = "SELECT sum(WeatherDelay) totalWeatherDelay,  airlineref.DESCRIPTION " +
          "\n  " +
          " FROM airline, airlineref \n  " +
          " WHERE airline.UniqueCarrier = airlineref.CODE" +
          " AND  Origin like '%SFO%' AND WeatherDelay > 0 \n" +
          " GROUP BY DESCRIPTION \n  limit 20"
      val Query5Result = snc.sql(query5)

      val startTimeQuery5 = System.currentTimeMillis
      val result5 = Query4Result.collect()
      totalTimeQuery5 += (System.currentTimeMillis - startTimeQuery5)
      query5ExecutionCount += 1
      //      startTime = System.currentTimeMillis
      //      pw.flush()
    }
    pw.println(s"\n****** countQueryWithGroupByOrderBy Execution " +
        s"took ${totalTimeQuery1} ms******")
    pw.println(s"\n****** countQueryWithGroupByOrderBy Execution " +
        s"count is :: ${query1ExecutionCount} ******")
    pw.println(s"\n****** avgArrDelayWithGroupByOrderByForScheduleQuery Execution " +
        s"took ${totalTimeQuery2} ms******")
    pw.println(s"\n****** avgArrDelayWithGroupByOrderByForScheduleQuery Execution " +
        s"count is :: ${query2ExecutionCount} ******")
    pw.println(s"\n****** avgArrDelayWithGroupByOrderByWithJoinForScheduleQuery Execution " +
        s"took ${totalTimeQuery3} ms******")
    pw.println(s"\n****** avgArrDelayWithGroupByOrderByWithJoinForScheduleQuery Execution " +
        s"count is :: ${query3ExecutionCount} ******")
    pw.println(s"\n****** avgArrDelayWithGroupByOrderByForTrendAnalysisQuery Execution " +
        s"took ${totalTimeQuery4} ms******")
    pw.println(s"\n****** avgArrDelayWithGroupByOrderByForTrendAnalysisQuery Execution " +
        s"count is :: ${query4ExecutionCount} ******")
    pw.println(s"\n****** sumWeatherDelayWithGroupByWithLimitQuery Execution " +
        s"took ${totalTimeQuery5} ms******")
    pw.println(s"\n****** sumWeatherDelayWithGroupByWithLimitQuery Execution " +
        s"count is :: ${query5ExecutionCount} ******")
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
