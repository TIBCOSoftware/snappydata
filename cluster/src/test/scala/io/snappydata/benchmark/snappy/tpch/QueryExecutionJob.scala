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

package io.snappydata.benchmark.snappy.tpch

import java.io.{File, FileOutputStream, PrintStream}

import scala.language.implicitConversions

import com.typesafe.config.Config

import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object QueryExecutionJob extends SnappySQLJob {

  var sqlSparkProperties: Array[String] = _
  var queries: Array[String] = _
  var isDynamic: Boolean = _
  var isResultCollection: Boolean = _
  var isSnappy: Boolean = true
  var warmUp: Integer = _
  var runsForAverage: Integer = _
  var threadNumber: Integer = _
  var traceEvents : Boolean = _
  var randomSeed : Integer = _

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext

    val avgFileStream: FileOutputStream = new FileOutputStream(
            new File(s"${threadNumber}_Snappy_AverageResponseTimes.csv"))
    val avgPrintStream: PrintStream = new PrintStream(avgFileStream)
    avgPrintStream.println(s"Query,AverageResponseTime")

    for (prop <- sqlSparkProperties) {
      snc.sql(s"set $prop")
    }

    // scalastyle:off println
    println(s"****************queries : $queries")
    // scalastyle:on println

    QueryExecutor.setRandomSeed(randomSeed)
    for (query <- queries) {
      QueryExecutor.execute(query, snc, isResultCollection, isSnappy,
        threadNumber, isDynamic, warmUp, runsForAverage, avgPrintStream)
    }
    avgPrintStream.close()
    avgFileStream.close()

    QueryExecutor.close
  }

  def main(args: Array[String]): Unit = {
    val isResultCollection = false
    val isSnappy = true

    val conf = new SparkConf()
        .setAppName("TPCH")
        .setMaster("snappydata://localhost:10334")
        .set("jobserver.enabled", "false")
    val sc = new SparkContext(conf)
    val snc =
      SnappyContext(sc)

    queries = Array("16")
    runJob(snc, null)
  }

  override def isValidJob(snSession: SnappySession, config: Config): SnappyJobValidation = {

    val sqlSparkProps = if (config.hasPath("sparkSqlProps")) {
      config.getString("sparkSqlProps")
    }
    else " "

    sqlSparkProperties = sqlSparkProps.split(",")

    val tempQueries = if (config.hasPath("queries")) {
      config.getString("queries")
    } else {
      return SnappyJobInvalid("Specify Query number to be executed")
    }

    // scalastyle:off println
    println(s"tempqueries : $tempQueries")
    queries = tempQueries.split("-")

    isDynamic = if (config.hasPath("isDynamic")) {
      config.getBoolean("isDynamic")
    } else {
      return SnappyJobInvalid("Specify whether to use dynamic paramters")
    }

    isResultCollection = if (config.hasPath("resultCollection")) {
      config.getBoolean("resultCollection")
    } else {
      return SnappyJobInvalid("Specify whether to to collect results")
    }

    warmUp = if (config.hasPath("warmUpIterations")) {
      config.getInt("warmUpIterations")
    } else {
      return SnappyJobInvalid("Specify number of warmup iterations ")
    }
    runsForAverage = if (config.hasPath("actualRuns")) {
      config.getInt("actualRuns")
    } else {
      return SnappyJobInvalid("Specify number of  iterations of which average result is " +
          "calculated")
    }

    threadNumber = if (config.hasPath("threadNumber")) {
      config.getInt("threadNumber")
    } else {
      1
    }

    traceEvents = if (config.hasPath("traceEvents")) {
      config.getBoolean("traceEvents")
    } else {
      false
    }

    randomSeed = if (config.hasPath("randomSeed")) {
      config.getInt("randomSeed")
    } else {
      42
    }

    SnappyJobValid()
  }
}
