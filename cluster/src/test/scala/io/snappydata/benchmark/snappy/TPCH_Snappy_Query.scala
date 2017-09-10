/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

package io.snappydata.benchmark.snappy

import java.io.{File, FileOutputStream, PrintStream}

import scala.language.implicitConversions

import com.typesafe.config.Config

import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

object TPCH_Snappy_Query extends SnappySQLJob {

  var sqlSparkProperties: Array[String] = _
  var queries: Array[String] = _
  var useIndex: Boolean = _
  var isResultCollection: Boolean = _
  var isSnappy: Boolean = true
  var warmUp: Integer = _
  var runsForAverage: Integer = _


  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext

    //     jobConfig.entrySet().asScala.foreach(entry => if (entry.getKey.startsWith("spark.sql
    // .")) {
    //       val entryString = entry.getKey + "=" + jobConfig.getString(entry.getKey)
    //       println("****************SparkSqlProp : " + entryString)
    //       snc.sql("set " + entryString)
    //     })

    val avgFileStream: FileOutputStream = new FileOutputStream(new File(s"Snappy_Average.out"))
    val avgPrintStream: PrintStream = new PrintStream(avgFileStream)

    for (prop <- sqlSparkProperties) {
      snc.sql(s"set $prop")
    }

    // scalastyle:off println
    println(s"****************queries : $queries")
    // scalastyle:on println

    for (i <- 1 to 1) {
      for (query <- queries)
        {
          TPCH_Snappy.execute(query, snc, isResultCollection, isSnappy, i, useIndex,
            warmUp, runsForAverage, avgPrintStream)
        }
    }
    avgPrintStream.close()
    avgFileStream.close()

    TPCH_Snappy.close
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("TPCH")
    queries = conf.get("spark.queries",
      "1-2-3-4-5-6-7-8-9-10-11-12-13-14-15-16-17-18-19-20-22-21").split("-")
    warmUp = conf.getInt("spark.warmUp", 2)
    runsForAverage = conf.getInt("spark.actualRuns", 3)
    isResultCollection = conf.getBoolean("spark.resultCollection", false)
    sqlSparkProperties = conf.get("spark.sparkSqlProps").split(",")
    val sc = new SparkContext(conf)
    val snc = new SnappySession(sc)
    runSnappyJob(snc, null)
  }

  override def isValidJob(snSession: SnappySession, config: Config): SnappyJobValidation = {

    val sqlSparkProps = if (config.hasPath("sparkSqlProps")) {
      config.getString("sparkSqlProps")
    }
    else " "

    sqlSparkProperties = sqlSparkProps.split(",")

    val tempqueries = if (config.hasPath("queries")) {
      config.getString("queries")
    } else {
      return SnappyJobInvalid("Specify Query number to be executed")
    }

    // scalastyle:off println
    println(s"tempqueries : $tempqueries")
    queries = tempqueries.split("-")

    useIndex = if (config.hasPath("useIndex")) {
      config.getBoolean("useIndex")
    } else {
      return SnappyJobInvalid("Specify whether to use Index")
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
    SnappyJobValid()
  }
}
