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

import java.io._

import com.typesafe.config.Config
import org.apache.spark.sql._
import scala.collection.mutable.Map
import org.apache.spark.{SparkConf, SparkContext}

import scala.language.implicitConversions

object TPCH_Snappy_Query_StreamExecution extends SnappySQLJob {

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
    var avgTime: Map[String, Long] = Map()
    for (i <- 1 to (warmUp + runsForAverage)) {
      for (query <- queries) {
        val executionTime : Long = TPCH_Snappy_StreamExecution.execute(query, snc,
          isResultCollection, isSnappy, i, useIndex)
        if (!isResultCollection) {
          var out: BufferedWriter = new BufferedWriter(new FileWriter(s"Snappy_$query.out", true));
          out.write( executionTime + "\n")
          out.close()
        }
        if (i > warmUp) {
          if (avgTime contains query) {
            avgTime(query) = avgTime.get(query).get + executionTime
          } else {
            avgTime += (query -> executionTime)
          }
        }
      }
    }
    for (query <- queries) {
      avgPrintStream.println(s"$query,${avgTime.get(query).get / runsForAverage}")
    }
    avgPrintStream.close()
    avgFileStream.close()
    TPCH_Snappy.close
  }

  def main(args: Array[String]): Unit = {
    val isResultCollection = false
    val isSnappy = true

    val conf = new SparkConf()
        .setAppName("TPCH")
        // .setMaster("local[6]")
        .setMaster("snappydata://localhost:10334")
        .set("jobserver.enabled", "false")
    val sc = new SparkContext(conf)
    val snc =
      SnappyContext(sc)


    snc.sql("set spark.sql.shuffle.partitions=6")
    queries = Array("16")
    runJob(snc, null)
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
    // scalastyle:on println
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
