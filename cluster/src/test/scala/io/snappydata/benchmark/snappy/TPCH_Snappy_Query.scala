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

package io.snappydata.benchmark.snappy

import java.io.{File, FileOutputStream, PrintStream}

import scala.language.implicitConversions

import com.typesafe.config.Config

import org.apache.spark.sql.{SnappyContext, SnappyJobInvalid, SnappyJobValid, SnappyJobValidation, SnappySQLJob}
import org.apache.spark.{SparkConf, SparkContext}

object TPCH_Snappy_Query extends SnappySQLJob{

  var sqlSparkProperties: Array[String] = _
  var queries:Array[String] = _
  var useIndex: Boolean = _
  var isResultCollection: Boolean = _
  var isSnappy: Boolean = true
  var warmUp: Integer = _
  var runsForAverage: Integer = _


   override def runSnappyJob(snc: SnappyContext, jobConfig: Config): Any = {

//     jobConfig.entrySet().asScala.foreach(entry => if (entry.getKey.startsWith("spark.sql.")) {
//       val entryString = entry.getKey + "=" + jobConfig.getString(entry.getKey)
//       println("****************SparkSqlProp : " + entryString)
//       snc.sql("set " + entryString)
//     })

     var avgFileStream: FileOutputStream = new FileOutputStream(new File(s"Snappy_Average.out"))
     var avgPrintStream:PrintStream = new PrintStream(avgFileStream)

     for(prop <- sqlSparkProperties) {
       snc.sql(s"set $prop")
     }

     println(s"****************queries : $queries")

     for(i <- 1 to 1) {
       for(query <- queries)
         query match {
           case "1" =>   TPCH_Snappy.execute("q1", snc, isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
           case "2" =>   TPCH_Snappy.execute("q2", snc,isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
           case "3"=>   TPCH_Snappy.execute("q3", snc, isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
           case "4" =>   TPCH_Snappy.execute("q4", snc, isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
           case "5" =>   TPCH_Snappy.execute("q5", snc, isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
           case "6" =>   TPCH_Snappy.execute("q6", snc, isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
           case "7" =>   TPCH_Snappy.execute("q7", snc, isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
           case "8" =>   TPCH_Snappy.execute("q8", snc, isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
           case "9" =>   TPCH_Snappy.execute("q9", snc, isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
           case "10" =>   TPCH_Snappy.execute("q10", snc, isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
           case "11" =>   TPCH_Snappy.execute("q11", snc, isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
           case "12" =>   TPCH_Snappy.execute("q12", snc, isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
           case "13" =>   TPCH_Snappy.execute("q13", snc, isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
           case "14" =>   TPCH_Snappy.execute("q14", snc, isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
           case "15" =>   TPCH_Snappy.execute("q15", snc, isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
           case "16" =>   TPCH_Snappy.execute("q16", snc, isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
           case "17" =>   TPCH_Snappy.execute("q17", snc, isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
           case "18" =>   TPCH_Snappy.execute("q18", snc, isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
           case "19" =>   TPCH_Snappy.execute("q19", snc,isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
           case "20" =>   TPCH_Snappy.execute("q20", snc, isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
           case "21" =>   TPCH_Snappy.execute("q21", snc,isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
           case "22" =>   TPCH_Snappy.execute("q22", snc, isResultCollection, isSnappy, i, useIndex, warmUp, runsForAverage, avgPrintStream)
             println("---------------------------------------------------------------------------------")
         }

     }
     avgPrintStream.close()
     avgFileStream.close()

     TPCH_Snappy.close()
   }

  def main (args: Array[String]): Unit = {
    val isResultCollection = false
    val isSnappy = true

    val conf = new SparkConf()
      .setAppName("TPCH")
      //.setMaster("local[6]")
      .setMaster("snappydata://localhost:10334")
      .set("jobserver.enabled", "false")
    val sc = new SparkContext(conf)
    val snc =
      SnappyContext(sc)


    snc.sql("set spark.sql.shuffle.partitions=6")
    queries = Array("16" )
    runJob(snc, null)
  }

   override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = {

     var sqlSparkProps = if (config.hasPath("sparkSqlProps")) {
       config.getString("sparkSqlProps")
     }
     else " "

     sqlSparkProperties = sqlSparkProps.split(",")

     var tempqueries = if (config.hasPath("queries")) {
       config.getString("queries")
     } else {
       return new SnappyJobInvalid("Specify Query number to be executed")
     }

     println(s"tempqueries : $tempqueries")
     queries = tempqueries.split("-")

     useIndex = if (config.hasPath("useIndex")) {
       config.getBoolean("useIndex")
     } else {
       return new SnappyJobInvalid("Specify whether to use Index")
     }

     isResultCollection = if (config.hasPath("resultCollection")) {
       config.getBoolean("resultCollection")
     } else {
       return new SnappyJobInvalid("Specify whether to to collect results")
     }

     warmUp = if (config.hasPath("warmUpIterations")) {
       config.getInt("warmUpIterations")
     } else {
       return new SnappyJobInvalid("Specify number of warmup iterations ")
     }
     runsForAverage = if (config.hasPath("actualRuns")) {
       config.getInt("actualRuns")
     } else {
       return new SnappyJobInvalid("Specify number of  iterations of which average result is calculated")
     }
     new SnappyJobValid()
   }
 }
