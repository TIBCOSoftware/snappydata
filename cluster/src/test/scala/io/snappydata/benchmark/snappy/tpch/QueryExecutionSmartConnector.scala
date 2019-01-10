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

import org.apache.spark.sql.{SnappySession, SparkSession}

/**
  * Created by kishor on 19/7/17.
  */
object QueryExecutionSmartConnector {

  def main(args: Array[String]) {

    val sc: SparkSession = SparkSession
        .builder
        .getOrCreate

    val queries = args(0).split("-")
    val sparkSqlProps = args(0).split(",")
    val isDynamic = args(2).toBoolean
    val isResultCollection = args(3).toBoolean
    val warmUpIterations = args(4).toInt
    val actualRuns = args(5).toInt
    val threadNumber = args(6).toInt

    var avgFileStream: FileOutputStream = new FileOutputStream(
      new File(s"${threadNumber}_Smart_Average.out"))
    var avgPrintStream: PrintStream = new PrintStream(avgFileStream)

    val snSession = new SnappySession(sc.sparkContext)

    for(prop <- sparkSqlProps) {
      // scalastyle:off println
      println(prop)
      sc.sql(s"set $prop")
    }

    for (i <- 1 to 1) {
      for (query <- queries) {
        QueryExecutor.execute(query, snSession.sqlContext, isResultCollection, false,
          threadNumber, isDynamic, warmUpIterations, actualRuns, avgPrintStream)
      }
    }
    QueryExecutor.close
    sc.stop()

  }
}
