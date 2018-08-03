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

package io.snappydata.benchmark.snappy.tpch

import java.io.{File, FileOutputStream, PrintStream}

import com.typesafe.config.Config
import io.snappydata.benchmark.{TPCHColumnPartitionedTable, TPCHReplicatedTable}

import org.apache.spark.sql.{SnappyJobInvalid, SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

/**
  * Created by kishor on 12/5/17.
  */
object SparkAppUsingJob  extends SnappySQLJob {

  var tpchDataPath: String = _
  var numberOfLoadStages: Integer = _
  var isParquet : Boolean = _
  var queries: Array[String] = _
  var sqlSparkProperties: Array[String] = _
  var isDynamic: Boolean = _
  var isResultCollection: Boolean = _
  var warmUp: Integer = _
  var runsForAverage: Integer = _
  var threadNumber: Integer = _

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext
    snc.sparkContext.hadoopConfiguration.set("fs.s3a.connection.maximum", "1000")

    val isSnappy = false
    val usingOptionString = null
    var loadPerfFileStream: FileOutputStream = new FileOutputStream(
      new File(s"${threadNumber}_Spark_LoadPerf.out"))
    var loadPerfPrintStream: PrintStream = new PrintStream(loadPerfFileStream)

    val avgFileStream: FileOutputStream = new FileOutputStream(
      new File(s"${threadNumber}_Spark_Average.out"))
    val avgPrintStream: PrintStream = new PrintStream(avgFileStream)

    snc.dropTable("NATION", ifExists = true)
    snc.dropTable("REGION", ifExists = true)
    snc.dropTable("SUPPLIER", ifExists = true)
    snc.dropTable("PARTSUPP", ifExists = true)
    snc.dropTable("PART", ifExists = true)
    snc.dropTable("CUSTOMER", ifExists = true)
    snc.dropTable("LINEITEM", ifExists = true)
    snc.dropTable("ORDERS", ifExists = true)

    TPCHReplicatedTable.createPopulateRegionTable(usingOptionString, snc, tpchDataPath,
      isSnappy, loadPerfPrintStream)
    TPCHReplicatedTable.createPopulateNationTable(usingOptionString, snc, tpchDataPath,
      isSnappy, loadPerfPrintStream)
    TPCHReplicatedTable.createPopulateSupplierTable(usingOptionString, snc, tpchDataPath,
      isSnappy, loadPerfPrintStream, numberOfLoadStages)

    TPCHColumnPartitionedTable.createPopulateOrderTable(snc, tpchDataPath, isSnappy,
      loadPerfPrintStream = loadPerfPrintStream, numberOfLoadingStages = numberOfLoadStages,
      isParquet = isParquet)
    TPCHColumnPartitionedTable.createPopulateLineItemTable(snc, tpchDataPath, isSnappy,
      loadPerfPrintStream = loadPerfPrintStream, numberOfLoadingStages = numberOfLoadStages,
      isParquet = isParquet)
    TPCHColumnPartitionedTable.createPopulateCustomerTable(snc, tpchDataPath, isSnappy,
      loadPerfPrintStream = loadPerfPrintStream, numberOfLoadingStages = numberOfLoadStages,
      isParquet = isParquet)
    TPCHColumnPartitionedTable.createPopulatePartTable(snc, tpchDataPath, isSnappy,
      loadPerfPrintStream = loadPerfPrintStream, numberOfLoadingStages = numberOfLoadStages,
      isParquet = isParquet)
    TPCHColumnPartitionedTable.createPopulatePartSuppTable(snc, tpchDataPath, isSnappy,
      loadPerfPrintStream = loadPerfPrintStream, numberOfLoadingStages = numberOfLoadStages,
      isParquet = isParquet)

    for(prop <- sqlSparkProperties) {
      // scalastyle:off println
      println(prop)
      snc.sql(s"set $prop")
    }

    for (i <- 1 to 1) {
      for (query <- queries) {
        QueryExecutor.execute(query, snc, isResultCollection, isSnappy,
          threadNumber, isDynamic, warmUp, runsForAverage, avgPrintStream)
      }
    }
    QueryExecutor.close

  }

  override def isValidJob(snSession: SnappySession, config: Config): SnappyJobValidation = {

    tpchDataPath = if (config.hasPath("dataLocation")) {
      config.getString("dataLocation")
    } else {
      "/QASNAPPY/TPCH/DATA/1"
    }


    numberOfLoadStages = if (config.hasPath("NumberOfLoadStages")) {
      config.getString("NumberOfLoadStages").toInt
    } else {
      1
    }

    isParquet = if (config.hasPath("isParquet")) {
      config.getBoolean("isParquet")
    } else {
      false
    }


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

    SnappyJobValid()
  }
}
