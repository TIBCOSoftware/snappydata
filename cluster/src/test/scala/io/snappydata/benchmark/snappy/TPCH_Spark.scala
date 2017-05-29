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

import java.io.{PrintStream, File, FileOutputStream}

import io.snappydata.benchmark.{TPCHColumnPartitionedTable, TPCHReplicatedTable}

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by kishor on 19/10/15.
 */
object TPCH_Spark {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("TPCH_Spark")

    /* .set("snappydata.store.locators","localhost:10334") */

    val usingOptionString = null
    val props = null
    var isSnappy = false
    val sc = new SparkContext(conf)
    val snc = new SQLContext(sc)
    val buckets = null
    val useIndex = false

    val path = args(0)
    val queries = args(1).split("-")
    var isResultCollection : Boolean = args(2).toBoolean
    var warmup : Integer = args(3).toInt
    var runsForAverage : Integer = args(4).toInt
    var sqlSparkProperties = args(5).split(",")

    var loadPerfFileStream: FileOutputStream = new FileOutputStream(new File(s"Spark_LoadPerf.out"))
    var loadPerfPrintStream: PrintStream = new PrintStream(loadPerfFileStream)

    var avgFileStream: FileOutputStream = new FileOutputStream(new File(s"Spark_Average.out"))
    var avgPrintStream: PrintStream = new PrintStream(avgFileStream)


    TPCHColumnPartitionedTable.createPopulateOrderTable(
      snc, path, isSnappy, buckets, loadPerfPrintStream)
    TPCHColumnPartitionedTable.createPopulateLineItemTable(
      snc, path, isSnappy, buckets, loadPerfPrintStream)
    TPCHReplicatedTable.createPopulateRegionTable(
      usingOptionString, snc, path, isSnappy, loadPerfPrintStream)
    TPCHReplicatedTable.createPopulateNationTable(
      usingOptionString, snc, path, isSnappy, loadPerfPrintStream)
    TPCHReplicatedTable.createPopulateSupplierTable(
      usingOptionString, snc, path, isSnappy, loadPerfPrintStream)
    TPCHColumnPartitionedTable.createPopulateCustomerTable(
      snc, path, isSnappy, buckets, loadPerfPrintStream)
    TPCHColumnPartitionedTable.createPopulatePartTable(
      snc, path, isSnappy, buckets, loadPerfPrintStream)
    TPCHColumnPartitionedTable.createPopulatePartSuppTable(
      snc, path, isSnappy, buckets, loadPerfPrintStream)

//    snc.sql(s"set spark.sql.shuffle.partitions=83")
//    snc.sql(s"set spark.sql.inMemoryColumnarStorage.compressed=false")
//    snc.sql(s"set spark.sql.autoBroadcastJoinThreshold=41943040")
    // snc.sql(s"set spark.sql.crossJoin.enabled = true")
    for(prop <- sqlSparkProperties) {
      // scalastyle:off println
      println(prop)
      snc.sql(s"set $prop")
    }

    for (i <- 1 to 1) {
      for (query <- queries) {
          TPCH_Snappy.execute(query, snc, isResultCollection, isSnappy, i, useIndex, warmup,
            runsForAverage, avgPrintStream)
      }
    }
    TPCH_Snappy.close
    sc.stop()

  }
}


