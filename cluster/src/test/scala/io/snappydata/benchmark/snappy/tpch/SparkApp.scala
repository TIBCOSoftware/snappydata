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

import io.snappydata.benchmark.{TPCHColumnPartitionedTable, TPCHReplicatedTable}

import org.apache.spark.sql.SparkSession

/**
  * Created by kishor on 11/5/17.
  */
object SparkApp {

  def main(args: Array[String]) {
    // val conf = new SparkConf().setAppName("TPCH_Spark")

    /* .set("snappydata.store.locators","localhost:10334") */

    val usingOptionString = null
    val sc: SparkSession = SparkSession
        .builder
        .appName("TPCH_Spark")
        .getOrCreate

    val tpchDataPath = args(0)
    val numberOfLoadStages = args(1).toInt
    val isParquet = args(2).toBoolean
    val queries = args(3).split("-")
    val sparkSqlProps = args(4).split(",")
    val isDynamic = args(5).toBoolean
    val isResultCollection = args(6).toBoolean
    val warmUpIterations = args(7).toInt
    val actualRuns = args(8).toInt
    val threadNumber = args(9).toInt
    val rePartition = args(10).toBoolean
    val isSupplierColumn = args(11).toBoolean
    val buckets_Supplier = args(12)
    var buckets_Order_Lineitem = args(13)
    val buckets_Cust_Part_PartSupp = args(14)


    var loadPerfFileStream: FileOutputStream = new FileOutputStream(
      new File(s"${threadNumber}_Spark_LoadPerf.out"))
    var loadPerfPrintStream: PrintStream = new PrintStream(loadPerfFileStream)

    var avgFileStream: FileOutputStream = new FileOutputStream(
      new File(s"${threadNumber}_Spark_Average.out"))
    var avgPrintStream: PrintStream = new PrintStream(avgFileStream)


    TPCHColumnPartitionedTable.createPopulateOrderTable(sc.sqlContext, tpchDataPath, false,
      if (rePartition) buckets_Order_Lineitem else "0",
      loadPerfPrintStream = loadPerfPrintStream, numberOfLoadingStage = numberOfLoadStages,
      isParquet = isParquet)
    TPCHColumnPartitionedTable.createPopulateLineItemTable(sc.sqlContext, tpchDataPath, false,
      if (rePartition) buckets_Order_Lineitem else "0",
      loadPerfPrintStream = loadPerfPrintStream, numberOfLoadingStage = numberOfLoadStages,
      isParquet = isParquet)
    TPCHColumnPartitionedTable.createPopulateCustomerTable(sc.sqlContext, tpchDataPath, false,
      if (rePartition) buckets_Cust_Part_PartSupp else "0",
      loadPerfPrintStream = loadPerfPrintStream, numberOfLoadingStage = numberOfLoadStages,
      isParquet = isParquet)
    TPCHColumnPartitionedTable.createPopulatePartTable(sc.sqlContext, tpchDataPath, false,
      if (rePartition) buckets_Cust_Part_PartSupp else "0",
      loadPerfPrintStream = loadPerfPrintStream, numberOfLoadingStage = numberOfLoadStages,
      isParquet = isParquet)
    TPCHColumnPartitionedTable.createPopulatePartSuppTable(sc.sqlContext, tpchDataPath, false,
      if (rePartition) buckets_Cust_Part_PartSupp else "0",
      loadPerfPrintStream = loadPerfPrintStream, numberOfLoadingStage = numberOfLoadStages,
      isParquet = isParquet)

    TPCHReplicatedTable.createPopulateRegionTable(usingOptionString, sc.sqlContext, tpchDataPath,
      false, loadPerfPrintStream)
    TPCHReplicatedTable.createPopulateNationTable(usingOptionString, sc.sqlContext, tpchDataPath,
      false, loadPerfPrintStream)
    if (isSupplierColumn) {
      TPCHColumnPartitionedTable.createAndPopulateSupplierTable(sc.sqlContext, tpchDataPath, false,
        if (rePartition) buckets_Supplier else "0",
        loadPerfPrintStream = loadPerfPrintStream, numberOfLoadingStage = numberOfLoadStages,
        isParquet = isParquet)
    } else {
      TPCHReplicatedTable.createPopulateSupplierTable(usingOptionString, sc.sqlContext,
        tpchDataPath, false, loadPerfPrintStream, numberOfLoadStages)
    }

    for(prop <- sparkSqlProps) {
      // scalastyle:off println
      println(prop)
      sc.sql(s"set $prop")
    }

    for (i <- 1 to 1) {
      for (query <- queries) {
        QueryExecutor.execute(query, sc.sqlContext, isResultCollection, false,
          threadNumber, isDynamic, warmUpIterations, actualRuns, avgPrintStream)
      }
    }
    QueryExecutor.close
    sc.stop()

  }
}
