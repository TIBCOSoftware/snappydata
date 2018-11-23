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

import io.snappydata.benchmark.{TPCHColumnPartitionedTable, TPCHReplicatedTable}

import org.apache.spark.sql.SparkSession

object SparkApp {

  def main(args: Array[String]) {

    val usingOptionString = null
    val sparkSession: SparkSession = SparkSession
        .builder
        // set local as master to debug the app on a local cluster
        // .master("local")
        .appName("TPCH_Spark")
        .getOrCreate

    val threadNumber = args(0).toInt
    val tpchDataPath = args(1)
    val numberOfLoadStages = args(2).toInt
    val isParquet = args(3).toBoolean
    val rePartition = args(4).toBoolean
    val isSupplierColumn = args(5).toBoolean
    val buckets_Supplier = args(6)
    val buckets_Order_Lineitem = args(7)
    val buckets_Cust_Part_PartSupp = args(8)

    val queries = args(9).split("-")
    val sparkSqlProps = args(10).split(",")
    val isDynamic = args(11).toBoolean
    val isResultCollection = args(12).toBoolean
    val warmUpIterations = args(13).toInt
    val actualRuns = args(14).toInt

    val traceEvents = args(15).toBoolean
    val cacheTables = args(16).toBoolean
    val randomSeed = args(17).toInt

    val loadPerfFileStream: FileOutputStream = new FileOutputStream(
      new File(s"${threadNumber}_Spark_LoadPerf.csv"))
    val loadPerfPrintStream: PrintStream = new PrintStream(loadPerfFileStream)
    loadPerfPrintStream.println(s"Table, CreationTime")

    val avgFileStream: FileOutputStream = new FileOutputStream(
      new File(s"${threadNumber}_Spark_Average.csv"))
    val avgPrintStream: PrintStream = new PrintStream(avgFileStream)
    avgPrintStream.println(s"Query,AverageResponseTime")

    // create tables : load tables from (Parquet/ tbl) files into spark cache

    TPCHReplicatedTable.createPopulateRegionTable(usingOptionString,
      sparkSession.sqlContext, tpchDataPath,
      isSnappy = false, loadPerfPrintStream,
      trace = traceEvents,
      cacheTables = cacheTables)

    TPCHReplicatedTable.createPopulateNationTable(usingOptionString,
      sparkSession.sqlContext, tpchDataPath,
      isSnappy = false, loadPerfPrintStream,
      trace = traceEvents,
      cacheTables = cacheTables)

    if (isSupplierColumn) {
      TPCHColumnPartitionedTable.createAndPopulateSupplierTable(
        sparkSession.sqlContext, tpchDataPath,
        isSnappy = false,
        buckets = if (rePartition) buckets_Supplier else "0",
        loadPerfPrintStream,
        numberOfLoadingStages = numberOfLoadStages,
        isParquet = isParquet,
        trace = traceEvents,
        cacheTables = cacheTables)
    } else {
      TPCHReplicatedTable.createPopulateSupplierTable(usingOptionString, sparkSession.sqlContext,
        tpchDataPath, isSnappy = false, loadPerfPrintStream,
        numberOfLoadingStages = numberOfLoadStages,
        trace = traceEvents,
        cacheTables = cacheTables)
    }

    TPCHColumnPartitionedTable.createPopulateOrderTable(sparkSession.sqlContext,
      tpchDataPath,
      isSnappy = false,
      buckets = if (rePartition) buckets_Order_Lineitem else "0",
      loadPerfPrintStream,
      numberOfLoadingStages = numberOfLoadStages,
      isParquet = isParquet,
      trace = traceEvents,
      cacheTables = cacheTables)

    TPCHColumnPartitionedTable.createPopulateLineItemTable(sparkSession.sqlContext,
      tpchDataPath,
      isSnappy = false,
      buckets = if (rePartition) buckets_Order_Lineitem else "0",
      loadPerfPrintStream = loadPerfPrintStream,
      numberOfLoadingStages = numberOfLoadStages,
      isParquet = isParquet,
      trace = traceEvents,
      cacheTables = cacheTables)

    TPCHColumnPartitionedTable.createPopulateCustomerTable(sparkSession.sqlContext,
      tpchDataPath,
      isSnappy = false,
      buckets = if (rePartition) buckets_Cust_Part_PartSupp else "0",
      loadPerfPrintStream = loadPerfPrintStream,
      numberOfLoadingStages = numberOfLoadStages,
      isParquet = isParquet,
      trace = traceEvents,
      cacheTables = cacheTables)

    TPCHColumnPartitionedTable.createPopulatePartTable(sparkSession.sqlContext,
      tpchDataPath,
      isSnappy = false,
      buckets = if (rePartition) buckets_Cust_Part_PartSupp else "0",
      loadPerfPrintStream = loadPerfPrintStream,
      numberOfLoadingStages = numberOfLoadStages,
      isParquet = isParquet,
      trace = traceEvents,
      cacheTables = cacheTables)

    TPCHColumnPartitionedTable.createPopulatePartSuppTable(sparkSession.sqlContext,
      tpchDataPath,
      isSnappy = false,
      if (rePartition) buckets_Cust_Part_PartSupp else "0",
      loadPerfPrintStream = loadPerfPrintStream,
      numberOfLoadingStages = numberOfLoadStages,
      isParquet = isParquet,
      trace = traceEvents,
      cacheTables = cacheTables)

    // set spark SQL properties and run queries

    for(prop <- sparkSqlProps) {
      // scalastyle:off println
      println(prop)
      sparkSession.sql(s"set $prop")
    }

    QueryExecutor.setRandomSeed(randomSeed)
    for (query <- queries) {
      QueryExecutor.execute(query, sparkSession.sqlContext, isResultCollection,
        isSnappy = false,
        threadNumber, isDynamic, warmUpIterations, actualRuns, avgPrintStream)
    }

    // cleanup

    loadPerfPrintStream.flush()
    loadPerfPrintStream.close()
    loadPerfFileStream.close()
    avgPrintStream.flush()
    avgPrintStream.close()
    avgFileStream.close()

    QueryExecutor.close
    sparkSession.stop()

  }
}
