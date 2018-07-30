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

object LoadTPCHDataSparkApp {

  def main(args: Array[String]) {

    val usingOptionString = null
    val sparkSession: SparkSession = SparkSession
        .builder
        .appName("TPCH_Spark")
        .getOrCreate

    val tpchDataPath = args(0)
    val numberOfLoadStages = args(1).toInt
    val isParquet = args(2).toBoolean
    val threadNumber = args(3).toInt
    val rePartition = args(4).toBoolean
    val isSupplierColumn = args(5).toBoolean
    val buckets_Supplier = args(6)
    var buckets_Order_Lineitem = args(7)
    val buckets_Cust_Part_PartSupp = args(8)


    var loadPerfFileStream: FileOutputStream = new FileOutputStream(
      new File(s"${threadNumber}_Spark_LoadPerf.csv"))
    var loadPerfPrintStream: PrintStream = new PrintStream(loadPerfFileStream)

    TPCHReplicatedTable.createPopulateRegionTable(usingOptionString, sparkSession.sqlContext, tpchDataPath,
      false, loadPerfPrintStream)
    TPCHReplicatedTable.createPopulateNationTable(usingOptionString, sparkSession.sqlContext, tpchDataPath,
      false, loadPerfPrintStream)
    if (isSupplierColumn) {
      TPCHColumnPartitionedTable.createAndPopulateSupplierTable(sparkSession.sqlContext, tpchDataPath, false,
        if (rePartition) buckets_Supplier else "0",
        loadPerfPrintStream = loadPerfPrintStream, numberOfLoadingStages = numberOfLoadStages,
        isParquet = isParquet)
    } else {
      TPCHReplicatedTable.createPopulateSupplierTable(usingOptionString, sparkSession.sqlContext,
        tpchDataPath, false, loadPerfPrintStream, numberOfLoadStages)
    }


    TPCHColumnPartitionedTable.createPopulateOrderTable(sparkSession.sqlContext, tpchDataPath, false,
      if (rePartition) buckets_Order_Lineitem else "0",
      loadPerfPrintStream = loadPerfPrintStream, numberOfLoadingStages = numberOfLoadStages,
      isParquet = isParquet)
    TPCHColumnPartitionedTable.createPopulateLineItemTable(sparkSession.sqlContext, tpchDataPath, false,
      if (rePartition) buckets_Order_Lineitem else "0",
      loadPerfPrintStream = loadPerfPrintStream, numberOfLoadingStages = numberOfLoadStages,
      isParquet = isParquet)
    TPCHColumnPartitionedTable.createPopulateCustomerTable(sparkSession.sqlContext, tpchDataPath, false,
      if (rePartition) buckets_Cust_Part_PartSupp else "0",
      loadPerfPrintStream = loadPerfPrintStream, numberOfLoadingStages = numberOfLoadStages,
      isParquet = isParquet)
    TPCHColumnPartitionedTable.createPopulatePartTable(sparkSession.sqlContext, tpchDataPath, false,
      if (rePartition) buckets_Cust_Part_PartSupp else "0",
      loadPerfPrintStream = loadPerfPrintStream, numberOfLoadingStages = numberOfLoadStages,
      isParquet = isParquet)
    TPCHColumnPartitionedTable.createPopulatePartSuppTable(sparkSession.sqlContext, tpchDataPath, false,
      if (rePartition) buckets_Cust_Part_PartSupp else "0",
      loadPerfPrintStream = loadPerfPrintStream, numberOfLoadingStages = numberOfLoadStages,
      isParquet = isParquet)

    sparkSession.stop()

  }
}
