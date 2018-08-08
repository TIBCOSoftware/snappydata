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

import org.apache.spark.sql.{SnappySession, SparkSession}

/**
  * Created by kishor on 19/7/17.
  */

object TableCreationSmartConnector {

  def main(args: Array[String]) {

    val sc: SparkSession = SparkSession
        .builder
        .getOrCreate

    val tpchDataPath = args(0)
    val numberOfLoadStages = args(1).toInt
    val isParquet = args(2).toBoolean
    val buckets_Order_Lineitem = args(3)
    val buckets_Cust_Part_PartSupp = args(4)
    val isSupplierColumn = args(5).toBoolean
    val buckets_Supplier = args(6)
    val redundancy = args(7)
    val persistence = args(8).toBoolean
    val persistence_Type = args(9)
    val threadNumber = args(10).toInt

    var usingOptionString = " USING row OPTIONS ()"
    if(persistence){
      usingOptionString = s" USING row OPTIONS (PERSISTENT '${persistence_Type}')"
    }

    val loadPerfFileStream: FileOutputStream = new FileOutputStream(
      new File(s"${threadNumber}_Smart_LoadPerf.out"))
    val loadPerfPrintStream: PrintStream = new PrintStream(loadPerfFileStream)

    val snSession = new SnappySession(sc.sparkContext)
    snSession.sparkContext.hadoopConfiguration.set("fs.s3a.connection.maximum", "1000")

    snSession.dropTable("NATION", ifExists = true)
    snSession.dropTable("REGION", ifExists = true)
    snSession.dropTable("SUPPLIER", ifExists = true)
    snSession.dropTable("PARTSUPP", ifExists = true)
    snSession.dropTable("LINEITEM_PART", ifExists = true)
    snSession.dropTable("PART", ifExists = true)
    snSession.dropTable("ORDERS_CUST", ifExists = true)
    snSession.dropTable("CUSTOMER", ifExists = true)
    snSession.dropTable("LINEITEM", ifExists = true)
    snSession.dropTable("ORDERS", ifExists = true)

    TPCHColumnPartitionedTable.createPopulateOrderTable(snSession.sqlContext, tpchDataPath, true,
      buckets_Order_Lineitem, loadPerfPrintStream, redundancy, persistence, persistence_Type,
      numberOfLoadStages.toInt, isParquet)

    TPCHColumnPartitionedTable.createPopulateLineItemTable(snSession.sqlContext, tpchDataPath, true,
      buckets_Order_Lineitem, loadPerfPrintStream, redundancy, persistence, persistence_Type,
      numberOfLoadStages.toInt, isParquet)
    TPCHColumnPartitionedTable.createPopulateCustomerTable(snSession.sqlContext, tpchDataPath, true,
      buckets_Cust_Part_PartSupp, loadPerfPrintStream, redundancy, persistence, persistence_Type,
      numberOfLoadStages.toInt, isParquet)
    TPCHColumnPartitionedTable.createPopulatePartTable(snSession.sqlContext, tpchDataPath, true,
      buckets_Cust_Part_PartSupp, loadPerfPrintStream, redundancy, persistence, persistence_Type,
      numberOfLoadStages.toInt, isParquet)
    TPCHColumnPartitionedTable.createPopulatePartSuppTable(snSession.sqlContext, tpchDataPath, true,
      buckets_Cust_Part_PartSupp, loadPerfPrintStream, redundancy, persistence, persistence_Type,
      numberOfLoadStages.toInt, isParquet)

    TPCHReplicatedTable.createPopulateRegionTable(usingOptionString, snSession.sqlContext,
      tpchDataPath, true, loadPerfPrintStream)
    TPCHReplicatedTable.createPopulateNationTable(usingOptionString, snSession.sqlContext,
      tpchDataPath, true, loadPerfPrintStream)

    if (isSupplierColumn) {
      TPCHColumnPartitionedTable.createAndPopulateSupplierTable(snSession.sqlContext, tpchDataPath,
        true, buckets_Supplier, loadPerfPrintStream, redundancy, persistence, persistence_Type,
        numberOfLoadStages.toInt, isParquet)
    } else {
      TPCHReplicatedTable.createPopulateSupplierTable(usingOptionString, snSession.sqlContext,
        tpchDataPath, true, loadPerfPrintStream, numberOfLoadStages.toInt)
    }

    sc.stop()

  }
}
