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

import java.io.{PrintStream, FileOutputStream, File}

import com.typesafe.config.Config
import io.snappydata.benchmark.{TPCHColumnPartitionedTable, TPCHReplicatedTable}

import org.apache.spark.sql._

object TPCH_Snappy_Tables extends SnappySQLJob {

  var tpchDataPath: String = _
  var buckets_Order_Lineitem: String = _
  var buckets_Cust_Part_PartSupp: String = _
  var buckets_Nation_Region_Supp: String = _
  var useIndex: Boolean = _
  var nation_Region_Supp_col: Boolean = _

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext
    val isSnappy = true

    val loadPerfFileStream: FileOutputStream = new FileOutputStream(new File("Snappy_LoadPerf.out"))
    val loadPerfPrintStream: PrintStream = new PrintStream(loadPerfFileStream)

    val usingOptionString =
      s"""
           USING row
           OPTIONS ()"""


    snc.dropTable("NATION", ifExists = true)
    snc.dropTable("REGION", ifExists = true)
    snc.dropTable("SUPPLIER", ifExists = true)
    snc.dropTable("PARTSUPP", ifExists = true)
    snc.dropTable("LINEITEM_PART", ifExists = true)
    snc.dropTable("PART", ifExists = true)
    snc.dropTable("ORDERS_CUST", ifExists = true)
    snc.dropTable("CUSTOMER", ifExists = true)
    snc.dropTable("LINEITEM", ifExists = true)
    snc.dropTable("ORDERS", ifExists = true)

    if (nation_Region_Supp_col) {
      TPCHColumnPartitionedTable.createAndPopulateNationTable(snc, tpchDataPath, isSnappy,
        buckets_Nation_Region_Supp, loadPerfPrintStream)
      TPCHColumnPartitionedTable.createAndPopulateRegionTable(snc, tpchDataPath, isSnappy,
        buckets_Nation_Region_Supp, loadPerfPrintStream)
      TPCHColumnPartitionedTable.createAndPopulateSupplierTable(snc, tpchDataPath, isSnappy,
        buckets_Nation_Region_Supp, loadPerfPrintStream)
    } else {
      TPCHReplicatedTable.createPopulateRegionTable(usingOptionString, snc, tpchDataPath, isSnappy,
        loadPerfPrintStream)
      TPCHReplicatedTable.createPopulateNationTable(usingOptionString, snc, tpchDataPath, isSnappy,
        loadPerfPrintStream)
      TPCHReplicatedTable.createPopulateSupplierTable(usingOptionString, snc, tpchDataPath,
        isSnappy,
        loadPerfPrintStream)
    }

    TPCHColumnPartitionedTable.createPopulateOrderTable(snc, tpchDataPath, isSnappy,
      buckets_Order_Lineitem, loadPerfPrintStream)
    TPCHColumnPartitionedTable.createPopulateLineItemTable(snc, tpchDataPath, isSnappy,
      buckets_Order_Lineitem, loadPerfPrintStream)
    TPCHColumnPartitionedTable.createPopulateCustomerTable(snc, tpchDataPath, isSnappy,
      buckets_Cust_Part_PartSupp, loadPerfPrintStream)
    TPCHColumnPartitionedTable.createPopulatePartTable(snc, tpchDataPath, isSnappy,
      buckets_Cust_Part_PartSupp, loadPerfPrintStream)
    TPCHColumnPartitionedTable.createPopulatePartSuppTable(snc, tpchDataPath, isSnappy,
      buckets_Cust_Part_PartSupp, loadPerfPrintStream)
    if (useIndex) {
      TPCHColumnPartitionedTable.createAndPopulateOrder_CustTable(snc, tpchDataPath, isSnappy,
        buckets_Cust_Part_PartSupp, loadPerfPrintStream)
      TPCHColumnPartitionedTable.createAndPopulateLineItem_partTable(snc, tpchDataPath, isSnappy,
        buckets_Cust_Part_PartSupp, loadPerfPrintStream)
    }
  }

  override def isValidJob(snSession: SnappySession, config: Config): SnappyJobValidation = {

    tpchDataPath = if (config.hasPath("dataLocation")) {
      config.getString("dataLocation")
    } else {
      "/QASNAPPY/TPCH/DATA/1"
    }

    buckets_Order_Lineitem = if (config.hasPath("Buckets_Order_Lineitem")) {
      config.getString("Buckets_Order_Lineitem")
    } else {
      "15"
    }


    buckets_Cust_Part_PartSupp = if (config.hasPath("Buckets_Cust_Part_PartSupp")) {
      config.getString("Buckets_Cust_Part_PartSupp")
    } else {
      "15"
    }

    buckets_Nation_Region_Supp = if (config.hasPath("Buckets_Nation_Region_Supp")) {
      config.getString("Buckets_Nation_Region_Supp")
    } else {
      "3"
    }

    nation_Region_Supp_col = if (config.hasPath("Nation_Region_Supp_col")) {
      config.getBoolean("Nation_Region_Supp_col")
    } else {
      false
    }

    if (!new File(tpchDataPath).exists()) {
      return SnappyJobInvalid("Incorrect tpch data path. " +
          "Specify correct location")
    }

    useIndex = if (config.hasPath("useIndex")) {
      config.getBoolean("useIndex")
    } else {
      return SnappyJobInvalid("Specify whether to use Index")
    }
    SnappyJobValid()
  }
}
