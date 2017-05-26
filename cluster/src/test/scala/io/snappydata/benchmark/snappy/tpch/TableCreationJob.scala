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
package io.snappydata.benchmark.snappy.tpch

import java.io.{PrintStream, FileOutputStream, File}

import com.typesafe.config.Config
import io.snappydata.benchmark.{TPCHColumnPartitionedTable, TPCHReplicatedTable}

import org.apache.spark.sql._

object TableCreationJob extends SnappySQLJob {

  var tpchDataPath: String = _
  var buckets_Order_Lineitem: String = _
  var buckets_Cust_Part_PartSupp: String = _
  var buckets_Nation_Region_Supp: String = _
  var nation_Region_Supp_col: Boolean = _
  var redundancy: String = _
  var persistence: Boolean = _
  var persistence_type: String = _
  var numberOfLoadStages : String = _
  var isParquet : Boolean = _

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext
    snc.sparkContext.hadoopConfiguration.set("fs.s3a.connection.maximum", "1000");
    val isSnappy = true

    val loadPerfFileStream: FileOutputStream = new FileOutputStream(new File("Snappy_LoadPerf.out"))
    val loadPerfPrintStream: PrintStream = new PrintStream(loadPerfFileStream)

    var usingOptionString = " USING row OPTIONS ()"
    if(persistence){
      usingOptionString = s" USING row OPTIONS (PERSISTENT '${persistence_type}')"
    }

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
        isSnappy, loadPerfPrintStream, numberOfLoadStages.toInt)
    }

    TPCHColumnPartitionedTable.createPopulateOrderTable(snc, tpchDataPath, isSnappy,
      buckets_Order_Lineitem, loadPerfPrintStream, redundancy, persistence, persistence_type,
      numberOfLoadStages.toInt, isParquet)
    TPCHColumnPartitionedTable.createPopulateLineItemTable(snc, tpchDataPath, isSnappy,
      buckets_Order_Lineitem, loadPerfPrintStream, redundancy, persistence, persistence_type,
      numberOfLoadStages.toInt, isParquet)
    TPCHColumnPartitionedTable.createPopulateCustomerTable(snc, tpchDataPath, isSnappy,
      buckets_Cust_Part_PartSupp, loadPerfPrintStream, redundancy, persistence, persistence_type,
      numberOfLoadStages.toInt, isParquet)
    TPCHColumnPartitionedTable.createPopulatePartTable(snc, tpchDataPath, isSnappy,
      buckets_Cust_Part_PartSupp, loadPerfPrintStream, redundancy, persistence, persistence_type,
      numberOfLoadStages.toInt, isParquet)
    TPCHColumnPartitionedTable.createPopulatePartSuppTable(snc, tpchDataPath, isSnappy,
      buckets_Cust_Part_PartSupp, loadPerfPrintStream, redundancy, persistence, persistence_type,
      numberOfLoadStages.toInt, isParquet)
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

    redundancy = if (config.hasPath("Redundancy")) {
      config.getString("Redundancy")
    } else {
      "0"
    }

    persistence = if (config.hasPath("Persistence")) {
      config.getBoolean("Persistence")
    } else {
      false
    }

    persistence_type = if (config.hasPath("Persistence_Type")) {
      config.getString("Persistence_Type")
    } else {
      "false"
    }

    numberOfLoadStages = if (config.hasPath("NumberOfLoadStages")) {
      config.getString("NumberOfLoadStages")
    } else {
      "1"
    }

    isParquet = if (config.hasPath("isParquet")) {
      config.getBoolean("isParquet")
    } else {
      false
    }

    /*if (!new File(tpchDataPath).exists()) {
      return SnappyJobInvalid("Incorrect tpch data path. " +
          "Specify correct location")
    }*/

    SnappyJobValid()
  }
}
