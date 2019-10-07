/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

package io.snappydata.benchmark.LoadPerformance

import java.io.{PrintStream, FileOutputStream, File}

import com.typesafe.config.Config
import io.snappydata.benchmark.TPCHColumnPartitionedTable

import org.apache.spark.sql._

/**
 * Created by kishor on 29/8/16.
 */
object BulkLoad_Snappy extends SnappySQLJob{

  var tpchDataPath: String = _
  var buckets_Order_Lineitem: String = _


  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val snc = snSession.sqlContext

    var loadPerfFileStream: FileOutputStream = new FileOutputStream(new File(s"BulkLoadPerf.out"))
    var loadPerfPrintStream:PrintStream = new PrintStream(loadPerfFileStream)

    val isSnappy = true
    val dbName = "TPCH"

    val usingOptionString = s"""
           USING row
           OPTIONS ()"""


    snc.sql("DROP TABLE IF EXISTS " + "LINEITEM")
    snc.sql("DROP TABLE IF EXISTS " + "ORDERS")

    TPCHColumnPartitionedTable.testLoadOrderTablePerformance(snc, tpchDataPath, isSnappy,
      buckets_Order_Lineitem,loadPerfPrintStream)
    TPCHColumnPartitionedTable.testLoadLineItemTablePerformance(snc, tpchDataPath, isSnappy,
      buckets_Order_Lineitem,loadPerfPrintStream)

    loadPerfPrintStream.close()
    loadPerfFileStream.close()
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


    if (!(new File(tpchDataPath)).exists()) {
      return new SnappyJobInvalid("Incorrect tpch data path. " +
          "Specify correct location")
    }

    SnappyJobValid()
  }
}
