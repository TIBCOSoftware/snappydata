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
package io.snappydata.hydra.northwind

import java.io.{File, FileOutputStream, PrintWriter}

import io.snappydata.hydra.SnappyTestUtils

import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkConf, SparkContext}

object CreateAndLoadNWTablesSparkApp {

  def main(args: Array[String]) {
    val conf = new SparkConf().
        setAppName("CreateAndLoadNWTablesSpark Application")
    val sc = SparkContext.getOrCreate(conf)
    val snc = SnappyContext(sc)
    val dataFilesLocation = args(0)
    // snc.sql("set spark.sql.shuffle.partitions=6")
    snc.setConf("dataFilesLocation", dataFilesLocation)
    NWQueries.snc = snc
    NWQueries.dataFilesLocation = dataFilesLocation
    val tableType = args(1)
    val createLargeOrderTable = args(2).toBoolean
    // scalastyle:off println
    val pw = new PrintWriter(new FileOutputStream(new File("CreateAndLoadNWTablesSparkApp.out"),
      true));
    pw.println(s"${SnappyTestUtils.logTime} dataFilesLocation : ${dataFilesLocation}")
    NWTestUtil.dropTables(snc)
    pw.println(s"${SnappyTestUtils.logTime} Create and load ${tableType} tables Test " +
        s"started")
    tableType match {
      case "ReplicatedRow" => NWTestUtil.createAndLoadReplicatedTables(snc)
      case "PartitionedRow" => NWTestUtil.createAndLoadPartitionedTables(snc, createLargeOrderTable)
      case "Column" => NWTestUtil.createAndLoadColumnTables(snc, createLargeOrderTable)
      case "Colocated" => NWTestUtil.createAndLoadColocatedTables(snc)
      case _ => // the default, catch-all
    }
    pw.println(s"${SnappyTestUtils.logTime} Create and load ${tableType} tables Test " +
        s"completed successfully.")
    pw.flush()
    if (createLargeOrderTable) {
      NWTestUtil.ingestMoreData(snc, 10)
    }
    pw.println(s"${SnappyTestUtils.logTime} Loaded more data successfully.")
    pw.close()
  }
}
