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
package io.snappydata.hydra.northwind

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkContext, SparkConf}

object CreateAndLoadNWTablesSparkApp {
  val conf = new SparkConf().
    setAppName("NWTestUtil Application")
  val sc = new SparkContext(conf)
  val snc = SnappyContext(sc)

  def main(args: Array[String]) {
    val dataFilesLocation = args(0)
    //snc.sql("set spark.sql.shuffle.partitions=6")
    NWQueries.snc = snc
    NWQueries.dataFilesLocation = dataFilesLocation
    val tableType = args(1)
    val pw = new PrintWriter(new FileOutputStream(new File("CreateAndLoadNWTablesSparkApp.out"), true));
    pw.println(s"dataFilesLocation : ${dataFilesLocation}")
    NWTestUtil.dropTables(snc)
    pw.println(s"Create and load ${tableType} tables Test started")
    tableType match {
      case "ReplicatedRow" => NWTestUtil.createAndLoadReplicatedTables(snc)
      case "PartitionedRow" => NWTestUtil.createAndLoadPartitionedTables(snc)
      case "Column" => NWTestUtil.createAndLoadColumnTables(snc)
      case "Colocated" => NWTestUtil.createAndLoadColocatedTables(snc)
      case _ => // the default, catch-all
    }
    pw.println(s"Create and load ${tableType} tables Test completed successfully")
    pw.close()
  }
}
