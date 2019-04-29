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
package io.snappydata.hydra.spva

import java.io.{File, FileOutputStream, PrintWriter}

import io.snappydata.hydra.northwind.NWTestJob
import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.{Failure, Success, Try}

object CreateAndLoadSPVATablesSparkApp {

  def main(args: Array[String]) {
    val connectionURL = args(args.length - 1)
    val conf = new SparkConf().
        setAppName("CreateAndLoadSPVATablesSpark Application").
        set("snappydata.connection", connectionURL)
    val sc = SparkContext.getOrCreate(conf)
    val snc = SnappyContext(sc)
    val dataFilesLocation = args(0)
    snc.setConf("dataFilesLocation", dataFilesLocation)
    SPVAQueries.snc = snc
    SPVAQueries.dataFilesLocation = dataFilesLocation
    val tableType = args(1)
    // scalastyle:off println
    val pw = new PrintWriter(new FileOutputStream(new File("CreateAndLoadSPVATablesSparkApp.out"),
      true))
    Try {
      pw.println(s"dataFilesLocation : ${dataFilesLocation}")
      SPVATestUtil.dropTables(snc)
      pw.println(s"Create and load ${tableType} tables Test started at : " + System.currentTimeMillis)
      tableType match {
        case "ReplicatedRow" => SPVATestUtil.createAndLoadReplicatedTables(snc)
        case "PartitionedRow" => SPVATestUtil.createAndLoadPartitionedTables(snc)
        case "Column" => SPVATestUtil.createAndLoadColumnTables(snc)
        case _ => // the default, catch-all
      }
      pw.println(s"Create and load ${tableType} tables Test completed successfully at : " + System
          .currentTimeMillis)
      pw.flush()
      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${NWTestJob.getCurrentDirectory}/CreateAndLoadSPVATablesSparkApp.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }
}
