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
import java.lang.System

import com.typesafe.config.Config
import io.snappydata.hydra.northwind
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}

class CreateAndLoadNWTablesJob extends SnappySQLJob {
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val pw = new PrintWriter(new FileOutputStream(new File("CreateAndLoadNWTablesJob.out"), true));
    val tableType = jobConfig.getString("tableType")
    Try {
      val snc = snSession.sqlContext
      snc.sql("set spark.sql.shuffle.partitions=23")
      val dataFilesLocation = jobConfig.getString("dataFilesLocation")
      snc.setConf("dataFilesLocation", dataFilesLocation)
      northwind.NWQueries.snc = snc
      pw.println(s"dataFilesLocation is : ${dataFilesLocation}")
      NWQueries.dataFilesLocation = dataFilesLocation
      NWTestUtil.dropTables(snc)
      pw.println(s"Create and load ${tableType} tables Test started at : " + System.currentTimeMillis)
      tableType match {
        case "ReplicatedRow" => NWTestUtil.createAndLoadReplicatedTables(snc)
        case "PartitionedRow" => NWTestUtil.createAndLoadPartitionedTables(snc)
        case "Column" => NWTestUtil.createAndLoadColumnTables(snc)
        case "Colocated" => NWTestUtil.createAndLoadColocatedTables(snc)
        case _ => // the default, catch-all
      }
      pw.println(s"Create and load ${tableType} tables Test completed successfully at : " + System.currentTimeMillis)
      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${NWTestJob.getCurrentDirectory}/CreateAndLoadNWTablesJob.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}

