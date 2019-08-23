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
package io.snappydata.hydra.spva

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import io.snappydata.hydra.northwind.NWTestJob
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

import scala.util.{Failure, Success, Try}

class CreateAnLoadSPVATablesJob extends SnappySQLJob {
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val pw = new PrintWriter(new FileOutputStream(new File("CreateAndLoadSPVATablesJob.out"), true));
    Try {
      val snc = snSession.sqlContext
      // scalastyle:off println
      val dataFilesLocation = jobConfig.getString("dataFilesLocation")
      pw.println(s"dataFilesLocation is : ${dataFilesLocation}")
      println(s"dataFilesLocation is : ${dataFilesLocation}")
      val tableType = jobConfig.getString("tableType")
      pw.println(s"tableType : " + tableType)
      println(s"tableType : " + tableType)
      snc.setConf("dataFilesLocation", dataFilesLocation)
      SPVAQueries.snc = snc
      SPVAQueries.dataFilesLocation = dataFilesLocation
      SPVATestUtil.dropTables(snc)
      pw.println(s"Create and load ${tableType} tables Test started at : " + System
          .currentTimeMillis)
      // snc.sql("CREATE SCHEMA IF NOT EXISTS SPD")
      tableType match {
        case "ReplicatedRow" => SPVATestUtil.createAndLoadReplicatedTables(snc)
        case "PartitionedRow" => SPVATestUtil.createAndLoadPartitionedTables(snc)
        case "Column" => SPVATestUtil.createAndLoadColumnTables(snc)
        case _ => // the default, catch-all
      }
      pw.println(s"Create and load ${tableType} tables Test completed successfully at : " +
          System.currentTimeMillis)
      pw.flush()
      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${NWTestJob.getCurrentDirectory}/CreateAndLoadSPVATablesJob.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}