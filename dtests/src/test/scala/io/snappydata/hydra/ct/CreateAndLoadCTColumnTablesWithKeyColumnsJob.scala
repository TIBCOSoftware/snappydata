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

package io.snappydata.hydra.ct

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}
import util.TestException

import scala.util.{Failure, Success, Try}

class CreateAndLoadCTColumnTablesWithKeyColumnsJob extends SnappySQLJob {

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val pw = new PrintWriter(new FileOutputStream(new File("CreateAndLoadCTColumnTablesWithKeyColumnsJob.out"), true));
    val tableType = jobConfig.getString("tableType")
    pw.println("In create and load tables Job")
    Try {
      val snc = snSession.sqlContext
      snc.sql("set spark.sql.shuffle.partitions=6")
      val dataFilesLocation = jobConfig.getString("dataFilesLocation")
      val redundancy = jobConfig.getString("redundancy")
      pw.println(s"Data files are at : ${dataFilesLocation}")
      snc.setConf("dataFilesLocation", dataFilesLocation)
      CTQueries.snc = snc
      CTTestUtil.dropTables(snc)
      pw.println(s"Create and load for ${tableType} tables has started...")
      pw.flush()
      tableType match {
        case "PersistentColumn" => CTTestUtil.createPersistColumnTablesWithKeyColumns(snc,
          jobConfig.getString ("persistenceMode"))
        case "ColocatedColumn" => CTTestUtil.createColocatedColumnTablesWithKeyColumns (snc,
          redundancy)
        case "EvictionColumn" => CTTestUtil.createColumnTablesWithEvictionAndKeyColumns(snc,
          redundancy)
        case "PersistentColocatedColumn" => CTTestUtil
            .createPersistColocatedColumnTablesWithKeyColumns (snc,
          redundancy, jobConfig.getString("persistenceMode"))
        case "ColocatedWithEvictionColumn" => CTTestUtil
            .createColocatedColumnTablesWithEvictionAndKeyColumns(snc,redundancy)
        case _ =>
          pw.println(s"Did not find any match for ${tableType} to create tables")
          pw.close()
          throw new TestException(s"Did not find any match for ${tableType} to create tables." +
              s" See ${CTTestUtil.getCurrentDirectory}/CreateAndLoadCTColumnTablesWithKeyColumnsJob.out")
      }
      pw.println("Tables are created. Now loading data.")
      pw.flush()
      CTTestUtil.loadTablesByRemovingDuplicateRecords(snc);
      println(s"Create and load for ${tableType} tables has completed successfully. " +
          s"See ${CTTestUtil.getCurrentDirectory}/CreateAndLoadCTColumnTablesWithKeyColumnsJob.out")
      pw.println(s"Create and load for ${tableType} tables has completed successfully")
      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${CTTestUtil.getCurrentDirectory}/CreateAndLoadCTColumnTablesWithKeyColumnsJob.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}