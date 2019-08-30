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

import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config
import io.snappydata.hydra.SnappyTestUtils
import util.TestException

import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

class CreateAndLoadCTTablesJob extends SnappySQLJob {

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val pw = new PrintWriter(new FileOutputStream(new File("CreateAndLoadCTTablesJob.out"), true));
    val tableType = jobConfig.getString("tableType")
    // scalastyle:off println
    Try {
      val snc = snSession.sqlContext
      snc.sql("set spark.sql.shuffle.partitions=6")
      val dataFilesLocation = jobConfig.getString("dataFilesLocation")
      val redundancy = jobConfig.getString("redundancy")
      pw.println(s"${SnappyTestUtils.logTime} dataFilesLocation : ${dataFilesLocation}")
      snc.setConf("dataFilesLocation", dataFilesLocation)
      CTQueries.snc = snc
      CTTestUtil.dropTables(snc)
      pw.println(s"${SnappyTestUtils.logTime} Create and load for ${tableType} tables" +
          s" has started...")
      pw.flush()
      tableType match {
        // replicated row tables
        case "Replicated" => CTTestUtil.createReplicatedRowTables(snc)
        case "PersistentReplicated" =>
          CTTestUtil.createPersistReplicatedRowTables(snc, jobConfig.getString("persistenceMode"))
        // partitioned row tables
        case "PartitionedRow" => CTTestUtil.createPartitionedRowTables(snc, redundancy)
        case "PersistentPartitionRow" => CTTestUtil.createPersistPartitionedRowTables(snc,
          redundancy, jobConfig.getString("persistenceMode"))
        case "ColocatedRow" => CTTestUtil.createColocatedRowTables(snc, redundancy)
        case "EvictionRow" => CTTestUtil.createRowTablesWithEviction(snc, redundancy)
        case "PersistentColocatedRow" => CTTestUtil.createPersistColocatedTables(snc, redundancy,
          jobConfig.getString("persistenceMode"))
        case "ColocatedWithEvictionRow" => CTTestUtil.createColocatedRowTablesWithEviction(snc,
          redundancy, jobConfig.getString("persistenceMode"))
        // column tables
        case "Column" => CTTestUtil.createColumnTables(snc, redundancy)
        case "PersistentColumn" =>
          CTTestUtil.createPersistColumnTables(snc, jobConfig.getString("persistenceMode"))
        case "ColocatedColumn" => CTTestUtil.createColocatedColumnTables(snc, redundancy)
        case "EvictionColumn" => CTTestUtil.createColumnTablesWithEviction(snc, redundancy)
        case "PersistentColocatedColumn" => CTTestUtil.createPersistColocatedColumnTables(snc,
          redundancy, jobConfig.getString("persistenceMode"))
        case "ColocatedWithEvictionColumn" =>
          CTTestUtil.createColocatedColumnTablesWithEviction(snc, redundancy)
        case _ =>
          pw.println(s"Did not find any match for ${tableType} to create tables")
          pw.close()
          throw new TestException(s"Did not find any match for ${tableType} to create tables." +
              s" See ${CTTestUtil.getCurrentDirectory}/CreateAndLoadCTTablesJob.out")
      }
      pw.println(s"${SnappyTestUtils.logTime} Tables are created. Now loading data.")
      pw.flush()
      CTTestUtil.loadTables(snc);
      println(s"Create and load for ${tableType} tables has completed successfully. " +
          s"See ${CTTestUtil.getCurrentDirectory}/CreateAndLoadCTTablesJob.out")
      pw.println(s"${SnappyTestUtils.logTime} Create and load for ${tableType} tables" +
          s" has completed successfully")
      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${CTTestUtil.getCurrentDirectory}/CreateAndLoadCTTablesJob.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}

