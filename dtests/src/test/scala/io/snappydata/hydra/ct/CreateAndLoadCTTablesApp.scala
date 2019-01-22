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

package io.snappydata.hydra.ct

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkContext, SparkConf}

object CreateAndLoadCTTablesApp {

  def main(args: Array[String]) {
    val connectionURL = args(args.length - 1)
    val conf = new SparkConf().
        setAppName("CreateAndLoadCTTables Application").
        set("snappydata.connection", connectionURL)
    val sc = SparkContext.getOrCreate(conf)
    val snc = SnappyContext(sc)
    val dataFilesLocation = args(0)
    snc.setConf("dataFilesLocation", dataFilesLocation)
    CTQueries.snc = snc
    val tableType = args(1)
    val redundancy = args(2)
    val persistenceMode = args(3)
    val pw = new PrintWriter(new FileOutputStream(new File("CreateAndLoadCTTablesApp.out"), true))
    // scalastyle:off println
    pw.println(s"dataFilesLocation : ${dataFilesLocation}")
    CTTestUtil.dropTables(snc)
    pw.println(s"Create and load for ${tableType} tables has started")
    pw.flush()
    tableType match {
      // replicated row tables
      case "Replicated" => CTTestUtil.createReplicatedRowTables(snc)
      case "PersistentReplicated" => CTTestUtil.createPersistReplicatedRowTables(snc,
        persistenceMode)
      // partitioned row tables
      case "PartitionedRow" => CTTestUtil.createPartitionedRowTables(snc, redundancy)
      case "PersistentPartitionRow" => CTTestUtil.createPersistPartitionedRowTables(snc,
        redundancy, persistenceMode)
      case "ColocatedRow" => CTTestUtil.createColocatedRowTables(snc, redundancy)
      case "EvictionRow" => CTTestUtil.createRowTablesWithEviction(snc, redundancy)
      case "PersistentColocatedRow" => CTTestUtil.createPersistColocatedTables(snc, redundancy,
        persistenceMode)
      case "ColocatedWithEvictionRow" => CTTestUtil.createColocatedRowTablesWithEviction(snc,
        redundancy, persistenceMode)
      // column tables
      case "Column" => CTTestUtil.createColumnTables(snc, redundancy)
      case "PersistentColumn" => CTTestUtil.createPersistColumnTables(snc, persistenceMode)
      case "ColocatedColumn" => CTTestUtil.createColocatedColumnTables(snc, redundancy)
      case "EvictionColumn" => CTTestUtil.createColumnTablesWithEviction(snc, redundancy)
      case "PersistentColocatedColumn" => CTTestUtil.createPersistColocatedColumnTables(snc,
        redundancy, persistenceMode)
      case "ColocatedWithEvictionColumn" => CTTestUtil.createColocatedColumnTablesWithEviction(snc,
        redundancy)
      case _ =>
        pw.println(s"Did not find any match for ${tableType} to create tables")
        pw.close()
        throw new Exception(s"Did not find any match for ${tableType} to create tables." +
            s" See ${CTTestUtil.getCurrentDirectory}/CreateAndLoadCTTablesApp.out")
    }
    pw.println("Tables are created. Now loading data.")
    pw.flush()
    CTTestUtil.loadTables(snc)
    println(s"Create and load for ${tableType} tables has completed successfully. " +
        s"See ${CTTestUtil.getCurrentDirectory}/CreateAndLoadCTTablesApp.out")
    pw.println(s"Create and load for ${tableType} tables has completed successfully")
    pw.close()
  }
}

