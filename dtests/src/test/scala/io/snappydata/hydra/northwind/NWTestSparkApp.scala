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

import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkConf, SparkContext}


object NWTestSparkApp {

  def main(args: Array[String]) {
    val connectionURL = args(args.length - 1)
    val conf = new SparkConf().
        setAppName("NWTestSpark Application_" + System.currentTimeMillis()).
        set("snappydata.connection", connectionURL)
    val sc = SparkContext.getOrCreate(conf)
    val snc = SnappyContext(sc)
    val dataFilesLocation = args(0)
    // scalastyle:off println
    println(s"dataFilesLocation : ${dataFilesLocation}")
    snc.setConf("dataFilesLocation", dataFilesLocation)
    snc.sql("set spark.sql.shuffle.partitions=6")
    NWQueries.snc = snc
    NWQueries.dataFilesLocation = dataFilesLocation
    val pw = new PrintWriter(new FileOutputStream(new File("NWTestSparkApp.out"), true));
    NWTestUtil.dropTables(snc)
    println("Test replicated row tables queries started")
    NWTestUtil.createAndLoadReplicatedTables(snc)
    NWTestUtil.validateQueries(snc, "Replicated Row Table", pw)
    println("Test replicated row tables queries completed successfully")
    NWTestUtil.dropTables(snc)
    println("Test partitioned row tables queries started")
    NWTestUtil.createAndLoadPartitionedTables(snc)
    NWTestUtil.validateQueries(snc, "Partitioned Row Table", pw)
    println("Test partitioned row tables queries completed successfully")
    NWTestUtil.dropTables(snc)
    println("Test column tables queries started")
    NWTestUtil.createAndLoadColumnTables(snc)
    NWTestUtil.validateQueries(snc, "Column Table", pw)
    println("Test column tables queries completed successfully")
    NWTestUtil.dropTables(snc)
    NWTestUtil.createAndLoadColocatedTables(snc)
    NWTestUtil.validateQueries(snc, "Colocated Table", pw)
    pw.close()
  }

}
