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
package io.snappydata.hydra.northWind

import java.io.{File, FileOutputStream, PrintWriter}

import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkContext, SparkConf}

object CreateAndLoadReplicatedRowTablesSparkApp {
  val conf = new SparkConf().
    setAppName("NWTestSparkApp Application")
  val sc = new SparkContext(conf)
  val snc = SnappyContext(sc)

  def main(args: Array[String]) {
    snc.sql("set spark.sql.shuffle.partitions=6")
    NWQueries.snc = snc
    val pw = new PrintWriter(new FileOutputStream(new File("NWTestSparkApp.out"), true));
    NWTestSparkApp.dropTables(snc)
    println("Create and load Replicated Row tables Test started")
    NWTestSparkApp.createAndLoadReplicatedTables(snc)
    NWTestSparkApp.validateQueries(snc, "Replicated Row Table", pw)
    println("Create and load Replicated Row tables Test completed successfully")
    pw.close()
  }
}
