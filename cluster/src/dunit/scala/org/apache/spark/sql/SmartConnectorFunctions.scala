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
package org.apache.spark.sql

import java.io.{File, FileOutputStream, PrintWriter}
import java.net.InetAddress

import io.snappydata.test.util.TestException

import org.apache.spark.{SparkConf, SparkContext}


class SmartConnectorFunctions {

}
object SmartConnectorFunctions {

  def queryValidationOnConnector(locatorNetPort: Int): Unit = {
    val hostName = InetAddress.getLocalHost.getHostName
    val conf = new SparkConf()
        .setAppName("test Application")
        .setMaster(s"spark://$hostName:7077")
        .set("spark.executor.extraClassPath",
          getEnvironmentVariable("SNAPPY_DIST_CLASSPATH"))
        .set("snappydata.connection", s"localhost:$locatorNetPort")

    val sc = SparkContext.getOrCreate(conf)
    val snc = SnappyContext(sc)

    TPCHUtils.queryExecution(snc, true)
    TPCHUtils.validateResult(snc, true)
  }
  def createTablesUsingConnector(locatorNetPort: Int): Unit = {
    val hostName = InetAddress.getLocalHost.getHostName
    val conf = new SparkConf()
        .setAppName("test Application")
        .setMaster(s"spark://$hostName:7077")
        .set("spark.executor.extraClassPath",
          getEnvironmentVariable("SNAPPY_DIST_CLASSPATH"))
        .set("snappydata.connection", s"localhost:$locatorNetPort")

    val sc = SparkContext.getOrCreate(conf)
    val snc = SnappyContext(sc)

    TPCHUtils.createAndLoadTables(snc, isSnappy = true)
  }

  def getEnvironmentVariable(env: String): String = {
    val value = scala.util.Properties.envOrElse(env, null)
    if (env == null) {
      throw new TestException(s"Environment variable $env is not defined")
    }
    value
  }
  def nwQueryValidationOnConnector(locatorNetPort: Int, tableType: String): Unit = {
    val hostName = InetAddress.getLocalHost.getHostName
    val conf = new SparkConf()
        .setAppName("test Application")
        .setMaster(s"spark://$hostName:7077")
        .set("spark.executor.extraClassPath",
          SmartConnectorFunctions.getEnvironmentVariable("SNAPPY_DIST_CLASSPATH"))
        .set("snappydata.connection", s"localhost:$locatorNetPort")

    val sc = SparkContext.getOrCreate(conf)
    val snc = SnappyContext(sc)
    snc.snappySession.externalCatalog.invalidateAll()
    val sqlContext = new SparkSession(sc).sqlContext
    val pw = new PrintWriter(new FileOutputStream(
      new File(s"ValidateNWQueries_$tableType.out"), true))
    try {
      NorthWindDUnitTest.createAndLoadSparkTables(sqlContext)
      // validateReplicatedTableQueries(snc)
      NorthWindDUnitTest.validateQueriesFullResultSet(snc, tableType, pw, sqlContext)
    } finally {
      pw.close()
    }
  }

}
