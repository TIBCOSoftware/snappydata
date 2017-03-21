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
package org.apache.spark.sql

import java.net.InetAddress

import io.snappydata.test.util.TestException

import org.apache.spark.{SparkConf, SparkContext}


class SmartConnectorFunctions {

}
object SmartConnectorFunctions {

  def queryValidationOnConnector(locatorPort: Int): Unit = {
    // Test setting locators property via environment variable.
    // Also enables checking for "spark." or "snappydata." prefix in key.
    System.setProperty("snappydata.store.locators", s"localhost:$locatorPort")
    val hostName = InetAddress.getLocalHost.getHostName
    val conf = new SparkConf()
        .setAppName("test Application")
        .setMaster(s"spark://$hostName:7077")
        .set("spark.executor.extraClassPath",
          getEnvironmentVariable("SNAPPY_DIST_CLASSPATH"))
        .set("snappydata.store.locators", s"localhost:$locatorPort")

    val sc = SparkContext.getOrCreate(conf)
    val snc = SnappyContext(sc)

    TPCHUtils.queryExecution(snc, true)
    TPCHUtils.validateResult(snc, true)
  }
  def createTablesUsingConnector(locatorPort: Int): Unit = {
    // Test setting locators property via environment variable.
    // Also enables checking for "spark." or "snappydata." prefix in key.
    val hostName = InetAddress.getLocalHost.getHostName
    System.setProperty("snappydata.store.locators", s"localhost:$locatorPort")
    val conf = new SparkConf()
        .setAppName("test Application")
        .setMaster(s"spark://$hostName:7077")
        .set("spark.executor.extraClassPath",
          getEnvironmentVariable("SNAPPY_DIST_CLASSPATH"))
        .set("snappydata.store.locators", s"localhost:$locatorPort")

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

}
