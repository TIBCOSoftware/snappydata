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
package io.snappydata.cluster

import java.io.PrintWriter
import java.nio.file.{Files, Paths}
import java.sql.{Connection, DriverManager, SQLException}

import io.snappydata.test.dunit.{AvailablePortHelper, DistributedTestBase}
import io.snappydata.test.util.TestException
import scala.sys.process._

import com.pivotal.gemfirexd.TestUtil
import org.junit.Assert

import org.apache.spark.Logging
import org.apache.spark.sql.collection.Utils

class SnappyRowStoreModeDUnit (s: String) extends DistributedTestBase(s) with Logging {

  private val snappyProductDir = getEnvironmentVariable("SNAPPY_HOME")

  val port: Int = AvailablePortHelper.getRandomAvailableTCPPort
  val netPort1: Int = AvailablePortHelper.getRandomAvailableTCPPort
  val netPort2: Int = AvailablePortHelper.getRandomAvailableTCPPort
  val netPort3: Int = AvailablePortHelper.getRandomAvailableTCPPort

  override def beforeClass(): Unit = {
    super.beforeClass()
    logInfo(s"Starting snappy rowstore cluster" +
        s" in $snappyProductDir/work with locator client port $netPort1")

    // delete any old work directory
    val workDir = new java.io.File(s"$snappyProductDir/work")
    if (workDir.exists()) {
      TestUtil.deleteDir(workDir)
    }
    // create locators and servers files
    val confDir = s"$snappyProductDir/conf"
    writeToFile(s"localhost  -peer-discovery-port=$port -client-port=$netPort1",
      s"$confDir/locators")
    writeToFile(
      s"""localhost  -locators=localhost[$port] -client-port=$netPort2
         |localhost  -locators=localhost[$port] -client-port=$netPort3
         |""".stripMargin, s"$confDir/servers")
    (snappyProductDir + "/sbin/snappy-start-all.sh rowstore").!!
  }

  override def afterClass(): Unit = {
    super.afterClass()

    logInfo(s"Stopping snappy rowstore cluster in $snappyProductDir/work")
    (snappyProductDir + "/sbin/snappy-stop-all.sh").!!
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "locators"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "leads"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "servers"))
  }

  def getEnvironmentVariable(env: String): String = {
    val value = scala.util.Properties.envOrElse(env, null)
    if (env == null) {
      throw new TestException(s"Environment variable $env is not defined")
    }
    value
  }

  private def writeToFile(str: String, fileName: String): Unit = {
    val pw = new PrintWriter(fileName)
    try {
      pw.write(str)
    } finally {
      pw.close()
    }
  }

  def getANetConnection(netPort: Int): Connection = {
    val driver = "io.snappydata.jdbc.ClientDriver"
    Utils.classForName(driver).newInstance
    DriverManager.getConnection("jdbc:gemfirexd://localhost:" + netPort + "/")
  }

  /*
   * Basic test to make sure that SnappyData rowstore mode works
   */
  def testRowStoreCluster(): Unit = {
    val conn = getANetConnection(netPort1)
    val s = conn.createStatement()
    try {
      s.execute("CREATE TABLE T1(COL1 INT, COL2 INT) PERSISTENT REPLICATE")
      s.execute("INSERT INTO T1 VALUES(1, 1), (2, 2), (3, 3),(4, 4), (5, 5)")
      s.execute("SELECT * FROM T1")
      val rs = s.getResultSet
      var cnt = 0
      while (rs.next()) {
        cnt += 1
      }
      assert(cnt == 5)

      try {
        s.execute("CREATE TABLE colTable(Col1 INT ,Col2 INT, Col3 INT)" +
            "USING column " +
            "options " +
            "(" +
            "BUCKETS '1'," +
            "REDUNDANCY '0')")
        Assert.fail(
          "Should have thrown an exception as rowstore does not support column tables")
      } catch {
        case sqe: SQLException =>
          if ("42X01" != sqe.getSQLState) {
            throw sqe
          }
      }
    } finally {
      s.execute("DROP TABLE IF EXISTS T1")
    }
  }
}
