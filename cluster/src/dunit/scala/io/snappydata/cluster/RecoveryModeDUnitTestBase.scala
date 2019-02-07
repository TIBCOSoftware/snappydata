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

import java.io.File
import java.nio.file.{Files, Paths}
import java.sql.Connection
import java.util.Properties

import scala.sys.process._

import com.pivotal.gemfirexd.Attribute
import io.snappydata.Constant
import io.snappydata.cluster.SplitClusterDUnitTest.logInfo
import io.snappydata.test.dunit.{AvailablePortHelper, DistributedTestBase, Host, VM}
import io.snappydata.util.TestUtils
import org.apache.commons.io.FileUtils

import org.apache.spark.Logging
import org.apache.spark.sql.SnappyContext

/**
 * Base class for recovery mode test suites
 */


class RecoveryModeDUnitTestBase(s: String) extends DistributedTestBase(s)
    with SplitClusterDUnitTestBase
    with Serializable {

  private[this] val bootProps: Properties = new Properties()

  bootProps.setProperty("log-file", "snappyStore.log")
  bootProps.setProperty("log-level", "config")
  bootProps.setProperty("statistic-archive-file", "snappyStore.gfs")
  bootProps.setProperty("spark.executor.cores", TestUtils.defaultCores.toString)
  System.setProperty(Constant.COMPRESSION_MIN_SIZE, compressionMinSize)


  private[this] var host: Host = _
  var vm0: VM = _
  var vm1: VM = _
  var vm2: VM = _
  var vm3: VM = _

  def startArgs: Array[AnyRef] = Array(
    RecoveryModeDUnitTestBase.locatorPort, bootProps).asInstanceOf[Array[AnyRef]]


  val jdbcUser1 = "APP"
  var user1Conn = null: Connection
  var snc = null: SnappyContext

  override def setUp(): Unit = {
    super.setUp()
  }

  override def locatorClientPort: Int = {
    RecoveryModeDUnitTestBase.locatorNetPort
  }

  override def startNetworkServers(): Unit = {}

  override protected def testObject = RecoveryModeDUnitTestBase

  override protected val sparkProductDir: String = testObject
      .getEnvironmentVariable("SNAPPY_HOME")

  protected val snappyProductDir = testObject.getEnvironmentVariable("SNAPPY_HOME")

  def getConn(u: String, setSNC: Boolean = false): Connection = {
    val props = new Properties()
    props.setProperty(Attribute.USERNAME_ATTR, u)
    props.setProperty(Attribute.PASSWORD_ATTR, u)
    if (setSNC) snc = testObject.getSnappyContextForConnector(locatorClientPort, props)
    SplitClusterDUnitTest.getConnection(locatorClientPort, props)
  }

  override def beforeClass(): Unit = {

    super.beforeClass()

    // create locators, leads and servers files
    val port = RecoveryModeDUnitTestBase.locatorPort
    val netPort = RecoveryModeDUnitTestBase.locatorNetPort
    val netPort2 = AvailablePortHelper.getRandomAvailableTCPPort
    val netPort3 = AvailablePortHelper.getRandomAvailableTCPPort
    logInfo(s"Starting snappy cluster in $snappyProductDir/work with locator client port $netPort")
    val confDir = s"$snappyProductDir/conf"
    val waitForInit = "-jobserver.waitForInitialization=true"
    val compressionArg = this.compressionArg

    writeToFile(
      s"localhost  -peer-discovery-port=$port -client-port=$netPort $compressionArg",
      s"$confDir/locators")
    writeToFile(s"localhost  -locators=localhost[$port] $waitForInit $compressionArg",
      s"$confDir/leads")
    writeToFile(
      s"""localhost  -locators=localhost[$port] -client-port=$netPort2 $compressionArg
         |localhost  -locators=localhost[$port] -client-port=$netPort3 $compressionArg
         |""".stripMargin, s"$confDir/servers")
    logInfo((snappyProductDir + "/sbin/snappy-start-all.sh").!!)

    logInfo(" -- starting spark cluster --")
    RecoveryModeDUnitTestBase.startSparkCluster(sparkProductDir)
    logInfo("-* started spark cluster successfully *-")

    val props = new Properties()
    props.setProperty(Attribute.USERNAME_ATTR, jdbcUser1)
    props.setProperty(Attribute.PASSWORD_ATTR, jdbcUser1)

    user1Conn = getConn(jdbcUser1, true)
    //    user1Conn = SplitClusterDUnitTest.getConnection(locatorClientPort, props)
    var stmt = user1Conn.createStatement()

    try {
      SplitClusterDUnitTest.createTableUsingJDBC("ColTab1", "column", user1Conn, stmt,
        Map("COLUMN_BATCH_SIZE" -> "1k"), true)
      stmt.execute("create view vw_coltab1 AS (select * from ColTab1)")
      val rs = stmt.executeQuery("select * from ColTab1")
      while (rs.next()) {
        val c1 = rs.getInt("col1")
        val c2 = rs.getString("col2")
        // scalastyle:off println
        println
        print("  -->>>> " + (c1, c2))
        println
        // scalastyle:on println
      }
      rs.close()
    } finally {
      stmt.close()
      user1Conn.close()
    }


    logInfo("stopping cluster - and restarting in recovery mode")

    logInfo((snappyProductDir + "/sbin/snappy-stop-all.sh").!!)
    logInfo("stopped cluster ")
    logInfo("restarting in recovery mode")

    logInfo((snappyProductDir + "/sbin/snappy-start-all.sh --recover").!!)

    logInfo("Cluster restarted... ")
    //     user1Conn = getConn(jdbcUser1, true)
    //     println("user1Conn string === " + user1Conn.toString)
  }

  override def afterClass(): Unit = {


    super.afterClass()
    SplitClusterDUnitSecurityTest.stopSparkCluster(sparkProductDir)

    logInfo(s"Stopping snappy cluster in $snappyProductDir/work")
    logInfo((snappyProductDir + "/sbin/snappy-stop-all.sh").!!)


    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "locators"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "leads"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "servers"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "job.config"))
    FileUtils.moveDirectory(new File(s"$snappyProductDir/work"), new File
    (s"$snappyProductDir/work-snap-backup"))
  }

}


object RecoveryModeDUnitTestBase extends SplitClusterDUnitTestObject {
  private val locatorPort = AvailablePortHelper.getRandomAvailableUDPPort
  private val locatorNetPort = AvailablePortHelper.getRandomAvailableTCPPort
  override def verifySplitModeOperations(tableType: String, isComplex: Boolean, props:
  Map[String, String]): Unit = {}
  override def createTablesAndInsertData(tableType: String): Unit = {}
  override def assertTableNotCachedInHiveCatalog(tableName: String): Unit = {}
  override def createComplexTablesAndInsertData(props: Map[String, String]): Unit = {}
  def startSparkCluster(productDir: String): Unit = {
    logInfo(s"Starting spark cluster in $productDir/work")
    logInfo((productDir + "/sbin/start-all.sh") !!)
  }
}
