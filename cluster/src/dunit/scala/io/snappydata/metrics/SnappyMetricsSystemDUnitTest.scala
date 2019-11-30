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
package io.snappydata.metrics

import java.io.{File, PrintWriter}
import java.nio.file.{Files, Paths}
import java.sql.{Connection, DriverManager}

import io.snappydata.cluster.{ClusterManagerTestBase, SplitClusterDUnitTest}
import io.snappydata.test.dunit.AvailablePortHelper
import org.apache.spark.Logging
import org.json4s.jackson.JsonMethods._
import java.util

import org.junit.Assert.assertEquals
import org.apache.spark.sql.collection.Utils
import org.json4s.DefaultFormats

import scala.collection.mutable
import scala.sys.process._
import scala.util.matching.Regex

class SnappyMetricsSystemDUnitTest(s: String)
    extends ClusterManagerTestBase(s) with Logging {


  val port = AvailablePortHelper.getRandomAvailableTCPPort
  val netPort = AvailablePortHelper.getRandomAvailableTCPPort
  val netPort2 = AvailablePortHelper.getRandomAvailableTCPPort
  val netPort3 = AvailablePortHelper.getRandomAvailableTCPPort
  val netPort4 = AvailablePortHelper.getRandomAvailableTCPPort
  val snappyProductDir = System.getenv("SNAPPY_HOME")

  override def beforeClass(): Unit = {
    logInfo(s"Starting snappy cluster in $snappyProductDir/work with locator client port $netPort")
    (s"mkdir -p $snappyProductDir/work/locator" +
        s" $snappyProductDir/work/lead1" +
        s" $snappyProductDir/work/lead2" +
        s" $snappyProductDir/work/server1" +
        s" $snappyProductDir/work/server2" +
        s" $snappyProductDir/work/server3").!!
    val confDir = s"$snappyProductDir/conf"
    val sobj = new SplitClusterDUnitTest(s)
    val pw = new PrintWriter(new File(s"$confDir/locators"))
    pw.write(s"localhost -dir=$snappyProductDir/work/locator" +
        s" -peer-discovery-port=$port -client-port=$netPort")
    pw.close()
    val pw1 = new PrintWriter(new File(s"$confDir/leads"))
    pw1.write(s"localhost -locators=localhost[$port] -dir=$snappyProductDir/work/lead1\n")
    pw1.write(s"localhost -locators=localhost[$port] -dir=$snappyProductDir/work/lead2")
    pw1.close()
    val pw2 = new PrintWriter(new File(s"$confDir/servers"))
    pw2.write(s"localhost -locators=localhost[$port] -client-port=$netPort2" +
        s" -dir=$snappyProductDir/work/server1\n")
    pw2.write(s"localhost -locators=localhost[$port] -client-port=$netPort3" +
        s" -dir=$snappyProductDir/work/server2\n")
    pw2.write(s"localhost -locators=localhost[$port] -client-port=$netPort4" +
        s" -dir=$snappyProductDir/work/server3")
    pw2.close()
    logInfo(s"Starting snappy cluster in $snappyProductDir/work")

    logInfo((snappyProductDir + "/sbin/snappy-start-all.sh").!!)
    Thread.sleep(100000)
  }

  override def afterClass(): Unit = {
    logInfo((snappyProductDir + "/sbin/snappy-stop-all.sh").!!)
    s"rm -rf $snappyProductDir/work".!!
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "locators"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "leads"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "servers"))
  }

  def jsonStrToMap(jsonStr: String): Map[String, AnyVal] = {
    implicit val formats: DefaultFormats = org.json4s.DefaultFormats
    parse(jsonStr).extract[Map[String, AnyVal]]
  }

  def collectJsonStats(): mutable.Map[String, AnyVal] = {
    val url = "http://localhost:5050/metrics/json/"
    val json = scala.io.Source.fromURL(url).mkString
    val data = jsonStrToMap(json)
    val rs = data.-("counters", "meters", "histograms", "timers", "version")
    val map = scala.collection.mutable.Map[String, AnyVal]()
    for ((k, v) <- rs) {
      if (k == "gauges") {
        val data1 = v.asInstanceOf[Map[String, AnyVal]]
        for ((k, v) <- data1) {
          val data2 = v.asInstanceOf[Map[String, AnyVal]].get("value")
          map.put(k, data2.get)
        }
      }
    }
    map
  }

  def containsWords(inputString: String, items: Array[String]): Boolean = {
    var found = true
    for (item <- items) {
      if (!inputString.contains(item)) {
        found = false
        return found
      }
    }
    found
  }

  def getConnection(): Connection = {
    val driver = "io.snappydata.jdbc.ClientDriver"
    Utils.classForName(driver).newInstance
    DriverManager.getConnection(s"jdbc:snappydata:thrift://localhost[$netPort]")
  }

  def testMetricsMonitoring(): Unit = {
    doTestMetricsWhenClusterStarted()
    doTestMetricsAfterTableCreation()
    doTestMetricsAfterTableDeletion()
  }

  def doTestMetricsWhenClusterStarted(): Unit = {
    val map = collectJsonStats()
    for ((k, v) <- map) {
      if (containsWords(k, Array("MemberMetrics", "connectorCount"))) {
        assertEquals(scala.math.BigInt(0), v)}
      if (containsWords(k, Array("MemberMetrics", "dataServerCount"))) {
        assertEquals(scala.math.BigInt(3), v)}
      if (containsWords(k, Array("MemberMetrics", "locatorCount"))) {
        assertEquals(scala.math.BigInt(1), v)}
      if (containsWords(k, Array("MemberMetrics", "leadCount"))) {
        assertEquals(scala.math.BigInt(2), v)}
      if (containsWords(k, Array("MemberMetrics", "totalMembersCount"))) {
        assertEquals(scala.math.BigInt(6), v)}
      if (containsWords(k, Array("TableMetrics", "embeddedTablesCount"))) {
        assertEquals(scala.math.BigInt(0), v)}
      if (containsWords(k, Array("TableMetrics", "externalTablesCount"))) {
        assertEquals(scala.math.BigInt(0), v)}
      if (containsWords(k, Array("TableMetrics", "columnTablesCount"))) {
        assertEquals(scala.math.BigInt(0), v)}
      if (containsWords(k, Array("TableMetrics", "rowTablesCount"))) {
        assertEquals(scala.math.BigInt(0), v)}
      if (containsWords(k, Array("lead1", "isActiveLead"))) {
        assertEquals(true, v)}
      if (containsWords(k, Array("lead2", "isActiveLead"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("locator", "isActiveLead"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("server1", "isActiveLead"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("server2", "isActiveLead"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("server3", "isActiveLead"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("lead1", "isLead"))) {
        assertEquals(true, v)}
      if (containsWords(k, Array("lead2", "isLead"))) {
        assertEquals(true, v)}
      if (containsWords(k, Array("locator", "isLead"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("server1", "isLead"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("server2", "isLead"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("server3", "isLead"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("lead1", "isLocator"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("lead2", "isLocator"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("locator", "isLocator"))) {
        assertEquals(true, v)}
      if (containsWords(k, Array("server1", "isLocator"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("server2", "isLocator"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("server3", "isLocator"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("lead1", "isDataServer"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("lead2", "isDataServer"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("locator", "isDataServer"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("server1", "isDataServer"))) {
        assertEquals(true, v)}
      if (containsWords(k, Array("server2", "isDataServer"))) {
        assertEquals(true, v)}
      if (containsWords(k, Array("server3", "isDataServer"))) {
        assertEquals(true, v)}
      if (containsWords(k, Array("lead1", "memberType"))) {
        assertEquals("LEAD", v)}
      if (containsWords(k, Array("lead2", "memberType"))) {
        assertEquals("LEAD", v)}
      if (containsWords(k, Array("locator", "memberType"))) {
        assertEquals("LOCATOR", v)}
      if (containsWords(k, Array("server1", "memberType"))) {
        assertEquals("DATA SERVER", v)}
      if (containsWords(k, Array("server2", "memberType"))) {
        assertEquals("DATA SERVER", v)}
      if (containsWords(k, Array("server3", "memberType"))) {
        assertEquals("DATA SERVER", v)}
      if (containsWords(k, Array("lead1", "logFile"))) {
        assertEquals("snappyleader.log", v)}
      if (containsWords(k, Array("lead2", "logFile"))) {
        assertEquals("snappyleader.log", v)}
      if (containsWords(k, Array("locator", "logFile"))) {
        assertEquals("snappylocator.log", v)}
      if (containsWords(k, Array("server1", "logFile"))) {
        assertEquals("snappyserver.log", v)}
      if (containsWords(k, Array("server2", "logFile"))) {
        assertEquals("snappyserver.log", v)}
      if (containsWords(k, Array("server3", "logFile"))) {
        assertEquals("snappyserver.log", v)}
      if (containsWords(k, Array("lead1", "shortDirName"))) {
        assertEquals("lead1", v)}
      if (containsWords(k, Array("lead2", "shortDirName"))) {
        assertEquals("lead2", v)}
      if (containsWords(k, Array("locator", "shortDirName"))) {
        assertEquals("locator", v)}
      if (containsWords(k, Array("server1", "shortDirName"))) {
        assertEquals("server1", v)}
      if (containsWords(k, Array("server2", "shortDirName"))) {
        assertEquals("server2", v)}
      if (containsWords(k, Array("server3", "shortDirName"))) {
        assertEquals("server3", v)}
      if (containsWords(k, Array("lead1", "status"))) {
        assertEquals("Running", v)}
      if (containsWords(k, Array("lead2", "status"))) {
        assertEquals("Running", v)}
      if (containsWords(k, Array("locator", "status"))) {
        assertEquals("Running", v)}
      if (containsWords(k, Array("server1", "status"))) {
        assertEquals("Running", v)}
      if (containsWords(k, Array("server2", "status"))) {
        assertEquals("Running", v)}
      if (containsWords(k, Array("server3", "status"))) {
        assertEquals("Running", v)}
      if (containsWords(k, Array("lead1", "diskStoreName"))) {
        assertEquals("GFXD-DEFAULT-DISKSTORE", v)}
      if (containsWords(k, Array("lead2", "diskStoreName"))) {
        assertEquals("GFXD-DEFAULT-DISKSTORE", v)}
      if (containsWords(k, Array("locator", "diskStoreName"))) {
        assertEquals("GFXD-DEFAULT-DISKSTORE", v)}
      if (containsWords(k, Array("server1", "diskStoreName"))) {
        assertEquals("GFXD-DEFAULT-DISKSTORE", v)}
      if (containsWords(k, Array("server2", "diskStoreName"))) {
        assertEquals("GFXD-DEFAULT-DISKSTORE", v)}
      if (containsWords(k, Array("server3", "diskStoreName"))) {
        assertEquals("GFXD-DEFAULT-DISKSTORE", v)}
      if (containsWords(k, Array("lead1", "fullDirName"))) {
        assertEquals(s"$snappyProductDir/work/lead1", v)}
      if (containsWords(k, Array("lead2", "fullDirName"))) {
        assertEquals(s"$snappyProductDir/work/lead2", v)}
      if (containsWords(k, Array("locator", "fullDirName"))) {
        assertEquals(s"$snappyProductDir/work/locator", v)}
      if (containsWords(k, Array("server1", "fullDirName"))) {
        assertEquals(s"$snappyProductDir/work/server1", v)}
      if (containsWords(k, Array("server2", "fullDirName"))) {
        assertEquals(s"$snappyProductDir/work/server2", v)}
      if (containsWords(k, Array("server3", "fullDirName"))) {
        assertEquals(s"$snappyProductDir/work/server3", v)}
    }
  }

  def doTestMetricsAfterTableCreation(): Unit = {
    val conn = getConnection()
    val stmt = conn.createStatement()
    val path = getClass.getResource("/northwind/orders" +
        ".csv").getPath
    stmt.execute(s"create external table test1 using csv options(path '${
      (path)}', header 'false', inferschema 'true')")
    stmt.execute("create table test2 using column options() as (select * from test1)")
    var rs = stmt.executeQuery("select * from test2")
    var rowCnt = 0
    while (rs.next()) {
      rowCnt = rowCnt + 1
    }
    stmt.execute("create table test(id int, str string) using row")
    stmt.execute("insert into test values(1, 'abc')")
    stmt.execute("insert into test values(2, 'cde')")
    rs = stmt.executeQuery("select * from test")
    var rowCnt1 = 0
    while (rs.next()) {
      rowCnt1 = rowCnt1 + 1
    }
    Thread.sleep(10000)
    val map = collectJsonStats()
    for ((k, v) <- map) {
      if (containsWords(k, Array("TableMetrics", "embeddedTablesCount"))) {
        assertEquals(scala.math.BigInt(2), v)}
      if (containsWords(k, Array("TableMetrics", "externalTablesCount"))) {
        assertEquals(scala.math.BigInt(1), v)}
      if (containsWords(k, Array("TableMetrics", "columnTablesCount"))) {
        assertEquals(scala.math.BigInt(1), v)}
      if (containsWords(k, Array("TableMetrics", "rowTablesCount"))) {
        assertEquals(scala.math.BigInt(1), v)}
      if (containsWords(k, Array("ExternalTableMetrics.app.test1", "tableType"))) {
        assertEquals("EXTERNAL", v)}
      if (containsWords(k, Array("ExternalTableMetrics.app.test1", "tableName"))) {
        assertEquals("app.test1", v)}
      if (containsWords(k, Array("ExternalTableMetrics.app.test1", "provider"))) {
        assertEquals("csv", v)}
      if (containsWords(k, Array("ExternalTableMetrics.app.test1", "dataSourcePath"))) {
        assertEquals(path, v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST2", "tableName"))) {
        assertEquals("APP.TEST2", v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST2", "isReplicatedTable"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST2", "sizeSpillToDisk"))) {
        assertEquals(scala.math.BigInt(0), v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST2", "isAnyBucketLost"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST2", "totalSize"))) {
        assertEquals(scala.math.BigInt(535128), v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST2", "redundancy"))) {
        assertEquals(scala.math.BigInt(0), v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST2", "rowCount"))) {
        assertEquals(scala.math.BigInt(831), v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST2", "isRedundancyImpaired"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST2", "bucketCount"))) {
        assertEquals(scala.math.BigInt(24), v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST2", "isColumnTable"))) {
        assertEquals(true, v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST2", "sizeInMemory"))) {
        assertEquals(scala.math.BigInt(535128), v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST.", "tableName"))) {
        assertEquals("APP.TEST", v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST.", "isReplicatedTable"))) {
        assertEquals(true, v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST.", "sizeSpillToDisk"))) {
        assertEquals(scala.math.BigInt(0), v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST.", "isAnyBucketLost"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST.", "totalSize"))) {
        assertEquals(scala.math.BigInt(6336), v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST.", "redundancy"))) {
        assertEquals(scala.math.BigInt(0), v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST.", "rowCount"))) {
        assertEquals(scala.math.BigInt(2), v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST.", "isRedundancyImpaired"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST.", "bucketCount"))) {
        assertEquals(scala.math.BigInt(1), v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST.", "isColumnTable"))) {
        assertEquals(false, v)}
      if (containsWords(k, Array("TableMetrics.APP.TEST.", "sizeInMemory"))) {
        assertEquals(scala.math.BigInt(6336), v)}
    }
  }

  def doTestMetricsAfterTableDeletion(): Unit = {
    val conn = getConnection()
    val stmt = conn.createStatement()
    stmt.execute("drop table test")
    stmt.execute("drop table test1")
    stmt.execute("drop table test2")
    Thread.sleep(10000)

    val map = collectJsonStats()
    for((k, v) <- map) {
      if (containsWords(k, Array("TableMetrics", "embeddedTablesCount"))) {
        assertEquals(scala.math.BigInt(0), v)}
      if (containsWords(k, Array("TableMetrics", "externalTablesCount"))) {
        assertEquals(scala.math.BigInt(0), v)}
      if (containsWords(k, Array("TableMetrics", "columnTablesCount"))) {
        assertEquals(scala.math.BigInt(0), v)}
      if (containsWords(k, Array("TableMetrics", "rowTablesCount"))) {
        assertEquals(scala.math.BigInt(0), v)}
    }
  }
}
