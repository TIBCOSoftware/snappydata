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

import java.net.InetAddress
import java.nio.file.{Files, Paths}
import java.sql.{Blob, Clob, Connection, ResultSet, SQLException, Statement, Timestamp}
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.language.{implicitConversions, postfixOps}
import scala.sys.process._
import scala.util.Random

import com.fasterxml.jackson.databind.ObjectMapper
import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.snappy.ComplexTypeSerializer
import io.snappydata.Constant
import io.snappydata.test.dunit.{AvailablePortHelper, DistributedTestBase, Host, VM}
import io.snappydata.util.TestUtils
import org.junit.Assert

import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.types.Decimal
import org.apache.spark.util.collection.OpenHashSet

/**
 * Basic tests for non-embedded mode connections to an embedded cluster.
 */
class SplitClusterDUnitTest(s: String)
    extends DistributedTestBase(s)
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

  if (Host.getHostCount > 0) {
    host = Host.getHost(0)
    vm0 = host.getVM(0)
    vm1 = host.getVM(1)
    vm2 = host.getVM(2)
    vm3 = host.getVM(3)
  }

  override def startArgs: Array[AnyRef] = Array(
    SplitClusterDUnitTest.locatorPort, bootProps).asInstanceOf[Array[AnyRef]]

  private val snappyProductDir =
    testObject.getEnvironmentVariable("SNAPPY_HOME")

  override protected val sparkOldProductDir: String =
    testObject.getEnvironmentVariable("APACHE_SPARK_OLD_HOME")

  protected val currentProductDir: String =
    testObject.getEnvironmentVariable("APACHE_SPARK_CURRENT_HOME")

  override protected def locatorClientPort = { testObject.locatorNetPort }

  override def beforeClass(): Unit = {
    super.beforeClass()

    // create locators, leads and servers files
    val port = SplitClusterDUnitTest.locatorPort
    val netPort = SplitClusterDUnitTest.locatorNetPort
    val netPort2 = AvailablePortHelper.getRandomAvailableTCPPort
    val netPort3 = AvailablePortHelper.getRandomAvailableTCPPort

    logInfo(s"Starting snappy cluster in $snappyProductDir/work with locator client port $netPort")

    val compressionArg = this.compressionArg
    val waitForInit = "-jobserver.waitForInitialization=true"
    val confDir = s"$snappyProductDir/conf"
    writeToFile(s"localhost  -peer-discovery-port=$port -client-port=$netPort",
      s"$confDir/locators")
    writeToFile(s"localhost  -locators=localhost[$port] $waitForInit $compressionArg",
      s"$confDir/leads")
    writeToFile(
      s"""localhost  -locators=localhost[$port] -client-port=$netPort2 $compressionArg
          |localhost  -locators=localhost[$port] -client-port=$netPort3 $compressionArg
          |""".stripMargin, s"$confDir/servers")
    (snappyProductDir + "/sbin/snappy-start-all.sh").!!

    vm3.invoke(getClass, "startSparkCluster", sparkOldProductDir)
  }

  override def afterClass(): Unit = {
    super.afterClass()
    vm3.invoke(getClass, "stopSparkCluster", sparkOldProductDir)

    logInfo(s"Stopping snappy cluster in $snappyProductDir/work")
    (snappyProductDir + "/sbin/snappy-stop-all.sh").!!
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "locators"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "leads"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "servers"))
  }

  override protected def startNetworkServers(): Unit = {
    // no change to network servers at runtime in this mode
  }

  override protected def testObject = SplitClusterDUnitTest

  // test to make sure that stock spark-shell works with SnappyData core jar
  def testSparkShell(): Unit = {
    testObject.invokeSparkShell(snappyProductDir, sparkOldProductDir, locatorClientPort, vm = vm3)
  }

  // test to make sure that stock spark-shell for latest Spark release works with JDBC pool jar
  def testSparkShellCurrent(): Unit = {
    testObject.invokeSparkShellCurrent(snappyProductDir, sparkOldProductDir, currentProductDir,
      locatorClientPort, new Properties(), vm3)
  }
}

object SplitClusterDUnitTest extends SplitClusterDUnitTestObject {

  private val locatorPort = AvailablePortHelper.getRandomAvailableTCPPort
  private val locatorNetPort = AvailablePortHelper.getRandomAvailableTCPPort

  override def assertTableNotCachedInHiveCatalog(tableName: String): Unit = {
  }

  override def createTablesAndInsertData(tableType: String): Unit = {
    val conn = getConnection(locatorNetPort)
    val stmt = conn.createStatement()

    createTableUsingJDBC("embeddedModeTable1", tableType, conn, stmt)
    selectFromTableUsingJDBC("embeddedModeTable1", 1005, stmt)

    createTableUsingJDBC("embeddedModeTable2", tableType, conn, stmt)
    selectFromTableUsingJDBC("embeddedModeTable2", 1005, stmt)

    stmt.close()
    conn.close()
    logInfo("Successful")
  }

  override def createComplexTablesAndInsertData(
      props: Map[String, String]): Unit = {
    val conn = getConnection(locatorNetPort)
    val stmt = conn.createStatement()

    createComplexTableUsingJDBC("embeddedModeTable1", conn, stmt, props)
    selectFromComplexTypeTableUsingJDBC("embeddedModeTable1", 1005, stmt)

    createComplexTableUsingJDBC("embeddedModeTable2", conn, stmt, props)
    selectFromComplexTypeTableUsingJDBC("embeddedModeTable2", 1005, stmt)

    stmt.close()
    conn.close()
    logInfo("Successful")
  }

  override def verifySplitModeOperations(tableType: String, isComplex: Boolean,
      props: Map[String, String]): Unit = {
    val conn = getConnection(locatorNetPort)
    val stmt = conn.createStatement()

    // embeddedModeTable1 is dropped in split mode. recreate it
    if (isComplex) {
      createComplexTableUsingJDBC("embeddedModeTable1", conn, stmt, props)
      selectFromComplexTypeTableUsingJDBC("embeddedModeTable1", 1005, stmt)
    } else {
      createTableUsingJDBC("embeddedModeTable1", tableType, conn, stmt, props)
      selectFromTableUsingJDBC("embeddedModeTable1", 1005, stmt)
    }

    // read data from splitModeTable1
    if (isComplex) {
      selectFromComplexTypeTableUsingJDBC("splitModeTable1", 1005, stmt)
    } else {
      selectFromTableUsingJDBC("splitModeTable1", 1005, stmt)
    }

    // drop table created in split mode
    stmt.execute("drop table if exists splitModeTable1")

    // recreate the dropped table
    if (isComplex) {
      createComplexTableUsingJDBC("splitModeTable1", conn, stmt, props)
      selectFromComplexTypeTableUsingJDBC("splitModeTable1", 1005, stmt)
    } else {
      createTableUsingJDBC("splitModeTable1", tableType, conn, stmt, props)
      selectFromTableUsingJDBC("splitModeTable1", 1005, stmt)
    }

    // check for SNAP-2156/2164
    var updateSql = "update splitModeTable1 set col1 = 100 where exists " +
        s"(select 1 from splitModeTable1 t where t.col1 = splitModeTable1.col1 and t.col1 = 1234)"
    assert(!stmt.execute(updateSql))
    assert(stmt.getUpdateCount >= 0) // random value can be 1234
    assert(stmt.executeUpdate(updateSql) == 0)
    updateSql = "update splitModeTable1 set col1 = 100 where exists " +
        s"(select 1 from splitModeTable1 t where t.col1 = splitModeTable1.col1 and t.col1 = 1)"
    assert(!stmt.execute(updateSql))
    assert(stmt.getUpdateCount >= 1)
    assert(stmt.executeUpdate(updateSql) == 0)
    updateSql = "update splitModeTable1 set col1 = 1 where exists " +
        s"(select 1 from splitModeTable1 t where t.col1 = splitModeTable1.col1 and t.col1 = 100)"
    assert(stmt.executeUpdate(updateSql) >= 1)
    assert(!stmt.execute(updateSql))
    assert(stmt.getUpdateCount == 0)

    // check exception should be proper (SNAP-1423/1386)
    try {
      stmt.execute("call sys.rebalance_all_bickets()")
    } catch {
      case sqle: SQLException if sqle.getSQLState == "42Y03" => // ignore
    }

    stmt.execute("drop table if exists embeddedModeTable1")
    stmt.execute("drop table if exists embeddedModeTable2")
    stmt.execute("drop table if exists splitModeTable1")

    stmt.close()
    conn.close()
    logInfo("Successful")
  }

  private def getPropertiesAsSQLString(props: Map[String, String]): String =
    if (props.isEmpty) ""
    else props.foldLeft(new StringBuilder) {
      case (sb, (k, v)) =>
        if (sb.isEmpty) sb.append(" OPTIONS(")
        else sb.append(", ")
        sb.append(k).append(' ').append('\'').append(v).append('\'')
    }.append(')').toString()

  def createTableUsingJDBC(tableName: String, tableType: String,
      conn: Connection, stmt: Statement,
      propsMap: Map[String, String] = props, addData: Boolean = true): Unit = {

    stmt.execute(s"drop table if exists $tableName")

    stmt.execute(
      s"""
        CREATE TABLE $tableName (
          col1 Int, col2 String, col3 Decimal
        ) USING $tableType${getPropertiesAsSQLString(propsMap)}""")

    if (addData) populateTable(tableName, conn)
  }

  def populateTable(tableName: String, conn: Connection): Unit = {
    val data = ArrayBuffer(Data(1, "2", Decimal("3.2")),
      Data(7, "8", Decimal("9.8")), Data(9, "2", Decimal("3.9")),
      Data(4, "2", Decimal("2.4")), Data(5, "6", Decimal("7.6")))
    for (_ <- 1 to 1000) {
      data += Data(Random.nextInt(), Integer.toString(Random.nextInt()),
        Decimal(Random.nextInt(100).toString + '.' + Random.nextInt(100)))
    }

    val pstmt = conn.prepareStatement(s"insert into $tableName values (?, ?, ?)")
    for (d <- data) {
      pstmt.setInt(1, d.col1)
      pstmt.setString(2, d.col2)
      pstmt.setBigDecimal(3, d.col3.toJavaBigDecimal)
      pstmt.addBatch()
    }
    pstmt.executeBatch()
    pstmt.close()
  }

  def createComplexTableUsingJDBC(tableName: String, conn: Connection,
      stmt: Statement, propsMap: Map[String, String] = props): ArrayBuffer[
      ComplexData] = {
    val keys = new OpenHashSet[Int](1005)
    val dec1 = Array(Decimal("4.92"), Decimal("51.98"))
    val dec2 = Array(Decimal("95.27"), Decimal("17.25"), Decimal("7583.2956"))
    val time = System.currentTimeMillis()
    val ts = Array(new Timestamp(time), new Timestamp(time + 123456L),
      new Timestamp(0L), new Timestamp(time - 12246L), new Timestamp(-1L))
    val m1 = Map(
      ts(0) -> Data(3, "8", Decimal("1.8")),
      ts(1) -> Data(5, "3", Decimal(".5")),
      ts(2) -> Data(8, "2", Decimal("2.1")))
    val m2 = Map(
      ts(3) -> Data(8, "3", Decimal("1.3")),
      ts(0) -> Data(7, "5", Decimal("7.6")),
      ts(4) -> Data(4, "8", Decimal("8.9")))
    val data = ArrayBuffer[ComplexData]()
    data += ComplexData(1, dec1, "3", m2, 7.56, Data(2, "8", Decimal("3.8")),
      dec1(0), ts(0))
    data += ComplexData(7, dec1, "8", m1, 8.45, Data(7, "4", Decimal("9")),
      dec2(0), ts(1))
    data += ComplexData(9, dec2, "2", m2, 12.33, Data(3, "1", Decimal("7.3")),
      dec1(1), ts(2))
    data += ComplexData(4, dec2, "2", m1, 92.85, Data(9, "3", Decimal("4.3")),
      dec2(1), ts(3))
    data += ComplexData(5, dec2, "7", m1, 5.28, Data(4, "8", Decimal("1.0")),
      dec2(2), ts(4))
    for (_ <- 1 to 1000) {
      var rnd: Long = 0L
      var rnd1 = 0
      do {
        rnd = Random.nextLong()
        rnd1 = rnd.asInstanceOf[Int]
      } while (keys.contains(rnd1))
      keys.add(rnd1)
      val rnd2 = (rnd >>> 32).asInstanceOf[Int]
      val drnd = Random.nextInt(65536)
      val drnd1 = drnd & 0xff
      val drnd2 = (drnd >>> 8) & 0xff
      val dec = if ((rnd1 % 2) == 0) dec1 else dec2
      val map = if ((rnd2 % 2) == 0) m1 else m2
      data += ComplexData(rnd1, dec, rnd2.toString, map, Random.nextDouble(),
        Data(rnd1, Integer.toString(rnd2), Decimal(drnd1.toString + '.' +
            drnd2.toString)), dec(1), ts(math.abs(rnd1) % 5))
    }
    stmt.execute(
      s"""
        CREATE TABLE $tableName (
          col1 Int,
          col2 Array<Decimal>,
          col3 String,
          col4 Map<Timestamp, Struct<col1: Int, col2: String, col3: Decimal(10,5)>>,
          col5 Double,
          col6 Struct<col1: Int, col2: String, col3: Decimal(10,5)>,
          col7 Decimal,
          col8 Timestamp
        ) USING column${getPropertiesAsSQLString(propsMap)}""")

    val pstmt = conn.prepareStatement(
      s"insert into $tableName values (?, ?, ?, ?, ?, ?, ?, ?)")
    val serializer1 = ComplexTypeSerializer.create(tableName, "col2", conn)
    val serializer2 = ComplexTypeSerializer.create(tableName, "col4", conn)
    val serializer3 = ComplexTypeSerializer.create(tableName, "col6", conn)

    // check failures with incompatible serialization first
    checkSerializationMetadata(ts, dec1, m2,
      Array(serializer1, serializer2, serializer3))

    // proper inserts
    for (d <- data) {
      pstmt.setInt(1, d.col1)
      pstmt.setBytes(2, serializer1.serialize(d.col2))
      pstmt.setString(3, d.col3)
      pstmt.setBytes(4, serializer2.serialize(d.col4))
      pstmt.setDouble(5, d.col5)
      // test with Product, Array, int[], Seq and Collection
      Random.nextInt(5) match {
        case 0 => pstmt.setBytes(6, serializer3.serialize(d.col6))
        case 1 => pstmt.setBytes(6, serializer3.serialize(
          d.col6.productIterator.toArray))
        case 2 => pstmt.setBytes(6, serializer3.serialize(
          Array(d.col6.col1, d.col6.col2, d.col6.col3)))
        case 3 => pstmt.setBytes(6, serializer3.serialize(
          d.col6.productIterator.toSeq))
        case 4 => pstmt.setBytes(6, serializer3.serialize(
          d.col6.productIterator.toSeq.asJava))
      }
      pstmt.setBigDecimal(7, d.col7.toJavaBigDecimal)
      pstmt.setTimestamp(8, d.col8)
      pstmt.addBatch()
    }
    pstmt.executeBatch()
    pstmt.close()

    data
  }

  private def checkSerializationMetadata(ts: Array[Timestamp],
      dec1: Array[Decimal], m2: Map[Timestamp, Data],
      serializers: Array[ComplexTypeSerializer]): Unit = {
    val m3 = Map(
      ts(3) -> Data3(8, 3, Decimal("8.1")),
      ts(0) -> Data3(7, 5, Decimal("5.7")),
      ts(4) -> Data3(4, 8, Decimal("9.4")))
    val data2 = ComplexData2(1, dec1, "3", m3, 7.56,
      Data(2, "8", Decimal("23.82")), dec1(0), ts(0))
    val data3 = ComplexData3(1, dec1, "3", m2, 7.56, Data2(2, "8", "3"),
      dec1(0), ts(0))

    val dec3 = Array(Decimal("4.92"), "51.98")
    val dec4 = Array("4.92", "51.98")
    try {
      serializers(0).serialize(dec3)
      Assert.fail("Expected an IllegalArgumentException")
    } catch {
      case _: IllegalArgumentException => // expected
    }
    try {
      serializers(0).serialize(dec4)
      Assert.fail("Expected an IllegalArgumentException")
    } catch {
      case _: IllegalArgumentException => // expected
    }
    try {
      serializers(1).serialize(data2.col4)
      Assert.fail("Expected an IllegalArgumentException")
    } catch {
      case _: IllegalArgumentException => // expected
    }
    try {
      serializers(2).serialize(data3.col6)
      Assert.fail("Expected an IllegalArgumentException")
    } catch {
      case _: IllegalArgumentException => // expected
    }

    // check validateAll and clearing of "validated" flag and reset for failures

    // for ARRAY serializers(0)
    serializers(0).serialize(dec1)
    try {
      serializers(0).serialize(dec4)
      Assert.fail("Expected an IllegalArgumentException")
    } catch {
      case _: IllegalArgumentException => // expected
    }
    try {
      serializers(0).serialize(dec4, true)
      Assert.fail("Expected an IllegalArgumentException")
    } catch {
      case _: IllegalArgumentException => // expected
    }
    try {
      serializers(0).serialize(dec3, true)
      Assert.fail("Expected an IllegalArgumentException")
    } catch {
      case _: IllegalArgumentException => // expected
    }

    // for MAP serializers(1)
    serializers(1).serialize(data3.col4)
    try {
      serializers(1).serialize(data2.col4, false)
      Assert.fail("Expected an IllegalArgumentException")
    } catch {
      case _: IllegalArgumentException => // expected
    }
    try {
      serializers(1).serialize(data2.col4, true)
      Assert.fail("Expected an IllegalArgumentException")
    } catch {
      case _: IllegalArgumentException => // expected
    }

    // STRUCT serializers(2) will succeed for incorrect calls without validation
    // since identity serializer for INT will accept a String just fine
    serializers(2).serialize(data2.col6)
    // no validation for next (incorrect) call but will still fail with
    // ClassCastException in generated code
    try {
      serializers(2).serialize(data3.col6)
      Assert.fail("Expected an IllegalArgumentException")
    } catch {
      case _: IllegalArgumentException => // expected
    }
    // ... but same should fail properly with validateAll
    try {
      serializers(2).serialize(data3.col6, true)
      Assert.fail("Expected an IllegalArgumentException")
    } catch {
      case _: IllegalArgumentException => // expected
    }
    // after an exception, validation should turn on again for next call
    try {
      serializers(2).serialize(data3.col6, false)
      Assert.fail("Expected an IllegalArgumentException")
    } catch {
      case _: IllegalArgumentException => // expected
    }
    // ... and remain on
    try {
      serializers(2).serialize(data3.col6, false)
      Assert.fail("Expected an IllegalArgumentException")
    } catch {
      case _: IllegalArgumentException => // expected
    }
    // ... and clear out again
    serializers(2).serialize(data2.col6)
    try {
      serializers(2).serialize(data3.col6)
      Assert.fail("Expected an IllegalArgumentException")
    } catch {
      case _: IllegalArgumentException => // expected
    }
  }

  def selectFromTableUsingJDBC(tableName: String,
      expectedLength: Int, stmt: Statement): Unit = {
    val rs = stmt.executeQuery(s"SELECT * FROM $tableName")
    var numResults = 0
    while (rs.next()) numResults += 1
    assert(numResults == expectedLength,
      s"Expected $expectedLength but got $numResults")
  }

  private implicit def clobString(clob: Clob): String =
    clob.getSubString(1L, clob.length().toInt)

  def selectFromComplexTypeTableUsingJDBC(tableName: String,
      expectedLength: Int, stmt: Statement): Unit = {
    val conn = stmt.getConnection
    val serializer1 = ComplexTypeSerializer.create(tableName, "col2", conn)
    val serializer2 = ComplexTypeSerializer.create(tableName, "col4", conn)
    val serializer3 = ComplexTypeSerializer.create(tableName, "col6", conn)

    var rs = stmt.executeQuery(s"SELECT * FROM $tableName --+ complexTypeAsJson(0)")
    var numResults = 0
    while (rs.next()) {
      // check access to complex types in different ways
      val res11 = serializer1.deserialize(rs.getBytes(2))
      val res12 = serializer2.deserialize(rs.getBytes("col4"))
      val res13 = serializer3.deserialize(rs.getBytes(6))

      val res21 = serializer1.deserialize(rs.getObject("col2")
          .asInstanceOf[Blob])
      val res22 = serializer2.deserialize(rs.getObject("col4")
          .asInstanceOf[Blob])
      val res23 = serializer3.deserialize(rs.getObject(6)
          .asInstanceOf[Blob])

      val res31 = serializer1.deserialize(rs.getBlob("col2"))
      val res32 = serializer2.deserialize(rs.getBlob(4))
      val res33 = serializer3.deserialize(rs.getBlob("col6"))

      numResults match {
        case 0 =>
          logInfo(s"Row1: Col2 = $res11 Col4 = $res12 Col6 = $res13")
        case 1 =>
          logInfo(s"Row2: Col2 = $res21 Col4 = $res22 Col6 = $res23")
        case 2 =>
          logInfo(s"Row3: Col2 = $res31 Col4 = $res32 Col6 = $res33")
        case _ =>
      }
      numResults += 1
    }
    assert(numResults == expectedLength,
      s"Expected $expectedLength but got $numResults")

    // also check access to complex types as string
    rs = stmt.executeQuery(
      s"SELECT * FROM $tableName --+ complexTypeAsJson(1)")
    checkComplexTypesAsJson(rs, expectedLength)
    rs = stmt.executeQuery(
      s"SELECT * /*+ complexTypeAsJson( true ) */ FROM $tableName")
    checkComplexTypesAsJson(rs, expectedLength)
  }

  private def checkComplexTypesAsJson(rs: ResultSet,
      expectedLength: Int): Unit = {
    var numResults = 0
    while (rs.next()) {
      // check access to complex types in different ways
      val res11 = rs.getString(2)
      val res12 = rs.getString("col4")
      val res13 = rs.getString(6)

      checkValidJson(res11, res12, res13)

      val res21: Clob = rs.getObject("col2").asInstanceOf[Clob]
      val res22: Clob = rs.getObject("col4").asInstanceOf[Clob]
      val res23: Clob = rs.getObject(6).asInstanceOf[Clob]

      checkValidJson(res21, res22, res23)

      val res31: Clob = rs.getClob("col2")
      val res32: Clob = rs.getClob(4)
      val res33: Clob = rs.getClob("col6")

      checkValidJson(res31, res32, res33)

      numResults match {
        case 0 =>
          logInfo(s"CRow1: Col2 = $res11 Col4 = $res12 Col6 = $res13")
        case 1 =>
          logInfo(s"CRow2: Col2 = $res21 Col4 = $res22 Col6 = $res23")
        case 2 =>
          logInfo(s"CRow3: Col2 = $res31 Col4 = $res32 Col6 = $res33")
        case _ =>
      }

      numResults += 1
    }
    assert(numResults == expectedLength,
      s"Expected $expectedLength but got $numResults")
  }

  private def checkValidJson(cs: Clob*): Unit = {
    for (c <- cs) {
      val s = c.getSubString(1, c.length().asInstanceOf[Int])
      checkValidJsonString(s)
    }
  }

  private def checkValidJson(s: String, ss: String*): Unit = {
    checkValidJsonString(s)
    for (str <- ss) checkValidJsonString(str)
  }

  private def checkValidJsonString(s: String): Unit = {
    logInfo(s"Checking valid JSON for $s")
    assert(s.trim().length() > 0)
    try {
      val parser = new ObjectMapper().getFactory.createParser(s)
      while (parser.nextToken() != null) {
      }
      return
    } catch {
      case e: Exception => throw new AssertionError(
        s"Exception in parsing as JSON: $s", e)
    }
    throw new AssertionError(s"Failed in parsing as JSON: $s")
  }

  def startSparkCluster(productDir: String): Unit = {
    logInfo(s"Starting spark cluster in $productDir/work")
    logInfo((productDir + "/sbin/start-all.sh") !!)
  }

  def stopSparkCluster(productDir: String): Unit = {
    stopSpark()
    logInfo(s"Stopping spark cluster in $productDir/work")
    logInfo((productDir + "/sbin/stop-all.sh") !!)
  }

  def stopSpark(): Unit = {
    logInfo(s" Stopping spark ")
    val sparkContext = SnappyContext.globalSparkContext
    if (sparkContext != null) sparkContext.stop()
  }

  private def runSparkShellSnappyPoolTest(stmt: Statement, sparkShellCommand: String): Unit = {
    // create and populate the tables for the pool driver test
    logInfo(s"About to invoke spark-shell with command: $sparkShellCommand")

    val allOutput = new StringBuilder
    val processLog = ProcessLogger(l => allOutput.append(l), l => allOutput.append(l))
    sparkShellCommand ! processLog
    var output = allOutput.toString()
    logInfo(output)
    output = output.replaceAll("NoSuchObjectException", "NoSuchObject")
    output = output.replaceAll("java.lang.ClassNotFoundException: " +
        "org.apache.spark.sql.internal.SnappyAQPSessionState", "AQP missing")
    assert(!output.contains("Exception"),
      s"Some exception stacktrace seen on spark-shell console: $output")
    assert(!output.contains("Error"), s"Some error seen on spark-shell console: $output")

    // accessing tables created through spark-shell
    val rs1 = stmt.executeQuery("select count(*) from testTable1")
    rs1.next()
    assert(rs1.getInt(1) == 2)

    val rs2 = stmt.executeQuery("select count(*) from testTable2")
    rs2.next()
    assert(rs2.getInt(1) == 2)

    // drop the tables
    stmt.execute("drop table testTable2")
    stmt.execute("drop table testTable1")
  }

  def invokeSparkShell(productDir: String, sparkProductDir: String, locatorClientPort: Int,
      props: Properties = new Properties(), vm: VM = null): Unit = {

    // stop any existing SparkContext, to make sure cpu core available for this test
    if (vm eq null) stopSpark()
    else vm.invoke(classOf[SplitClusterDUnitTest], "stopSpark")

    // perform some operation thru spark-shell
    val jars = Files.newDirectoryStream(Paths.get(s"$productDir/../distributions/"),
      "snappydata-core*.jar")
    var securityConf = ""
    if (props.containsKey(Attribute.USERNAME_ATTR)) {
      securityConf = s" --conf spark.snappydata.store.user=${props.getProperty(Attribute
          .USERNAME_ATTR)}" +
          s" --conf spark.snappydata.store.password=${props.getProperty(Attribute.PASSWORD_ATTR)}"
    }
    val snappyDataCoreJar = jars.iterator().next().toAbsolutePath.toString
    // SparkSqlTestCode.txt file contains the commands executed on spark-shell
    val scriptFile: String = getClass.getResource("/SparkSqlTestCode.txt").getPath
    val scriptFile2: String = getClass.getResource("/SnappySqlPoolTestCode.txt").getPath
    val hostName = InetAddress.getLocalHost.getHostName
    val sparkShellCommand = s"$sparkProductDir/bin/spark-shell --master spark://$hostName:7077" +
        " --conf spark.snappydata.connection=localhost:" + locatorClientPort +
        " --conf spark.sql.catalogImplementation=in-memory" +
        s" --jars $snappyDataCoreJar" +
        securityConf +
        s" -i $scriptFile -i $scriptFile2"

    logInfo(s"Invoking spark-shell: $sparkShellCommand")
    val conn = getConnection(locatorClientPort, props)
    val stmt = conn.createStatement()

    runSparkShellSnappyPoolTest(stmt, sparkShellCommand)

    // accessing tables created through spark-shell
    val rs1 = stmt.executeQuery("select count(*) from coltable")
    rs1.next()
    assert(rs1.getInt(1) == 5)

    val rs2 = stmt.executeQuery("select count(*) from rowtable")
    rs2.next()
    assert(rs2.getInt(1) == 5)

    // drop the tables
    stmt.execute("drop table rowtable")
    stmt.execute("drop table coltable")

    stmt.close()
    conn.close()
  }

  def invokeSparkShellCurrent(productDir: String, sparkOldProductDir: String,
      sparkCurrentProductDir: String, locatorClientPort: Int, props: Properties, vm: VM): Unit = {
    // stop existing spark cluster and start with current Spark version; stop on vm3 to also close
    // any existing SparkContext (subsequent tests will need to recreate the SparkContext)
    if (vm eq null) stopSparkCluster(sparkOldProductDir)
    else vm.invoke(classOf[SplitClusterDUnitTest], "stopSparkCluster", sparkOldProductDir)
    startSparkCluster(sparkCurrentProductDir)
    try {
      // perform some operations through spark-shell using JDBC pool driver API on current Spark
      val jars = Files.newDirectoryStream(Paths.get(s"$productDir/../distributions/"),
        "snappydata-jdbc_*.jar")
      var securityConf = ""
      if (props.containsKey(Attribute.USERNAME_ATTR)) {
        securityConf = s" --conf spark.snappydata.user=" +
            props.getProperty(Attribute.USERNAME_ATTR) +
            s" --conf spark.snappydata.password=${props.getProperty(Attribute.PASSWORD_ATTR)}"
      }
      val snappyJdbcJar = jars.iterator().next().toAbsolutePath.toString
      // SnappySqlPoolTestCode.txt file contains the commands executed on spark-shell
      val scriptFile: String = getClass.getResource("/SnappySqlPoolTestCode.txt").getPath
      val hostName = InetAddress.getLocalHost.getHostName
      val sparkShellCommand = s"$sparkCurrentProductDir/bin/spark-shell " +
          s"--master spark://$hostName:7077 --conf spark.snappydata.connection=localhost:" +
          locatorClientPort + " --conf spark.sql.catalogImplementation=in-memory" +
          s" --jars $snappyJdbcJar" + securityConf + s" -i $scriptFile"

      val conn = getConnection(locatorClientPort, props)
      val stmt = conn.createStatement()

      runSparkShellSnappyPoolTest(stmt, sparkShellCommand)

      stmt.close()
      conn.close()
    } finally {
      stopSparkCluster(sparkCurrentProductDir)
      startSparkCluster(sparkOldProductDir)
    }
  }
}

case class Data2(col1: Int, col2: String, col3: String)

case class Data3(col1: Int, col2: Int, col3: Decimal)

case class ComplexData2(col1: Int, col2: Array[Decimal], col3: String,
    col4: Map[Timestamp, Data3], col5: Double, col6: Data, col7: Decimal,
    col8: Timestamp)

case class ComplexData3(col1: Int, col2: Array[Decimal], col3: String,
    col4: Map[Timestamp, Data], col5: Double, col6: Data2, col7: Decimal,
    col8: Timestamp)
