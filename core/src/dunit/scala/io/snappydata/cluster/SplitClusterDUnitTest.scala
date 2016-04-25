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
package io.snappydata.cluster

import java.io.PrintWriter
import java.sql.{Connection, DriverManager, Statement, Timestamp}
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps
import scala.sys.process._
import scala.util.Random

import com.pivotal.gemfirexd.snappy.ComplexTypeSerializer
import io.snappydata.Constant
import io.snappydata.test.dunit.{AvailablePortHelper, DistributedTestBase, Host, VM}

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.{Decimal, StructType}

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

  override def startArgs = Array(SplitClusterDUnitTest.locatorPort,
    bootProps).asInstanceOf[Array[AnyRef]]

  private val snappyProductDir =
    testObject.getEnvironmentVariable("SNAPPY_HOME")

  override protected val productDir =
    testObject.getEnvironmentVariable("APACHE_SPARK_HOME")

  // see comments in SNAP-606 about Apache Spark filtering out "spark.*"
  // properties for this, so fails with just "snappydata." prefix
  override protected val locatorProperty = "spark.snappydata.store.locators"

  override def beforeClass(): Unit = {
    super.beforeClass()

    println(s"Starting snappy cluster in $snappyProductDir/work")
    // create locators, leads and servers files
    val port = SplitClusterDUnitTest.locatorPort
    val netPort = SplitClusterDUnitTest.locatorNetPort
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    val netPort2 = AvailablePortHelper.getRandomAvailableTCPPort
    val netPort3 = AvailablePortHelper.getRandomAvailableTCPPort
    val confDir = s"$snappyProductDir/conf"
    writeToFile(s"localhost  -peer-discovery-port=$port -client-port=$netPort",
      s"$confDir/locators")
    writeToFile(s"localhost  -locators=localhost[$port] -client-port=$netPort1",
      s"$confDir/leads")
    writeToFile(s"""localhost  -locators=localhost[$port] -client-port=$netPort2
                   |localhost  -locators=localhost[$port] -client-port=$netPort3
                   |""".stripMargin, s"$confDir/servers")
    (snappyProductDir + "/sbin/snappy-start-all.sh").!!

    vm3.invoke(getClass, "startSparkCluster", productDir)
  }

  override def afterClass(): Unit = {
    super.afterClass()
    vm3.invoke(getClass, "stopSparkCluster", productDir)

    println(s"Stopping snappy cluster in $snappyProductDir/work")
    (snappyProductDir + "/sbin/snappy-stop-all.sh").!!
    //Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "locators"))
    //Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "leads"))
    //Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "servers"))
  }

  private def writeToFile(str: String, fileName: String): Unit = {
    val pw = new PrintWriter(fileName)
    try {
      pw.write(str)
    } finally {
      pw.close()
    }
  }

  override protected def startNetworkServers(num: Int): Unit = {
    // no change to network servers at runtime in this mode
  }

  override protected def testObject = SplitClusterDUnitTest
}

object SplitClusterDUnitTest extends SplitClusterDUnitTestObject {

  private val locatorPort = AvailablePortHelper.getRandomAvailableTCPPort
  private val locatorNetPort = AvailablePortHelper.getRandomAvailableTCPPort

  private def getConnection(netPort: Int) = DriverManager.getConnection(
    s"jdbc:${Constant.JDBC_URL_PREFIX}localhost:$netPort")

  override def createTablesAndInsertData(tableType: String): Unit = {
    val conn = getConnection(locatorNetPort)
    val stmt = conn.createStatement()

    createTableUsingJDBC("embeddedModeTable1", tableType, conn, stmt)
    selectFromTableUsingJDBC("embeddedModeTable1", 1005, stmt)

    createTableUsingJDBC("embeddedModeTable2", tableType, conn, stmt)
    selectFromTableUsingJDBC("embeddedModeTable2", 1005, stmt)

    stmt.close()
    conn.close()
    println("Successful")
  }

  override def createComplexTablesAndInsertData(
      props: Map[String, String]): Unit = {
    val conn = getConnection(locatorNetPort)
    val stmt = conn.createStatement()

    createComplexTableUsingJDBC("embeddedModeTable1", conn, stmt, props)
    selectFromTableUsingJDBC("embeddedModeTable1", 1005, stmt)

    createComplexTableUsingJDBC("embeddedModeTable2", conn, stmt, props)
    selectFromTableUsingJDBC("embeddedModeTable2", 1005, stmt)

    stmt.close()
    conn.close()
    println("Successful")
  }

  override def verifySplitModeOperations(tableType: String, isComplex: Boolean,
      props: Map[String, String]): Unit = {
    val conn = getConnection(locatorNetPort)
    val stmt = conn.createStatement()

    // embeddedModeTable1 is dropped in split mode. recreate it
    /*
    // remove below once SNAP-653 is fixed
    val numPartitions = props.getOrElse("buckets", "113").toInt
    StoreUtils.removeCachedObjects(snc, "EMBEDDEDMODETABLE1", numPartitions,
      registerDestroy = true)
    */
    if (isComplex) {
      createComplexTableUsingJDBC("embeddedModeTable1", conn, stmt, props)
    } else {
      createTableUsingJDBC("embeddedModeTable1", tableType, conn, stmt, props)
    }
    selectFromTableUsingJDBC("embeddedModeTable1", 1005, stmt)

    stmt.execute("drop table if exists embeddedModeTable1")

    // embeddedModeTable2 still exists drop it
    stmt.execute("drop table if exists embeddedModeTable2")

    // read data from splitModeTable1
    selectFromTableUsingJDBC("splitModeTable1", 1005, stmt)

    // drop table created in split mode
    stmt.execute("drop table if exists splitModeTable1")

    // recreate the dropped table
    if (isComplex) {
      createComplexTableUsingJDBC("splitModeTable1", conn, stmt, props)
    } else {
      createTableUsingJDBC("splitModeTable1", tableType, conn, stmt, props)
    }
    selectFromTableUsingJDBC("splitModeTable1", 1005, stmt)
    stmt.execute("drop table if exists splitModeTable1")

    stmt.close()
    conn.close()
    println("Successful")
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
      propsMap: Map[String, String] = props): Unit = {
    val data = ArrayBuffer(Data(1, 2, 3), Data(7, 8, 9), Data(9, 2, 3),
      Data(4, 2, 3), Data(5, 6, 7))
    for (i <- 1 to 1000) {
      data += Data(Random.nextInt(), Random.nextInt(), Random.nextInt())
    }

    stmt.execute(s"""
        CREATE TABLE $tableName (
          col1 Int, col2 Int, col3 Int
        ) USING $tableType${getPropertiesAsSQLString(propsMap)}""")

    val pstmt = conn.prepareStatement(
      s"insert into $tableName values (?, ?, ?)")
    for (d <- data) {
      pstmt.setInt(1, d.col1)
      pstmt.setInt(2, d.col2)
      pstmt.setInt(3, d.col3)
      pstmt.addBatch()
    }
    pstmt.executeBatch()
    pstmt.close()
  }

  def createComplexTableUsingJDBC(tableName: String, conn: Connection,
      stmt: Statement, propsMap: Map[String, String] = props): Unit = {
    val dec1 = Array(Decimal("4.92"), Decimal("51.98"))
    val dec2 = Array(Decimal("95.27"), Decimal("17.25"), Decimal("7583.2956"))
    val time = System.currentTimeMillis()
    val ts = Array(new Timestamp(time), new Timestamp(time + 123456L),
      new Timestamp(0L), new Timestamp(time - 12246L), new Timestamp(-1L))
    val m1 = Map(
      ts(0) -> Data(3, 8, 1),
      ts(1) -> Data(5, 3, 0),
      ts(2) -> Data(8, 2, 1))
    val m2 = Map(
      ts(3) -> Data(8, 3, 1),
      ts(0) -> Data(7, 5, 7),
      ts(4) -> Data(4, 8, 9))
    val data = ArrayBuffer[ComplexData]()
    data += ComplexData(1, dec1, "3", m2, 7.56, Data(2, 8, 3), dec1(0), ts(0))
    data += ComplexData(7, dec1, "8", m1, 8.45, Data(7, 4, 9), dec2(0), ts(1))
    data += ComplexData(9, dec2, "2", m2, 12.33, Data(3, 1, 7), dec1(1), ts(2))
    data += ComplexData(4, dec2, "2", m1, 92.85, Data(9, 3, 4), dec2(1), ts(3))
    data += ComplexData(5, dec2, "7", m1, 5.28, Data(4, 8, 1), dec2(2), ts(4))
    for (i <- 1 to 1000) {
      val rnd = Random.nextLong()
      val rnd1 = rnd.asInstanceOf[Int]
      val rnd2 = (rnd >>> 32).asInstanceOf[Int]
      val dec = if ((rnd1 % 2) == 0) dec1 else dec2
      val map = if ((rnd2 % 2) == 0) m1 else m2
      data += ComplexData(rnd1, dec, rnd2.toString,
        map, Random.nextDouble(), Data(rnd1, rnd2, rnd1), dec(1),
        ts(math.abs(rnd1) % 5))
    }
    stmt.execute(s"""
        CREATE TABLE $tableName (
          col1 Int,
          col2 Array<Decimal>,
          col3 String,
          col4 Map<Timestamp, Struct<col1: Int, col2: Int>>,
          col5 Double,
          col6 Struct<col1: Int, col2: Int>,
          col7 Decimal,
          col8 Timestamp
        ) USING column${getPropertiesAsSQLString(propsMap)}""")

    val schema = ScalaReflection.schemaFor[ComplexData].dataType
        .asInstanceOf[StructType]
    val pstmt = conn.prepareStatement(
      s"insert into $tableName values (?, ?, ?, ?, ?, ?, ?, ?)")
    val serializer1 = ComplexTypeSerializer.create(schema(1))
    val serializer2 = ComplexTypeSerializer.create(schema(3))
    val serializer3 = ComplexTypeSerializer.create(schema(5))
    for (d <- data) {
      pstmt.setInt(1, d.col1)
      pstmt.setBytes(2, serializer1.getBytes(d.col2))
      pstmt.setString(3, d.col3)
      pstmt.setBytes(4, serializer2.getBytes(d.col4))
      pstmt.setDouble(5, d.col5)
      // test with Product, Array, int[], Seq and Collection
      Random.nextInt(5) match {
        case 0 => pstmt.setBytes(6, serializer3.getBytes(d.col6))
        case 1 => pstmt.setBytes(6, serializer3.getBytes(
          d.col6.productIterator.toArray))
        case 2 => pstmt.setBytes(6, serializer3.getBytes(
          Array(d.col6.col1, d.col6.col2, d.col6.col3)))
        case 3 => pstmt.setBytes(6, serializer3.getBytes(
          d.col6.productIterator.toSeq))
        case 4 => pstmt.setBytes(6, serializer3.getBytes(
          d.col6.productIterator.toSeq.asJava))
      }
      pstmt.setBigDecimal(7, d.col7.toJavaBigDecimal)
      pstmt.setTimestamp(8, d.col8)
      pstmt.addBatch()
    }
    pstmt.executeBatch()
    pstmt.close()
  }

  def selectFromTableUsingJDBC(tableName: String,
      expectedLength: Int, stmt: Statement): Unit = {
    val rs = stmt.executeQuery(s"SELECT * FROM $tableName")
    var numResults = 0
    while (rs.next()) numResults += 1
    assert(numResults == expectedLength,
      s"Expected $expectedLength but got $numResults")
  }
}
