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
package io.snappydata.dunit.externalstore

import java.net.InetAddress
import java.sql.Timestamp
import java.util.Properties

import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps
import scala.sys.process._
import scala.util.Random

import io.snappydata.dunit.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.AvailablePortHelper
import io.snappydata.test.util.TestException

import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.{AnalysisException, SnappyContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Basic tests for non-embedded mode connections to an embedded cluster.
 */
class ExternalShellDUnitTest(s: String)
    extends ClusterManagerTestBase(s) with Serializable {

  import ExternalShellDUnitTest._

  override val locatorNetPort = AvailablePortHelper.getRandomAvailableTCPPort

  override def beforeClass(): Unit = {
    super.beforeClass()
    vm3.invoke(this.getClass, "startSparkCluster")
  }

  override def afterClass(): Unit = {
    super.afterClass()
    vm3.invoke(this.getClass, "stopSparkCluster")
  }

  def doTestColumnTableCreation(skewTaskDistribution : Boolean = false): Unit = {
    vm0.invoke(classOf[ClusterManagerTestBase], "startNetServer",
      AvailablePortHelper.getRandomAvailableTCPPort)
    vm1.invoke(classOf[ClusterManagerTestBase], "startNetServer",
      AvailablePortHelper.getRandomAvailableTCPPort)
    if (!skewTaskDistribution) {
      vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer",
        AvailablePortHelper.getRandomAvailableTCPPort)
    }
    StoreUtils.skewTaskDistributionForTests = skewTaskDistribution

    // Embedded Cluster Operations
    createTablesAndInsertData("column")

    // StandAlone Spark Cluster Operations
    vm3.invoke(this.getClass, "verifyEmbeddedTablesAndCreateNewInShell",
      startArgs :+ "column" :+ Boolean.box(false) :+ props)

    // Embedded Cluster Verifying the Spark Cluster Operations
    verifyShellModeOperations("column", isComplex = false, props)

    println("Test Completed Successfully")
  }

  def testColumnTableCreation(): Unit = {
    doTestColumnTableCreation()
  }

  def testRemoteIteratorSNAP652(): Unit = {
    doTestColumnTableCreation(true)
  }

  def testRowTableCreation(): Unit = {
    vm1.invoke(classOf[ClusterManagerTestBase], "startNetServer",
      AvailablePortHelper.getRandomAvailableTCPPort)
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer",
      AvailablePortHelper.getRandomAvailableTCPPort)

    // Embedded Cluster Operations
    createTablesAndInsertData("row")

    // StandAlone Spark Cluster Operations
    vm3.invoke(this.getClass, "verifyEmbeddedTablesAndCreateNewInShell",
      startArgs :+ "row" :+ Boolean.box(false) :+ props)

    // Embedded Cluster Verifying the Spark Cluster Operations
    verifyShellModeOperations("row", isComplex = false, props)

    println("Test Completed Successfully")
  }

  def testComplexTypesForColumnTables_SNAP643(): Unit = {
    vm0.invoke(classOf[ClusterManagerTestBase], "startNetServer",
      AvailablePortHelper.getRandomAvailableTCPPort)
    vm1.invoke(classOf[ClusterManagerTestBase], "startNetServer",
      AvailablePortHelper.getRandomAvailableTCPPort)
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer",
      AvailablePortHelper.getRandomAvailableTCPPort)

    // Embedded Cluster Operations
    val snc = SnappyContext(sc)

    val props = Map("buckets" -> "7")
    createComplexTableUsingDataSourceAPI(snc, "embeddedModeTable1",
      "column", props)
    selectFromTable(snc, "embeddedModeTable1", 1005)

    createComplexTableUsingDataSourceAPI(snc, "embeddedModeTable2",
      "column", props)
    selectFromTable(snc, "embeddedModeTable2", 1005)

    println("Successful")

    // StandAlone Spark Cluster Operations
    vm3.invoke(this.getClass, "verifyEmbeddedTablesAndCreateNewInShell",
      startArgs :+ "column" :+ Boolean.box(true) :+ props)

    // Embedded Cluster Verifying the Spark Cluster Operations
    verifyShellModeOperations("column", isComplex = true, props)

    println("Test Completed Successfully")
  }
}

object ExternalShellDUnitTest {

  def sc = ClusterManagerTestBase.sc

  val props = Map.empty[String, String]

  def createTablesAndInsertData(tableType: String): Unit = {
    val snc = SnappyContext(sc)

    createTableUsingDataSourceAPI(snc, "embeddedModeTable1", tableType)
    selectFromTable(snc, "embeddedModeTable1", 1005)

    createTableUsingDataSourceAPI(snc, "embeddedModeTable2", tableType)
    selectFromTable(snc, "embeddedModeTable2", 1005)

    println("Successful")
  }

  def verifyShellModeOperations(tableType: String, isComplex: Boolean,
      props: Map[String, String]): Unit = {
    // embeddedModeTable1 is dropped in shell mode. recreate it
    val snc = SnappyContext(sc)
    // remove below once SNAP-653 is fixed
    val numPartitions = props.getOrElse("buckets", "113").toInt
    StoreUtils.removeCachedObjects(snc, "EMBEDDEDMODETABLE1", numPartitions,
      registerDestroy = true)
    if (isComplex) {
      createComplexTableUsingDataSourceAPI(snc, "embeddedModeTable1",
        tableType, props)
    } else {
      createTableUsingDataSourceAPI(snc, "embeddedModeTable1",
        tableType, props)
    }
    selectFromTable(snc, "embeddedModeTable1", 1005)

    snc.dropTable("embeddedModeTable1", ifExists = true)

    // embeddedModeTable2 still exists drop it
    snc.dropTable("embeddedModeTable2", ifExists = true)

    // read data from shellModeTable1
    selectFromTable(snc, "shellModeTable1", 1005)

    // drop table created in shell mode
    snc.dropTable("shellModeTable1", ifExists = true)

    // recreate the dropped table
    if (isComplex) {
      createComplexTableUsingDataSourceAPI(snc, "shellModeTable1",
        tableType, props)
    } else {
      createTableUsingDataSourceAPI(snc, "shellModeTable1",
        tableType, props)
    }
    selectFromTable(snc, "shellModeTable1", 1005)
    snc.dropTable("shellModeTable1", ifExists = true)

    println("Successful")
  }

  def verifyEmbeddedTablesAndCreateNewInShell(locatorPort: Int,
      prop: Properties, tableType: String, isComplex: Boolean,
      props: Map[String, String]): Unit = {

    val hostName = InetAddress.getLocalHost.getHostName
    val conf = new SparkConf()
        .setAppName("test Application")
        .setMaster(s"spark://$hostName:7077")
        .set("snappydata.store.locators", s"localhost:$locatorPort")
        .set("spark.executor.extraClassPath",
          getEnvironmentVariable("SNAPPY_DIST_CLASSPATH"))

    val sc = SparkContext.getOrCreate(conf)
    val snc = SnappyContext(sc)

    // try to create the table already created in embedded mode.
    // it should throw the table exist exception.
    var tableAlreadyExistException: Exception = null
    try {
      if (isComplex) {
        createComplexTableUsingDataSourceAPI(snc, "embeddedModeTable1",
          tableType, props)
      } else {
        createTableUsingDataSourceAPI(snc, "embeddedModeTable1",
          tableType, props)
      }
    } catch {
      case e: AnalysisException => tableAlreadyExistException = e
    }
    assert(tableAlreadyExistException != null)
    assert(tableAlreadyExistException.getMessage.toLowerCase.contains(
      "Table embeddedModeTable1 already exists.".toLowerCase))

    // select the data from table created in embedded mode
    selectFromTable(snc, "embeddedModeTable1", 1005)

    // drop the table created in embedded mode
    snc.dropTable("embeddedModeTable1", ifExists = true)

    // select the data from table created in embedded mode
    selectFromTable(snc, "embeddedModeTable2", 1005)

    // remove below once SNAP-653 is fixed
    val numPartitions = props.getOrElse("buckets", "113").toInt
    StoreUtils.removeCachedObjects(snc, "SHELLMODETABLE1", numPartitions,
      registerDestroy = true)
    // create a table in shell mode
    if (isComplex) {
      createComplexTableUsingDataSourceAPI(snc, "shellModeTable1",
        tableType, props)
    } else {
      createTableUsingDataSourceAPI(snc, "shellModeTable1",
        tableType, props)
    }
    selectFromTable(snc, "shellModeTable1", 1005)

    println("Successful")
  }

  def createTableUsingDataSourceAPI(snc: SnappyContext,
      tableName: String, tableType: String,
      propsMap: Map[String, String] = props): Unit = {
    val context = snc.sparkContext
    val data = ArrayBuffer(Array(1, 2, 3), Array(7, 8, 9), Array(9, 2, 3),
      Array(4, 2, 3), Array(5, 6, 7))
    1 to 1000 foreach { _ =>
      data += Array.fill(3)(Random.nextInt())
    }
    val rdd = context.parallelize(data, data.length).map(s =>
      Data(s(0), s(1), s(2)))

    val dataDF = snc.createDataFrame(rdd)

    snc.createTable(tableName, tableType, dataDF.schema, propsMap)
    dataDF.write.insertInto(tableName)
  }

  def selectFromTable(snc: SnappyContext, tableName: String,
      expectedLength: Int): Unit = {
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect()
    assert(r.length == expectedLength,
      s"Expected $expectedLength but got ${r.length}")
  }

  def createComplexTableUsingDataSourceAPI(snc: SnappyContext,
      tableName: String, tableType: String,
      props: Map[String, String]): Unit = {
    val context = snc.sparkContext
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
    1 to 1000 foreach { _ =>
      val rnd = Random.nextLong()
      val rnd1 = rnd.asInstanceOf[Int]
      val rnd2 = (rnd >>> 32).asInstanceOf[Int]
      val dec = if ((rnd1 % 2) == 0) dec1 else dec2
      val map = if ((rnd2 % 2) == 0) m1 else m2
      data += ComplexData(rnd1, dec, rnd2.toString,
        map, Random.nextDouble(), Data(rnd1, rnd2, rnd1), dec(1),
        ts(math.abs(rnd1) % 5))
    }
    val rdd = context.parallelize(data, data.length)
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable(tableName, tableType, dataDF.schema, props)
    dataDF.write.insertInto(tableName)
  }

  def getEnvironmentVariable(env: String): String = {
    val value = scala.util.Properties.envOrElse(env, null)
    if (env == null) {
      throw new TestException(s" Environment variable $env is not defined")
    }
    value
  }

  def startSparkCluster(): Unit = {
    (getEnvironmentVariable("SNAPPY_HOME") + "/sbin/start-all.sh") !!
  }

  def stopSparkCluster(): Unit = {
    SnappyContext.stop()
    (getEnvironmentVariable("SNAPPY_HOME") + "/sbin/stop-all.sh") !!
  }
}

case class ComplexData(col1: Int, col2: Array[Decimal], col3: String,
    col4: Map[Timestamp, Data], col5: Double, col6: Data, col7: Decimal,
    col8: Timestamp)
