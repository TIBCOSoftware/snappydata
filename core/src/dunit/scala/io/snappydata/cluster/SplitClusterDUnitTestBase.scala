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

import java.net.InetAddress
import java.sql.Timestamp
import java.util.Properties

import scala.collection.mutable.ArrayBuffer
import scala.language.postfixOps
import scala.sys.process._
import scala.util.Random

import io.snappydata.test.dunit.VM
import io.snappydata.test.util.TestException
import org.junit.Assert

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.collection.{Utils, WrappedInternalRow}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.{AnalysisException, SnappyContext}
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * Basic tests for non-embedded mode connections to an embedded cluster.
 */
trait SplitClusterDUnitTestBase extends Logging {


  def vm0: VM

  def vm1: VM

  def vm2: VM

  def vm3: VM

  protected def startArgs: Array[AnyRef]

  protected def testObject: SplitClusterDUnitTestObject

  protected def props = testObject.props

  protected def productDir: String

  protected def locatorProperty: String

  protected def startNetworkServers(num: Int): Unit

  protected def batchSize = testObject.batchSize

  def doTestColumnTableCreation(skewServerDistribution: Boolean): Unit = {
    if (skewServerDistribution) {
      startNetworkServers(2)
    } else {
      startNetworkServers(3)
    }

    // Embedded Cluster Operations
    testObject.createTablesAndInsertData("column")

    // StandAlone Spark Cluster Operations
    vm3.invoke(getClass, "verifyEmbeddedTablesAndCreateInSplitMode",
      startArgs :+ "column" :+ Boolean.box(false) :+ props :+ locatorProperty)

    // make sure that table dropped from external cluster is not cached
    // in catalog of embedded mode cluster
    testObject.assertTableNotCachedInHiveCatalog("APP.EMBEDDEDMODETABLE1")

    // Embedded Cluster Verifying the Spark Cluster Operations
    testObject.verifySplitModeOperations("column", isComplex = false, props)

    // make sure that table dropped from embedded cluster is not cached
    // in catalog of external cluster
    vm3.invoke(getClass, "assertTableNotCachedInHiveCatalog", "APP.SPLITMODETABLE1")

    logInfo("Test Completed Successfully")
  }

  def doTestRowTableCreation(skewServerDistribution: Boolean): Unit = {
    if (skewServerDistribution) {
      startNetworkServers(2)
    } else {
      startNetworkServers(3)
    }

    // Embedded Cluster Operations
    testObject.createTablesAndInsertData("row")

    // StandAlone Spark Cluster Operations
    vm3.invoke(getClass, "verifyEmbeddedTablesAndCreateInSplitMode",
      startArgs :+ "row" :+ Boolean.box(false) :+ props :+ locatorProperty)

    // Embedded Cluster Verifying the Spark Cluster Operations
    testObject.verifySplitModeOperations("row", isComplex = false, props)

  }

  def doTestComplexTypesForColumnTables_SNAP643(
      skewServerDistribution: Boolean): Unit = {
    if (skewServerDistribution) {
      startNetworkServers(2)
    } else {
      startNetworkServers(3)
    }

    // Embedded Cluster Operations
    val props = Map("buckets" -> "7")
    testObject.createComplexTablesAndInsertData(props)

    // StandAlone Spark Cluster Operations
    vm3.invoke(getClass, "verifyEmbeddedTablesAndCreateInSplitMode",
      startArgs :+ "column" :+ Boolean.box(true) :+ props :+ locatorProperty)

    // Embedded Cluster Verifying the Spark Cluster Operations
    testObject.verifySplitModeOperations("column", isComplex = true, props)
  }

  protected def skewNetworkServers: Boolean = false

  final def testColumnTableCreation(): Unit = {
    doTestColumnTableCreation(skewNetworkServers)
  }

  final def testRowTableCreation(): Unit = {
    doTestRowTableCreation(skewNetworkServers)
  }

  final def testComplexTypesForColumnTables_SNAP643(): Unit = {
    doTestComplexTypesForColumnTables_SNAP643(skewNetworkServers)
  }
}

trait SplitClusterDUnitTestObject extends Logging {

  val props = Map.empty[String, String]

  val batchSize = 100

  def createTablesAndInsertData(tableType: String): Unit

  def createComplexTablesAndInsertData(props: Map[String, String]): Unit

  def verifySplitModeOperations(tableType: String, isComplex: Boolean,
      props: Map[String, String]): Unit

  def assertTableNotCachedInHiveCatalog(tableName: String): Unit

  def verifyEmbeddedTablesAndCreateInSplitMode(locatorPort: Int,
      prop: Properties, tableType: String, isComplex: Boolean,
      props: Map[String, String], locatorProp: String): Unit = {

    // Test setting locators property via environment variable.
    // Also enables checking for "spark." or "snappydata." prefix in key.
    System.setProperty(locatorProp, s"localhost:$locatorPort")
    val hostName = InetAddress.getLocalHost.getHostName
    val conf = new SparkConf()
        .setAppName("test Application")
        .setMaster(s"spark://$hostName:7077")
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
      "already exists.".toLowerCase), tableAlreadyExistException.getMessage)

    // select the data from table created in embedded mode
    selectFromTable(snc, "embeddedModeTable1", 1005)

    // drop the table created in embedded mode
    snc.dropTable("embeddedModeTable1", ifExists = true)

    // select the data from table created in embedded mode
    selectFromTable(snc, "embeddedModeTable2", 1005)

    var expected = Seq.empty[ComplexData]
    // create a table in split mode
    if (isComplex) {
      expected = createComplexTableUsingDataSourceAPI(snc, "splitModeTable1",
        tableType, props)
    } else {
      createTableUsingDataSourceAPI(snc, "splitModeTable1",
        tableType, props)
    }

    selectFromTable(snc, "splitModeTable1", 1005, expected)

    logInfo("Successful")
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
      Data(s(0), Integer.toString(s(1)),
        Decimal(s(2).toString + '.' + math.abs(s(0)))))

    val dataDF = snc.createDataFrame(rdd)

    snc.createTable(tableName, tableType, dataDF.schema, propsMap)
    dataDF.write.insertInto(tableName)
  }

  def selectFromTable(snc: SnappyContext, tableName: String,
      expectedLength: Int,
      expected: Seq[ComplexData] = Seq.empty[ComplexData]): Unit = {
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect()
    assert(r.length == expectedLength,
      s"Expected $expectedLength but got ${r.length}")
    if (expected.nonEmpty) {
      // check the data equivalence
      val schema = result.schema
      val converter = Utils.createCatalystConverter(schema)
      val expectedMap = expected.map(d => (d.col1, new WrappedInternalRow(
        converter(d).asInstanceOf[InternalRow], schema, None))).toMap
      r.foreach { row =>
        Assert.assertEquals(expectedMap(row.getInt(0)), row)
      }
    }
  }

  def createComplexTableUsingDataSourceAPI(snc: SnappyContext,
      tableName: String, tableType: String,
      props: Map[String, String]): ArrayBuffer[ComplexData] = {
    val keys = new OpenHashSet[Int](1005)
    val context = snc.sparkContext
    val dec1 = Array(Decimal("4.92"), Decimal("51.98"))
    val dec2 = Array(Decimal("95.27"), Decimal("17.25"), Decimal("7583.2956"))
    val time = System.currentTimeMillis()
    val ts = Array(new Timestamp(time), new Timestamp(time + 123456L),
      new Timestamp(0L), new Timestamp(time - 12246L), new Timestamp(-1L))
    val m1 = Map(
      ts(0) -> Data(3, "8", Decimal("1.8")),
      ts(1) -> Data(5, "3", Decimal("0.3")),
      ts(2) -> Data(8, "2", Decimal("2.1")))
    val m2 = Map(
      ts(3) -> Data(8, "3", Decimal("8.0")),
      ts(0) -> Data(7, "5", Decimal("7.5")),
      ts(4) -> Data(4, "8", Decimal("4.9")))
    val data = ArrayBuffer[ComplexData]()
    data += ComplexData(1, dec1, "3", m2, 7.56, Data(2, "8", Decimal("3.2")),
      dec1(0), ts(0))
    data += ComplexData(7, dec1, "8", m1, 8.45, Data(7, "4", Decimal("4.9")),
      dec2(0), ts(1))
    data += ComplexData(9, dec2, "2", m2, 12.33, Data(3, "1", Decimal("1.7")),
      dec1(1), ts(2))
    data += ComplexData(4, dec2, "2", m1, 92.85, Data(9, "3", Decimal("9.4")),
      dec2(1), ts(3))
    data += ComplexData(5, dec2, "7", m1, 5.28, Data(4, "8", Decimal("1.8")),
      dec2(2), ts(4))
    for (i <- 1 to 1000) {
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
            drnd2)), dec(1), ts(math.abs(rnd1) % 5))
    }
    val rdd = context.parallelize(data, data.length)
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable(tableName, tableType, dataDF.schema, props)
    dataDF.write.insertInto(tableName)

    data
  }

  def getEnvironmentVariable(env: String): String = {
    val value = scala.util.Properties.envOrElse(env, null)
    if (env == null) {
      throw new TestException(s"Environment variable $env is not defined")
    }
    value
  }

  def startSparkCluster(productDir: String): Unit = {
    logInfo(s"Starting spark cluster in $productDir/work")
    (productDir + "/sbin/start-all.sh") !!
  }

  def stopSparkCluster(productDir: String): Unit = {
    val sparkContext = SnappyContext.globalSparkContext
    logInfo(s"Stopping spark cluster in $productDir/work")
    if (sparkContext != null) sparkContext.stop()
    (productDir + "/sbin/stop-all.sh") !!
  }
}

case class IndexData(col1: Int, col2: Int, col3: Decimal)

case class Data(col1: Int, col2: String, col3: Decimal)

case class ComplexData(col1: Int, col2: Array[Decimal], col3: String,
    col4: Map[Timestamp, Data], col5: Double, col6: Data, col7: Decimal,
    col8: Timestamp)
