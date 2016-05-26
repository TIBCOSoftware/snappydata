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

import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.{AnalysisException, SnappyContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Basic tests for non-embedded mode connections to an embedded cluster.
 */
trait SplitClusterDUnitTestBase {

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

    // Embedded Cluster Verifying the Spark Cluster Operations
    testObject.verifySplitModeOperations("column", isComplex = false, props)

    println("Test Completed Successfully")
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

    println("Test Completed Successfully")
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

    println("Test Completed Successfully")
  }

  def testColumnTableCreation(): Unit = {
    doTestColumnTableCreation(skewServerDistribution = false)
  }

  def testRowTableCreation(): Unit = {
    doTestRowTableCreation(skewServerDistribution = false)
  }

  def testComplexTypesForColumnTables_SNAP643(): Unit = {
    doTestComplexTypesForColumnTables_SNAP643(skewServerDistribution = false)
  }
}

trait SplitClusterDUnitTestObject {

  val props = Map.empty[String, String]

  def createTablesAndInsertData(tableType: String): Unit

  def createComplexTablesAndInsertData(props: Map[String, String]): Unit

  def verifySplitModeOperations(tableType: String, isComplex: Boolean,
      props: Map[String, String]): Unit

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
      "Table embeddedModeTable1 already exists.".toLowerCase),
      tableAlreadyExistException.getMessage)

    // select the data from table created in embedded mode
    selectFromTable(snc, "embeddedModeTable1", 1005)

    // drop the table created in embedded mode
    snc.dropTable("embeddedModeTable1", ifExists = true)

    // select the data from table created in embedded mode
    selectFromTable(snc, "embeddedModeTable2", 1005)

    // remove below once SNAP-653 is fixed
    val numPartitions = props.getOrElse("buckets", "113").toInt
    StoreUtils.removeCachedObjects(snc, "SPLITMODETABLE1", numPartitions,
      registerDestroy = true)
    // create a table in split mode
    if (isComplex) {
      createComplexTableUsingDataSourceAPI(snc, "splitModeTable1",
        tableType, props)
    } else {
      createTableUsingDataSourceAPI(snc, "splitModeTable1",
        tableType, props)
    }
    selectFromTable(snc, "splitModeTable1", 1005)

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
      Data(s(0), Integer.toString(s(1)), s(2)))

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
      ts(0) -> Data(3, "8", 1),
      ts(1) -> Data(5, "3", 0),
      ts(2) -> Data(8, "2", 1))
    val m2 = Map(
      ts(3) -> Data(8, "3", 1),
      ts(0) -> Data(7, "5", 7),
      ts(4) -> Data(4, "8", 9))
    val data = ArrayBuffer[ComplexData]()
    data += ComplexData(1, dec1, "3", m2, 7.56, Data(2, "8", 3), dec1(0), ts(0))
    data += ComplexData(7, dec1, "8", m1, 8.45, Data(7, "4", 9), dec2(0), ts(1))
    data += ComplexData(9, dec2, "2", m2, 12.33, Data(3, "1", 7), dec1(1), ts(2))
    data += ComplexData(4, dec2, "2", m1, 92.85, Data(9, "3", 4), dec2(1), ts(3))
    data += ComplexData(5, dec2, "7", m1, 5.28, Data(4, "8", 1), dec2(2), ts(4))
    1 to 1000 foreach { _ =>
      val rnd = Random.nextLong()
      val rnd1 = rnd.asInstanceOf[Int]
      val rnd2 = (rnd >>> 32).asInstanceOf[Int]
      val dec = if ((rnd1 % 2) == 0) dec1 else dec2
      val map = if ((rnd2 % 2) == 0) m1 else m2
      data += ComplexData(rnd1, dec, rnd2.toString,
        map, Random.nextDouble(), Data(rnd1, Integer.toString(rnd2), rnd1),
        dec(1), ts(math.abs(rnd1) % 5))
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

  def startSparkCluster(productDir: String): Unit = {
    println(s"Starting spark cluster in $productDir/work")
    (productDir + "/sbin/start-all.sh") !!
  }

  def stopSparkCluster(productDir: String): Unit = {
    val sparkContext = SnappyContext.globalSparkContext
    println(s"Stopping spark cluster in $productDir/work")
    if(sparkContext != null) sparkContext.stop()
    (productDir + "/sbin/stop-all.sh") !!
  }
}

case class Data(col1: Int, col2: String, col3: Int)

case class ComplexData(col1: Int, col2: Array[Decimal], col3: String,
    col4: Map[Timestamp, Data], col5: Double, col6: Data, col7: Decimal,
    col8: Timestamp)
