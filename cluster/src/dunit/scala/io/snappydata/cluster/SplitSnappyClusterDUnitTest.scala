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
import java.util.Properties

import scala.language.postfixOps

import io.snappydata.core.TestData2
import io.snappydata.store.ClusterSnappyJoinSuite
import io.snappydata.test.dunit.AvailablePortHelper

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{SaveMode, SnappyContext}
import org.apache.spark.sql.store.StoreUtils

/**
 * Basic tests for non-embedded mode connections to an embedded cluster.
 */
class SplitSnappyClusterDUnitTest(s: String)
    extends ClusterManagerTestBase(s)
    with SplitClusterDUnitTestBase
    with Serializable {

  override val locatorNetPort = AvailablePortHelper.getRandomAvailableTCPPort

  override protected val productDir =
    testObject.getEnvironmentVariable("SNAPPY_HOME")

  override protected val locatorProperty = "snappydata.store.locators"

  override def beforeClass(): Unit = {
    super.beforeClass()
    vm3.invoke(getClass, "startSparkCluster", productDir)
  }

  override def afterClass(): Unit = {
    super.afterClass()
    vm3.invoke(getClass, "stopSparkCluster", productDir)
  }

  override protected def startNetworkServers(num: Int): Unit = {
    if (num > 3 || num < 1) {
      throw new IllegalArgumentException(
        s"unexpected number of network servers to start: $num")
    }
    vm0.invoke(classOf[ClusterManagerTestBase], "startNetServer",
      AvailablePortHelper.getRandomAvailableTCPPort)
    if (num > 1) {
      vm1.invoke(classOf[ClusterManagerTestBase], "startNetServer",
        AvailablePortHelper.getRandomAvailableTCPPort)
    }
    if (num > 2) {
      vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer",
        AvailablePortHelper.getRandomAvailableTCPPort)
    }
  }

  override protected def testObject = SplitSnappyClusterDUnitTest


  override def testColumnTableCreation(): Unit = {
    // skip the non-skewed test since it is already run in Spark+Snappy mode
    doTestColumnTableCreation(skewServerDistribution = true)
  }

  override def testRowTableCreation(): Unit = {
    doTestRowTableCreation(skewServerDistribution = true)
  }

  override def testComplexTypesForColumnTables_SNAP643(): Unit = {
    doTestComplexTypesForColumnTables_SNAP643(skewServerDistribution = true)
  }


  def testCollocatedJoinInSplitModeRowTable(): Unit = {
    startNetworkServers(3)
    testObject.createRowTableForCollocatedJoin()
    vm3.invoke(getClass, "checkCollocatedJoins", startArgs :+ locatorProperty :+ "PR_TABLE1" :+
        "PR_TABLE2")
  }

  def testCollocatedJoinInSplitModeColumnTable(): Unit = {
    startNetworkServers(3)
    testObject.createColumnTableForCollocatedJoin()
    vm3.invoke(getClass, "checkCollocatedJoins", startArgs :+ locatorProperty :+ "PR_TABLE3" :+
        "PR_TABLE4")
  }
}

object SplitSnappyClusterDUnitTest extends SplitClusterDUnitTestObject {

  def sc = ClusterManagerTestBase.sc

  override def createTablesAndInsertData(tableType: String): Unit = {
    val snc = SnappyContext(sc)

    createTableUsingDataSourceAPI(snc, "embeddedModeTable1", tableType)
    selectFromTable(snc, "embeddedModeTable1", 1005)

    createTableUsingDataSourceAPI(snc, "embeddedModeTable2", tableType)
    selectFromTable(snc, "embeddedModeTable2", 1005)

    println("Successful")
  }

  override def createComplexTablesAndInsertData(
      props: Map[String, String]): Unit = {
    val snc = SnappyContext(sc)

    createComplexTableUsingDataSourceAPI(snc, "embeddedModeTable1",
      "column", props)
    selectFromTable(snc, "embeddedModeTable1", 1005)

    createComplexTableUsingDataSourceAPI(snc, "embeddedModeTable2",
      "column", props)
    selectFromTable(snc, "embeddedModeTable2", 1005)

    println("Successful")
  }

  override def verifySplitModeOperations(tableType: String, isComplex: Boolean,
      props: Map[String, String]): Unit = {
    // embeddedModeTable1 is dropped in split mode. recreate it
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

    // read data from splitModeTable1
    selectFromTable(snc, "splitModeTable1", 1005)

    // drop table created in split mode
    snc.dropTable("splitModeTable1", ifExists = true)

    // recreate the dropped table
    if (isComplex) {
      createComplexTableUsingDataSourceAPI(snc, "splitModeTable1",
        tableType, props)
    } else {
      createTableUsingDataSourceAPI(snc, "splitModeTable1",
        tableType, props)
    }
    selectFromTable(snc, "splitModeTable1", 1005)
    snc.dropTable("splitModeTable1", ifExists = true)

    println("Successful")
  }

  def createRowTableForCollocatedJoin(): Unit = {

    val snc = SnappyContext(sc)
    val dimension1 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 10 + 1)))
    val refDf = snc.createDataFrame(dimension1)
    snc.sql("DROP TABLE IF EXISTS PR_TABLE1")

    snc.sql("CREATE TABLE PR_TABLE1(OrderId INT NOT NULL,description String, " +
        "OrderRef INT) USING row " +
        "options (" +
        "PARTITION_BY 'OrderId, OrderRef')")

    refDf.write.insertInto("PR_TABLE1")

    snc.sql("DROP TABLE IF EXISTS PR_TABLE2")

    snc.sql("CREATE TABLE PR_TABLE2(OrderId INT NOT NULL,description String, " +
        "OrderRef INT) USING row options (" +
        "PARTITION_BY 'OrderId,OrderRef'," +
        "COLOCATE_WITH 'PR_TABLE1')")

    val dimension2 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 5 + 1)))

    val dimensionDf = snc.createDataFrame(dimension2)
    dimensionDf.write.insertInto("PR_TABLE2")


  }

  def createColumnTableForCollocatedJoin(): Unit = {

    val snc = SnappyContext(sc)
    val dimension1 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 10 + 1)))
    val refDf = snc.createDataFrame(dimension1)
    snc.sql("DROP TABLE IF EXISTS PR_TABLE3")

    snc.sql("CREATE TABLE PR_TABLE3(OrderId INT, description String, " +
        "OrderRef INT) USING column " +
        "options (" +
        "PARTITION_BY 'OrderId,OrderRef')")

    refDf.write.format("column").mode(SaveMode.Append).options(props)
        .saveAsTable("PR_TABLE3")

    val countdf = snc.sql("select * from PR_TABLE3")
    assert(countdf.count() == 1000)

    snc.sql("DROP TABLE IF EXISTS PR_TABLE4")

    snc.sql("CREATE TABLE PR_TABLE4(OrderId INT ,description String, " +
        "OrderRef INT) USING column options (" +
        "PARTITION_BY 'OrderId,OrderRef'," +
        "COLOCATE_WITH 'PR_TABLE3')")

    val dimension2 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 5 + 1)))

    val dimensionDf = snc.createDataFrame(dimension2)
    dimensionDf.write.insertInto("PR_TABLE4")
    val countdf1 = snc.sql("select * from PR_TABLE4")
    assert(countdf1.count() == 1000)


  }


  def checkCollocatedJoins(locatorPort: Int, prop: Properties, locatorProp: String, table1 :
  String, table2 : String): Unit ={
    // Test setting locators property via environment variable.
    // Also enables checking for "spark." or "snappydata." prefix in key.
    System.setProperty(locatorProp, s"localhost:$locatorPort")
    val hostName = InetAddress.getLocalHost.getHostName
    val conf = new SparkConf()
        .setAppName("test Application")
        .setMaster(s"spark://$hostName:7077")
        .set("spark.executor.extraClassPath",
          getEnvironmentVariable("SNAPPY_DIST_CLASSPATH"))
        .set("spark.testing.reservedMemory", "0")

    val sc = SparkContext.getOrCreate(conf)
    val snc = SnappyContext(sc)

    val testJoins = new ClusterSnappyJoinSuite()
    testJoins.partitionToPartitionJoinAssertions(snc, table1, table2)

    println("Successful")

  }
}
