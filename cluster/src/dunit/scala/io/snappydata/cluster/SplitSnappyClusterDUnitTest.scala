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

import scala.language.postfixOps

import io.snappydata.test.dunit.AvailablePortHelper

import org.apache.spark.sql.SnappyContext
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
    // skip this test here since it is already run in Spark+Snappy mode
    // and testComplexTypesForColumnTables... is more comprehensive
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
}
