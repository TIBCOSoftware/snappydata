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
package io.snappydata.externalstore

import java.sql.{DriverManager, Connection, SQLException}

import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.AvailablePortHelper

import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.sql.{SaveMode, SnappyContext, TableNotFoundException}

/**
 * Some basic tests to detect catalog inconsistency and repair it
 */
class CatalogConsistencyDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  private def getClientConnection(netPort: Int,
      routeQuery: Boolean = true): Connection = {
    val driver = "com.pivotal.gemfirexd.jdbc.ClientDriver"
    Class.forName(driver).newInstance //scalastyle:ignore
    var url: String = null
    if (!routeQuery) {
      url = "jdbc:snappydata://localhost:" + netPort + "/route-query=false"
    } else {
      url = "jdbc:snappydata://localhost:" + netPort + "/"
    }

    DriverManager.getConnection(url)
  }

  private def createTables(snc: SnappyContext): Unit = {
    val props = Map("PERSISTENT" -> "")

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable("column_table1", "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("column_table1")
    snc.createTable("column_table2", "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("column_table2")
  }

  // assert that, table is not in Hive catalog and store DD
  private def assertTableDoesNotExist(netPort1: Int, snc: SnappyContext): Any = {
    try {
      // table should not exist in the Hive catalog
      snc.catalog.lookupRelation(snc.catalog.newQualifiedTableName("column_table1"))
    } catch {
      case t: TableNotFoundException => // expected exception
      case unknown: Throwable => throw unknown
    }

    val routeQueryDisabledConn = getClientConnection(netPort1, false)
    // should throw an exception since the catalog is repaired and table entry
    // should have been removed
    try {
      // table should not exist in the store DD
      routeQueryDisabledConn.createStatement().executeQuery("select * from column_table1")
    } catch {
      case se: SQLException if (se.getSQLState.equals("42X05")) =>
      case unknown: Throwable => throw unknown
    }
  }

  def testHiveStoreEntryMissingForTable(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    val snc = SnappyContext(sc)

    createTables(snc)

    // remove the table entry from Hive store but not from store DD
    snc.catalog.unregisterDataSourceTable(snc.catalog.newQualifiedTableName("column_table1"), None)

    try {
      snc.catalog.lookupRelation(snc.catalog.newQualifiedTableName("column_table1"))
    } catch {
      case t: TableNotFoundException => // expected exception
      case unknown: Throwable => throw unknown
    }

    val connection = getClientConnection(netPort1)
    // repair the catalog
    connection.createStatement().executeQuery("VALUES SYS.CHECK_CATALOG(1)").next()
    //    FabricDatabase.checkSnappyCatalogConsistency(GemFireXDUtils.createNewInternalConnection(false));

    assertTableDoesNotExist(netPort1, snc)

    val result = snc.sql("SELECT * FROM column_table2")
    assert(result.collect.length == 5)

  }

  def testStoreDDEntryMissingForTable(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    val snc = SnappyContext(sc)

    createTables(snc)
    // drop table from store DD
    val routeQueryDisabledConn = getClientConnection(netPort1, false)
    routeQueryDisabledConn.createStatement().execute("drop table " +
        ColumnFormatRelation.cachedBatchTableName("column_table1"))
    routeQueryDisabledConn.createStatement().execute("drop table column_table1")

    // make sure that the table exists in Hive metastore
    assert(snc.catalog.lookupRelation(snc.catalog.newQualifiedTableName("column_table1"))
        != None)

    val connection = getClientConnection(netPort1)
    // repair the catalog
    connection.createStatement().executeQuery("VALUES SYS.CHECK_CATALOG(1)").next()

    assertTableDoesNotExist(netPort1, snc)

    val result = snc.sql("SELECT * FROM column_table2")
    assert(result.collect.length == 5)
  }

  def testCatalogRepairedWhenLeadRestarted(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    var snc = SnappyContext(sc)

    createTables(snc)
    // stop spark
    val sparkContext = SnappyContext.globalSparkContext
    if(sparkContext != null) sparkContext.stop()
    ClusterManagerTestBase.stopAny()

    // drop table from store DD
    val routeQueryDisabledConn = getClientConnection(netPort1, false)
    routeQueryDisabledConn.createStatement().execute("drop table " +
        ColumnFormatRelation.cachedBatchTableName("column_table1"))
    routeQueryDisabledConn.createStatement().execute("drop table column_table1")

    ClusterManagerTestBase.startSnappyLead(locatorPort, bootProps)
    snc = SnappyContext(sc)

    assertTableDoesNotExist(netPort1, snc)

    val result = snc.sql("SELECT * FROM column_table2")
    assert(result.collect.length == 5)
  }

}
