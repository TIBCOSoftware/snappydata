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
package io.snappydata.externalstore

import java.sql.{Connection, DriverManager, SQLException}

import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.AvailablePortHelper

import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.sql.sources.JdbcExtendedUtils
import org.apache.spark.sql.{AnalysisException, SaveMode, SnappyContext, TableNotFoundException}

/**
 * Some basic tests to detect catalog inconsistency and repair it
 */
class CatalogConsistencyDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  private def getClientConnection(netPort: Int,
      routeQuery: Boolean = true): Connection = {
    val driver = "io.snappydata.jdbc.ClientDriver"
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
    val props = Map("PERSISTENT" -> "sync")

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable("column_table1", "column", dataDF.schema, props)
    snc.createTable("column_table2", "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("column_table2")

    snc.sql("create stream table tweetsTable (id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) using twitter_stream options (" +
        "consumerKey '***REMOVED***', " +
        "consumerSecret '***REMOVED***', " +
        "accessToken '***REMOVED***', " +
        "accessTokenSecret '***REMOVED***', " +
        "rowConverter 'io.snappydata.streaming.TweetToRowsConverter')")
  }

  // assert that table is not in Hive catalog and store DD
  private def assertTableDoesNotExist(netPort1: Int, snc: SnappyContext): Any = {
    try {
      // table should not exist in the Hive catalog
      snc.snappySession.sessionCatalog.lookupRelation(
        snc.snappySession.tableIdentifier("column_table1"))
    } catch {
      case t: TableNotFoundException => // expected exception
      case unknown: Throwable => throw unknown
    }

    val routeQueryDisabledConn = getClientConnection(netPort1, false)
    try {
      // make sure that the column buffer does not exist
      routeQueryDisabledConn.createStatement().executeQuery(
        "select * from " + ColumnFormatRelation.columnBatchTableName("app.column_table1"))
    } catch {
      case se: SQLException if (se.getSQLState.equals("42X05")) =>
      case unknown: Throwable => throw unknown
    }
  }

  def verifyTables(snc: SnappyContext): Unit = {
    val result = snc.sql("SELECT * FROM column_table2")
    assert(result.collect.length == 5)
    // below call should not throw an exception
    snc.snappySession.sessionCatalog.lookupRelation(
      snc.snappySession.tableIdentifier("tweetsTable"))
  }

  def testHiveStoreEntryMissingForTable(): Unit = {
    val snc = SnappyContext(sc)

    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    vm1.invoke(classOf[ClusterManagerTestBase], "startNetServer",
      AvailablePortHelper.getRandomAvailableTCPPort)
    vm0.invoke(classOf[ClusterManagerTestBase], "startNetServer",
      AvailablePortHelper.getRandomAvailableTCPPort)

    createTables(snc)

    // remove column_table1 entry from Hive store but not from store DD
    snc.snappySession.sessionCatalog.dropTable(
      snc.snappySession.tableIdentifier("column_table1"), ignoreIfNotExists = false, purge = false)

    try {
      snc.snappySession.sessionCatalog.lookupRelation(
        snc.snappySession.tableIdentifier("column_table1"))
    } catch {
      case t: TableNotFoundException => // expected exception
      case unknown: Throwable => throw unknown
    }

    val connection = getClientConnection(netPort1)
    // repair the catalog
    connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('true', 'true')")
    // column_table1 should not be found in either catalog after repair
    assertTableDoesNotExist(netPort1, snc)
    // other tables should exist
    verifyTables(snc)

  }

  def testStoreDDEntryMissingForTable(): Unit = {
    val snc = SnappyContext(sc)

    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    vm1.invoke(classOf[ClusterManagerTestBase], "startNetServer",
      AvailablePortHelper.getRandomAvailableTCPPort)
    vm0.invoke(classOf[ClusterManagerTestBase], "startNetServer",
      AvailablePortHelper.getRandomAvailableTCPPort)

    createTables(snc)

    // drop column_table1 from store DD
    val routeQueryDisabledConn = getClientConnection(netPort1, false)
    routeQueryDisabledConn.createStatement().execute("drop table " +
        ColumnFormatRelation.columnBatchTableName("app.column_table1"))
    routeQueryDisabledConn.createStatement().execute("drop table column_table1")

    // make sure that the table exists in Hive metastore
    assert(JdbcExtendedUtils.tableExistsInMetaData("APP", "COLUMN_TABLE1", routeQueryDisabledConn))

    val connection = getClientConnection(netPort1)
    // repair the catalog
    connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('true', 'true')")
    // column_table1 should not be found in either catalog after repair
    assertTableDoesNotExist(netPort1, snc)
    // other tables should exist
    verifyTables(snc)
  }

  // Hive entry missing but DD entry exists
  def testCatalogRepairedWhenLeadStopped1(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    var snc = SnappyContext(sc)

    createTables(snc)
    // remove column_table1 entry from Hive store but not from store DD
    snc.snappySession.sessionCatalog.dropTable(
      snc.snappySession.tableIdentifier("column_table1"), ignoreIfNotExists = false, purge = false)

    // stop spark
    val sparkContext = SnappyContext.globalSparkContext
    if(sparkContext != null) sparkContext.stop()
    ClusterManagerTestBase.stopAny()

    val connection = getClientConnection(netPort1)
    // repair the catalog
    // does not actually repair, just adds warning to log file
    connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('false', 'false')")
    // actually repair the catalog
    connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('true', 'true')")

    ClusterManagerTestBase.startSnappyLead(ClusterManagerTestBase.locatorPort, bootProps)
    snc = SnappyContext(sc)
    // column_table1 should not be found in either catalog after repair
    assertTableDoesNotExist(netPort1, snc)

    // other tables should exist
    verifyTables(snc)
  }

  // Hive entry exists but DD entry missing
  def testCatalogRepairedWhenLeadStopped2(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    var snc = SnappyContext(sc)

    createTables(snc)
 
    // drop column_table1 from store DD
    val routeQueryDisabledConn = getClientConnection(netPort1, false)
    routeQueryDisabledConn.createStatement().execute("drop table " +
        ColumnFormatRelation.columnBatchTableName("app.column_table1"))
    routeQueryDisabledConn.createStatement().execute("drop table column_table1")

    // make sure that the table exists in Hive metastore
    assert(JdbcExtendedUtils.tableExistsInMetaData("APP", "COLUMN_TABLE1", routeQueryDisabledConn))

    // stop spark
    val sparkContext = SnappyContext.globalSparkContext
    if(sparkContext != null) sparkContext.stop()
    ClusterManagerTestBase.stopAny()

    val connection = getClientConnection(netPort1)
    // repair the catalog
    // does not actually repair, just adds warning to log file
    connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('false', 'false')")
    // actually repair the catalog
    connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('true', 'true')")

    ClusterManagerTestBase.startSnappyLead(ClusterManagerTestBase.locatorPort, bootProps)
    snc = SnappyContext(sc)
    // column_table1 should not be found in either catalog after repair
    assertTableDoesNotExist(netPort1, snc)

    // other tables should exist
    verifyTables(snc)
  }


  def testConsistencyWithCollocatedTables(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    var snc = SnappyContext(sc)

    val baseRowTable = "ORDER_DETAILS_ROW"
    val colloactedRowTable = "EXEC_DETAILS_ROW"

    val baseColumnTable = "ORDER_DETAILS_COL"
    val colloactedColumnTable = "EXEC_DETAILS_COL"


    snc.sql(s"create table $baseRowTable(SINGLE_ORDER_DID BIGINT ,SYS_ORDER_ID VARCHAR(64) , " +
        "SYS_ORDER_VER INTEGER ,DATA_SNDG_SYS_NM VARCHAR(128)) USING row OPTIONS(BUCKETS '16', " +
        "REDUNDANCY '1', EVICTION_BY 'LRUHEAPPERCENT', PERSISTENT 'ASYNCHRONOUS',PARTITION_BY  " +
        "'SINGLE_ORDER_DID')");


    snc.sql(s"create table $colloactedRowTable(EXEC_DID BIGINT,SYS_EXEC_VER INTEGER,SYS_EXEC_ID " +
        "VARCHAR (64),TRD_DATE VARCHAR(20),ALT_EXEC_ID VARCHAR(64)) USING row OPTIONS" +
        s"(COLOCATE_WITH '$baseRowTable', BUCKETS '16', REDUNDANCY '1', EVICTION_BY  " +
        "'LRUHEAPPERCENT', PERSISTENT 'ASYNCHRONOUS',PARTITION_BY 'EXEC_DID')");



    snc.sql(s"create table $baseColumnTable(SINGLE_ORDER_DID BIGINT ,SYS_ORDER_ID VARCHAR(64) ," +
        s"SYS_ORDER_VER INTEGER ,DATA_SNDG_SYS_NM VARCHAR(128)) USING column OPTIONS(BUCKETS " +
        s"'16', REDUNDANCY '1', EVICTION_BY 'LRUHEAPPERCENT', PERSISTENT 'ASYNCHRONOUS'," +
        s"PARTITION_BY  'SINGLE_ORDER_DID')");

    snc.sql(s"create table $colloactedColumnTable(EXEC_DID BIGINT,SYS_EXEC_VER INTEGER," +
        s"SYS_EXEC_ID VARCHAR(64),TRD_DATE VARCHAR(20),ALT_EXEC_ID VARCHAR(64)) USING column " +
        s"OPTIONS (COLOCATE_WITH '$baseColumnTable', BUCKETS '16', REDUNDANCY '1', EVICTION_BY " +
        s"'LRUHEAPPERCENT', PERSISTENT 'ASYNCHRONOUS',PARTITION_BY 'EXEC_DID')");

    try {
      // This should throw an exception
      snc.sql(s"drop table $baseRowTable")
      assert(assertion = false, "expected the drop to fail")
    } catch {
      case ae: AnalysisException =>
        // Expected Exception and assert message
        assert(ae.getMessage.contains("APP.ORDER_DETAILS_ROW cannot be dropped because of " +
            "dependent objects: APP.EXEC_DETAILS_ROW"))
    }

    // stop spark
    val sparkContext = SnappyContext.globalSparkContext
    if (sparkContext != null) sparkContext.stop()

    ClusterManagerTestBase.stopAny()
    ClusterManagerTestBase.startSnappyLead(ClusterManagerTestBase.locatorPort, bootProps)
    snc = SnappyContext(sc)
    try {
      // This should throw an exception
      snc.sql(s"drop table $baseRowTable")
      assert(assertion = false, "expected the drop to fail")
    } catch {
      case ae: AnalysisException =>
        // Expected Exception and assert message
        assert(ae.getMessage.contains("APP.ORDER_DETAILS_ROW cannot be dropped because of " +
            "dependent objects: APP.EXEC_DETAILS_ROW"))
    }

    snc.sql(s"drop table $colloactedColumnTable")
    snc.sql(s"drop table $baseColumnTable")

    snc.sql(s"drop table $colloactedRowTable")
    snc.sql(s"drop table $baseRowTable")
  }
}
