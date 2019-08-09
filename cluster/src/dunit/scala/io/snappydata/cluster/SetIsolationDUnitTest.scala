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

import java.sql.{Connection, DriverManager, SQLException, Statement}
import java.util
import java.util.Properties

import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import io.snappydata.test.dunit.AvailablePortHelper

import org.apache.spark.Logging

class SetIsolationDUnitTest (val s: String)
    extends ClusterManagerTestBase(s) with Logging with DisableSparkTestingFlag {

  private def createTables(conn: Connection): Unit = {
    val stmt = conn.createStatement()
    stmt.execute("create table rowtable(col1 int, col2 int, col3 int)" +
        " using row options (partition_by 'col1')")
    stmt.execute("create table coltable(col1 int, col2 int, col3 int)" +
        " using column options (partition_by 'col1')")

    for (i <- 1 to 100) {
      stmt.execute(s"insert into rowtable values ($i, $i, $i)")
      stmt.execute(s"insert into coltable values ($i, $i, $i)")
    }
  }

  private def validateTableData(conn: Connection) = {
    val stmt1 = conn.createStatement()
    var rs1 = stmt1.executeQuery("select count(*) from rowtable")
    assert(rs1.next())
    assert(rs1.getInt(1) == 100, "result mismatch")

    rs1 = stmt1.executeQuery("select count(*) from coltable")
    assert(rs1.next())
    assert(rs1.getInt(1) == 100, "result mismatch")
  }

  // queries not allowed on a column table inside a transaction
  def checkUnsupportedQueries(stmt: Statement, query: String,
      expectedSqlState: String = SQLState.SNAPPY_OP_DISALLOWED_ON_COLUMN_TABLES): Unit = {
    try {
      // tx not allowed as on column tables
      stmt.execute(query)
      assert(false, "query should have failed as tx on column table is not allowed")
    } catch {
      case sq: SQLException if expectedSqlState.
          startsWith(sq.getSQLState) => // expected
    }
  }

  def performOperationsOnTable(conn: Connection, tableName: String): Unit = {
    val stmt1 = conn.createStatement()
    var rs1 = stmt1.executeQuery(s"select count(*) from $tableName")
    assert(rs1.next())
    assert(rs1.getInt(1) == 100, "result mismatch")
    // insert data
    logInfo(s"inserting a row in $tableName")
    stmt1.execute(s"insert into $tableName values(101, 101, 101)")
    logInfo(s"select count from $tableName")
    rs1 = stmt1.executeQuery(s"select count(*) from $tableName")
    assert(rs1.next())
    var cnt = rs1.getInt(1)
    assert(cnt == 101, s"result mismatch. Actual numRows = $cnt. Expect numRows = 101")
    // delete
    stmt1.execute(s"delete from $tableName where col1 = 101")
    rs1 = stmt1.executeQuery(s"select count(*) from $tableName")
    assert(rs1.next())
    cnt = rs1.getInt(1)
    assert(cnt == 100, s"Expected 100 but got $cnt")
    stmt1.close()
  }

  def testSetIsolationLevel(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    var conn = getANetConnection(netPort1)

    logInfo("Creating tables for the test")
    createTables(conn)

    // with autocommit true transactions on row and column table are allowed
    logInfo("setting autocommit true")
    conn.setAutoCommit(true)
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
    validateTableData(conn)
    performOperationsOnTable(conn, "rowtable")
    performOperationsOnTable(conn, "coltable")
    conn.close()

    // with autocommit false transactions allowed on row tables only
    logInfo("setting autocommit false")
    conn = getANetConnection(netPort1)
    conn.setAutoCommit(false)
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
    performOperationsOnTable(conn, "rowtable")
    conn.commit()

    val stmt1 = conn.createStatement()
    logInfo("checking unsupported queries on column tables")
    // queries involving column tables
    checkUnsupportedQueries(stmt1, "select count(*) from coltable")
    checkUnsupportedQueries(stmt1, "insert into coltable values(101, 101, 101)")
    checkUnsupportedQueries(stmt1, "insert into rowtable select col1, col2, col3 from coltable")
    checkUnsupportedQueries(stmt1, "put into coltable values(101, 101, 101)")
    checkUnsupportedQueries(stmt1, "put into rowtable select col1, col2, col3 from coltable")
    checkUnsupportedQueries(stmt1, "delete from coltable where col1 = 101")
    checkUnsupportedQueries(stmt1, "delete from rowtable where col1 in " +
        "(select col1 from coltable)")
    checkUnsupportedQueries(stmt1, "update coltable set col2 = 101")
    checkUnsupportedQueries(stmt1, "update coltable set col2 = 101 where col2 in " +
        "(select col1 from coltable)")

    // queries involving row tables that should not get routed when
    // autocommit is false (for example even if there is a syntax error)
    logInfo("checking unsupported queries on row tables")
    checkUnsupportedQueries(stmt1, "select * from rowtable limit 1", SQLState.LANG_SYNTAX_ERROR)
    checkUnsupportedQueries(stmt1, "select rowtable.col1 as rc1, coltable.col1 as cc1" +
        " from rowtable, coltable where rowtable.col1 = coltable.col1", SQLState.NOT_COLOCATED_WITH)
    stmt1.close()
    conn.close()

    // user sets route-query=false, query involving column table should error out
    val queryRoutingDisabledConn = getANetConnection(netPort1, disableQueryRouting = true)
    val stmt2 = queryRoutingDisabledConn.createStatement()
    checkUnsupportedQueries(stmt2, "select count(*) from coltable")
    queryRoutingDisabledConn.close()
  }

  // With autocommit false transactions on column tables
  // are allowed when "allow-explicit-commit" is set to true
  def testSNAP3088(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    // test when allow-explicit-commit is set to true in properties
    val props = new Properties()
    props.setProperty("allow-explicit-commit", "true")
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://localhost:" + netPort1, props)

    logInfo("Creating tables for the test")
    createTables(conn)

    logInfo("setting autocommit false")
    conn.setAutoCommit(false)
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
    performOperationsOnTable(conn, "coltable")
    performOperationsOnTable(conn, "rowtable")

    val stmt1 = conn.createStatement()
    // few queries involving column tables that should not throw exception
    stmt1.execute("insert into rowtable select col1, col2, col3 from coltable")
    var rs1 = stmt1.executeQuery("select count(*) from rowtable")
    assert(rs1.next())
    val numRows1 = rs1.getInt(1)
    assert(numRows1 == 200, s"Did not get expected count. Expected = 200, actual = $numRows1")
    rs1.close()

    stmt1.execute("insert into coltable values(101, 101, 101)")
    // rollback will be a no-op
    conn.rollback()
    rs1 = stmt1.executeQuery("select count(*) from coltable")
    assert(rs1.next())
    val numRows2 = rs1.getInt(1)
    assert(numRows2 == 101, s"Did not get expected count. Expected = 101, actual = $numRows2")
    rs1.close()

    // commit will be a no-op
    conn.commit()
    conn.close()

    // also test when property allow-explicit-commit set in the connection URL
    val conn2 = DriverManager.getConnection(
      "jdbc:snappydata://localhost:" + netPort1 + "/allow-explicit-commit=true")
    conn2.createStatement().execute("drop table coltable")
    conn2.createStatement().execute("drop table rowtable")
    logInfo("Creating tables for the test")
    createTables(conn2)
    logInfo("setting autocommit false")
    // this will be ignored
    conn2.setAutoCommit(false)
    conn2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
    performOperationsOnTable(conn2, "coltable")
    performOperationsOnTable(conn2, "rowtable")
    val stmt2 = conn2.createStatement()
    stmt2.execute("insert into coltable values(101, 101, 101)")
    // rollback will be a no-op
    conn2.rollback()
    val rs2 = stmt2.executeQuery("select count(*) from coltable")
    assert(rs2.next())
    val numRows3 = rs2.getInt(1)
    assert(numRows3 == 101, s"Did not get expected count. Expected = 101, actual = $numRows2")
    rs2.close()
    // commit will be a no-op
    conn2.commit()
    conn2.close()

    // explicitly set "allow-explicit-commit" to false
    // transaction on column tables will throw error
    props.setProperty("allow-explicit-commit", "false")
    val conn3 = DriverManager.getConnection(
      "jdbc:snappydata://localhost:" + netPort1, props)
    logInfo("Creating tables for the test")
    conn3.createStatement().execute("drop table coltable")
    conn3.createStatement().execute("drop table rowtable")
    createTables(conn3)
    logInfo("setting autocommit false")
    conn3.setAutoCommit(false)
    conn3.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
    val stmt3 = conn3.createStatement()
    checkUnsupportedQueries(stmt3, "select count(*) from coltable")
    conn3.close()
  }

  var gotConflict = false

  /**
    * Test conflicts. Copied part of
    * rowstore test TransactionDUnit#testCommitWithConflicts()
    */
  def testCommitWithConflicts() {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    val conn = getANetConnection(netPort1)
    logInfo(s"testCommitWithConflicts: test with isolation level TRANSACTION_READ_COMMITTED")
    doTestCommitWithConflicts(netPort1, conn, Connection.TRANSACTION_READ_COMMITTED)
    conn.close()

    val conn2 = getANetConnection(netPort1)
    logInfo(s"testCommitWithConflicts: test with isolation level TRANSACTION_REPEATABLE_READ")
    doTestCommitWithConflicts(netPort1, conn2, Connection.TRANSACTION_REPEATABLE_READ)
    conn2.close()
  }

  /**
    * Test put into on replicated table - SNAP2082
    */
  def testPutIntoOnReplicatedTables() {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    val conn = getANetConnection(netPort1)
    logInfo(s"testPutIntoOnReplicatedTables: test with isolation level TRANSACTION_READ_COMMITTED")
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
    val st = conn.createStatement()
    st.execute("CREATE TABLE APP.TABLE2 ( FIELD1 VARCHAR(36) NOT NULL " +
      "PRIMARY KEY, FIELD2 INT, FIELD3 VARCHAR(36) NOT NULL)")
    st.executeUpdate("PUT INTO APP.TABLE2(FIELD1, FIELD2, FIELD3)  VALUES ('key1',1,'value1')")
    conn.commit()
    val rs1 = st.executeQuery("select * from table2")
    assert(rs1.next())
    assert(rs1.getString(3).equals("value1"))
    st.executeUpdate("PUT INTO APP.TABLE2(FIELD1, FIELD2, FIELD3)  VALUES ('key1',1,'value1')")
    conn.commit()
    st.executeUpdate("PUT INTO APP.TABLE2(FIELD1, FIELD2, FIELD3)  VALUES ('key1',1,'value11')")
    conn.commit()
    val rs2 = st.executeQuery("select * from table2")
    assert(rs2.next())
    assert(rs2.getString(3).equals("value11"))
    conn.close()
  }

  private def doTestCommitWithConflicts(netPort1: Int, conn: Connection, isolationLevel: Int) = {
    conn.setAutoCommit(false)
    val st = conn.createStatement
//    st.execute("create schema tran")
    st.execute("Create table tran.t1 (c1 int not null primary key, c2 int not null) using row")
    conn.commit()
    this.gotConflict = false
    logInfo(s"doTestCommitWithConflicts: setting isolation level $isolationLevel")

    conn.setTransactionIsolation(isolationLevel)
    st.execute("insert into tran.t1 values (10, 10)")
    st.execute("insert into tran.t1 values (20, 10)")
    st.execute("insert into tran.t1 values (30, 10)")
    val otherTxOk = Array[Boolean](false)
    val otherTx = new Thread(new Runnable() {
      def run() {
        try {
          val otherConn = getANetConnection(netPort1)
          otherConn.setTransactionIsolation(isolationLevel)
          otherConn.setAutoCommit(false)
          val otherSt = otherConn.createStatement
          try {
            otherSt.execute("insert into tran.t1 values (10, 20)")
            otherTxOk(0) = true
          }
          catch {
            case sqle: SQLException => {
              if ("X0Z02" == sqle.getSQLState) {
                gotConflict = true
                otherConn.rollback()
              }
              else throw sqle
            }
          } finally {
            otherConn.close()
          }
        }
        catch {
          case se: SQLException => {
            gotConflict = false
            assert(false, s"unexpected exception $se")
          }
        }
      }
    })
    otherTx.start()
    otherTx.join()
    assert(!otherTxOk(0))
    assert(this.gotConflict, "expected conflict")
    this.gotConflict = false


    val ps = conn.prepareStatement("select * from tran.t1")
    conn.commit()
    // check that the value should be that of first transaction
    var rs = ps.executeQuery
    var expectedKeys = Array[Int](10, 20, 30)
    var numRows = 0
    while (rs.next) {
      numRows += 1
      val key = rs.getInt(1)
      val index = util.Arrays.binarySearch(expectedKeys, key)
      assert(index >= 0, "Expected to find the key: ")
      expectedKeys(index) = Integer.MIN_VALUE + 1
      util.Arrays.sort(expectedKeys)
      val v = rs.getInt(2)
      assert(v == 10, s"Second column should be 10. Actual is $v")
    }
    assert(numRows == 3, "ResultSet should have three rows")
    rs.close()
    st.close()
    conn.commit()
    conn.setTransactionIsolation(Connection.TRANSACTION_NONE)
    conn.createStatement().execute("drop table tran.t1")
  }
}
