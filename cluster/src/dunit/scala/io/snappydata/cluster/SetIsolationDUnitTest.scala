/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import java.sql.{Connection, SQLException, Statement}

import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import io.snappydata.test.dunit.AvailablePortHelper

import org.apache.spark.Logging

class SetIsolationDUnitTest (val s: String)
    extends ClusterManagerTestBase(s) with Logging {

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
    assert(rs1.getInt(1) == 100, "result mismatch")
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
    checkUnsupportedQueries(stmt1, "delete from coltable where col1 = 101")
    checkUnsupportedQueries(stmt1, "delete from rowtable where col1 in " +
        "(select col1 from coltable)")
    checkUnsupportedQueries(stmt1, "update coltable set col2 = 101")
    checkUnsupportedQueries(stmt1, "update coltable set col2 = 101 where col2 in " +
        "(select col1 from coltable)")

    // queries involving row tables that should not get routed
    // (for example is there is a syntax error)
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

}
