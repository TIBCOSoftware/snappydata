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
//    stmt.execute("create table coltable(col1 int, col2 int, col3 int)" +
//        " using column options (partition_by 'col1')")

    for (i <- 1 to 100) {
      stmt.execute(s"insert into rowtable values ($i, $i, $i)")
//      stmt.execute(s"insert into coltable values ($i, $i, $i)")
    }
  }

  private def validateTableData(conn: Connection) = {
    val stmt1 = conn.createStatement()
    var rs1 = stmt1.executeQuery("select count(*) from rowtable")
    assert(rs1.next())
    assert(rs1.getInt(1) == 100, "result mismatch")

//    rs1 = stmt1.executeQuery("select count(*) from coltable")
//    assert(rs1.next())
//    assert(rs1.getInt(1) == 100, "result mismatch")
  }

  // queries not allowed on a column table inside a transaction
  def checkTxQueryOnColumnTable(stmt: Statement, query: String): Unit = {
    try {
      // tx not allowed as on column tables
      stmt.execute(query)
      assert(false, "query should have failed as tx on column table is not allowed")
    } catch {
      case se: SQLException if SQLState.SNAPPY_TX_DISALLOWED_ON_COLUMN_TABLES.
          startsWith(se.getSQLState) => // expected
    }
  }

  def testSetIsolationLevel(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)

    createTables(conn)
    try {
      // tx not allowed when query routing isn't disabled
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
      assert(false, "op should have failed as TX is not allowed when query routing is enabled")
    } catch {
      case se: SQLException if SQLState.SNAPPY_TX_DISALLOWED_WITH_ROUTE_QUERY_DISABLED.
          startsWith(se.getSQLState) => // expected
    }

    // transaction none should be allowed
    conn.setTransactionIsolation(Connection.TRANSACTION_NONE)
    validateTableData(conn)
    conn.close()

    val routeQueryDisabledConn = getANetConnection(netPort1, disableQueryRouting = true)
    // tx allowed as on row tables as query routing is disabled
    routeQueryDisabledConn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
//    routeQueryDisabledConn.setAutoCommit(true)

    val stmt1 = routeQueryDisabledConn.createStatement()
    var rs1 = stmt1.executeQuery("select count(*) from rowtable")
    assert(rs1.next())
    assert(rs1.getInt(1) == 100, "result mismatch")
    // insert data
    logInfo("inserting a row in rowtable")
    stmt1.execute("insert into rowtable values(101, 101, 101)")
    logInfo("select count from rowtable")
    rs1 = stmt1.executeQuery("select count(*) from rowtable")
    assert(rs1.next())
    var cnt = rs1.getInt(1)
    assert(cnt == 101, s"result mismatch. Actual numRows = $cnt. Expect numRows = 101")
    // delete
    stmt1.execute("delete from rowtable where col1 = 101")
    rs1 = stmt1.executeQuery("select count(*) from rowtable")
    assert(rs1.next())
    assert(rs1.getInt(1) == 100, "result mismatch")

//    checkTxQueryOnColumnTable(stmt1, "select count(*) from coltable")
//    checkTxQueryOnColumnTable(stmt1, "insert into coltable values(101, 101, 101)")
//    checkTxQueryOnColumnTable(stmt1, "delete from coltable where col1 = 101")
//    checkTxQueryOnColumnTable(stmt1, "update coltable set col1 = 101")
//    checkTxQueryOnColumnTable(stmt1, "select rowtable.col1 as rc1, coltable.col1 as cc1" +
//        " from rowtable, coltable where rowtable.col1 = coltable.col1")

  }

}
