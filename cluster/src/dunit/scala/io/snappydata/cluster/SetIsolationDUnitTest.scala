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
  }

  def testSetIsolationLevel(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)

    createTables(conn)

    // transaction none should be allowed
    conn.setAutoCommit(true)
    conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
    validateTableData(conn)
    performOperationsOnTable(conn, "rowtable")
    performOperationsOnTable(conn, "columntable")

//    checkTxQueryOnColumnTable(stmt1, "select count(*) from coltable")
//    checkTxQueryOnColumnTable(stmt1, "insert into coltable values(101, 101, 101)")
//    checkTxQueryOnColumnTable(stmt1, "delete from coltable where col1 = 101")
//    checkTxQueryOnColumnTable(stmt1, "update coltable set col1 = 101")
//    checkTxQueryOnColumnTable(stmt1, "select rowtable.col1 as rc1, coltable.col1 as cc1" +
//        " from rowtable, coltable where rowtable.col1 = coltable.col1")

  }

}
