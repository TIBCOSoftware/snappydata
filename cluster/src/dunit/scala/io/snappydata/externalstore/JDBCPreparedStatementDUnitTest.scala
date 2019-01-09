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

import java.sql.PreparedStatement
import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.AvailablePortHelper
import org.junit.Assert.assertEquals
import org.apache.spark.Logging


// scalastyle:off println

class JDBCPreparedStatementDUnitTest(s: String) extends ClusterManagerTestBase(s)
    with Logging {

  def testPreparedStatementToExecuteSingleInsertUpdateDeleteQuery(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)
    val stmt = conn.createStatement()
    stmt.execute("drop table if exists t1")
    stmt.execute("create table t1(id integer, fs integer) using column options" +
        "(key_columns 'id', COLUMN_MAX_DELTA_ROWS '7', BUCKETS '2')")
    var ps: PreparedStatement = null

    val query = "insert into t1 values(?,?)"
    ps = conn.prepareStatement(query)
    for (i <- 0 until 20) {
      ps.setInt(1, i)
      ps.setInt(2, i + 10)
      ps.executeUpdate()
    }
    var rscnt = stmt.executeQuery("select count(*) from t1")
    rscnt.next()
    assertEquals(20, rscnt.getInt(1))

    val rs = stmt.executeQuery("select * from t1 order by id")
    var i = 0
    while (rs.next()) {
      assertEquals(i, rs.getInt(1))
      assertEquals(i + 10, rs.getInt(2))
      i = i + 1
    }

    val query1 = "update t1 set fs = ? where fs = ?"
    ps = conn.prepareStatement(query1)
    var no = 0
    for (i <- 0 until 20) {
      if (i % 2 == 0) {
        no = i + 100
      } else {
        no = i + 1000
      }
      ps.setInt(1, no)
      ps.setInt(2, i + 10)
      ps.executeUpdate()
    }

    rscnt = stmt.executeQuery("select count(*) from t1")
    rscnt.next()
    assertEquals(20, rscnt.getInt(1))

    val rs1 = stmt.executeQuery("select * from t1 order by id")
    var i2 = 0
    while (rs1.next()) {
      if (i2 % 2 == 0) {
        no = i2 + 100
      } else {
        no = i2 + 1000
      }
      assertEquals(i2, rs1.getInt(1))
      assertEquals(no, rs1.getInt(2))
      i2 = i2 + 1
    }

    val query2 = "delete from t1 where id = ?"
    ps = conn.prepareStatement(query2)
    for (i2 <- 0 until 20) {
      ps.setInt(1, i2)
      ps.executeUpdate()
    }

    rscnt = stmt.executeQuery("select count(*) from t1")
    rscnt.next()
    assertEquals(0, rscnt.getInt(1))
  }

  def testPreparedStatementToExecuteSinglePutUpdateQuery(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)
    val stmt = conn.createStatement()
    stmt.execute("drop table if exists t2")
    stmt.execute("create table t2(id integer, fs string) using column options" +
        "(key_columns 'id', COLUMN_MAX_DELTA_ROWS '7', BUCKETS '2')")
    var ps: PreparedStatement = null

    val query3 = "put into t2 values(?,?)"
    ps = conn.prepareStatement(query3)
    for (i3 <- 0 until 20) {
      ps.setInt(1, i3)
      ps.setString(2, "t2-Name" + i3)
      ps.executeUpdate()
    }

    var rscnt = stmt.executeQuery("select count(*) from t2")
    rscnt.next()
    assertEquals(20, rscnt.getInt(1))

    val rs3 = stmt.executeQuery("select * from t2 order by id")
    var i = 0
    while (rs3.next()) {
      assertEquals(i, rs3.getInt(1))
      assertEquals("t2-Name" + i, rs3.getString(2))
      i = i + 1
    }
    var str = ""
    val query4 = "update t2 set fs = ? where fs = ?"
    ps = conn.prepareStatement(query4)
    for (i1 <- 0 until 20) {
      if (i1 % 2 == 0) {
        str = "t2-Name-2222"
      } else {
        str = "t2-Name-0"
      }
      ps.setString(1, str)
      ps.setString(2, "t2-Name" + i1)
      ps.executeUpdate()
    }

    rscnt = stmt.executeQuery("select count(*) from t2")
    rscnt.next()
    assertEquals(20, rscnt.getInt(1))

    val rs4 = stmt.executeQuery("select * from t2 order by id")
    var i1 = 0
    while (rs4.next()) {
      if (i1 % 2 == 0) {
        str = "t2-Name-2222"
      } else {
        str = "t2-Name-0"
      }
      assertEquals(i1, rs4.getInt(1))
      assertEquals(str, rs4.getString(2))
      i1 = i1 + 1
    }

  }

  def testPreparedStatementToExecuteInsertUpdateDeleteBulkQueries(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)
    val stmt = conn.createStatement()
    stmt.execute("drop table if exists t7")
    stmt.execute("create table t3(id integer, fs integer) using column options" +
        "(key_columns 'id', COLUMN_MAX_DELTA_ROWS '7', BUCKETS '2')")
    var ps: PreparedStatement = null

    val query = "insert into t3 values(?,?)"
    ps = conn.prepareStatement(query)
    for (i <- 0 until 20) {
      ps.setInt(1, i)
      ps.setInt(2, i + 10)
      ps.addBatch()
      if (i % 10 == 0) {
        ps.executeBatch()
      }
    }
    ps.executeBatch()

    var rscnt = stmt.executeQuery("select count(*) from t3")
    rscnt.next()
    assertEquals(20, rscnt.getInt(1))

    val rs = stmt.executeQuery("select * from t3 order by id")
    var i = 0
    while (rs.next()) {
      assertEquals(i, rs.getInt(1))
      assertEquals(i + 10, rs.getInt(2))
      i = i + 1
    }

    val query1 = "update t3 set fs = ? where fs = ?"
    ps = conn.prepareStatement(query1)
    var fs1 = 0
    for (i <- 0 until 20) {
      if (i % 2 == 0) {
        fs1 = i + 100
      } else {
        fs1 = i + 1000
      }
      ps.setInt(1, fs1)
      ps.setInt(2, i + 10)
      ps.addBatch()
      if (i % 10 == 0) {
        ps.executeBatch()
      }
    }
    ps.executeBatch()

    rscnt = stmt.executeQuery("select count(*) from t3")
    rscnt.next()
    assertEquals(20, rscnt.getInt(1))

    val rs1 = stmt.executeQuery("select * from t3 order by id")
    var i2 = 0
    var no = 0
    while (rs1.next()) {
      if (i2 % 2 == 0) {
        no = i2 + 100
      } else {
        no = i2 + 1000
      }
      assertEquals(i2, rs1.getInt(1))
      assertEquals(no, rs1.getInt(2))
      i2 = i2 + 1
    }

    val query2 = "delete from t3 where id = ?"
    ps = conn.prepareStatement(query2)
    for (i2 <- 0 until 20) {
      ps.setInt(1, i2)
      ps.executeUpdate()
    }

    rscnt = stmt.executeQuery("select count(*) from t3")
    rscnt.next()
    assertEquals(0, rscnt.getInt(1))
  }

  def testPreparedStatementToExecutePutUpdateBulkQueries(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)
    val stmt = conn.createStatement()
    stmt.execute("drop table if exists t4")
    stmt.execute("create table t4(id integer, fs string) using column options" +
        "(key_columns 'id', COLUMN_MAX_DELTA_ROWS '7', BUCKETS '2')")
    var ps: PreparedStatement = null

    val query3 = "put into t4 values(?,?)"
    ps = conn.prepareStatement(query3)
    for (i <- 0 until 20) {
      ps.setInt(1, i)
      ps.setString(2, "t4-Name" + i)
      ps.addBatch()
      if (i % 10 == 0) {
        ps.executeBatch()
      }
    }
    ps.executeBatch()

    var rscnt = stmt.executeQuery("select count(*) from t4")
    rscnt.next()
    assertEquals(20, rscnt.getInt(1))

    val rs3 = stmt.executeQuery("select * from t4 order by id")
    var i = 0
    while (rs3.next()) {
      assertEquals(i, rs3.getInt(1))
      assertEquals("t4-Name" + i, rs3.getString(2))
      i = i + 1
    }

    val query4 = "update t4 set fs = ? where fs = ?"
    ps = conn.prepareStatement(query4)
    var str = ""
    for (i <- 0 until 20) {
      if (i % 2 == 0) {
        str = "t4-Name-2222"
      } else {
        str = "t4-Name-0"
      }
      ps.setString(1, str)
      ps.setString(2, "t4-Name" + i)
      ps.addBatch()
      if (i % 10 == 0) {
        ps.executeBatch()
      }
    }
    ps.executeBatch()

    rscnt = stmt.executeQuery("select count(*) from t4")
    rscnt.next()
    assertEquals(20, rscnt.getInt(1))

    val rs4 = stmt.executeQuery("select * from t4 order by id")
    var i1 = 0
    while (rs4.next()) {
      if (i1 % 2 == 0) {
        str = "t4-Name-2222"
      } else {
        str = "t4-Name-0"
      }
      assertEquals(i1, rs4.getInt(1))
      assertEquals(str, rs4.getString(2))
      i1 = i1 + 1
    }
  }

}