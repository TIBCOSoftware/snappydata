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

  val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort

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

  def testPreparedStatementToExecuteInsertUpdateDeleteBulkQueries(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)
    val stmt = conn.createStatement()
    stmt.execute("drop table if exists t3")
    stmt.execute("create table t3(id integer, fs integer) using column options" +
        "(key_columns 'id', COLUMN_MAX_DELTA_ROWS '7', BUCKETS '2')")
    var ps: PreparedStatement = null

    val query = "insert into t3 values(?,?)"
    ps = conn.prepareStatement(query)
    for (i <- 1 to 20) {
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
    var i = 1
    while (rs.next()) {
      assertEquals(i, rs.getInt(1))
      assertEquals(i + 10, rs.getInt(2))
      i = i + 1
    }

    val query1 = "update t3 set fs = ? where fs = ?"
    ps = conn.prepareStatement(query1)
    var fs1 = 0
    for (i <- 1 to 20) {
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
    var i2 = 1
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
    for (i2 <- 1 to 20) {
      ps.setInt(1, i2)
      ps.addBatch()
      if (i2 % 10 == 0) {
        ps.executeBatch()
      }
    }
    ps.executeBatch()

    rscnt = stmt.executeQuery("select count(*) from t3")
    rscnt.next()
    assertEquals(0, rscnt.getInt(1))
  }

  def insertRecords(s: Int, e: Int): (Int, Int) = {
    var numRows = 0
    var ps: PreparedStatement = null
    val conn = getANetConnection(netPort1)
    val query = "insert into t3 values(?,?)"
    ps = conn.prepareStatement(query)
    for (i <- s to e) {
      ps.setInt(1, i)
      ps.setString(2, "str" + i)
      ps.addBatch()
      if (i % 10 == 0) {
        var records = ps.executeBatch()
        numRows += records.length
      }
    }
    var records = ps.executeBatch()
    numRows += records.length
    (1, numRows)
  }

  def updateRecords(val1: Int, val2: Int): (Int, Int) = {
    var numRows = 0
    var ps: PreparedStatement = null
    val conn = getANetConnection(netPort1)
    val query1 = "update t3 set fs = ? where id = ?"
    ps = conn.prepareStatement(query1)
    var fs1 = 1
    for (i <- val1 to val2) {
      ps.setString(1, "temp" + i)
      ps.setInt(2, i)
      ps.addBatch()
      if (i % 10 == 0) {
        var records = ps.executeBatch()
        numRows += records.length
      }
    }
    var records = ps.executeBatch()
    numRows += records.length
    (1, numRows)
  }

  def deleteRecords(val1: Int, val2: Int): (Int, Int) = {
    var numRows = 0
    var ps: PreparedStatement = null
    val conn = getANetConnection(netPort1)
    val query2 = "delete from t3 where id = ?"
    ps = conn.prepareStatement(query2)
    for (i2 <- val1 to val2) {
      ps.setInt(1, i2)
      ps.addBatch()
      if (i2 % 10 == 0) {
        var records = ps.executeBatch()
        numRows += records.length
      }
    }
    var records = ps.executeBatch()
    numRows += records.length
    (1, numRows)
  }

  def testConcurrentBatchDmlQueriesUsingPreparedStatement(): Unit = {
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)
    val stmt = conn.createStatement()
    stmt.execute("drop table if exists t3")
    stmt.execute("create table t3(id integer, fs string) using column options" +
        "(key_columns 'id', COLUMN_MAX_DELTA_ROWS '7', BUCKETS '2')")

    var thrCount1: Integer = 0
    var insertedRecords = 0
    val colThread1 = new Thread(new Runnable {def run() {
      (1 to 5) foreach (i => {
        var result = insertRecords(1, 10)
        thrCount1 += result._1
        insertedRecords += result._2
      })
    }
    })
    colThread1.start()

    var thrCount2: Integer = 0
    val colThread2 = new Thread(new Runnable {def run() {
      (1 to 5) foreach (i => {
        var result = insertRecords(11, 20)
        thrCount2 += result._1
        insertedRecords += result._2
      })
    }
    })
    colThread2.start()

    colThread1.join()
    colThread2.join()

    var rscnt = stmt.executeQuery("select count(*) from t3")
    rscnt.next()
    assertEquals(100, rscnt.getInt(1))
    assertEquals(100, insertedRecords)

    val rs = stmt.executeQuery("select * from t3 order by id")


    var i = 1
    var cnt = 0

    while (rs.next()) {
      if (cnt == 5) {
        i = i + 1
        cnt = 0
      }
      assertEquals(i, rs.getInt(1))
      assertEquals("str" + i, rs.getString(2))
      cnt = cnt + 1
    }

    var thrCount3: Integer = 0
    var updatedRecords = 0
    val colThread3 = new Thread(new Runnable {def run() {
      (1 to 5) foreach (i => {
        var result = updateRecords(1, 20)
        thrCount3 += result._1
        updatedRecords += result._2
      })
    }
    })
    colThread3.start()

    var thrCount4: Integer = 0
    val colThread4 = new Thread(new Runnable {def run() {
      (1 to 5) foreach (i => {
        var result = updateRecords(11, 20)
        thrCount4 += result._1
        updatedRecords += result._2
      })
    }
    })
    colThread4.start()

    var thrCount5: Integer = 0
    val colThread5 = new Thread(new Runnable {def run() {
      (1 to 5) foreach (i => {
        var result = updateRecords(21, 30)
        thrCount5 += result._1
        updatedRecords += result._2
      })
    }
    })
    colThread5.start()

    colThread3.join()
    colThread4.join()
    colThread5.join()


    rscnt = stmt.executeQuery("select count(*) from t3")
    rscnt.next()
    assertEquals(100, rscnt.getInt(1))
    assertEquals(200, updatedRecords)

    var rs1 = stmt.executeQuery("select * from t3 order by id")
    var i2 = 1
    cnt = 0
    while (rs1.next()) {
      if (cnt == 5) {
        i2 = i2 + 1
        cnt = 0
      }
      assertEquals(i2, rs1.getInt(1))
      assertEquals("temp" + i2, rs1.getString(2))
      cnt = cnt + 1
    }

    var thrCount6: Integer = 0
    var deletedRecords = 0
    val colThread6 = new Thread(new Runnable {def run() {
      (1 to 5) foreach (i => {
        var result = deleteRecords(1, 10)
        thrCount6 += result._1
        deletedRecords += result._2
      })
    }
    })
    colThread6.start()

    var thrCount7: Integer = 0
    val colThread7 = new Thread(new Runnable {def run() {
      (1 to 5) foreach (i => {
        var result = deleteRecords(11, 20)
        thrCount7 += result._1
        deletedRecords += result._2
      })
    }
    })
    colThread7.start()

    var thrCount8: Integer = 0
    val colThread8 = new Thread(new Runnable {def run() {
      (1 to 5) foreach (i => {
        var result = deleteRecords(11, 20)
        thrCount8 += result._1
        deletedRecords += result._2
      })
    }
    })
    colThread8.start()

    colThread6.join()
    colThread7.join()
    colThread8.join()

    rscnt = stmt.executeQuery("select count(*) from t3")
    rscnt.next()
    assertEquals(0, rscnt.getInt(1))
    assertEquals(150, deletedRecords)

  }

}