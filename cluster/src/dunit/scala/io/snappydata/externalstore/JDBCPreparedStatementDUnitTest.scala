/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

import java.sql.{PreparedStatement, SQLException}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{CyclicBarrier, Executors, TimeoutException}

import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Try

import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.AvailablePortHelper
import org.junit.Assert
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
        val records = ps.executeBatch()
        records.foreach(r => numRows += r)
      }
    }
    val records = ps.executeBatch()
    records.foreach(r => numRows += r)
    (1, numRows)
  }

  def updateRecords(val1: Int, val2: Int): (Int, Int) = {
    var numRows = 0
    var ps: PreparedStatement = null
    val conn = getANetConnection(netPort1)
    val query1 = "update t3 set fs = ? where fs = ?"
    ps = conn.prepareStatement(query1)
    for (i <- val1 to val2) {
      ps.setString(1, "temp" + i)
      ps.setString(2, "str" + i)
      ps.addBatch()
      if (i % 10 == 0) {
        val records = ps.executeBatch()
        records.foreach(r => numRows += r)
      }
    }
    val records = ps.executeBatch()
    records.foreach(r => numRows += r)
    (1, numRows)
  }

  def deleteRecords(val1: Int, val2: Int): (Int, Int) = {
    var numRows = 0
    var ps: PreparedStatement = null
    val conn = getANetConnection(netPort1)
    val query2 = "delete from t3 where fs = ?"
    ps = conn.prepareStatement(query2)
    for (i2 <- val1 to val2) {
      ps.setString(1, "temp" + i2)
      ps.addBatch()
      if (i2 % 10 == 0) {
        val records = ps.executeBatch()
        records.foreach(r => numRows += r)
      }
    }
    val records = ps.executeBatch()
    records.foreach(r => numRows += r)
    (1, numRows)
  }

  def testComplexDataTypes(): Unit = {
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)
    val stmt = conn.createStatement()
    stmt.execute("drop table if exists t4")
    try {
      stmt.execute(
        """create table t4(col1 Array<String>, col2 int,  col3 Map<Int,Boolean>,
          | col4 Struct<f1:float,f2:int,f3:short>) USING column
          |  options(buckets '2', COLUMN_MAX_DELTA_ROWS '1')""".stripMargin)

      stmt.execute("insert into t4 select Array('11','3','4'), 1, Map(1,true), Struct(15.4f,34,4)")
      stmt.execute("insert into t4 select null, 2, Map(1,true), null")
      stmt.execute("insert into t4 select Array('11','3','4'), 3, null, Struct(15.4f,34,4)")
      val rs = stmt.executeQuery("select * from t4 order by col2")

      assert(rs.next())
      assertEquals("{\"col_0\":[\"11\",\"3\",\"4\"]}", rs.getString(1))
      assertEquals(1, rs.getInt(2))
      assertEquals("{\"col_1\":{\"1\":true}}", rs.getString(3))
      assertEquals("{\"col_2\":{\"f1\":15.4,\"f2\":34,\"f3\":4}}", rs.getString(4))

      assert(rs.next())
      assertEquals(null, rs.getString(1))
      assertEquals(2, rs.getInt(2))
      assertEquals("{\"col_1\":{\"1\":true}}", rs.getString(3))
      assertEquals(null, rs.getString(4))

      assert(rs.next())
      assertEquals("{\"col_0\":[\"11\",\"3\",\"4\"]}", rs.getString(1))
      assertEquals(3, rs.getInt(2))
      assertEquals(null, rs.getString(3))
      assertEquals("{\"col_2\":{\"f1\":15.4,\"f2\":34,\"f3\":4}}", rs.getString(4))

      assert(!rs.next())
    } finally {
      stmt.execute("drop table if exists t4")
    }
  }

  def testConcurrentBatchDmlQueriesUsingPreparedStatement(): Unit = {
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)
    val stmt = conn.createStatement()
    stmt.execute("drop table if exists t3")
    stmt.execute("create table t3(id integer, fs string) using column options" +
        "(COLUMN_MAX_DELTA_ROWS '7', BUCKETS '2')")

    val insertedRecords = new AtomicInteger(0)
    val colThread1 = new Thread(new Runnable {
      def run(): Unit = {
        (1 to 5) foreach { _ =>
          val result = insertRecords(1, 10)
          insertedRecords.getAndAdd(result._2)
        }
      }
    })
    colThread1.start()

    val colThread2 = new Thread(new Runnable {
      def run(): Unit = {
        (1 to 5) foreach { _ =>
          val result = insertRecords(11, 20)
          insertedRecords.getAndAdd(result._2)
        }
      }
    })
    colThread2.start()

    colThread1.join()
    colThread2.join()

    conn.commit()
    var rscnt = stmt.executeQuery("select count(*) from t3")
    rscnt.next()
    assertEquals(100, rscnt.getInt(1))
    assertEquals(100, insertedRecords.get())

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

    val updatedRecords = new AtomicInteger(0)
    val colThread3 = new Thread(new Runnable {
      def run(): Unit = {
        (1 to 5) foreach { _ =>
          val result = updateRecords(1, 20)
          updatedRecords.getAndAdd(result._2)
        }
      }
    })
    colThread3.start()

    val colThread4 = new Thread(new Runnable {
      def run(): Unit = {
        (1 to 5) foreach { _ =>
          val result = updateRecords(11, 20)
          updatedRecords.getAndAdd(result._2)
        }
      }
    })
    colThread4.start()

    val colThread5 = new Thread(new Runnable {
      def run(): Unit = {
        (1 to 5) foreach { _ =>
          val result = updateRecords(21, 30)
          updatedRecords.getAndAdd(result._2)
        }
      }
    })
    colThread5.start()

    colThread3.join()
    colThread4.join()
    colThread5.join()

    rscnt = stmt.executeQuery("select count(*) from t3")
    rscnt.next()
    assertEquals(100, rscnt.getInt(1))
    assertEquals(100, updatedRecords.get())

    val rs1 = stmt.executeQuery("select * from t3 order by id")
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

    val deletedRecords = new AtomicInteger(0)
    val colThread6 = new Thread(new Runnable {
      def run(): Unit = {
        (1 to 5) foreach { _ =>
          val result = deleteRecords(1, 20)
          deletedRecords.getAndAdd(result._2)
        }
      }
    })
    colThread6.start()

    val colThread7 = new Thread(new Runnable {
      def run(): Unit = {
        (1 to 5) foreach { _ =>
          val result = deleteRecords(11, 20)
          deletedRecords.getAndAdd(result._2)
        }
      }
    })
    colThread7.start()

    val colThread8 = new Thread(new Runnable {
      def run(): Unit = {
        (1 to 5) foreach { _ =>
          val result = deleteRecords(21, 30)
          deletedRecords.getAndAdd(result._2)
        }
      }
    })
    colThread8.start()

    colThread6.join()
    colThread7.join()
    colThread8.join()

    rscnt = stmt.executeQuery("select count(*) from t3")
    rscnt.next()
    assertEquals(0, rscnt.getInt(1))
    assertEquals(100, deletedRecords.get())
  }

  def testQueryCancellation(): Unit = {
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)
    val stmt = conn.createStatement()
    val table = "t4"
    stmt.execute("drop table if exists " + table)
    // Populating table with large amount of data so that the select query will run for
    // significantly long duration.
    stmt.execute(
      s"""create table $table (col1 int, col2  int) using column as
         |select id as col1, id as col2 from range(10000000)""".stripMargin)
    val barrier = new CyclicBarrier(2)
    try {
      implicit val context: ExecutionContextExecutor =
        ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor())
      val f = Future {
        println("Firing select...")
        try {
          barrier.await()
          stmt.executeQuery(s"select avg(col1) from $table group by col2 order by col2")
          println("Firing select... Done.")
          // Assert.fail("The query execution should have cancelled.")
        } catch {
          case e: SQLException =>
            val expectedMessage = "The statement has been cancelled due to a user request."
            assert("XCL56".equals(e.getSQLState) && e.getMessage.contains(expectedMessage))
        }
      }
      barrier.await()
      // wait for select query submission
      Thread.sleep(1000)
      println("Firing cancel")
      stmt.cancel()
      println("Firing cancel... Done")

      import scala.concurrent.duration._
      println("Awaiting result of the future.")
      try {
        Await.result(f, 60.seconds)
      } catch {
        case _: TimeoutException => Assert.fail("Query didn't get cancelled in stipulated time.")
      }
    } finally {
      stmt.execute(s"drop table if exists $table")
      Try(stmt.close())
      conn.close()
    }
  }
}
