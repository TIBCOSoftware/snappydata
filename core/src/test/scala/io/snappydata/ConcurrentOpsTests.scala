/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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

package io.snappydata

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import io.snappydata.core.TestData
import org.scalatest.Assertions

import org.apache.spark.Logging
import org.apache.spark.sql.{Encoders, SnappySession}

object ConcurrentOpsTests extends Assertions with Logging {

  def testSimpleLockInsert(session: SnappySession): Unit = {
    val tableName = "ColumnTable"
    session.sql(s"drop table if exists $tableName")
    session.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    val rdd = session.sparkContext.parallelize(
      (1 to 2000).map(i => TestData(i, i.toString)))
    val dataDF = session.createDataFrame(rdd)
    dataDF.write.insertInto(tableName)

    val t = new Thread(new Runnable {
      override def run(): Unit = {
        val snc = new SnappySession(session.sparkContext)
        val rdd = session.sparkContext.parallelize(
          (1 to 2000).map(i => TestData(i, i.toString)))

        val dataDF = snc.createDataFrame(rdd)
        import org.apache.spark.sql.snappy._
        dataDF.write.insertInto(tableName)
        dataDF.write.putInto(tableName)
        dataDF.write.deleteFrom(tableName)
      }
    })
    t.start()
    t.join()
    session.sql(s"select * from $tableName").count()
  }

  def testSimpleLockUpdate(session: SnappySession): Unit = {
    val tableName = "ColumnTable"
    session.sql(s"drop table if exists $tableName")
    session.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    val rdd = session.sparkContext.parallelize(
      (1 to 2000).map(i => TestData(i, i.toString)))
    val dataDF = session.createDataFrame(rdd)
    dataDF.write.insertInto(tableName)
    session.sql(s"update $tableName set value='${Thread.currentThread().getId}'")

    val t = new Thread(new Runnable {
      override def run(): Unit = {
        val snc = new SnappySession(session.sparkContext)
        val rdd = session.sparkContext.parallelize(
          (1 to 2000).map(i => TestData(i, i.toString)))

        val dataDF = snc.createDataFrame(rdd)
        snc.sql(s"update $tableName set value='${Thread.currentThread().getId}'")
        dataDF.write.insertInto(tableName)
      }
    })
    t.start()
    t.join()
  }

  def testSimpleLockDeleteFrom(session: SnappySession): Unit = {
    val tableName = "ColumnTable"
    session.sql(s"drop table if exists $tableName")
    session.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    val rdd = session.sparkContext.parallelize(
      (1 to 2000).map(i => TestData(i, i.toString)))
    val dataDF = session.createDataFrame(rdd)
    import org.apache.spark.sql.snappy._
    dataDF.write.putInto(tableName)
    dataDF.write.deleteFrom(tableName)

    val t = new Thread(new Runnable {
      override def run(): Unit = {
        val snc = new SnappySession(session.sparkContext)
        val rdd = session.sparkContext.parallelize(
          (1 to 2000).map(i => TestData(i, i.toString)))

        val dataDF = snc.createDataFrame(rdd)
        import org.apache.spark.sql.snappy._
        dataDF.write.putInto(tableName)
        dataDF.write.deleteFrom(tableName)
      }
    })
    t.start()
    t.join()
  }

  def testSimpleLockPutInto(session: SnappySession): Unit = {
    val tableName = "ColumnTable"
    session.sql(s"drop table if exists $tableName")
    session.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    val rdd = session.sparkContext.parallelize(
      (1 to 2000).map(i => TestData(i, i.toString)))
    val dataDF = session.createDataFrame(rdd)
    import org.apache.spark.sql.snappy._
    dataDF.write.putInto(tableName)

    val t = new Thread(new Runnable {
      override def run(): Unit = {
        val snc = new SnappySession(session.sparkContext)
        val rdd = session.sparkContext.parallelize(
          (1 to 2000).map(i => TestData(i, i.toString)))

        val dataDF = snc.createDataFrame(rdd)
        import org.apache.spark.sql.snappy._
        dataDF.write.putInto(tableName)
        dataDF.write.deleteFrom(tableName)
      }
    })
    t.start()
    t.join()
  }

  def testConcurrentPutInto(snc: SnappySession): Unit = {
    val tableName = "ColumnTable"
    snc.sql(s"drop table if exists $tableName")
    snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    val doPut = (n: Int) => Future {
      val newSnc = new SnappySession(snc.sparkContext)
      // test both local caching and distributed caching
      val dataDF = if ((n & 0x1) != 0) {
        val rdd = newSnc.sparkContext.parallelize(
          (1 to 2000).map(i => TestData(i, i.toString)))
        newSnc.createDataFrame(rdd)
      } else {
        newSnc.createDataset((1 to 2000).map(i => TestData(i, i.toString)))(Encoders.product)
      }
      import org.apache.spark.sql.snappy._
      dataDF.write.putInto(tableName)
      val result = newSnc.sql("SELECT * FROM " + tableName)
      val r2 = result.collect()
      assert(r2.length == 2000)
    }

    val putTasks = Array.tabulate(10)(doPut)
    putTasks.foreach(Await.result(_, Duration.Inf))

    val putTasks2 = Array.tabulate(5)(doPut)
    putTasks2.foreach(Await.result(_, Duration.Inf))

    val result = snc.sql("SELECT * FROM " + tableName)
    val r2 = result.collect()
    assert(r2.length == 2000)

    logInfo("Successful")
  }

  def testConcurrentUpdate(session: SnappySession): Unit = {
    val tableName = "ColumnTable"
    session.sql(s"drop table if exists $tableName")
    session.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    val rdd = session.sparkContext.parallelize(
      (1 to 2000).map(i => TestData(i, i.toString)))
    val dataDF = session.createDataFrame(rdd)
    dataDF.write.insertInto(tableName)
    val result = session.sql("SELECT * FROM " + tableName)
    val r2 = result.collect()
    assert(r2.length == 2000)

    val doUpdate = () => Future {
      val snc = new SnappySession(session.sparkContext)
      snc.sql(s"update $tableName set value='${Thread.currentThread().getId}'")
    }

    val putTasks = Array.fill(10)(doUpdate())
    putTasks.foreach(Await.result(_, Duration.Inf))

    val r3 = result.collect()
    assert(r3.length == 2000)

    logInfo("Successful")
  }

  def testConcurrentDelete(session: SnappySession): Unit = {
    val tableName = "ColumnTable"
    session.sql(s"drop table if exists $tableName")
    session.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")


    val rdd = session.sparkContext.parallelize(
      (1 to 2000).map(i => TestData(i, i.toString)))
    val dataDF = session.createDataFrame(rdd)
    dataDF.write.insertInto(tableName)
    val result = session.sql("SELECT * FROM " + tableName)
    val r2 = result.collect()
    assert(r2.length == 2000)

    val doDelete = () => Future {
      val snc = new SnappySession(session.sparkContext)
      snc.sql("delete FROM " + tableName)
      val result = snc.sql("SELECT * FROM " + tableName)
      val r2 = result.collect()
      assert(r2.length == 0)
    }

    val putTasks = Array.fill(10)(doDelete())
    putTasks.foreach(Await.result(_, Duration.Inf))

    val r3 = session.sql("SELECT * FROM " + tableName).collect()
    assert(r3.length == 0)

    logInfo("Successful")
  }

  def testConcurrentPutIntoUpdate(session: SnappySession): Unit = {
    val tableName = "ColumnTable"
    session.sql(s"drop table if exists $tableName")

    session.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    val doPut = (n: Int) => Future {
      val snc = new SnappySession(session.sparkContext)
      // test both local caching and distributed caching
      val dataDF = if ((n & 0x1) != 0) {
        val rdd = snc.sparkContext.parallelize(
          (1 to 2000).map(i => TestData(i, i.toString)))
        snc.createDataFrame(rdd)
      } else {
        snc.createDataset((1 to 2000).map(i => TestData(i, i.toString)))(Encoders.product)
      }
      import org.apache.spark.sql.snappy._
      dataDF.write.putInto(tableName)
      val result = snc.sql("SELECT * FROM " + tableName)
      val r2 = result.collect()
      assert(r2.length == 2000)
    }

    val doUpdate = () => Future {
      val snc = new SnappySession(session.sparkContext)
      snc.sql(s"update $tableName set value='${Thread.currentThread().getId}'")
    }

    val putTasks = Array.tabulate(6)(doPut)
    val putTasks2 = Array.fill(5)(doUpdate())
    putTasks.foreach(Await.result(_, Duration.Inf))
    putTasks2.foreach(Await.result(_, Duration.Inf))

    val result = session.sql("SELECT * FROM " + tableName)
    val r2 = result.collect()
    assert(r2.length == 2000)

    logInfo("Successful")
  }

  def testAllOpsConcurrent(session: SnappySession): Unit = {
    val tableName = "ColumnTable"
    session.sql(s"drop table if exists $tableName")

    session.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    val doInsert = (n: Int) => Future {
      val snc = new SnappySession(session.sparkContext)
      // test both local and distributed DFs
      val dataDF = if ((n & 0x1) != 0) {
        val rdd = snc.sparkContext.parallelize(
          (1 to 2000).map(i => TestData(i, i.toString)))
        snc.createDataFrame(rdd)
      } else {
        snc.createDataset((1 to 2000).map(i => TestData(i, i.toString)))(Encoders.product)
      }
      import org.apache.spark.sql.snappy._
      dataDF.write.putInto(tableName)
      val result = snc.sql("SELECT * FROM " + tableName)
      val r2 = result.collect()
      assert(r2.length % 2000 == 0)
    }

    val doPut = (n: Int) => Future {
      val snc = new SnappySession(session.sparkContext)
      // test both local caching and distributed caching
      val dataDF = if ((n & 0x1) != 0) {
        val rdd = snc.sparkContext.parallelize(
          (1 to 2000).map(i => TestData(i, i.toString)))
        snc.createDataFrame(rdd)
      } else {
        snc.createDataset((1 to 2000).map(i => TestData(i, i.toString)))(Encoders.product)
      }
      import org.apache.spark.sql.snappy._
      dataDF.write.putInto(tableName)
      val result = snc.sql("SELECT * FROM " + tableName)
      val r2 = result.collect()
      assert(r2.length % 2000 == 0)
    }

    val doUpdate = () => Future {
      val snc = new SnappySession(session.sparkContext)
      snc.sql(s"update $tableName set value='${Thread.currentThread().getId}'")
      val result = snc.sql("SELECT * FROM " + tableName)
      val r2 = result.collect()
      assert(r2.length % 2000 == 0)
    }

    val doDelete = () => Future {
      val snc = new SnappySession(session.sparkContext)
      snc.sql("delete FROM " + tableName)
      val result = snc.sql("SELECT * FROM " + tableName)
      val r2 = result.collect()
      assert(r2.length % 2000 == 0)
    }

    val insertTasks = Array.tabulate(6)(doInsert)
    val putTasks = Array.tabulate(6)(doPut)
    val updateTasks = Array.fill(5)(doUpdate())
    val deleteTasks = Array.fill(5)(doDelete())

    putTasks.foreach(Await.result(_, Duration.Inf))
    insertTasks.foreach(Await.result(_, Duration.Inf))
    deleteTasks.foreach(Await.result(_, Duration.Inf))
    updateTasks.foreach(Await.result(_, Duration.Inf))

    val result = session.sql("SELECT * FROM " + tableName)
    val r2 = result.collect()
    assert(r2.length % 2000 == 0)

    logInfo("Successful")
  }

  def testConcurrentPutIntoMultipleTables(session: SnappySession): Unit = {

    val tableName = "ColumnTable"
    session.sql(s"drop table if exists $tableName")
    val tableName2 = "ColumnTable2"
    session.sql(s"drop table if exists $tableName2")
    val tableName3 = "ColumnTable3"
    session.sql(s"drop table if exists $tableName3")
    val tableName4 = "ColumnTable4"
    session.sql(s"drop table if exists $tableName4")

    session.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    session.sql(s"CREATE TABLE $tableName2(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    session.sql(s"CREATE TABLE $tableName3(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    session.sql(s"CREATE TABLE $tableName4(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    val doPut = (table: String, n: Int) => Future {
      val snc = new SnappySession(session.sparkContext)
      // test both local caching and distributed caching
      val dataDF = if ((n & 0x1) != 0) {
        val rdd = snc.sparkContext.parallelize(
          (1 to 2000).map(i => TestData(i, i.toString)))
        snc.createDataFrame(rdd)
      } else {
        snc.createDataset((1 to 2000).map(i => TestData(i, i.toString)))(Encoders.product)
      }
      import org.apache.spark.sql.snappy._
      dataDF.write.putInto(table)
    }


    val putTasks = Array.tabulate(5)(doPut(tableName, _))
    val putTasks2 = Array.tabulate(5)(doPut(tableName2, _))
    val putTasks3 = Array.tabulate(5)(doPut(tableName3, _))
    val putTasks4 = Array.tabulate(5)(doPut(tableName4, _))


    putTasks.foreach(Await.result(_, Duration.Inf))
    putTasks2.foreach(Await.result(_, Duration.Inf))
    putTasks3.foreach(Await.result(_, Duration.Inf))
    putTasks4.foreach(Await.result(_, Duration.Inf))

    Seq(tableName, tableName2, tableName3, tableName4).foreach(table => {
      val result = session.sql("SELECT * FROM " + table).collect()
      assert(result.length == 2000)
    })
    logInfo("Successful")
  }

  def testConcurrentDeleteFromMultipleTables(session: SnappySession): Unit = {
    val tableName = "ColumnTable"
    session.sql(s"drop table if exists $tableName")
    val tableName2 = "ColumnTable2"
    session.sql(s"drop table if exists $tableName2")
    val tableName3 = "ColumnTable3"
    session.sql(s"drop table if exists $tableName3")
    val tableName4 = "ColumnTable4"
    session.sql(s"drop table if exists $tableName4")

    session.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    session.sql(s"CREATE TABLE $tableName2(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    session.sql(s"CREATE TABLE $tableName3(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    session.sql(s"CREATE TABLE $tableName4(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    val doInsert = (table: String) => Future {
      val snc = new SnappySession(session.sparkContext)
      val rdd = session.sparkContext.parallelize(
        (1 to 2000).map(i => TestData(i, i.toString)))
      val dataDF = snc.createDataFrame(rdd)
      dataDF.write.insertInto(table)
      val result = snc.sql("SELECT * FROM " + table)
      val r2 = result.collect()
      assert(r2.length == 2000)
    }

    val inserts = Seq(tableName, tableName2, tableName3, tableName4).map(doInsert(_))
    inserts.foreach(Await.result(_, Duration.Inf))

    var counter = new AtomicInteger(0)

    val doDelete = (table: String, maxKey: Int, n: Int) => Future {
      val snc = new SnappySession(session.sparkContext)
      // test both local and distributed DFs
      val dataDF = if ((n & 0x1) != 0) {
        val rdd = snc.sparkContext.parallelize(
          (1 to 2000).map(i => TestData(i, i.toString)))
        snc.createDataFrame(rdd)
      } else {
        snc.createDataset((1 to 2000).map(i => TestData(i, i.toString)))(Encoders.product)
      }
      import org.apache.spark.sql.snappy._
      logInfo(s"SKSK for table $table the count is " + dataDF.filter(s"key1 <= $maxKey").count())
      dataDF.filter(s"key1 <= $maxKey").write.deleteFrom(table)

      val result = snc.sql("SELECT * FROM " + table)
      val r2 = result.collect()
      logInfo(s"SKSK The size of $table is ${r2.length}")
    }

    val delTasks = Array.tabulate(5)(doDelete(tableName, counter.addAndGet(500), _))
    counter = new AtomicInteger(0)
    val delTasks2 = Array.tabulate(5)(doDelete(tableName2, counter.addAndGet(500), _))
    counter = new AtomicInteger(0)
    val delTasks3 = Array.tabulate(5)(doDelete(tableName3, counter.addAndGet(500), _))
    counter = new AtomicInteger(0)
    val delTasks4 = Array.tabulate(5)(doDelete(tableName4, counter.addAndGet(500), _))

    delTasks.foreach(Await.result(_, Duration.Inf))
    delTasks2.foreach(Await.result(_, Duration.Inf))
    delTasks3.foreach(Await.result(_, Duration.Inf))
    delTasks4.foreach(Await.result(_, Duration.Inf))

    Seq(tableName, tableName2, tableName3, tableName4).foreach(table => {
      val result = session.sql("SELECT * FROM " + table).collect()
      assert(result.length == 0)
    })
    logInfo("Successful")
  }
}
