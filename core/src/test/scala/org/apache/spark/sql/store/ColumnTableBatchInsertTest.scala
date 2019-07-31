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
package org.apache.spark.sql.store

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import io.snappydata.SnappyFunSuite
import io.snappydata.core.{Data, TestData}
import org.scalatest.{Assertions, BeforeAndAfter}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.sql._
import org.apache.spark.{Logging, SparkContext}

class ColumnTableBatchInsertTest extends SnappyFunSuite
    with Logging
    with BeforeAndAfter {

  val tableName: String = "ColumnTable"
  val tableName2: String = "ColumnTable2"
  val tableName3: String = "ColumnTable3"
  val tableName4: String = "ColumnTable4"

  val props = Map.empty[String, String]

  after {
    snc.dropTable(tableName, ifExists = true)
    snc.dropTable(tableName2, ifExists = true)
    snc.dropTable(tableName3, ifExists = true)
    snc.dropTable(tableName4, ifExists = true)
  }

  test("test the shadow table creation") {
    snc.sql(s"DROP TABLE IF EXISTS $tableName")
    val df = snc.sql(s"CREATE TABLE $tableName(Col1 INT ,Col2 INT, Col3 INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Col1'," +
        "BUCKETS '1')")

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.insertInto(tableName)
    val r2 = result.collect
    assert(r2.length == 5)
    logInfo("Successful")
  }

  test("test the overwrite table after reading itself") {
    snc.sql(s"DROP TABLE IF EXISTS $tableName")
    val df = snc.sql(s"CREATE TABLE $tableName(Col1 INT ,Col2 INT, Col3 INT) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Col1'," +
      "BUCKETS '1')")
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.insertInto(tableName)

    val result = snc.sql("SELECT * FROM " + tableName)

    try {
      result.write.format("column").mode(SaveMode.Overwrite).saveAsTable(tableName)
      fail("Expected AnalysisException while overwriting table which is also being read from")
    }
    catch {
      case ae: AnalysisException => assert(ae.getMessage().contains("Cannot overwrite table"))
      case t: Throwable => fail("Unexpected Exception ", t)
    }
    try {
      result.write.format("column").mode(SaveMode.Overwrite).saveAsTable(tableName)
      fail("Expected AnalysisException while overwriting table which is also being read from")
    }
    catch {
      case ae: AnalysisException => assert(ae.getMessage().contains("Cannot overwrite table"))
      case t: Throwable => fail("Unexpected Exception ", t)
    }

  }


  test("test the shadow table creation heavy insert") {
    // snc.sql(s"DROP TABLE IF EXISTS $tableName")

    val df = snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "BUCKETS '1')")

    // val r = result.collect
    // assert(r.length == 0)

    var rdd = sc.parallelize(
      (1 to 10).map(i => TestData(i, i.toString)))
    var rd2 = rdd.repartition(1)
    val dataDF = snc.createDataFrame(rd2)
    dataDF.write.insertInto(tableName)
    val result = snc.sql("SELECT * FROM " + tableName)

    val t = new Thread(new Runnable() {
      override def run(): Unit = {
        rdd = sc.parallelize(
          (11 to 20).map(i => TestData(i, i.toString)))
        rd2 = rdd.repartition(1)
        val dataDF = snc.createDataFrame(rd2)
        dataDF.write.insertInto(tableName)
        val result = snc.sql("SELECT * FROM " + tableName)
        val r2 = result.collect

        assert(r2.length == 20)
      }
    })
    t.start()
    t.join()

    val r2 = result.collect
    assert(r2.length == 20)
    logInfo("Successful")
  }

  test("test the concurrent putInto") {
    // snc.sql(s"DROP TABLE IF EXISTS $tableName")

    val df = snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    import scala.concurrent.ExecutionContext.Implicits.global
    val doPut = () => Future {
      val snc = new SnappySession(sc)
      val rdd = snc.sparkContext.parallelize(
        (1 to 200000).map(i => TestData(i, i.toString)))
      logInfo(s"SKKS The total parallelism is ${rdd.getNumPartitions}")
      val dataDF = snc.createDataFrame(rdd)
      import org.apache.spark.sql.snappy._
      dataDF.write.putInto(tableName)
      val result = snc.sql("SELECT * FROM " + tableName)
      val r2 = result.collect
      assert(r2.length == 200000)
    }

    snc.sparkContext.addSparkListener(new BasicJobCounter)

    val putTasks = Array.fill(10)(doPut())
    putTasks.foreach(Await.result(_, Duration.Inf))

    val putTasks2 = Array.fill(5)(doPut())
    putTasks2.foreach(Await.result(_, Duration.Inf))

    val result = snc.sql("SELECT * FROM " + tableName)
    val r2 = result.collect
    assert(r2.length == 200000)

    logInfo("Successful")
  }

  private class BasicJobCounter extends SparkListener {
    var count = 0

    override def onJobStart(job: SparkListenerJobStart): Unit = {
      this.synchronized {
        assert(count == 0)
        logInfo(s"SK The concurrent job count in jobstart is $count , job is $job")
        count += 1
      }
    }

    override def onJobEnd(job: SparkListenerJobEnd): Unit = {
      this.synchronized {
        count -= 1
        logInfo(s"SK The concurrent job counton jobend is $count , job is $job")
      }
    }
  }

  test("test the concurrent update") {
    // snc.sql(s"DROP TABLE IF EXISTS $tableName")

    val df = snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    val rdd = snc.sparkContext.parallelize(
      (1 to 200000).map(i => TestData(i, i.toString)))
    val dataDF = snc.createDataFrame(rdd)
    dataDF.write.insertInto(tableName)
    val result = snc.sql("SELECT * FROM " + tableName)
    val r2 = result.collect
    assert(r2.length == 200000)

    import scala.concurrent.ExecutionContext.Implicits.global

    val doUpdate = () => Future {
      val snc = new SnappySession(sc)
      val rdd = snc.sparkContext.parallelize(
        (1 to 200000).map(i => TestData(i, i.toString)))
      logInfo(s"SKKS The total parallelism is ${rdd.getNumPartitions}")
      val dataDF = snc.createDataFrame(rdd)
      snc.sql(s"update ${tableName} set value='${Thread.currentThread().getId}'")
    }

    sc.addSparkListener(new BasicJobCounter)

    val putTasks = Array.fill(10)(doUpdate())
    putTasks.foreach(Await.result(_, Duration.Inf))

    val r3 = result.collect
    assert(r3.length == 200000)

    logInfo("Successful")
  }

  test("test the concurrent deleteFrom") {
    // snc.sql(s"DROP TABLE IF EXISTS $tableName")

    val df = snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")


    val rdd = snc.sparkContext.parallelize(
      (1 to 200000).map(i => TestData(i, i.toString)))
    val dataDF = snc.createDataFrame(rdd)
    dataDF.write.insertInto(tableName)
    val result = snc.sql("SELECT * FROM " + tableName)
    val r2 = result.collect
    assert(r2.length == 200000)

    import scala.concurrent.ExecutionContext.Implicits.global
    val doDelete = () => Future {
      val snc = new SnappySession(sc)
      snc.sql("delete FROM " + tableName)
      val result = snc.sql("SELECT * FROM " + tableName)
      val r2 = result.collect
      assert(r2.length == 0)
    }
    sc.addSparkListener(new BasicJobCounter)

    val putTasks = Array.fill(10)(doDelete())
    putTasks.foreach(Await.result(_, Duration.Inf))

    val r3 = snc.sql("SELECT * FROM " + tableName).collect()
    assert(r3.length == 0)

    logInfo("Successful")
  }


  test("test the concurrent putInto/update") {
    // snc.sql(s"DROP TABLE IF EXISTS $tableName")

    val df = snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    import scala.concurrent.ExecutionContext.Implicits.global
    val doPut = () => Future {
      val snc = new SnappySession(sc)
      val rdd = snc.sparkContext.parallelize(
        (1 to 200000).map(i => TestData(i, i.toString)))
      logInfo(s"SKKS The total parallelism is ${rdd.getNumPartitions}")
      val dataDF = snc.createDataFrame(rdd)
      import org.apache.spark.sql.snappy._
      dataDF.write.putInto(tableName)
      val result = snc.sql("SELECT * FROM " + tableName)
      val r2 = result.collect
      assert(r2.length == 200000)
    }

    val doUpdate = () => Future {
      val snc = new SnappySession(sc)
      val rdd = snc.sparkContext.parallelize(
        (1 to 200000).map(i => TestData(i, i.toString)))
      logInfo(s"SKKS The total parallelism is ${rdd.getNumPartitions}")
      val dataDF = snc.createDataFrame(rdd)
      snc.sql(s"update ${tableName} set value='${Thread.currentThread().getId}'")
    }

    sc.addSparkListener(new BasicJobCounter)
    val putTasks = Array.fill(5)(doPut())
    val putTasks2 = Array.fill(5)(doUpdate())
    putTasks.foreach(Await.result(_, Duration.Inf))
    putTasks2.foreach(Await.result(_, Duration.Inf))

    val result = snc.sql("SELECT * FROM " + tableName)
    val r2 = result.collect
    assert(r2.length == 200000)

    logInfo("Successful")
  }

  test("test the concurrent insert/putInto/update/deleteFrom") {
    // snc.sql(s"DROP TABLE IF EXISTS $tableName")

    val df = snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    import scala.concurrent.ExecutionContext.Implicits.global

    val doInsert = () => Future {
      val snc = new SnappySession(sc)
      val rdd = snc.sparkContext.parallelize(
        (1 to 200000).map(i => TestData(i, i.toString)))
      logInfo(s"SKKS The total parallelism is ${rdd.getNumPartitions}")
      val dataDF = snc.createDataFrame(rdd)
      import org.apache.spark.sql.snappy._
      dataDF.write.putInto(tableName)
      val result = snc.sql("SELECT * FROM " + tableName)
      val r2 = result.collect
      assert(r2.length % 200000 == 0)
    }

    val doPut = () => Future {
      val snc = new SnappySession(sc)
      val rdd = snc.sparkContext.parallelize(
        (1 to 200000).map(i => TestData(i, i.toString)))
      logInfo(s"SKKS The total parallelism is ${rdd.getNumPartitions}")
      val dataDF = snc.createDataFrame(rdd)
      import org.apache.spark.sql.snappy._
      dataDF.write.putInto(tableName)
      val result = snc.sql("SELECT * FROM " + tableName)
      val r2 = result.collect
      assert(r2.length % 200000 == 0)
    }

    val doUpdate = () => Future {
      val snc = new SnappySession(sc)
      val rdd = snc.sparkContext.parallelize(
        (1 to 200000).map(i => TestData(i, i.toString)))
      logInfo(s"SKKS The total parallelism is ${rdd.getNumPartitions}")
      val dataDF = snc.createDataFrame(rdd)
      snc.sql(s"update ${tableName} set value='${Thread.currentThread().getId}'")
      val result = snc.sql("SELECT * FROM " + tableName)
      val r2 = result.collect
      assert(r2.length % 200000 == 0)
    }

    val doDelete = () => Future {
      snc.sql("delete FROM " + tableName)
      val result = snc.sql("SELECT * FROM " + tableName)
      val r2 = result.collect
      assert(r2.length % 200000 == 0)
    }

    sc.addSparkListener(new BasicJobCounter)

    val insertTasks = Array.fill(5)(doInsert())
    val putTasks = Array.fill(5)(doPut())
    val updateTasks = Array.fill(5)(doUpdate())
    val deleteTasks = Array.fill(5)(doDelete())

    putTasks.foreach(Await.result(_, Duration.Inf))
    insertTasks.foreach(Await.result(_, Duration.Inf))
    deleteTasks.foreach(Await.result(_, Duration.Inf))
    updateTasks.foreach(Await.result(_, Duration.Inf))

    val result = snc.sql("SELECT * FROM " + tableName)
    val r2 = result.collect
    assert(r2.length % 200000 == 0)

    logInfo("Successful")
  }

  test("test the concurrent putInto in multiple Column tables") {
    // snc.sql(s"DROP TABLE IF EXISTS $tableName")

    snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    snc.sql(s"CREATE TABLE $tableName2(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    snc.sql(s"CREATE TABLE $tableName3(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    snc.sql(s"CREATE TABLE $tableName4(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    import scala.concurrent.ExecutionContext.Implicits.global
    val doPut = (table: String) => Future {
      val snc = new SnappySession(sc)
      val rdd = snc.sparkContext.parallelize(
        (1 to 200000).map(i => TestData(i, i.toString)))
      logInfo(s"SKKS The total parallelism is ${rdd.getNumPartitions}")
      val dataDF = snc.createDataFrame(rdd)
      import org.apache.spark.sql.snappy._
      dataDF.write.putInto(table)
      val result = snc.sql("SELECT * FROM " + table)
      val r2 = result.collect
      assert(r2.length == 200000)
    }


    val putTasks = Array.fill(5)(doPut(tableName))
    val putTasks2 = Array.fill(5)(doPut(tableName2))
    val putTasks3 = Array.fill(5)(doPut(tableName3))
    val putTasks4 = Array.fill(5)(doPut(tableName4))


    putTasks.foreach(Await.result(_, Duration.Inf))
    putTasks2.foreach(Await.result(_, Duration.Inf))
    putTasks3.foreach(Await.result(_, Duration.Inf))
    putTasks4.foreach(Await.result(_, Duration.Inf))

    Seq(tableName, tableName2, tableName3, tableName4).foreach(table => {
      val result = snc.sql("SELECT * FROM " + table).collect()
      assert(result.length == 200000)
    })
    logInfo("Successful")
  }

  // disabled since this hangs in BaseColumnFormatRelation.withTableWriteLock
  ignore("test the concurrent deleteFrom in multiple Column tables") {
    // snc.sql(s"DROP TABLE IF EXISTS $tableName")

    snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    snc.sql(s"CREATE TABLE $tableName2(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    snc.sql(s"CREATE TABLE $tableName3(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    snc.sql(s"CREATE TABLE $tableName4(Key1 INT ,Value STRING) " +
      "USING column " +
      "options " +
      "(" +
      "PARTITION_BY 'Key1'," +
      "KEY_COLUMNS 'Key1'," +
      "BUCKETS '1')")

    import scala.concurrent.ExecutionContext.Implicits.global
    val doPut = (table: String) => Future {
      val rdd = sc.parallelize(
        (1 to 200000).map(i => TestData(i, i.toString)))
      logInfo(s"SKKS The total parallelism is ${rdd.getNumPartitions}")
      val dataDF = snc.createDataFrame(rdd)
      import org.apache.spark.sql.snappy._
      dataDF.write.putInto(table)
      val result = snc.sql("SELECT * FROM " + table)
      val r2 = result.collect
      assert(r2.length == 200000)
    }


    val putTasks = Array.fill(5)(doPut(tableName))
    val putTasks2 = Array.fill(5)(doPut(tableName2))
    val putTasks3 = Array.fill(5)(doPut(tableName3))
    val putTasks4 = Array.fill(5)(doPut(tableName4))


    putTasks.foreach(Await.result(_, Duration.Inf))
    putTasks2.foreach(Await.result(_, Duration.Inf))
    putTasks3.foreach(Await.result(_, Duration.Inf))
    putTasks4.foreach(Await.result(_, Duration.Inf))

    Seq(tableName, tableName2, tableName3, tableName4).foreach(table => {
      val result = snc.sql("SELECT * FROM " + table).collect()
      assert(result.length == 200000)
    })
    logInfo("Successful")
  }


  test("test the shadow table creation without partition by clause") {
    // snc.sql(s"DROP TABLE IF EXISTS $tableName")

    val df = snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING) " +
        "USING column " +
        "options " +
        "(" +
        "BUCKETS '1')")

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)

    val rdd = sc.parallelize(
      (1 to 19999).map(i => TestData(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.insertInto(tableName)
    val r2 = result.collect
    assert(r2.length == 19999)
    logInfo("Successful")
  }

  test("test the shadow table with persistence") {
    // snc.sql(s"DROP TABLE IF EXISTS $tableName")

    val df = snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING)" +
        "USING column " +
        "options " +
        "(" +
        "PERSISTENT 'ASYNCHRONOUS'," +
        "BUCKETS '100')")

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)

    val rdd = sc.parallelize(
      (1 to 19999).map(i => TestData(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.insertInto(tableName)

    val r2 = result.collect
    assert(r2.length == 19999)
    logInfo("Successful")
  }

  test("test the shadow table with eviction") {
    // snc.sql(s"DROP TABLE IF EXISTS $tableName")

    val df = snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING)" +
        "USING column " +
        "options " +
        "(" +
        "BUCKETS '100')")

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)

    val rdd = sc.parallelize(
      (1 to 19999).map(i => TestData(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.insertInto(tableName)
    val r2 = result.collect
    assert(r2.length == 19999)
    logInfo("Successful")
  }

  test("test the shadow table with options on compressed table") {
    val df = snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING)" +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "BUCKETS '213'," +
        "REDUNDANCY '2')")

    val result = snc.sql("SELECT Key1 FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)

    val rdd = sc.parallelize(
      (1 to 19999).map(i => TestData(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.insertInto(tableName)

    val r2 = result.collect

    val r3 = mutable.HashSet[Int]()
    r2.map(i => {
      r3.add(i.getInt(0))
    })

    (1 to 19999).map(i => {
      if (!r3.contains(i)) logInfo(s"Does not contain $i")
    })

    assert(r2.length == 19999)

    logInfo("Successful")
  }

  test("test the shadow table with eviction options on compressed table") {
    val df = snc.sql(s"CREATE TABLE $tableName(Key1 INT ,Value STRING)" +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "BUCKETS '213'," +
        "REDUNDANCY '2'," +
        "EVICTION_BY 'LRUMEMSIZE 200')")

    val result = snc.sql("SELECT Value FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)

    val rdd = sc.parallelize(
      (1 to 19999).map(i => TestData(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.insertInto(tableName)
    val r2 = result.collect
    assert(r2.length == 19999)
    logInfo("Successful")
  }

  test("test create table as select with alias") {
    val rowTable = "rowTable"
    val colTable = "colTable"
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(rowTable, "row", dataDF.schema, props)
    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable(rowTable)

    snc.createTable(colTable, "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(colTable)


    val tempRowTableName = "testRowTable"
    val tempColTableName = "testcolTable"

    snc.sql("DROP TABLE IF EXISTS " + tempRowTableName)
    snc.sql("CREATE TABLE " + tempRowTableName +
        " AS (SELECT col1 as field1,col2 as field2 FROM " + rowTable + ")")
    var testResults1 = snc.sql("SELECT * FROM " + tempRowTableName).collect
    assert(testResults1.length == 5)
    snc.sql("DROP TABLE IF EXISTS " + tempRowTableName)

    snc.sql("DROP TABLE IF EXISTS " + tempRowTableName)
    snc.sql("CREATE TABLE " + tempRowTableName +
        " AS (SELECT col1 as field1,col2 as field2 FROM " + colTable + ")")
    var testResults2 = snc.sql("SELECT * FROM " + tempRowTableName).collect
    assert(testResults2.length == 5)
    snc.sql("DROP TABLE IF EXISTS " + tempRowTableName)


    snc.sql("DROP TABLE IF EXISTS " + tempColTableName)
    snc.sql("CREATE TABLE " + tempColTableName +
        " USING COLUMN OPTIONS() AS (SELECT col1 as field1,col2 as field2 FROM " + rowTable + ")")
    var testResults3 = snc.sql("SELECT * FROM " + tempColTableName).collect
    assert(testResults3.length == 5)
    snc.sql("DROP TABLE IF EXISTS " + tempColTableName)


    snc.sql("DROP TABLE IF EXISTS " + tempColTableName)
    snc.sql("CREATE TABLE " + tempColTableName +
        " USING COLUMN OPTIONS() AS (SELECT col1 as field1,col2 as field2 FROM " + colTable + ")")
    var testResults4 = snc.sql("SELECT * FROM " + tempColTableName).collect
    assert(testResults4.length == 5)
    snc.sql("DROP TABLE IF EXISTS " + tempColTableName)

    snc.sql("DROP TABLE IF EXISTS " + rowTable)
    snc.sql("DROP TABLE IF EXISTS " + colTable)


  }

  test("test table with column name having slash") {
    snc.sql(s"DROP TABLE IF EXISTS $tableName")
    val df = snc.sql("CREATE TABLE ColumnTable(\"a/b\" INT ,Col2 INT, Col3 INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'col2'," +
        "BUCKETS '1')")


    snc.sql("CREATE TABLE rowTable(\"a/b\" INT ,Col2 INT, Col3 INT) " +
        "USING row " +
        "options " +
        "()")
    snc.sql("insert into ColumnTable(\"a/b\",col2,col3) values(1,2,3)")
    snc.sql("insert into rowTable(\"a/b\",col2,col3)values(1,2,3)")
    val result = snc.sql("SELECT col2+1 FROM " + tableName)
    val r = result.collect()
    assert(r.length == 1)

    val result1 = snc.sql("SELECT \"a/b\"/1 FROM " + tableName)
    val r1 = result1.collect()
    snc.sql("SELECT \"a/b\"/1 FROM rowTable").collect()
    assert(r1.length == 1)

    snc.sql("drop table if exists columntable")
    snc.sql("drop table if exists rowtable")
    logInfo("Successful")
  }

  test("Spark caching using SQL") {
    val snappy = this.snc.snappySession
    ColumnTableBatchInsertTest.testSparkCachingUsingSQL(sc, snappy.sql, snappy.catalog.isCached,
      df => snappy.sharedState.cacheManager.lookupCachedData(df).isDefined)
  }
}

object ColumnTableBatchInsertTest extends Assertions {

  def testSparkCachingUsingSQL(sc: SparkContext, executeSQL: String => Dataset[Row],
      isTableCached: String => Boolean, isCached: Dataset[Row] => Boolean): Unit = {
    executeSQL("cache table cachedTable1 as select id, rand() from range(1000000)")
    // check that table has been cached and materialized
    assert(isTableCached("cachedTable1"))
    var rddInfos = sc.ui.get.storageListener.rddInfoList
    assert(rddInfos.length === 1)
    assert(rddInfos.head.name.contains("Range (0, 1000000"))

    assert(executeSQL("select count(*) from cachedTable1").collect()(0).getLong(0) === 1000000)
    rddInfos = sc.ui.get.storageListener.rddInfoList
    assert(rddInfos.length === 1)
    assert(rddInfos.head.name.contains("Range (0, 1000000"))

    executeSQL("uncache table cachedTable1")
    assert(!isTableCached("cachedTable1"))
    rddInfos = sc.ui.get.storageListener.rddInfoList
    assert(rddInfos.length === 0)

    // temporary table should still exist
    assert(executeSQL("select count(*) from cachedTable1").collect()(0).getLong(0) === 1000000)

    executeSQL("cache lazy table cachedTable2 as select id, rand() from range(500000)")
    assert(isTableCached("cachedTable2"))
    // check that cache has not been materialized yet
    rddInfos = sc.ui.get.storageListener.rddInfoList
    assert(rddInfos.length === 0)
    assert(executeSQL("select count(*) from cachedTable2").collect()(0).getLong(0) === 500000)
    rddInfos = sc.ui.get.storageListener.rddInfoList
    assert(rddInfos.length === 1)
    assert(rddInfos.head.name.contains("Range (0, 500000"))

    // drop table directly without explicit uncache should also do it
    val table = executeSQL("select * from cachedTable2")
    executeSQL("drop table cachedTable2")
    assert(!isCached(table))
    rddInfos = sc.ui.get.storageListener.rddInfoList
    assert(rddInfos.length === 0)

    executeSQL("drop table cachedTable1")
  }
}
