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
/*
 * Some initial code adapted from https://github.com/apache/spark/pull/13899 having
 * the below license.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.benchmark

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import io.snappydata.SnappyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.execution.benchmark.ColumnCacheBenchmark.addCaseWithCleanup
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Benchmark

class ColumnCacheBenchmark extends SnappyFunSuite {

  private val cores = math.min(16, Runtime.getRuntime.availableProcessors())

  override def beforeAll(): Unit = {
    super.beforeAll()
    stopAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    stopAll()
  }

  override protected def newSparkConf(
      addOn: SparkConf => SparkConf = null): SparkConf =
    TAQTest.newSparkConf(addOn)

  private lazy val sparkSession = new SparkSession(sc)
  private lazy val snappySession = snc.snappySession

  ignore("cache with randomized keys - insert") {
    benchmarkRandomizedKeys(size = 50000000, queryPath = false)
  }

  test("cache with randomized keys - query") {
    benchmarkRandomizedKeys(size = 50000000, queryPath = true)
  }

  ignore("PutInto Vs Insert") {
    benchMarkForPutIntoColumnTable(size = 50000000)
  }

  test("Performance and validity check for SNAP-2118") {
    val snappy = this.snappySession
    snappy.sql("DROP TABLE IF EXISTS TABLE1")
    snappy.sql(
      """
        |create  table TABLE1(
        |        id integer,
        |        month integer,
        |        val1 decimal(28,10),
        |        name varchar(200),
        |        val_name varchar(200),
        |        year integer,
        |        type_id integer,
        |        val_id integer)
        |using column options (partition_by 'id')
      """.stripMargin)

    snappy.sql("DROP TABLE IF EXISTS TABLE2")
    snappy.sql(
      """
        |CREATE  TABLE TABLE2(
        |        role_id INTEGER,
        |        id INTEGER,
        |        group_id INTEGER,
        |        group_name VARCHAR(200),
        |        name2 VARCHAR(200))
        |USING COLUMN OPTIONS (PARTITION_BY 'id', COLOCATE_WITH 'TABLE1')
      """.stripMargin)

    snappy.sql("DROP TABLE IF EXISTS TABLE3")
    snappy.sql(
      """
        |CREATE  TABLE TABLE3(
        |        type_id INTEGER,
        |        target_name VARCHAR(200),
        |        factor DECIMAL(32,16))
        |USING COLUMN OPTIONS (PARTITION_BY 'type_id');
      """.stripMargin)

    val numVals = 100
    val numTypes = 100
    val numRoles = 100
    val numGroups = 100
    val numNames = 1000
    val numIds = 5000

    val numIters = 10

    val numElems1 = 12 * numVals * numIds
    val numElems2 = 4 * numIds
    val numElems3 = numTypes * numTypes

    var ds1 = snappy.range(numElems1).selectExpr(s"(id % $numIds) as id",
      s"cast((id / ($numVals * $numIds)) as int) as month",
      "cast ((rand() * 100.0) as decimal(28, 10)) as val1",
      s"concat('cmd_', cast((id % $numNames) as string)) as name",
      s"concat('val_', cast(cast((id / (12 * $numIds)) as int) as string)) as val_name",
      "((id % 2) + 2014) as year", s"(id % $numTypes) type_id", s"(id % $numVals) val_id")
    ds1.cache()
    ds1.count()
    ds1.write.insertInto("TABLE1")

    val ds2 = snappy.range(numElems2).selectExpr(s"cast((rand() * $numRoles) as int)",
      s"id % $numIds", s"id % $numGroups", "concat('grp_', cast((id % 100) as string))",
      "concat('site_', cast((id % 1000) as string))")
    ds2.write.insertInto("TABLE2")

    val ds3 = snappy.range(numElems3).selectExpr(s"id % $numTypes",
      s"concat('type_', cast(cast((id / $numTypes) as int) as string))", "rand() * 100.0")
    ds3.write.insertInto("TABLE3")

    val sql = "select b.group_name, a.name, " +
        "sum(a.val1 * c.factor) " +
        "from TABLE1 a, TABLE2 b, TABLE3 c " +
        "where a.id = b.id and a.year = 2015 and " +
        "a.val_name like 'val\\_42%' and b.role_id = 99 and c.type_id = a.type_id and " +
        "c.target_name = 'type_36' group by b.group_name, a.name"

    val benchmark = new Benchmark("SNAP-2118 with random data", numElems1)

    var expectedResult: Array[Row] = null
    benchmark.addCase("smj", numIters, () => snappy.sql("set snappydata.hashJoinSize=-1"),
      () => {}) { i =>
      if (i == 1) expectedResult = snappy.sql(sql).collect()
      else snappy.sql(sql).collect()
    }
    benchmark.addCase("hash", numIters, () => snappy.sql("set snappydata.hashJoinSize=1g"),
      () => {}) { i =>
      if (i == 1) ColumnCacheBenchmark.collect(snappy.sql(sql), expectedResult)
      else snappy.sql(sql).collect()
    }
    benchmark.run()

    // also check with null values and updates (SNAP-2088)

    snappy.truncateTable("table1")
    // null values every 8th row
    ds1 = ds1.selectExpr("id", "month", "(case when (id & 7) = 0 then null else val1 end) val1",
      "name", "(case when (id & 7) = 0 then null else val_name end) as val_name",
      "year", "type_id", "(case when (id & 7) = 0 then null else val_id end) val_id")
    ds1.createOrReplaceTempView("TABLE1_TEMP1")
    ds1.write.insertInto("table1")

    expectedResult = snappy.sql(sql.replace("TABLE1", "TABLE1_TEMP1")).collect()
    ColumnCacheBenchmark.collect(snappy.sql(sql), expectedResult)

    // even more null values every 4th row but on TABLE1 these are set using update
    ds1 = ds1.selectExpr("id", "month", "(case when (id & 3) = 0 then null else val1 end) val1",
      "name", "(case when (id & 3) = 0 then null else val_name end) as val_name",
      "year", "type_id", "(case when (id & 3) = 0 then null else val_id end) val_id")
    ds1.createOrReplaceTempView("TABLE1_TEMP2")

    expectedResult = snappy.sql(sql.replace("TABLE1", "TABLE1_TEMP2")).collect()
    snappy.sql("update table1 set val1 = null, val_name = null, val_id = null where (id & 3) = 0")
    ColumnCacheBenchmark.collect(snappy.sql(sql), expectedResult)

    // more update statements but these don't change anything rather change to same value
    snappy.sql("update table1 set val1 = case when (id & 3) = 0 then null else val1 end, " +
        "val_name = case when (id & 3) = 0 then null else val_name end, " +
        "val_id = case when (id & 3) = 0 then null else val_id end where (id % 10) = 0")
    ColumnCacheBenchmark.collect(snappy.sql(sql), expectedResult)

    snappy.sql("DROP TABLE IF EXISTS TABLE3")
    snappy.sql("DROP TABLE IF EXISTS TABLE2")
    snappy.sql("DROP TABLE IF EXISTS TABLE1")
  }

  test("insert more than 64K data") {
    snc.conf.setConfString(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
    createAndTestBigTable()

    createAndTestTableWithNulls(size = 20000, numCols = 300)
    createAndTestTableWithNulls(size = 100000, numCols = 20)

    snc.conf.setConfString(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key,
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.defaultValueString)
  }

  test("PutInto wide column table") {
    snc.conf.setConfString(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
    createAndTestPutIntoInBigTable()
  }

  private def doGC(): Unit = {
    System.gc()
    System.runFinalization()
    System.gc()
    System.runFinalization()
  }

  private def benchMarkForPutIntoColumnTable(size: Int, numIters: Int = 10): Unit = {
    val benchmark = new Benchmark("PutInto Vs Insert", size)
    val sparkSession = this.sparkSession
    val snappySession = this.snappySession
    import org.apache.spark.sql.snappy._
    if (GemFireCacheImpl.getCurrentBufferAllocator.isDirect) {
      logInfo("ColumnCacheBenchmark: using off-heap for performance comparison")
    } else {
      logInfo("ColumnCacheBenchmark: using heap for performance comparison")
    }

    val testDF2 = snappySession.range(size)
        .selectExpr("id", "(rand() * 1000.0) as k")

    def prepare(): Unit = {
      doGC()
      sparkSession.sql("drop table if exists test")
      snappySession.sql("create table test (id bigint not null, k double not null) " +
          s"using column options(partition_by 'id', buckets '$cores', key_columns 'id')")
    }

    def cleanup(): Unit = {
      snappySession.sql("drop table if exists test")
      doGC()
    }

    def testCleanup(): Unit = {
      snappySession.sql("truncate table if exists test")
    }

    // As expected putInto is two times slower than a simple insert
    addCaseWithCleanup(benchmark, "Insert", numIters, prepare, cleanup, testCleanup) { _ =>
      testDF2.write.insertInto("test")
    }
    addCaseWithCleanup(benchmark, "PutInto", numIters, prepare, cleanup, testCleanup) { _ =>
      testDF2.write.putInto("test")
    }
    benchmark.run()
  }

  /**
   * Benchmark caching randomized keys created from a range.
   */
  private def benchmarkRandomizedKeys(size: Int, queryPath: Boolean,
      numIters: Int = 10, runSparkCaching: Boolean = true): Unit = {
    val benchmark = new Benchmark("Cache random keys", size)
    val sparkSession = this.sparkSession
    val snappySession = this.snappySession
    if (GemFireCacheImpl.getCurrentBufferAllocator.isDirect) {
      logInfo("ColumnCacheBenchmark: using off-heap for performance comparison")
    } else {
      logInfo("ColumnCacheBenchmark: using heap for performance comparison")
    }
    sparkSession.sql("drop table if exists test")
    snappySession.sql("drop table if exists test")
    val testDF = sparkSession.range(size)
        .selectExpr("id", "(rand() * 1000.0) as k")
    val testDF2 = snappySession.range(size)
        .selectExpr("id", "(rand() * 1000.0) as k")
    testDF.createOrReplaceTempView("test")

    val query = "select avg(k), avg(id) from test"
    val expectedAnswer = sparkSession.sql(query).collect().toSeq
    val expectedAnswer2 = testDF2.selectExpr("avg(k)", "avg(id)").collect().toSeq

    /**
     * Add a benchmark case, optionally specifying whether to cache the dataset.
     */
    def addBenchmark(name: String, cache: Boolean, params: Map[String, String] = Map(),
        snappy: Boolean = false): Unit = {
      val defaults = params.keys.flatMap { k => sparkSession.conf.getOption(k).map((k, _)) }
      val defaults2 = params.keys.flatMap { k => snappySession.conf.getOption(k).map((k, _)) }

      def prepare(): Unit = {
        params.foreach { case (k, v) => sparkSession.conf.set(k, v) }
        params.foreach { case (k, v) => snappySession.conf.set(k, v) }
        sparkSession.catalog.clearCache()
        doGC()
        if (cache) {
          testDF.createOrReplaceTempView("test")
          sparkSession.catalog.cacheTable("test")
        } else if (snappy) {
          snappySession.sql("drop table if exists test")
          snappySession.sql("create table test (id bigint not null, k double not null) " +
              s"using column options(buckets '$cores')")
          testDF2.write.insertInto("test")
        }
        if (snappy) {
          snappySession.sql("set snappydata.linkPartitionsToBuckets=true")
          val results = snappySession.sql("select count(*), spark_partition_id() " +
              "from test group by spark_partition_id()").collect().toSeq
          snappySession.sql("set snappydata.linkPartitionsToBuckets=false")
          val counts = results.map(_.getLong(0))
          // expect the counts to not vary by more than 800k (max 200k per batch)
          val min = counts.min
          val max = counts.max
          assert(max - min <= 800000, "Unexpectedly large data skew: " +
              results.map(r => s"${r.getInt(1)}=${r.getLong(0)}").mkString(","))
          // check for SNAP-2200 by forcing overflow with updates
          snappySession.sql("update test set id = id + 1")
          snappySession.sql("update test set k = k + 1.0")
          ColumnCacheBenchmark.collect(snappySession.sql(
            "select max(id), min(id) from test"), Seq(Row(size, 1L)))
          // repopulate for the benchmark test
          snappySession.sql("truncate table test")
          testDF2.write.insertInto("test")
          ColumnCacheBenchmark.collect(snappySession.sql(query), expectedAnswer2)
        } else {
          ColumnCacheBenchmark.collect(sparkSession.sql(query), expectedAnswer)
        }
        testCleanup()
      }

      def cleanup(): Unit = {
        defaults.foreach { case (k, v) => sparkSession.conf.set(k, v) }
        defaults2.foreach { case (k, v) => snappySession.conf.set(k, v) }
        sparkSession.catalog.clearCache()
        snappySession.sql("drop table if exists test")
        doGC()
      }

      def testCleanup(): Unit = {
        if (!queryPath) {
          if (snappy) {
            snappySession.sql("truncate table if exists test")
          } else {
            sparkSession.catalog.clearCache()
          }
          doGC()
        }
      }

      addCaseWithCleanup(benchmark, name, numIters, prepare, cleanup, testCleanup) { _ =>
        if (queryPath) {
          if (snappy) {
            ColumnCacheBenchmark.collect(snappySession.sql(query), expectedAnswer2)
          } else {
            ColumnCacheBenchmark.collect(sparkSession.sql(query), expectedAnswer)
          }
        } else {
          // also benchmark the time it takes to build the column buffers
          if (snappy) {
            testDF2.write.insertInto("test")
          } else {
            if (cache) {
              sparkSession.catalog.cacheTable("test")
              sparkSession.sql("select count(*) from test").collect()
            }
          }
        }
      }
    }

    // Benchmark cases:
    //   (1) Caching with defaults
    //   (2) Caching with SnappyData column batches with defaults

    if (runSparkCaching) {
      addBenchmark("cache = T", cache = true, Map.empty)
    }
    addBenchmark("snappy = T", cache = false, Map.empty, snappy = true)

    benchmark.run()
  }

  private def createAndTestPutIntoInBigTable(): Unit = {
    snappySession.sql("drop table if exists wide_table")
    snappySession.sql("drop table if exists wide_table1")
    import org.apache.spark.sql.snappy._
    val size = 100
    val num_col = 300
    val str = (1 to num_col).map(i => s" '$i' as C$i")
    val testDF = snappySession.range(size).select(str.map { expr =>
      Column(snappySession.sessionState.sqlParser.parseExpression(expr))
    }: _*)


    testDF.collect()
    val sql = (1 to num_col).map(i => s"C$i STRING").mkString(",")
    snappySession.sql(s"create table wide_table($sql) " +
        s" using column options(key_columns 'C2,C3')")
    snappySession.sql(s"create table wide_table1($sql) " +
        s" using column options()")
    // Creating another table for Range related issue SNAP-2142
    testDF.write.insertInto("wide_table")
    testDF.write.insertInto("wide_table1")
    val tableDF = snappySession.table("wide_table1")
    tableDF.write.putInto("wide_table")
  }

  private def createAndTestBigTable(): Unit = {
    snappySession.sql("drop table if exists wide_table")

    val size = 100
    val num_col = 300
    val str = (1 to num_col).map(i => s" '$i' as C$i")
    val testDF = snappySession.range(size).select(str.map { expr =>
      Column(snappySession.sessionState.sqlParser.parseExpression(expr))
    }: _*)


    testDF.collect()
    val sql = (1 to num_col).map(i => s"C$i STRING").mkString(",")
    snappySession.sql(s"create table wide_table($sql) using column")
    snappySession.sql(s"create table wide_table1($sql) using column")
    testDF.write.insertInto("wide_table")
    testDF.write.insertInto("wide_table1")

    val uniqDf = snappySession.table("wide_table").dropDuplicates(Array("C1"))
    logInfo("Number of unique rows in wide_table = " + uniqDf.count())
    // check fallback plans being invoked via API
    logInfo(uniqDf.collect().mkString("\n"))
    // and also via SQL
    val s = (2 to num_col).map(i => s"last(C$i)").mkString(",")
    snappySession.sql(s"select C1, $s from wide_table group by C1").collect()

    val df = snappySession.sql("select *" +
        " from wide_table a , wide_table1 b where a.c1 = b.c1 and a.c1 = '1'")
    df.collect()

    val df0 = snappySession.sql(s"select * from wide_table")
    df0.collect()

    val avgProjections = (1 to num_col).map(i => s"AVG(C$i)").mkString(",")
    val df1 = snappySession.sql(s"select $avgProjections from wide_table")
    df1.collect()

    val df2 = snappySession.sql(s"select $avgProjections from wide_table where C1 = '1' ")
    df2.collect()
  }

  private def createAndTestTableWithNulls(size: Int, numCols: Int): Unit = {
    snappySession.sql("drop table if exists nulls_table")

    val str = (1 to numCols).map(i =>
      s" (case when rand() < 0.5 then null else '$i' end) as C$i")
    val testDF = snappySession.range(size).select(str.map { expr =>
      Column(snappySession.sessionState.sqlParser.parseExpression(expr))
    }: _*)

    val sql = (1 to numCols).map(i => s"C$i STRING").mkString(",")
    snappySession.sql(s"create table nulls_table($sql) using column")
    testDF.write.insertInto("nulls_table")

    assert(snappySession.sql(s"select count(*) from nulls_table")
        .collect()(0).getLong(0) == size)
  }
}

object ColumnCacheBenchmark {

  /**
   * Collect a [[Dataset[Row]] and check whether the collected result matches
   * the expected answer.
   */
  def collect(df: Dataset[Row], expectedAnswer: Seq[Row]): Unit = {
    QueryTest.checkAnswer(df, expectedAnswer, checkToRDD = false) match {
      case Some(errMessage) => throw new RuntimeException(errMessage)
      case None => // all good
    }
  }

  def applySchema(df: Dataset[Row], newSchema: StructType): Dataset[Row] = {
    df.sqlContext.internalCreateDataFrame(df.queryExecution.toRdd, newSchema)
  }

  def addCaseWithCleanup(
      benchmark: Benchmark,
      name: String,
      numIters: Int = 0,
      prepare: () => Unit,
      cleanup: () => Unit,
      testCleanup: () => Unit,
      testPrepare: () => Unit = () => Unit)(f: Int => Unit): Unit = {
    val timedF = (timer: Benchmark.Timer) => {
      testPrepare()
      timer.startTiming()
      f(timer.iteration)
      timer.stopTiming()
      testCleanup()
    }
    benchmark.benchmarks += Benchmark.Case(name, timedF, numIters, prepare, cleanup)
  }
}
