/*
 * Adapted from https://github.com/apache/spark/pull/13899 having the below
 * license.
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
/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

package org.apache.spark.sql.execution.benchmark

import io.snappydata.SnappyFunSuite

import org.apache.spark.SparkConf
import org.apache.spark.memory.SnappyUnifiedMemoryManager
import org.apache.spark.sql._
import org.apache.spark.sql.execution.benchmark.ColumnCacheBenchmark.addCaseWithCleanup
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Benchmark

class ColumnCacheBenchmark extends SnappyFunSuite {

  override protected def newSparkConf(
      addOn: SparkConf => SparkConf = null): SparkConf = {
    val cores = math.min(8, Runtime.getRuntime.availableProcessors())
    val conf = new SparkConf()
        .setIfMissing("spark.master", s"local[$cores]")
        .setAppName("microbenchmark")
    conf.set("snappydata.store.critical-heap-percentage", "95")
    conf.set("snappydata.store.memory-size", "1200m")
    conf.set("spark.memory.manager", classOf[SnappyUnifiedMemoryManager].getName)
    conf.set("spark.serializer", "org.apache.spark.serializer.PooledKryoSerializer")
    conf.set("spark.closure.serializer", "org.apache.spark.serializer.PooledKryoSerializer")
    if (addOn != null) {
      addOn(conf)
    }
    conf
  }

  private lazy val sparkSession = new SparkSession(sc)
  private lazy val snappySession = snc.snappySession


  ignore("cache with randomized keys - insert") {
    benchmarkRandomizedKeys(size = 50000000, queryPath = false)
  }

  test("insert more than 64K data") {
    snc.conf.setConfString(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
    createAndTestBigTable()

    createAndTestTableWithNulls(size = 20000, numCols = 300)
    createAndTestTableWithNulls(size = 100000, numCols = 20)

    snc.conf.setConfString(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key,
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.defaultValueString)
  }

  test("cache with randomized keys - query") {
    benchmarkRandomizedKeys(size = 50000000, queryPath = true)
  }

  private def doGC(): Unit = {
    System.gc()
    System.runFinalization()
    System.gc()
    System.runFinalization()
  }

  /**
   * Benchmark caching randomized keys created from a range.
   */
  private def benchmarkRandomizedKeys(size: Int, queryPath: Boolean,
      numIters: Int = 10, runSparkCaching: Boolean = true): Unit = {
    val benchmark = new Benchmark("Cache random keys", size)
    sparkSession.sql("drop table if exists test")
    snappySession.sql("drop table if exists test")
    val testDF = sparkSession.range(size)
        .selectExpr("id", "floor(rand() * 10000) as k")
    val testDF2 = snappySession.range(size)
        .selectExpr("id", "floor(rand() * 10000) as k")
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
          snappySession.sql(s"create table test (id bigint not null, " +
              s"k bigint not null) using column")
          testDF2.write.insertInto("test")
        }
        if (snappy) {
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


  private def createAndTestBigTable(): Unit = {
    snappySession.sql("drop table if exists wide_table")

    val size = 100
    val num_col = 300
    val str = (1 to num_col).map(i => s" '$i' as C$i")
    val testDF = snappySession.range(size).select(str.map { expr =>
      Column(sparkSession.sessionState.sqlParser.parseExpression(expr))
    }: _*)


    testDF.collect()
    val sql = (1 to num_col).map(i => s"C$i STRING").mkString(",")
    snappySession.sql(s"create table wide_table($sql) using column")
    snappySession.sql(s"create table wide_table1($sql) using column")
    testDF.write.insertInto("wide_table")
    testDF.write.insertInto("wide_table1")

    val uniqDf = snappySession.table("wide_table").dropDuplicates(Array("C1"))
    uniqDf.count()
    // check fallback plans being invoked via API
    uniqDf.show()
    // and also via SQL
    val s = (2 to num_col).map(i => s"last(C$i)").mkString(",")
    snappySession.sql(s"select C1, $s from wide_table group by C1").show()

    val df = snappySession.sql("select *" +
      " from wide_table a , wide_table1 b where a.c1 = b.c1 and a.c1 = '1'")
    df.collect()

    val df0 = snappySession.sql(s"select * from wide_table")
    df0.collect()
    df0.show()

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
      Column(sparkSession.sessionState.sqlParser.parseExpression(expr))
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
