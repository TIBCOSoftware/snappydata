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
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, QueryTest, Row, SparkSession}
import org.apache.spark.util.Benchmark


class ColumnCacheBenchmark extends SnappyFunSuite {

  override protected def newSparkConf(
      addOn: SparkConf => SparkConf = null): SparkConf = {
    val conf = new SparkConf()
        .setMaster("local[*]")
        .setAppName("microbenchmark")
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

  test("cache with randomized keys - query") {
    benchmarkRandomizedKeys(size = 50000000, queryPath = true)
  }

  def addCaseWithCleanup(
      benchmark: Benchmark,
      name: String,
      numIters: Int = 0,
      prepare: () => Unit,
      cleanup: () => Unit,
      testCleanup: () => Unit)(f: Int => Unit): Unit = {
    val timedF = (timer: Benchmark.Timer) => {
      timer.startTiming()
      f(timer.iteration)
      timer.stopTiming()
      testCleanup()
    }
    benchmark.benchmarks += Benchmark.Case(name, timedF, numIters, prepare, cleanup)
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
}
