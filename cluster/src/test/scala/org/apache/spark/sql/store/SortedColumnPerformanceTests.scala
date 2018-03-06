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

package org.apache.spark.sql.store

import io.snappydata.Property

import org.apache.spark.SparkConf
import org.apache.spark.memory.SnappyUnifiedMemoryManager
import org.apache.spark.sql.execution.benchmark.ColumnCacheBenchmark
import org.apache.spark.sql.execution.columnar.ColumnTableScan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.SnappySession
import org.apache.spark.util.{Benchmark, QueryBenchmark}
import org.apache.spark.sql.snappy._

/**
 * Tests for column table having sorted columns.
 */
class SortedColumnPerformanceTests extends ColumnTablesTestBase {

  val cores: Int = math.min(16, Runtime.getRuntime.availableProcessors())

  override def newSparkConf(addOn: SparkConf => SparkConf = null): SparkConf = {
    val conf = new SparkConf()
        .setIfMissing("spark.master", s"local[$cores]")
        .setAppName("microbenchmark")
    conf.set("snappydata.store.critical-heap-percentage", "95")
    if (SnappySession.isEnterpriseEdition) {
      conf.set("snappydata.store.memory-size", "1200m")
    }
    conf.set("spark.memory.manager", classOf[SnappyUnifiedMemoryManager].getName)
    conf.set("spark.serializer", "org.apache.spark.serializer.PooledKryoSerializer")
    conf.set("spark.closure.serializer", "org.apache.spark.serializer.PooledKryoSerializer")
    if (addOn != null) {
      addOn(conf)
    }
    conf
  }

  private def doGC(): Unit = {
    System.gc()
    System.runFinalization()
    System.gc()
    System.runFinalization()
  }

  test("insert performance") {
    val snc = this.snc.snappySession
    val colTableName = "colDeltaTable"
    val numElements = 9999551
    val numBuckets = cores
    val numIters = 2

    benchmarkInsert(snc, colTableName, numBuckets, numElements, numIters, "insert")
  }

  def benchmarkInsert(session: SnappySession, colTableName: String, numBuckets: Int,
      numElements: Long, numIters: Int, queryMark: String,
      doVerifyFullSize: Boolean = false): Unit = {
    val benchmark = new Benchmark(s"Benchmark $queryMark", numElements, outputPerIteration = true)
    val insertDF = session.read.load(SortedColumnTests.filePathInsert(numElements))
    val updateDF = session.read.load(SortedColumnTests.filePathUpdate(numElements))

    def execute(): Unit = {
      insertDF.write.insertInto(colTableName)
      try {
        ColumnTableScan.setCaseOfSortedInsertValue(true)
        // To force SMJ
        session.conf.set(Property.HashJoinSize.name, "-1")
        session.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
        updateDF.write.putInto(colTableName)
      } finally {
        ColumnTableScan.setCaseOfSortedInsertValue(false)
        session.conf.unset(Property.HashJoinSize.name)
        session.conf.unset(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key)
      }
    }

    def addBenchmark(name: String, params: Map[String, String] = Map()): Unit = {
      val defaults = params.keys.flatMap {
        k => session.conf.getOption(k).map((k, _))
      }

      def prepare(): Unit = {
        params.foreach { case (k, v) => session.conf.set(k, v) }
        SortedColumnTests.verfiyInsertDataExists(numElements, session)
        SortedColumnTests.verfiyUpdateDataExists(numElements, session)
        SortedColumnTests.createColumnTable(session, colTableName, numBuckets, numElements)
        doGC()
      }

      def cleanup(): Unit = {
        SnappySession.clearAllCache()
        defaults.foreach { case (k, v) => session.conf.set(k, v) }
        doGC()
      }

      def testCleanup(): Unit = {
        session.sql(s"truncate table $colTableName")
        doGC()
      }

      ColumnCacheBenchmark.addCaseWithCleanup(benchmark, name, numIters,
        prepare, cleanup, testCleanup) { _ => execute() }
    }

    try {
      session.conf.set(Property.ColumnMaxDeltaRows.name, "100")
      session.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
      session.conf.set(SQLConf.WHOLESTAGE_FALLBACK.key, "false")

      // Get numbers
      addBenchmark(s"$queryMark", Map.empty)
      benchmark.run()

      // Now verify
      if (doVerifyFullSize) {
        execute()
        SortedColumnTests.verifyTotalRows(session, colTableName, numElements, finalCall = true)
      }
    } finally {
      session.sql(s"drop table $colTableName")
      session.conf.unset(Property.ColumnBatchSize.name)
      session.conf.unset(Property.ColumnMaxDeltaRows.name)
    }
  }

  test("PointQuery performance") {
    val snc = this.snc.snappySession
    val colTableName = "colDeltaTable"
    val numElements = 9999551
    val numBuckets = cores
    val numIters = 100
    failedCount = 0
    benchmarkQuery(snc, colTableName, numBuckets, numElements, numIters,
      "PointQuery")(executeQuery_PointQuery)
    // scalastyle:off
    println(s"Failed query count = $failedCount out of $numIters")
    // scalastyle:on
  }

  test("RangeQuery performance") {
    val snc = this.snc.snappySession
    val colTableName = "colDeltaTable"
    val numElements = 9999551
    val numBuckets = cores
    val numIters = 10
    failedCount = 0
    benchmarkQuery(snc, colTableName, numBuckets, numElements, numIters,
      "RangeQuery")(executeQuery_RangeQuery)
    // scalastyle:off
    println(s"Failed query count = $failedCount out of $numIters")
    // scalastyle:on
  }

  var failedCount = 0

  def executeQuery_PointQuery(session: SnappySession, benchmark: QueryBenchmark,
      colTableName: String, numIters: Int, iterCount: Int): Boolean = {
    val param = benchmark.firstRandomValue
    val query = s"select * from $colTableName where id = $param"
    // scalastyle:off
    // println(s"Query = $query")
    // scalastyle:on
    val expectedNumResults = 1
    val result = session.sql(query).collect()
    val passed = result.length === expectedNumResults
    if (!passed) {
      failedCount += 1
    }
    passed
  }

  def executeQuery_RangeQuery(session: SnappySession, benchmark: QueryBenchmark,
      colTableName: String, numIters: Int, iterCount: Int): Boolean = {
    val (low, high) = if (benchmark.firstRandomValue < benchmark.secondRandomValue) {
      (benchmark.firstRandomValue, benchmark.secondRandomValue)
    } else (benchmark.secondRandomValue, benchmark.firstRandomValue)
    val query = s"select * from $colTableName where id between $low and $high"
    // scalastyle:off
    // println(s"Query = $query")
    // scalastyle:on
    val expectedNumResults = high - low + 1
    val result = session.sql(query).collect()
    val passed = result.length === expectedNumResults
    if (!passed) {
      failedCount += 1
    }
    passed
  }

  def benchmarkQuery(session: SnappySession, colTableName: String, numBuckets: Int,
      numElements: Long, numIters: Int, queryMark: String, doVerifyFullSize: Boolean = false)
      (f : (SnappySession, QueryBenchmark, String, Int, Int) => Boolean): Unit = {
    val benchmark = new QueryBenchmark(numElements, s"Benchmark $queryMark", numElements,
      outputPerIteration = true)
    val insertDF = session.read.load(SortedColumnTests.filePathInsert(numElements))
    val updateDF = session.read.load(SortedColumnTests.filePathUpdate(numElements))

    def addBenchmark(name: String, params: Map[String, String] = Map()): Unit = {
      val defaults = params.keys.flatMap {
        k => session.conf.getOption(k).map((k, _))
      }

      def prepare(): Unit = {
        params.foreach { case (k, v) => session.conf.set(k, v) }
        SortedColumnTests.verfiyInsertDataExists(numElements, session)
        SortedColumnTests.verfiyUpdateDataExists(numElements, session)
        SortedColumnTests.createColumnTable(session, colTableName, numBuckets, numElements)
        insertDF.write.insertInto(colTableName)
        try {
          ColumnTableScan.setCaseOfSortedInsertValue(true)
          // To force SMJ
          session.conf.set(Property.HashJoinSize.name, "-1")
          session.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
          updateDF.write.putInto(colTableName)
        } finally {
          ColumnTableScan.setCaseOfSortedInsertValue(false)
          session.conf.unset(Property.HashJoinSize.name)
          session.conf.unset(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key)
        }
        if (doVerifyFullSize) {
          SortedColumnTests.verifyTotalRows(session, colTableName, numElements, finalCall = true)
        }
        doGC()
      }

      def cleanup(): Unit = {
        SnappySession.clearAllCache()
        defaults.foreach { case (k, v) => session.conf.set(k, v) }
        doGC()
      }

      def testCleanup(): Unit = {
        doGC()
      }

      SortedColumnPerformanceBenchmark.addCaseWithCleanup(benchmark, name, numIters,
        prepare, cleanup, testCleanup) { i => f(session, benchmark, colTableName, numIters, i)}
    }

    try {
      session.conf.set(Property.ColumnMaxDeltaRows.name, "100")
      session.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
      session.conf.set(SQLConf.WHOLESTAGE_FALLBACK.key, "false")

      // Get numbers
      addBenchmark(s"$queryMark", Map.empty)
      benchmark.run()
    } finally {
      session.sql(s"drop table $colTableName")
      session.conf.unset(Property.ColumnBatchSize.name)
      session.conf.unset(Property.ColumnMaxDeltaRows.name)
    }
  }
}

object SortedColumnPerformanceBenchmark {

  def addCaseWithCleanup(
      benchmark: QueryBenchmark,
      name: String,
      numIters: Int = 0,
      prepare: () => Unit,
      cleanup: () => Unit,
      testCleanup: () => Unit,
      testPrepare: () => Unit = () => Unit)(f: Int => Boolean): Unit = {
    val timedF = (timer: Benchmark.Timer) => {
      testPrepare()
      timer.startTiming()
      val ret = f(timer.iteration)
      timer.stopTiming()
      testCleanup()
      ret
    }
    benchmark.benchmarks += QueryBenchmark.Case(name, timedF, numIters, prepare, cleanup)
  }
}
