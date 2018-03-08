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
        SortedColumnTests.verfiyInsertDataExists(session, numElements)
        SortedColumnTests.verfiyUpdateDataExists(session, numElements)
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
    benchmarkQuery(snc, colTableName, numBuckets, numElements, numIters,
      "PointQuery")(executeQuery_PointQuery)
    // while (true) {}
  }

  test("RangeQuery performance") {
    val snc = this.snc.snappySession
    val colTableName = "colDeltaTable"
    val numElements = 9999551
    val numBuckets = cores
    val numIters = 21
    benchmarkQuery(snc, colTableName, numBuckets, numElements, numIters,
      "RangeQuery")(executeQuery_RangeQuery)
    // while (true) {}
  }

  var lastFailedIteration: Int = Int.MinValue

  def executeQuery_PointQuery(session: SnappySession, benchmark: QueryBenchmark,
      colTableName: String, numIters: Int, iterCount: Int): Boolean = {
    val param = if (iterCount != lastFailedIteration) {
      SortedColumnPerformanceTests.getParam(iterCount,
        SortedColumnPerformanceTests.params)
    } else benchmark.firstRandomValue
    val query = s"select * from $colTableName where id = $param"
    val expectedNumResults = 1
    val result = session.sql(query).collect()
    val passed = result.length === expectedNumResults
    if (!passed && lastFailedIteration == -1) {
      lastFailedIteration = iterCount
    }
    // scalastyle:off
    // println(s"Query = $query result=${result.length}")
    // scalastyle:on
    passed
  }

  def executeQuery_RangeQuery(session: SnappySession, benchmark: QueryBenchmark,
      colTableName: String, numIters: Int, iterCount: Int): Boolean = {
    val param1 = if (iterCount != lastFailedIteration) {
      SortedColumnPerformanceTests.getParam(iterCount,
        SortedColumnPerformanceTests.params1)
    } else benchmark.firstRandomValue
    val param2 = if (iterCount != lastFailedIteration) {
      SortedColumnPerformanceTests.getParam(iterCount,
        SortedColumnPerformanceTests.params2)
    } else benchmark.secondRandomValue
    val (low, high) = if (param1 < param2) { (param1, param2)} else (param2, param1)
    val query = s"select * from $colTableName where id between $low and $high"
    val expectedNumResults = high - low + 1
    val result = session.sql(query).collect()
    val passed = result.length === expectedNumResults
    if (!passed && lastFailedIteration == -1) {
      lastFailedIteration = iterCount
    }
    // scalastyle:off
    // println(s"Query = $query result=${result.length}")
    // scalastyle:on
    passed
  }

  def benchmarkQuery(session: SnappySession, colTableName: String, numBuckets: Int,
      numElements: Long, numIters: Int, queryMark: String, doVerifyFullSize: Boolean = false,
      numTimesInsert: Int = 1, numTimesUpdate: Int = 1)
      (f : (SnappySession, QueryBenchmark, String, Int, Int) => Boolean): Unit = {
    val benchmark = new QueryBenchmark(s"Benchmark $queryMark", numElements,
      outputPerIteration = true)
    SortedColumnTests.verfiyInsertDataExists(session, numElements, numTimesInsert)
    SortedColumnTests.verfiyUpdateDataExists(session, numElements)
    val insertDF = session.read.load(SortedColumnTests.filePathInsert(numElements, numTimesInsert))
    val updateDF = session.read.load(SortedColumnTests.filePathUpdate(numElements))

    def addBenchmark(name: String, params: Map[String, String] = Map()): Unit = {
      val defaults = params.keys.flatMap {
        k => session.conf.getOption(k).map((k, _))
      }

      def prepare(): Unit = {
        params.foreach { case (k, v) => session.conf.set(k, v) }
        SortedColumnTests.verfiyInsertDataExists(session, numElements, numTimesInsert)
        SortedColumnTests.verfiyUpdateDataExists(session, numElements)
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

      SortedColumnPerformanceTests.addCaseWithCleanup(benchmark, name, numIters,
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

object SortedColumnPerformanceTests {

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

  val params = Array  (1748981, 521261, 932953, 8855876, 6213481, 7497521, 7063387, 6908865,
    4666582, 6493780, 7522471, 8617087, 3195550, 4790161, 292940, 3170210, 2963200, 4481357,
    9874906, 378370, 7303872, 9766688, 8851182, 4770273, 3568512, 7986913, 9033644, 7809670,
    9008007, 632935, 4714841, 8622943, 7078912, 9858132, 4009212, 560532, 55314, 1469933,
    7724720, 8906016, 734710, 8394979, 8448291, 6396324, 6036375, 9776527, 3496425, 5845993,
    5996891, 5966411, 3430005, 6294156, 4712711, 8026640, 7347798, 9366221, 667155, 5560304,
    2479895, 5099551, 4225090, 4248452, 4841571, 4611993, 4363580, 8272673, 6329953, 4432732,
    5262377, 8260924, 621702, 4330873, 7574409, 379220, 4981152, 9570474, 9184751, 6483674,
    9742252, 8549523, 7446628, 2813292, 3200422, 8886971, 9846161, 2103312, 2012965, 1885533,
    6084932, 3881321, 9211413, 8306575, 9982050, 7330093, 7419325, 1699405, 9785377, 1004950,
    5666421, 4129766)

  val params1 = Array(5003237, 8216891, 5215953, 147475, 6720184, 9131449, 1711876, 635681,
    7828721, 6458443, 5107480, 5869009, 8509160, 2063669, 469304, 1833691, 7481021, 7162603,
    9761242, 9447476, 8565115)
  val params2 = Array(9897441, 9883536, 9843223, 9624340, 9874827, 9667476, 9565207, 9879844,
    9520205, 9648435, 9999052, 9529024, 9661119, 9979787, 9770410, 9986959, 9399090, 9367289,
    9863085, 9963517, 9741129)

  def getParam(iterCount: Int, arr: Array[Int]): Int = {
    val index = if (iterCount < 0) 0 else iterCount % arr.length
    arr(index)
  }
}
