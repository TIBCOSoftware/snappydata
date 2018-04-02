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

import scala.concurrent.duration.FiniteDuration

import io.snappydata.Property

import org.apache.spark.SparkConf
import org.apache.spark.memory.SnappyUnifiedMemoryManager
import org.apache.spark.sql.execution.benchmark.ColumnCacheBenchmark
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{DataFrame, DataFrameReader, SnappySession}
import org.apache.spark.util.{Benchmark, QueryBenchmark}
import org.apache.spark.sql.snappy._
import scala.concurrent.duration._

/**
 * Tests for column table having sorted columns.
 */
class SortedColumnPerformanceTests extends ColumnTablesTestBase {

  override def newSparkConf(addOn: SparkConf => SparkConf = null): SparkConf = {
    val conf = new SparkConf()
        .setIfMissing("spark.master", s"local[${SortedColumnPerformanceTests.cores}]")
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

  test("PointQuery performance") {
    val session = this.snc.snappySession
    val colTableName = "colDeltaTable"
    val numElements = 999551
    val numTimesInsert = 199
    val numTimesUpdate = 1

    val totalElements = (numElements * 0.6 * numTimesUpdate +
        numElements * 0.4 * numTimesUpdate).toLong
    val numBuckets = 4
    val numIters = 1000

    SortedColumnTests.verfiyInsertDataExists(session, numElements, multiple = 1)
    SortedColumnTests.verfiyInsertDataExists(session, numElements, numTimesInsert)
    SortedColumnTests.verfiyUpdateDataExists(session, numElements, numTimesUpdate)
    val dataFrameReader : DataFrameReader = session.read
    val insertDF: DataFrame = dataFrameReader.load(SortedColumnTests.filePathInsert(numElements,
      multiple = 1))
    val updateDF: DataFrame = dataFrameReader.load(SortedColumnTests.filePathUpdate(numElements,
      numTimesUpdate))

    def prepare(): Unit = {
      SortedColumnTests.createColumnTable(session, colTableName, numBuckets, numElements)
      try {
        session.conf.set(Property.ColumnBatchSize.name, "24M") // default
        session.conf.set(Property.ColumnMaxDeltaRows.name, "100")
        var j = 0
        while (j < numTimesInsert) {
          insertDF.write.insertInto(colTableName)
          j += 1
        }
        updateDF.write.putInto(colTableName)
      } finally {
        session.conf.unset(Property.ColumnBatchSize.name)
        session.conf.unset(Property.ColumnMaxDeltaRows.name)
      }
    }

    val benchmark = new Benchmark("PointQuery", totalElements)
    var iter = 1
    benchmark.addCase("Master", numIters, prepare) { _ =>
      SortedColumnPerformanceTests.executeQuery_PointQuery(session, colTableName, iter,
        numTimesInsert, numTimesUpdate = 1)
      iter += 1
    }
    benchmark.run()
    // Thread.sleep(50000000)
  }

  test("JoinQuery performance") {
    val session = this.snc.snappySession
    val colTableName = "colDeltaTable"
    val joinTableName = "joinDeltaTable"
    val numElements = 100000000
    val numTimesInsert = 1
    val numTimesUpdate = 1

    val totalElements = (numElements * 0.6 * numTimesUpdate +
        numElements * 0.4 * numTimesUpdate).toLong
    val numBuckets = 4
    val numIters = 100

    SortedColumnTests.verfiyInsertDataExists(session, numElements, numTimesInsert)
    SortedColumnTests.verfiyUpdateDataExists(session, numElements, numTimesUpdate)
    val dataFrameReader : DataFrameReader = session.read
    val insertDF: DataFrame = dataFrameReader.load(SortedColumnTests.filePathInsert(numElements,
      numTimesInsert))
    val updateDF: DataFrame = dataFrameReader.load(SortedColumnTests.filePathUpdate(numElements,
      numTimesUpdate))

    def prepare(): Unit = {
      SortedColumnTests.createColumnTable(session, colTableName, numBuckets, numElements)
      SortedColumnTests.createColumnTable(session, joinTableName, numBuckets, numElements,
        Some(colTableName))
      try {
        session.conf.set(Property.ColumnBatchSize.name, "24M") // default
        session.conf.set(Property.ColumnMaxDeltaRows.name, "100")
        var j = 0
        while (j < numTimesInsert) {
          insertDF.write.insertInto(colTableName)
          insertDF.write.insertInto(joinTableName)
          j += 1
        }
        j = 0
        while (j < numTimesInsert) {
          updateDF.write.putInto(colTableName)
          updateDF.write.putInto(joinTableName)
          j += 1
        }
      } finally {
        session.conf.unset(Property.ColumnBatchSize.name)
        session.conf.unset(Property.ColumnMaxDeltaRows.name)
      }
    }

    val benchmark = new Benchmark("JoinQuery", totalElements)
    var iter = 1
    benchmark.addCase("Master", numIters, prepare) { _ =>
      SortedColumnPerformanceTests.executeQuery_JoinQuery(session, colTableName, joinTableName,
        iter, numTimesInsert, numTimesUpdate = 1)
      iter += 1
    }
    benchmark.run()
    // Thread.sleep(50000000)
  }

  test("insert performance") {
    val snc = this.snc.snappySession
    val colTableName = "colDeltaTable"
    val numElements = 9999551
    val numBuckets = SortedColumnPerformanceTests.cores
    val numIters = 2

    SortedColumnPerformanceTests.benchmarkInsert(snc, colTableName, numBuckets, numElements,
      numIters, "insert")
  }

  ignore("Old PointQuery performance") {
    val snc = this.snc.snappySession
    val colTableName = "colDeltaTable"
    val numElements = 999551
    val numBuckets = 3
    val numIters = 100
    SortedColumnPerformanceTests.benchmarkQuery(snc, colTableName, numBuckets, numElements,
      numIters, "PointQuery", numTimesInsert = 200,
      doVerifyFullSize = true)(SortedColumnPerformanceTests.executeQuery_PointQuery_mt)
    // Thread.sleep(5000000)
  }

  test("PointQuery performance multithreaded 1") {
    val snc = this.snc.snappySession
    SortedColumnPerformanceTests.mutiThreadedPointQuery(snc, numThreads = 1)
    // Thread.sleep(5000000)
  }

  test("PointQuery performance multithreaded 4") {
    val snc = this.snc.snappySession
    val totalNumThreads = SortedColumnPerformanceTests.cores
    SortedColumnPerformanceTests.mutiThreadedPointQuery(snc, totalNumThreads)
    // Thread.sleep(5000000)
  }

  test("PointQuery performance multithreaded 8") {
    val snc = this.snc.snappySession
    val totalNumThreads = 2 * SortedColumnPerformanceTests.cores
    SortedColumnPerformanceTests.mutiThreadedPointQuery(snc, totalNumThreads)
    // Thread.sleep(5000000)
  }

  test("PointQuery performance multithreaded 16") {
    val snc = this.snc.snappySession
    val totalNumThreads = 4 * SortedColumnPerformanceTests.cores
    SortedColumnPerformanceTests.mutiThreadedPointQuery(snc, totalNumThreads)
    // Thread.sleep(5000000)
  }

  test("PointQuery performance multithreaded 32") {
    val snc = this.snc.snappySession
    val totalNumThreads = 4 * SortedColumnPerformanceTests.cores
    SortedColumnPerformanceTests.mutiThreadedPointQuery(snc, totalNumThreads)
    // Thread.sleep(5000000)
  }

  ignore("Old RangeQuery performance") {
    val snc = this.snc.snappySession
    val colTableName = "colDeltaTable"
    val numElements = 999551
    val numBuckets = 3
    val numIters = 21
    SortedColumnPerformanceTests.benchmarkQuery(snc, colTableName, numBuckets, numElements,
      numIters, "RangeQuery", numTimesInsert = 10,
      doVerifyFullSize = true)(SortedColumnPerformanceTests.executeQuery_RangeQuery_mt)
    // Thread.sleep(5000000)
  }

  ignore("Old JoinQuery performance") {
    val snc = this.snc.snappySession
    val colTableName = "colDeltaTable"
    val jnTableName = "joinDeltaTable"
    val numElements = 999551
    val numBuckets = 3
    val numIters = 1
    SortedColumnPerformanceTests.benchmarkQuery(snc, colTableName, numBuckets, numElements,
      numIters, "JoinQuery", numTimesInsert = 200, doVerifyFullSize = true,
      joinTableName = Some(jnTableName))(SortedColumnPerformanceTests.executeQuery_JoinQuery_mt)
    // Thread.sleep(5000000)
  }
}

object SortedColumnPerformanceTests {
  val cores: Int = math.min(16, Runtime.getRuntime.availableProcessors())

  def executeQuery_PointQuery(session: SnappySession, colTableName: String, iterCount: Int,
      numTimesInsert: Int, numTimesUpdate: Int): Unit = {
    val param = getParam(iterCount, params)
    val query = s"select * from $colTableName where id = $param"
    val expectedNumResults = if (param % 10 < 6) numTimesInsert else numTimesUpdate
    val result = session.sql(query).collect()
    val passed = result.length == expectedNumResults
    // scalastyle:off
    // println(s"Query = $query result=${result.length} $expectedNumResults $iterCount")
    // scalastyle:on
    passed
  }

  def executeQuery_JoinQuery(session: SnappySession, colTableName: String, joinTableName: String,
      iterCount: Int, numTimesInsert: Int, numTimesUpdate: Int): Unit = {
    val query = s"select AVG(A.id), COUNT(B.id) " +
        s" from $colTableName A inner join $joinTableName B where A.id = B.id"
    val result = session.sql(query).collect()
    // scalastyle:off
    if (iterCount < 5) {
      println(s"Query = $query result=${result.length}")
      result.foreach(r => {
        val avg = r.getDouble(0)
        val count = r.getLong(1)
        print(s"[$avg, $count], ")
      })
      println()
    }
    // scalastyle:on
  }

  private def doGC(): Unit = {
    System.gc()
    System.runFinalization()
    System.gc()
    System.runFinalization()
  }

  def benchmarkInsert(session: SnappySession, colTableName: String, numBuckets: Int,
      numElements: Long, numIters: Int, queryMark: String,
      doVerifyFullSize: Boolean = false): Unit = {
    val benchmark = new Benchmark(s"Benchmark $queryMark", numElements, outputPerIteration = true)
    SortedColumnTests.verfiyInsertDataExists(session, numElements)
    SortedColumnTests.verfiyUpdateDataExists(session, numElements)
    val dataFrameReader : DataFrameReader = session.read
    val insertDF : DataFrame = dataFrameReader.load(SortedColumnTests.filePathInsert(numElements,
      multiple = 1))
    val updateDF : DataFrame = dataFrameReader.load(SortedColumnTests.filePathUpdate(numElements,
      multiple = 1))

    def execute(): Unit = {
      insertDF.write.insertInto(colTableName)
      updateDF.write.putInto(colTableName)
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
        SortedColumnTests.verifyTotalRows(session, colTableName, numElements, finalCall = true,
          numTimesInsert = 1, numTimesUpdate = 1)
      }
    } finally {
      session.sql(s"drop table $colTableName")
      session.conf.unset(Property.ColumnBatchSize.name)
      session.conf.unset(Property.ColumnMaxDeltaRows.name)
      session.conf.unset(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key)
      session.conf.unset(SQLConf.WHOLESTAGE_FALLBACK.key)
    }
  }

  def executeQuery_PointQuery_mt(session: SnappySession, colTableName: String,
      joinTableName: String, numIters: Int, iterCount: Int, numThreads: Int, threadId: Int,
      isMultithreaded: Boolean, numTimesInsert: Int, numTimesUpdate: Int): Boolean = {
    val param = getParam(iterCount, params)
    val query = s"select * from $colTableName where id = $param"
    val expectedNumResults = if (param % 10 < 6) numTimesInsert else numTimesUpdate
    val result = session.sql(query).collect()
    val passed = result.length == expectedNumResults
    // scalastyle:off
    // println(s"Query = $query result=${result.length} $expectedNumResults $iterCount" +
    //    s" $numThreads $threadId")
    // scalastyle:on
    passed
  }

  def executeQuery_RangeQuery_mt(session: SnappySession, colTableName: String,
      joinTableName: String, numIters: Int, iterCount: Int, numThreads: Int, threadId: Int,
      isMultithreaded: Boolean, numTimesInsert: Int, numTimesUpdate: Int): Boolean = {
    val param1 = getParam(iterCount, params1)
    val param2 = getParam(iterCount, params2)
    val (low, high) = if (param1 < param2) { (param1, param2)} else (param2, param1)
    val query = s"select * from $colTableName where id between $low and $high"
    val expectedNumResults = getParam(iterCount, params3)
    val result = session.sql(query).collect()
    val passed = isMultithreaded || result.length == expectedNumResults
    // scalastyle:off
    // println(s"Query = $query result=${result.length} $passed $expectedNumResults")
    // scalastyle:on
    passed
  }

  def executeQuery_JoinQuery_mt(session: SnappySession, colTableName: String,
      joinTableName: String, numIters: Int, iterCount: Int, numThreads: Int, threadId: Int,
      isMultithreaded: Boolean, numTimesInsert: Int, numTimesUpdate: Int): Boolean = {
    val param = getParam(iterCount, params)
    val query = s"select * from $colTableName A inner join $joinTableName B on A.id = B.id"
    val joinDF = session.sql(query)
    var i = 0
    joinDF.foreach(_ => i += 1)
    val expectedNumResults = i
    val result = i
    val passed = result == expectedNumResults
    // scalastyle:off
    // println(s"Query = $query iterCount=$iterCount result=$result $passed $expectedNumResults")
    // scalastyle:on
    passed
  }

  // scalastyle:off
  def benchmarkQuery(session: SnappySession, colTableName: String, numBuckets: Int,
      numElements: Long, numIters: Int, queryMark: String, isMultithreaded: Boolean = false,
      doVerifyFullSize: Boolean = false, numTimesInsert: Int = 1, numTimesUpdate: Int = 1,
      totalThreads: Int = 1, runTime: FiniteDuration = 2.seconds,
      joinTableName: Option[String] = None)
      // scalastyle:on
      (f : (SnappySession, String, String, Int, Int, Int, Int, Boolean, Int,
          Int) => Boolean): Unit = {
    val benchmark = new QueryBenchmark(s"Benchmark $queryMark", isMultithreaded, numElements,
      outputPerIteration = true, numThreads = totalThreads, minTime = runTime)
    SortedColumnTests.verfiyInsertDataExists(session, numElements, multiple = 1)
    SortedColumnTests.verfiyInsertDataExists(session, numElements, numTimesInsert)
    SortedColumnTests.verfiyUpdateDataExists(session, numElements, numTimesUpdate)
    val dataFrameReader : DataFrameReader = session.read
    val insertDF : DataFrame = dataFrameReader.load(SortedColumnTests.filePathInsert(numElements,
      multiple = 1))
    val updateDF : DataFrame = dataFrameReader.load(SortedColumnTests.filePathUpdate(numElements,
      numTimesUpdate))
    val sessionArray = new Array[SnappySession](totalThreads)
    sessionArray.indices.foreach(i => {
      sessionArray(i) = session.newSession()
      sessionArray(i).conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
      sessionArray(i).conf.set(SQLConf.WHOLESTAGE_FALLBACK.key, "false")
      sessionArray(i).conf.set(Property.ForceLinkPartitionsToBuckets.name, "true") // remove ?
    })

    def addBenchmark(name: String, params: Map[String, String] = Map()): Unit = {
      val defaults = params.keys.flatMap {
        k => session.conf.getOption(k).map((k, _))
      }

      def prepare(): Unit = {
        params.foreach { case (k, v) => session.conf.set(k, v) }
        SortedColumnTests.createColumnTable(session, colTableName, numBuckets, numElements)
        if (joinTableName.isDefined) {
          SortedColumnTests.createColumnTable(session, joinTableName.get, numBuckets, numElements,
            Some(colTableName))
        }
        try {
          session.conf.set(Property.ColumnBatchSize.name, "24M") // default
          session.conf.set(Property.ColumnMaxDeltaRows.name, "100")
          session.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
          session.conf.set(SQLConf.WHOLESTAGE_FALLBACK.key, "false")
          session.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
          var j = 0
          while (j < numTimesInsert) {
            insertDF.write.insertInto(colTableName)
            if (joinTableName.isDefined) {
              insertDF.write.insertInto(joinTableName.get)
            }
            j += 1
          }
          updateDF.write.putInto(colTableName)
          if (joinTableName.isDefined) {
            updateDF.write.putInto(joinTableName.get)
          }
          if (doVerifyFullSize) {
            SortedColumnTests.verifyTotalRows(session, colTableName, numElements, finalCall = true,
              numTimesInsert, numTimesUpdate)
            if (joinTableName.isDefined) {
              SortedColumnTests.verifyTotalRows(session, joinTableName.get, numElements,
                finalCall = true, numTimesInsert, numTimesUpdate)
            }
          }
        } finally {
          session.conf.unset(Property.ColumnBatchSize.name)
          session.conf.unset(Property.ColumnMaxDeltaRows.name)
          session.conf.unset(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key)
          session.conf.unset(SQLConf.WHOLESTAGE_FALLBACK.key)
          session.conf.unset(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key)
        }
        doGC()
      }

      def cleanup(): Unit = {
        sessionArray.indices.foreach(i => {
          sessionArray(i).clear()
          session.conf.unset(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key)
          session.conf.unset(SQLConf.WHOLESTAGE_FALLBACK.key)
          session.conf.unset(Property.ForceLinkPartitionsToBuckets.name)
        })
        SnappySession.clearAllCache()
        defaults.foreach { case (k, v) => session.conf.set(k, v) }
        doGC()
      }

      def testCleanup(): Unit = {
        doGC()
      }

      addCaseWithCleanup(benchmark, name, numIters, prepare,
        cleanup, testCleanup, isMultithreaded) { (iteratorIndex, threadId) =>
        f(sessionArray(threadId), colTableName, joinTableName.getOrElse("TableIsNotAvailiable"),
          numIters, iteratorIndex, totalThreads, threadId, isMultithreaded, numTimesInsert,
          numTimesUpdate)}
    }

    try {
      session.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
      session.conf.set(SQLConf.WHOLESTAGE_FALLBACK.key, "false")
      session.conf.set(Property.ForceLinkPartitionsToBuckets.name, "true") // remove ?

      // Get numbers
      addBenchmark(s"$queryMark", Map.empty)
      benchmark.run()
    } finally {
      try {
        session.sql(s"drop table $colTableName")
        if (joinTableName != null) {
          session.sql(s"drop table $joinTableName")
        }
      } catch {
        case _: Throwable =>
      }
      session.conf.unset(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key)
      session.conf.unset(SQLConf.WHOLESTAGE_FALLBACK.key)
      session.conf.unset(Property.ForceLinkPartitionsToBuckets.name)
    }
  }

  def mutiThreadedPointQuery(snc: SnappySession, numThreads: Int): Unit = {
    val colTableName = "colDeltaTable"
    val numElements = 999551
    val numBuckets = 3
    val numIters = 100
    val totalTime: FiniteDuration = new FiniteDuration(5, MINUTES)
    SortedColumnPerformanceTests.benchmarkQuery(snc, colTableName, numBuckets, numElements,
      numIters, "PointQuery multithreaded", numTimesInsert = 200, isMultithreaded = true,
      doVerifyFullSize = false, totalThreads = numThreads,
      runTime = totalTime)(SortedColumnPerformanceTests.executeQuery_PointQuery_mt)
  }

  val params = Array  (424281, 587515, 907730, 122421, 735695, 964648, 450150, 904625, 562060,
    496352, 745467, 823402, 988429, 311420, 394233, 30710, 653570, 236224, 987974, 653351, 826605,
    245093, 707312, 14213, 733602, 344160, 367710, 578064, 416602, 302421, 618862, 804150, 371841,
    402904, 691030, 246012, 156893, 379762, 775281, 109154, 693942, 121663, 762882, 367055, 836784,
    508941, 606644, 331100, 958543, 15944, 89403, 181845, 562542, 809723, 736823, 708541, 546835,
    384221, 899713, 689019, 946529, 679341, 953504, 420572, 52560, 845940, 541859, 33211, 63201,
    212861, 306901, 572094, 974953, 683232, 371095, 944829, 842675, 4273, 778735, 38911, 337234,
    975956, 648772, 103573, 381675, 153332, 682242, 269472, 940261, 989084, 569925, 922990, 65745,
    713571, 952867, 631447, 352805, 671402, 188913, 111165)

  val params1 = Array(435446, 668235, 698906, 9965, 923490, 970342, 971528, 924912, 210063, 514387,
    185010, 316700, 201191, 129476, 186458, 120609, 55514, 88575, 125345, 580302, 615387)
  val params2 = Array(63648, 770312, 344177, 328320, 126064, 636422, 7245, 327093, 906825, 45465,
    93499, 285349, 807082, 290182, 872723, 752484, 562808, 243877, 194831, 737899, 465701)
  val params3 = Array(2379519, 653292, 2270272, 2037464, 5103522, 2137098, 6171405, 3826048,
    4459294, 3001100, 585675, 200651, 3877716, 1028514, 4392106, 4044019, 3246679, 993932, 444706,
    1008620, 958004)

  def getParam(iterCount: Int, arr: Array[Int]): Int = {
    val index = if (iterCount < 0) 0 else iterCount % arr.length
    arr(index)
  }

  def addCaseWithCleanup(
      benchmark: QueryBenchmark,
      name: String,
      numIters: Int = 0,
      prepare: () => Unit,
      cleanup: () => Unit,
      testCleanup: () => Unit,
      isMultithreaded: Boolean,
      testPrepare: () => Unit = () => Unit)(f: (Int, Int) => Boolean): Unit = {
    val timedF = (timer: Benchmark.Timer, threadId: Int) => {
      if (!isMultithreaded) {
        testPrepare()
        timer.startTiming()
      }
      val ret = f(timer.iteration, threadId)
      if (!isMultithreaded) {
        testCleanup()
        timer.stopTiming()
      }
      ret
    }
    benchmark.benchmarks += QueryBenchmark.Case(name, timedF, numIters, prepare, cleanup)
  }
}
