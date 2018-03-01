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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.SnappySession
import org.apache.spark.util.Benchmark
import org.apache.spark.sql.snappy._

/**
 * Tests for column table having sorted columns.
 */
class SortedColumnPerformanceTests extends ColumnTablesTestBase {

  val cores = math.min(16, Runtime.getRuntime.availableProcessors())
  val params1 = Array(9897441, 1891255, 1757452, 9032268, 5816419, 8216891, 8754361, 9843223, 1710564, 2063902, 1248727, 9190220, 9624340, 1260846, 4261500, 6720184, 7931244, 9667476, 6490434, 9565207, 889821, 9879844, 3175650, 9198616, 4245217, 2877506, 1815296, 2369297, 6614546, 7843683, 9520205, 9648435, 9999052, 1142096, 6110060, 8519520, 122012, 6753598, 9529024, 2392002, 5736161, 7597741, 9661119, 2067235, 2944637, 8563933, 9979787, 7037189, 7425039, 3211201, 9770410, 1833691, 5971758, 7989612, 742007, 6482434, 4525179, 5242269, 4036180, 5046420, 5166391, 9335631, 599172, 52447, 1828811, 282922, 4246768, 4610412, 5100035, 6842462, 2150423, 6388472, 9399090, 8329511, 2501608, 7981119, 7757555, 6114453, 8242861, 2310103, 9287338, 9367289, 124702, 2458996, 277888, 1777816, 9761242, 8549981, 2409869, 8269475, 3925428, 8895795, 3616194, 9447476, 7400767, 393980, 9741129, 6333710, 5026825, 3530164)
  val params2 = Array(5003237, 1435358, 6121973, 5279568, 2789158, 9883536, 722353, 5215953, 7558178, 5258491, 3766600, 1015397, 147475, 7210484, 9165479, 9874827, 3112913, 9131449, 837588, 1711876, 5520763, 635681, 4708813, 2019587, 3191206, 2134644, 9063655, 8688740, 5582278, 7931457, 7828721, 6458443, 5107480, 5322005, 2556579, 3321390, 2030249, 2286212, 5869009, 6764444, 8462700, 6157189, 8509160, 7714377, 5402279, 9289405, 2063669, 1683461, 6713163, 1112404, 469304, 9986959, 3817237, 7316005, 5634591, 5788888, 6765241, 2777353, 2279939, 912389, 2139298, 1763126, 6855701, 1005755, 6193509, 6798822, 8243022, 3245550, 8051872, 6343670, 5016771, 599987, 7481021, 6965434, 4278096, 6381866, 7264783, 4333405, 5929651, 6250666, 4651526, 7162603, 5747190, 2811019, 8513965, 3239368, 9863085, 2052690, 3982257, 7584449, 7020757, 2046008, 888925, 9963517, 2763324, 7145188, 8565115, 8158010, 4605674, 6999623)

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
      updateDF.write.putInto(colTableName)
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
    benchmarkQuery(snc, colTableName, numBuckets, numElements, numIters,
      "PointQuery")(executeQuery_PointQuery)
  }

  test("RangeQuery performance") {
    val snc = this.snc.snappySession
    val colTableName = "colDeltaTable"
    val numElements = 9999551
    val numBuckets = cores
    val numIters = 100
    benchmarkQuery(snc, colTableName, numBuckets, numElements, numIters,
      "RangeQuery")(executeQuery_RangeQuery)
  }

  def executeQuery_PointQuery(session: SnappySession, colTableName: String,
      numIters: Int, iterCount: Int): Unit = {
    val index = if (iterCount < 0) 0 else iterCount % params1.length
    val query = s"select * from $colTableName where id = ${params1(index)}"
    // scalastyle:off
    // println(s"Query = $query")
    // scalastyle:on
    val expectedNumResults = 1
    val result = session.sql(query).collect()
    assert(result.length === expectedNumResults)
  }

  def executeQuery_RangeQuery(session: SnappySession, colTableName: String,
      numIters: Int, iterCount: Int): Unit = {
    val index1 = if (iterCount < 0) 0 else iterCount % params1.length
    val index2 = if (iterCount < 0) 0 else iterCount % params2.length
    val (low, high) = if (params1(index1) < params2(index2)) {
      (params1(index1), params2(index2))
    } else (params2(index2), params1(index1))
    val query = s"select * from $colTableName where id between $low and $high"
    // scalastyle:off
    // println(s"Query = $query")
    // scalastyle:on
    val expectedNumResults = high - low + 1
    val result = session.sql(query).collect()
    assert(result.length === expectedNumResults)
  }

  def benchmarkQuery(session: SnappySession, colTableName: String, numBuckets: Int,
      numElements: Long, numIters: Int, queryMark: String, doVerifyFullSize: Boolean = false)
      (f : (SnappySession, String, Int, Int) => Unit): Unit = {
    val benchmark = new Benchmark(s"Benchmark $queryMark", numElements, outputPerIteration = true)
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
        updateDF.write.putInto(colTableName)
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

      ColumnCacheBenchmark.addCaseWithCleanup(benchmark, name, numIters,
        prepare, cleanup, testCleanup) { i => f(session, colTableName, numIters, i)}
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
