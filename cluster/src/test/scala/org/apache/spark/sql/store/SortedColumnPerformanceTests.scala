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
import org.apache.spark.sql.{Dataset, Row, SnappySession}
import org.apache.spark.util.Benchmark
import org.apache.spark.sql.snappy._

/**
 * Tests for column table having sorted columns.
 */
class SortedColumnPerformanceTests extends ColumnTablesTestBase {

  val cores = math.min(16, Runtime.getRuntime.availableProcessors())
  val params1 = Array(13569076, 17998179, 9419419, 44644913, 1174748, 76505417, 9699570, 96549560,
    44684874, 67674113, 38839260, 58716946, 33068333, 97110819, 51279965, 46250194, 65832886,
    98992603, 7126269, 9845093, 2877558, 60244101, 63160992, 82282474, 81673698, 91631052,
    77717653, 67373948, 37385679, 75961207, 78434315, 98825634, 43882466, 18018786, 7808668,
    19632801, 30125691, 55476715, 43826489, 41326443, 13466708, 30848640, 47382305, 12067044,
    81566988, 21935941, 18178122, 42720070, 81200959, 16285044, 84284114, 93065798, 12464370,
    5222864, 22389603, 97753722, 83899690, 90357881, 22993529, 10708639, 39246871, 55653767,
    55909650, 94994773, 92211265, 95744008, 70720796, 86107549, 34261186, 72191283, 75277341,
    64128962, 93371222, 94035378, 96367676, 6787521, 82245290, 75838815, 77014387, 88435719,
    93890505, 87861082, 90636665, 99129488, 30432779, 29502950, 31753051, 68481084, 19986638,
    56221463, 21589819, 96818165, 70554438, 65748901, 61371509, 93856783, 24039784, 51810391,
    55405955, 62556824)
  val params2 = Array(2822682, 96317373, 23875999, 67328324, 70202326, 14652637, 70699805,
    33034895, 9104168, 15399707, 26459422, 79150390, 23757838, 67460883, 23426218, 58726742,
    12520090, 21885426, 9118939, 27821302, 81399634, 96658989, 38587123, 75822699, 55853922,
    57289458, 28375985, 80840956, 75546714, 49473471, 19073208, 29467000, 34507804, 1748290,
    61236038, 64227216, 58175833, 96048793, 79804735, 85856134, 13616414, 53002385, 15917176,
    54710826, 61796296, 99304626, 62877552, 28173172, 63626381, 97972909, 41824553, 48946074,
    8442458, 50791296, 65872661, 39681364, 20903728, 64098634, 57273553, 19998263, 69477566,
    64713280, 30414720, 33248367, 4623977, 80330125, 18667890, 85186129, 43397844, 74268955,
    50410992, 97795647, 87207454, 58249717, 45604904, 89560569, 3713215, 22181406, 33083136,
    29096063, 23838745, 77591496, 22705892, 37387169, 63285346, 49230400, 51166869, 73514000,
    31413134, 94888264, 25813680, 24970373, 6287985, 47167479, 82493598, 7588308, 28311104,
    86403816, 5200693, 71129592)

  override def newSparkConf(addOn: SparkConf => SparkConf = null): SparkConf = {
    val conf = new SparkConf()
        .setIfMissing("spark.master", s"local[$cores]")
        .setAppName("microbenchmark")
    conf.set("snappydata.store.critical-heap-percentage", "95")
    if (SnappySession.isEnterpriseEdition) {
      conf.set("snappydata.store.memory-size", "2400m")
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
    val numElements = 99999551
    val numBuckets = cores
    val numIters = 2

    benchmarkInsert(snc, colTableName, numBuckets, numElements, numIters)
  }

  def benchmarkInsert(session: SnappySession, colTableName: String, numBuckets: Int,
      numElements: Long, numIters: Int): Unit = {
    val benchmark = new Benchmark("Benchmark Insert", numElements, outputPerIteration = true)
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
      addBenchmark(s"Benchmark Insert", Map.empty)
      benchmark.run()

      // Now verify
      execute()
      SortedColumnTests.verifyTotalRows(session, colTableName, numElements, finalCall = true)
    } finally {
      session.sql(s"drop table $colTableName")
      session.conf.unset(Property.ColumnBatchSize.name)
      session.conf.unset(Property.ColumnMaxDeltaRows.name)
    }
  }

  test("query performance") {
    val snc = this.snc.snappySession
    val colTableName = "colDeltaTable"
    val numElements = 99999551
    val numBuckets = cores
    val numIters = 100

    benchmarkQuery(snc, colTableName, numBuckets, numElements, numIters)(executeQuery_PointQuery)
    benchmarkQuery(snc, colTableName, numBuckets, numElements, numIters)(executeQuery_RangeQuery)
  }

  def executeQuery_PointQuery(session: SnappySession, colTableName: String,
      numIters: Int, iterCount: Int): Unit = {
    val index = if (iterCount < 0) 0 else iterCount % params1.length
    val query = s"select * from $colTableName where id = ${params1(index)}"
    // scalastyle:off
    println(s"Query = $query")
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
    println(s"Query = $query")
    // scalastyle:on
    val expectedNumResults = 1
    val result = session.sql(query).collect()
    assert(result.length === expectedNumResults)
  }

  def benchmarkQuery(session: SnappySession, colTableName: String, numBuckets: Int,
      numElements: Long, numIters: Int)(f : (SnappySession, String, Int, Int) => Unit): Unit = {
    val benchmark = new Benchmark("Benchmark Query", numElements, outputPerIteration = true)
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
        SortedColumnTests.verifyTotalRows(session, colTableName, numElements, finalCall = true)
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
      addBenchmark(s"Benchmark Query", Map.empty)
      benchmark.run()
    } finally {
      session.sql(s"drop table $colTableName")
      session.conf.unset(Property.ColumnBatchSize.name)
      session.conf.unset(Property.ColumnMaxDeltaRows.name)
    }
  }
}
