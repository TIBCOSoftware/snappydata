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
    val numIters = 10

    benchmarkQuery(snc, colTableName, numBuckets, numElements, numIters)(executeQuery_PointQuery)
  }

  def executeQuery_PointQuery(session: SnappySession, colTableName: String,
      numIters: Int, iterCount: Int): Unit = {
    val params = Array(1, 2, 3, 4, 5)
    val index = if (iterCount < 0) 0 else iterCount % params.length
    val query = s"select * from $colTableName where id = ${params(index)}"
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
