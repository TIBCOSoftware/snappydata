/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
import org.apache.spark.sql._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{Decimal, DecimalType, StructType}
import org.apache.spark.util.Benchmark
import org.apache.spark.util.random.XORShiftRandom

class TPCETrade extends SnappyFunSuite {

  override protected def newSparkConf(
      addOn: SparkConf => SparkConf = null): SparkConf = {
    val numProcessors = Runtime.getRuntime.availableProcessors()
    val conf = new SparkConf()
        .setIfMissing("spark.master", s"local[$numProcessors]")
        .setAppName("microbenchmark")
    conf.set("spark.sql.shuffle.partitions", numProcessors.toString)
    conf.set(SQLConf.COLUMN_BATCH_SIZE.key, "100000")
    conf.set("snappydata.store.eviction-heap-percentage", "90")
    conf.set("snappydata.store.critical-heap-percentage", "95")
    if (addOn != null) {
      addOn(conf)
    }
    conf
  }

  private lazy val snappySession = snc.snappySession

  import snappySession.implicits._

  ignore("cache with random data - read path only") {
    benchmarkRandomizedKeys(size = 1000000000L, readPathOnly = true)
  }

  private def collect(df: DataFrame): Unit = {
    df.collect()
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
   *
   * NOTE: When running this benchmark, you will get a lot of WARN logs complaining that the
   * shuffle files do not exist. This is intentional; we delete the shuffle files manually
   * after every call to `collect` to avoid the next run to reuse shuffle files written by
   * the previous run.
   */
  private def benchmarkRandomizedKeys(size: Long, readPathOnly: Boolean): Unit = {
    val numIters = 20
    val benchmark = new Benchmark("Cache random data", size)
    snappySession.sql("drop table if exists trade")
    val tradeDF = snappySession.range(size).mapPartitions { itr =>
      val rnd = new XORShiftRandom
      val syms = TPCETrade.SYMBOLS
      val numSyms = syms.length
      itr.map(id => Trade(syms(rnd.nextInt(numSyms)),
        Decimal(rnd.nextInt(10000000), 8, 2)))
        //rnd.nextDouble()))
    }
    val dataDF = snappySession.internalCreateDataFrame(
      tradeDF.queryExecution.toRdd,
      StructType(tradeDF.schema.fields.map { f =>
          f.dataType match {
            case d: DecimalType =>
              f.copy(dataType = DecimalType(8, 2), nullable = false)
            case _ => f.copy(nullable = false)
          }
      }))
    dataDF.createOrReplaceTempView("trade")
    val query = "select avg(bid) from trade"

    /**
     * Add a benchmark case, optionally specifying whether to cache the dataset.
     */
    def addBenchmark(name: String, cache: Boolean, params: Map[String, String] = Map(),
        snappy: Boolean = false): Unit = {
      val defaults = params.keys.flatMap { k => snappySession.conf.getOption(k).map((k, _)) }
      def prepare(): Unit = {
        params.foreach { case (k, v) => snappySession.conf.set(k, v) }
        doGC()
        if (cache) {
          dataDF.createOrReplaceTempView("trade")
          snappySession.catalog.cacheTable("trade")
        } else if (snappy) {
          snappySession.sql("drop table if exists trade")
          snappySession.sql(s"${TPCETrade.sql} using column")
          if (readPathOnly) {
            dataDF.write.insertInto("trade")
          }
        }
        if (readPathOnly) {
          collect(snappySession.sql(query))
          testCleanup()
        }
        doGC()
      }
      def cleanup(): Unit = {
        defaults.foreach { case (k, v) => snappySession.conf.set(k, v) }
        snappySession.sql("drop table if exists trade")
        doGC()
      }
      def testCleanup(): Unit = {
      }
      addCaseWithCleanup(benchmark, name, numIters, prepare, cleanup, testCleanup) { _ =>
        if (readPathOnly) {
          collect(snappySession.sql(query))
        } else {
          // also benchmark the time it takes to build the column buffers
          if (snappy) {
            snappySession.sql("truncate table trade")
            dataDF.write.insertInto("trade")
          }
          val ds2 = snappySession.sql(query)
          collect(ds2)
          collect(ds2)
        }
      }
    }

    // All of these are codegen = T hashmap = T
    snappySession.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    snappySession.conf.set(SQLConf.VECTORIZED_AGG_MAP_MAX_COLUMNS.key, "1024")

    // Benchmark cases:
    //   (1) No caching
    //   (2) Caching without compression
    //   (3) Caching with compression
    //   (4) Caching with snappydata without compression
    //   (5) Caching with snappydata with compression
    // addBenchmark("cache = F", cache = false)
    /*
    addBenchmark("cache = T compress = F", cache = true, Map(
      // SQLConf.CACHE_CODEGEN.key -> "false",
      SQLConf.COMPRESS_CACHED.key -> "false"
    ))
    addBenchmark("cache = T compress = F", cache = true, Map(
      // SQLConf.CACHE_CODEGEN.key -> "false",
      SQLConf.COMPRESS_CACHED.key -> "false"
    ))
    */

    /*
    addBenchmark("cache = F snappyCompress = F", cache = false, Map(
      SQLConf.COMPRESS_CACHED.key -> "false"
    ), snappy = true)
    */
    addBenchmark("cache = F snappyCompress = T", cache = false, Map(
      SQLConf.COMPRESS_CACHED.key -> "true"
    ), snappy = true)
    benchmark.run()
  }
}

case class Trade(sym: String, bid: Decimal)

object TPCETrade {
  val SYMBOLS: Array[String] = Array("IBM", "YHOO", "GOOG", "MSFT", "AOL",
    "APPL", "ORCL", "SAP", "DELL", "RHAT", "NOVL", "HP")

  val sql: String =
    s"""
       |CREATE TABLE trade (
       |   sym CHAR(15) NOT NULL,
       |   bid DECIMAL(8, 2) NOT NULL
       |)
     """.stripMargin
}
