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

import java.sql.{Date, DriverManager, Timestamp}
import java.util.{Calendar, GregorianCalendar}

import com.typesafe.config.Config
import io.snappydata.SnappyFunSuite

import org.apache.spark.sql._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{Decimal, StringType, StructField, StructType}
import org.apache.spark.util.Benchmark
import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.{Logging, SparkConf}

class TPCETrade extends SnappyFunSuite {

  override protected def newSparkConf(
      addOn: SparkConf => SparkConf = null): SparkConf = {
    val numProcessors = Runtime.getRuntime.availableProcessors()
    val conf = new SparkConf()
        .setIfMissing("spark.master", s"local[$numProcessors]")
        .setAppName("microbenchmark")
    conf.set("spark.sql.shuffle.partitions", numProcessors.toString)
    // conf.set(SQLConf.COLUMN_BATCH_SIZE.key, "100000")
    conf.set("snappydata.store.eviction-heap-percentage", "90")
    conf.set("snappydata.store.critical-heap-percentage", "95")
    conf.set("spark.serializer", "org.apache.spark.serializer.PooledKryoSerializer")
    conf.set("spark.closure.serializer", "org.apache.spark.serializer.PooledKryoSerializer")
    if (addOn != null) {
      addOn(conf)
    }
    conf
  }

  private lazy val snappySession = snc.snappySession

  test("select queries with random data") {
    val quoteSize = 3400000L
    val tradeSize = 500000L
    val numDays = 1
    val numIters = 10
    TPCETradeTest.benchmarkRandomizedKeys(snappySession, quoteSize, tradeSize,
      quoteSize, numDays, queryNumber = 1, numIters, doInit = true)
    TPCETradeTest.benchmarkRandomizedKeys(snappySession, quoteSize, tradeSize,
      tradeSize, numDays, queryNumber = 2, numIters, doInit = false)
    TPCETradeTest.benchmarkRandomizedKeys(snappySession, quoteSize, tradeSize,
      tradeSize, numDays, queryNumber = 3, numIters, doInit = false)
  }

  ignore("basic query performance with JDBC") {
    val numRuns = 1000
    val numIters = 1000
    val conn = DriverManager.getConnection("jdbc:snappydata://localhost:1527")
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery("values dsid()")
    rs.next()
    logInfo(s"Connected to server ${rs.getString(1)}")
    rs.close()
    for (i <- 1 to numRuns) {
      val start = System.nanoTime()
      for (j <- 1 to numIters) {
        val rs = stmt.executeQuery("select * from citi_order where id=1000 " +
            "--GEMFIREXD-PROPERTIES executionEngine=Spark")
        var count = 0
        while (rs.next()) {
          count += 1
        }
        assert(count == 1)
      }
      val end = System.nanoTime()
      val millis = (end - start) / 1000000.0
      logInfo(s"Time taken for $numIters runs = ${millis}ms, average = ${millis / numIters}ms")
    }
    stmt.close()
    conn.close()
  }
}

class TPCETradeJob extends SnappySQLJob with Logging {

  override def runSnappyJob(sc: SnappyContext, jobConfig: Config): Any = {
    val numRuns = 1000
    val numIters = 1000
    val session = sc.snappySession
    for (i <- 1 to numRuns) {
      val start = System.nanoTime()
      for (j <- 1 to numIters) {
        session.sql("select * from citi_order where id=1000 " +
            "--GEMFIREXD-PROPERTIES executionEngine=Spark").collectInternal()
      }
      val end = System.nanoTime()
      val millis = (end - start) / 1000000.0
      logInfo(s"Time taken for $numIters runs = ${millis}ms, average = ${millis / numIters}ms")
    }
    Boolean.box(true)
  }

  def runSnappyJob2(sc: SnappyContext, jobConfig: Config): Any = {
    // SCALE OUT case with 10 billion rows
    val quoteSize = 8500000000L
    val tradeSize = 1250000000L
    val numDays = 16
    val numIters = 10
    sc.conf.setConfString("spark.sql.shuffle.partitions", "16")
    TPCETradeTest.benchmarkRandomizedKeys(sc.snappySession,
      quoteSize, tradeSize, quoteSize, numDays, queryNumber = 1, numIters,
      doInit = true, runSparkCaching = false)
    TPCETradeTest.benchmarkRandomizedKeys(sc.snappySession,
      quoteSize, tradeSize, tradeSize, numDays, queryNumber = 2, numIters,
      doInit = false, runSparkCaching = false)
    TPCETradeTest.benchmarkRandomizedKeys(sc.snappySession,
      quoteSize, tradeSize, tradeSize, numDays, queryNumber = 3, numIters,
      doInit = false, runSparkCaching = false)
    Boolean.box(true)
  }

  override def isValidJob(sc: SnappyContext,
      config: Config): SnappyJobValidation = SnappyJobValid()
}

case class Quote(sym: String, ex: String, bid: Double, time: Timestamp,
    date: Date)

case class Trade(sym: String, ex: String, price: String, time: Timestamp,
    date: Date, size: Double)

object TPCETradeTest extends Logging {

  val HASH_OPTIMIZED = "spark.sql.hash.optimized"

  val EXCHANGES: Array[String] = Array("NYSE", "NASDAQ", "AMEX", "TSE",
    "LON", "BSE", "BER", "EPA", "TYO")
  /*
  val SYMBOLS: Array[String] = Array("IBM", "YHOO", "GOOG", "MSFT", "AOL",
    "APPL", "ORCL", "SAP", "DELL", "RHAT", "NOVL", "HP")
  */
  val ALL_SYMBOLS: Array[String] = {
    val syms = new Array[String](400)
    for (i <- 0 until 10) {
      syms(i) = s"SY0$i"
    }
    for (i <- 10 until 100) {
      syms(i) = s"SY$i"
    }
    for (i <- 100 until 400) {
      syms(i) = s"S$i"
    }
    syms
  }
  val SYMBOLS: Array[String] = ALL_SYMBOLS.take(100)

  val sqlQuote: String =
    s"""
       |CREATE TABLE quote (
       |   sym CHAR(4) NOT NULL,
       |   ex VARCHAR(64) NOT NULL,
       |   bid DOUBLE NOT NULL,
       |   time TIMESTAMP NOT NULL,
       |   date DATE NOT NULL
       |)
     """.stripMargin
  val sqlTrade: String =
    s"""
       |CREATE TABLE trade (
       |   sym CHAR(4) NOT NULL,
       |   ex VARCHAR(64) NOT NULL,
       |   price DECIMAL(10,4) NOT NULL,
       |   time TIMESTAMP NOT NULL,
       |   date DATE NOT NULL,
       |   size DOUBLE NOT NULL
       |)
     """.stripMargin


  private val d = "2016-06-06"
  // private val s = "SY23"
  val cacheQueries = Array(
    "select cQuote.sym, last(bid) from cQuote join cS " +
        s"on (cQuote.sym = cS.sym) where date='$d' group by cQuote.sym",
    "select cTrade.sym, ex, last(price) from cTrade join cS " +
        s"on (cTrade.sym = cS.sym) where date='$d' group by cTrade.sym, ex",
    "select cTrade.sym, hour(time), avg(size) from cTrade join cS " +
        s"on (cTrade.sym = cS.sym) where date='$d' group by cTrade.sym, hour(time)" /* ,
    "select * from (select time, price, sym from cTrade where " +
        s"date='$d' and sym='$s') t " +
        "left outer join (select time, bid, sym from cQuote where " +
        s"date='$d' and sym='$s') q " +
        s"on q.time=(select max(time) from q where time<=t.time and sym='$s') " +
        "where price<bid" */
  )
  val queries = Array(
    "select quote.sym, last(bid) from quote join S " +
        s"on (quote.sym = S.sym) where date='$d' group by quote.sym",
    "select trade.sym, ex, last(price) from trade join S " +
        s"on (trade.sym = S.sym) where date='$d' group by trade.sym, ex",
    "select trade.sym, hour(time), avg(size) from trade join S " +
        s"on (trade.sym = S.sym) where date='$d' group by trade.sym, hour(time)" /* ,
    "select * from (select time, price, sym from trade where " +
        s"date='$d' and sym='$s') t " +
        "left outer join (select time ,bid, sym from quote where " +
        s"date='$d' and sym='$s') q " +
        s"on q.time=(select max(time) from q where time<=t.time and sym='$s') " +
        "where price<bid" */
  )

  private def collect(df: DataFrame): Unit = {
    val result = df.collect()
    // scalastyle:off
    println(s"Count = ${result.length}")
    // scalastyle:on
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
    benchmark.benchmarks += Benchmark.Case(name, timedF, numIters,
      prepare, cleanup)
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
  def benchmarkRandomizedKeys(session: SparkSession, quoteSize: Long,
      tradeSize: Long, size: Long, numDays: Int, queryNumber: Int,
      numIters: Int, doInit: Boolean, runSparkCaching: Boolean = true): Unit = {
    import session.implicits._

    val benchmark = new Benchmark("Cache random data", size)
    val quoteDF = session.range(quoteSize).mapPartitions { itr =>
      val rnd = new XORShiftRandom
      val syms = ALL_SYMBOLS
      val numSyms = syms.length
      val exs = EXCHANGES
      val numExs = exs.length
      var day = 0
      // month is 0 based
      var cal = new GregorianCalendar(2016, 5, day + 6)
      var date = new Date(cal.getTimeInMillis)
      var dayCounter = 0
      itr.map { id =>
        val sym = syms(rnd.nextInt(numSyms))
        val ex = exs(rnd.nextInt(numExs))
        if (numDays > 1) {
          dayCounter += 1
          // change date after some number of iterations
          if (dayCounter == 10000) {
            day = (day + 1) % numDays
            cal = new GregorianCalendar(2016, 5, day + 6)
            date = new Date(cal.getTimeInMillis)
            dayCounter = 0
          }
        }
        cal.set(Calendar.HOUR, rnd.nextInt(8))
        cal.set(Calendar.MINUTE, rnd.nextInt(60))
        cal.set(Calendar.SECOND, rnd.nextInt(60))
        cal.set(Calendar.MILLISECOND, rnd.nextInt(1000))
        val time = new Timestamp(cal.getTimeInMillis)
        Quote(sym, ex, rnd.nextDouble() * 100000, time, date)
      }
    }
    val tradeDF = session.range(tradeSize).mapPartitions { itr =>
      val rnd = new XORShiftRandom
      val syms = ALL_SYMBOLS
      val numSyms = syms.length
      val exs = EXCHANGES
      val numExs = exs.length
      var day = 0
      // month is 0 based
      var cal = new GregorianCalendar(2016, 5, day + 6)
      var date = new Date(cal.getTimeInMillis)
      var dayCounter = 0
      itr.map { id =>
        val sym = syms(rnd.nextInt(numSyms))
        val ex = exs(rnd.nextInt(numExs))
        if (numDays > 1) {
          dayCounter += 1
          // change date after some number of iterations
          if (dayCounter == 10000) {
            // change date
            day = (day + 1) % numDays
            cal = new GregorianCalendar(2016, 5, day + 6)
            date = new Date(cal.getTimeInMillis)
            dayCounter = 0
          }
        }
        cal.set(Calendar.HOUR, rnd.nextInt(8))
        cal.set(Calendar.MINUTE, rnd.nextInt(60))
        cal.set(Calendar.SECOND, rnd.nextInt(60))
        cal.set(Calendar.MILLISECOND, rnd.nextInt(1000))
        val time = new Timestamp(cal.getTimeInMillis)
        val dec = Decimal(rnd.nextInt(100000000), 10, 4).toString
        Trade(sym, ex, dec, time, date, rnd.nextDouble() * 1000)
      }
    }
    val quoteDataDF = session.internalCreateDataFrame(
      quoteDF.queryExecution.toRdd,
      StructType(quoteDF.schema.fields.map(_.copy(nullable = false))))
    val tradeDataDF = session.internalCreateDataFrame(
      tradeDF.queryExecution.toRdd,
      StructType(tradeDF.schema.fields.map(_.copy(nullable = false))))

    val sDF = session.createDataset(SYMBOLS)
    val symDF = session.internalCreateDataFrame(
      sDF.queryExecution.toRdd,
      StructType(Array(StructField("SYM", StringType, nullable = false))))

    quoteDataDF.createOrReplaceTempView("cQuote")
    tradeDataDF.createOrReplaceTempView("cTrade")
    symDF.createOrReplaceTempView("cS")

    /**
     * Add a benchmark case, optionally specifying whether to cache the DataSet.
     */
    def addBenchmark(name: String, cache: Boolean,
        params: Map[String, String] = Map(), query: String,
        snappy: Boolean, init: Boolean): Unit = {
      val defaults = params.keys.flatMap {
        k => session.conf.getOption(k).map((k, _))
      }
      def prepare(): Unit = {
        params.foreach { case (k, v) => session.conf.set(k, v) }
        doGC()
        if (cache) {
          SnappyAggregation.enableOptimizedAggregation = false
          session.catalog.cacheTable("cQuote")
          session.catalog.cacheTable("cTrade")
          session.catalog.cacheTable("cS")
        } else {
          assert(snappy, "Only cache=T or snappy=T supported")
          SnappyAggregation.enableOptimizedAggregation =
              params.getOrElse(HASH_OPTIMIZED, "true").toBoolean
          if (init) {
            session.sql("drop table if exists quote")
            session.sql("drop table if exists trade")
            session.sql("drop table if exists S")
            session.sql(s"$sqlQuote using column")
            session.sql(s"$sqlTrade using column")
            session.sql(s"CREATE TABLE S (sym CHAR(4) NOT NULL)")
            quoteDataDF.write.insertInto("quote")
            tradeDataDF.write.insertInto("trade")
            symDF.write.insertInto("S")
          }
        }
        session.sql(query).collect()
        testCleanup()
        doGC()
      }
      def cleanup(): Unit = {
        SnappySession.clearPlanCache()
        defaults.foreach { case (k, v) => session.conf.set(k, v) }
        doGC()
      }
      def testCleanup(): Unit = {
      }
      addCaseWithCleanup(benchmark, name, numIters, prepare,
        cleanup, testCleanup) { _ =>
        collect(session.sql(query))
      }
    }

    session.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    session.conf.set(SQLConf.WHOLESTAGE_FALLBACK.key, "false")
    session.conf.set(SQLConf.VECTORIZED_AGG_MAP_MAX_COLUMNS.key, "1024")

    // Benchmark cases:
    //   (1) Caching with column batch compression
    //   (2) Column table compression
    //   (3) Column table compression, optimized group by and local join
    var init = doInit

    if (runSparkCaching) {
      addBenchmark(s"Q$queryNumber: cache = T compress = T",
        cache = true, Map(
          SQLConf.COMPRESS_CACHED.key -> "true"
        ), query = cacheQueries(queryNumber - 1), snappy = false, init)
    }

    /*
    addBenchmark(s"Q$queryNumber: cache = F snappyCompress = T, opt = F",
      cache = false, Map(
        SQLConf.COMPRESS_CACHED.key -> "true",
        HASH_OPTIMIZED -> "false"
      ), query = queries(queryNumber - 1), snappy = true, init)
    init = false
    */

    addBenchmark(s"Q$queryNumber: cache = F snappyCompress = T, opt = T",
      cache = false, Map(
        SQLConf.COMPRESS_CACHED.key -> "true",
        HASH_OPTIMIZED -> "true"
      ), query = queries(queryNumber - 1), snappy = true, init)
    init = false

    benchmark.run()
  }
}
