/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
import java.time.{ZoneId, ZonedDateTime}

import scala.util.Random

import com.typesafe.config.Config
import io.snappydata.{Property, SnappyFunSuite}
import org.scalatest.Assertions

import org.apache.spark.memory.SnappyUnifiedMemoryManager
import org.apache.spark.sql._
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.benchmark.TAQTest.CreateOp
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{Decimal, DecimalType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Benchmark
import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.{Logging, SparkConf, SparkContext}

class TAQTest extends SnappyFunSuite {

  override protected def newSparkConf(
      addOn: SparkConf => SparkConf = null): SparkConf =
    TAQTest.newSparkConf(addOn)

  override def beforeAll(): Unit = {
    super.beforeAll()
    stopAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    stopAll()
  }

  test("select queries with random data (eviction) - insert") {
    val quoteSize = 34000000L
    val tradeSize = 5000000L
    val numDays = 1
    val numIters = 3
    TAQTest.benchmarkRandomizedKeys(sc, quoteSize, tradeSize,
      quoteSize, numDays, queryNumber = 1, numIters, doInit = true,
      op = CreateOp.Quote, runSparkCaching = false)
    TAQTest.benchmarkRandomizedKeys(sc, quoteSize, tradeSize,
      tradeSize, numDays, queryNumber = 2, numIters, doInit = false,
      op = CreateOp.Trade, runSparkCaching = false)
  }

  test("select queries with random data - query") {
    val quoteSize = 3400000L
    val tradeSize = 500000L
    val numDays = 1
    val numIters = 10
    TAQTest.benchmarkRandomizedKeys(sc, quoteSize, tradeSize,
      quoteSize, numDays, queryNumber = 1, numIters, doInit = true)
    TAQTest.benchmarkRandomizedKeys(sc, quoteSize, tradeSize,
      tradeSize, numDays, queryNumber = 2, numIters, doInit = false)
    TAQTest.benchmarkRandomizedKeys(sc, quoteSize, tradeSize,
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
    for (_ <- 1 to numRuns) {
      val start = System.nanoTime()
      for (_ <- 1 to numIters) {
        // val rs = stmt.executeQuery("select * from citi_order where id=1000")
        val rs = stmt.executeQuery("select count(*) from citi_order")
        var count = 0
        while (rs.next()) {
          count += 1
        }
        assert(count == 1)
      }
      val end = System.nanoTime()
      val millis = (end - start) / 1000000.0
      logInfo(s"Time taken for $numIters runs = ${millis}ms, " +
          s"average = ${millis / numIters}ms")
    }
    stmt.close()
    conn.close()
  }
}

class TAQTestJob extends SnappySQLJob with Logging {

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val sc = snSession.sparkContext
    // SCALE OUT case with 10 billion rows
    val quoteSize = 8500000000L
    val tradeSize = 1250000000L
    val numDays = 16
    val numIters = 10
    TAQTest.benchmarkRandomizedKeys(sc,
      quoteSize, tradeSize, quoteSize, numDays, queryNumber = 1, numIters,
      doInit = true, runSparkCaching = false)
    TAQTest.benchmarkRandomizedKeys(sc,
      quoteSize, tradeSize, tradeSize, numDays, queryNumber = 2, numIters,
      doInit = false, runSparkCaching = false)
    TAQTest.benchmarkRandomizedKeys(sc,
      quoteSize, tradeSize, tradeSize, numDays, queryNumber = 3, numIters,
      doInit = false, runSparkCaching = false)
    Boolean.box(true)
  }

  def runSnappyJob2(sc: SnappyContext, jobConfig: Config): Any = {
    val numRuns = 1000
    val numIters = 1000
    val session = sc.snappySession
    for (_ <- 1 to numRuns) {
      val start = System.nanoTime()
      for (_ <- 1 to numIters) {
        Utils.sqlInternal(session, "select * from citi_order where id=1000 " +
            "--GEMFIREXD-PROPERTIES executionEngine=Spark").collectInternal()
      }
      val end = System.nanoTime()
      val millis = (end - start) / 1000000.0
      logInfo(s"Time taken for $numIters runs = ${millis}ms, " +
          s"average = ${millis / numIters}ms")
    }
    Boolean.box(true)
  }

  override def isValidJob(snSession: SnappySession,
      config: Config): SnappyJobValidation = SnappyJobValid()
}

case class Quote(sym: UTF8String, ex: UTF8String, bid: Double,
    time: Timestamp, date: Date)

case class Trade(sym: UTF8String, ex: UTF8String, price: Decimal,
    time: Timestamp, date: Date, size: Double, c1: Array[UTF8String],
    c2: Map[UTF8String, Double])

object TAQTest extends Logging with Assertions {

  private[benchmark] var COLUMN_TABLE = true

  val EXCHANGES: Array[String] = Array("NYSE", "NASDAQ", "AMEX", "TSE",
    "LON", "BSE", "BER", "EPA", "TYO")
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
       |   size DOUBLE NOT NULL,
       |   c1 ARRAY<STRING> NOT NULL,
       |   c2 MAP<STRING, Double> NOT NULL
       |)
     """.stripMargin

  private val d = "2016-06-06"
  // private val s = "SY23"
  val cacheQueries2 = Array(
    "select avg(bid) from cQuote",
    "select sym, avg(bid) from cQuote group by sym",
    "select sym, last(price) from cTrade group by sym",
    "select cQuote.sym, last(bid) from cQuote join cS " +
        s"on (cQuote.sym = cS.sym) where date='$d' group by cQuote.sym"
  )
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
  val queries2 = Array(
    "select avg(bid) from quote",
    "select sym, avg(bid) from quote group by sym",
    "select sym, last(price) from trade group by sym",
    "select quote.sym, last(bid) from quote join S " +
        s"on (quote.sym = S.sym) where date='$d' group by quote.sym"
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
  val expectedResultSizes = Array(
    100,
    900,
    800
  )

  object CreateOp extends Enumeration {
    type Type = Value
    val Read, Quote, Trade = Value
  }

  private def collect(df: Dataset[Row], expectedNumResults: Int): Unit = {
    val result = df.collect()
    assert(result.length === expectedNumResults)
  }

  private def doGC(): Unit = {
    System.gc()
    System.runFinalization()
    System.gc()
    System.runFinalization()
  }

  private val random = new Random()

  def newSparkConf(addOn: SparkConf => SparkConf = null): SparkConf = {
    val cores = math.min(16, Runtime.getRuntime.availableProcessors())
    val conf = new SparkConf()
        .setIfMissing("spark.master", s"local[$cores]")
        .setAppName("microbenchmark")
    conf.set("snappydata.store.critical-heap-percentage", "95")
    if (SparkSupport.isEnterpriseEdition) {
      conf.set("snappydata.store.memory-size", "1200m")
    }
    conf.set("spark.memory.manager", classOf[SnappyUnifiedMemoryManager].getName)
        .set("spark.serializer", "org.apache.spark.serializer.PooledKryoSerializer")
        .set("spark.closure.serializer", "org.apache.spark.serializer.PooledKryoSerializer")
        .set("snappydata.sql.planCaching", random.nextBoolean().toString)

    if (addOn != null) {
      addOn(conf)
    }
    conf
  }

  /**
   * Benchmark caching randomized keys created from a range.
   */
  def benchmarkRandomizedKeys(sc: SparkContext, quoteSize: Long,
      tradeSize: Long, size: Long, numDays: Int, queryNumber: Int,
      numIters: Int, doInit: Boolean, op: CreateOp.Type = CreateOp.Read,
      runSparkCaching: Boolean = true): Unit = {

    val spark = new SparkSession(sc)
    val session = new SnappySession(sc)

    import session.implicits._

    val benchmark = new Benchmark("Cache random data", size)
    val quoteRDD = sc.range(0, quoteSize).mapPartitions { itr =>
      val rnd = new XORShiftRandom
      val syms = ALL_SYMBOLS.map(UTF8String.fromString)
      val numSyms = syms.length
      val exs = EXCHANGES.map(UTF8String.fromString)
      val numExs = exs.length
      var day = 0
      val zoneId = ZoneId.systemDefault()
      var cal = ZonedDateTime.of(2016, 6, day + 6, 0, 0, 0, 0, zoneId)
      var millisTime = cal.toInstant.toEpochMilli
      var date = new Date(millisTime)
      var dayCounter = 0
      itr.map { id =>
        val sym = syms(math.abs(rnd.nextInt() % numSyms))
        val ex = exs(math.abs(rnd.nextInt() % numExs))
        if (numDays > 1) {
          dayCounter += 1
          // change date after some number of iterations
          if (dayCounter == 10000) {
            day = (day + 1) % numDays
            cal = ZonedDateTime.of(2016, 6, day + 6, 0, 0, 0, 0, zoneId)
            millisTime = cal.toInstant.toEpochMilli
            date = new Date(millisTime)
            dayCounter = 0
          }
        }
        val gid = (id % 400).toInt
        // reset the timestamp every once in a while
        if (gid == 0) {
          // seconds < 59 so that millis+gid does not overflow into next hour
          cal = ZonedDateTime.of(2016, 6, day + 6, rnd.nextInt() & 0x07,
            math.abs(rnd.nextInt() % 60), math.abs(rnd.nextInt() % 59),
            math.abs(rnd.nextInt() % 1000000000), zoneId)
          millisTime = cal.toInstant.toEpochMilli
        }
        val time = new Timestamp(millisTime + gid)
        Quote(sym, ex, rnd.nextDouble() * 1000.0, time, date)
      }
    }
    val tradeRDD = sc.range(0, tradeSize).mapPartitions { itr =>
      val rnd = new XORShiftRandom
      val syms = ALL_SYMBOLS.map(UTF8String.fromString)
      val numSyms = syms.length
      val exs = EXCHANGES.map(UTF8String.fromString)
      val numExs = exs.length
      var day = 0
      val zoneId = ZoneId.systemDefault()
      var cal = ZonedDateTime.of(2016, 6, day + 6, 0, 0, 0, 0, zoneId)
      var millisTime = cal.toInstant.toEpochMilli
      var date = new Date(millisTime)
      var dayCounter = 0
      itr.map { id =>
        val sym = syms(math.abs(rnd.nextInt() % numSyms))
        val ex = exs(math.abs(rnd.nextInt() % numExs))
        if (numDays > 1) {
          dayCounter += 1
          // change date after some number of iterations
          if (dayCounter == 10000) {
            // change date
            day = (day + 1) % numDays
            cal = ZonedDateTime.of(2016, 6, day + 6, 0, 0, 0, 0, zoneId)
            millisTime = cal.toInstant.toEpochMilli
            date = new Date(millisTime)
            dayCounter = 0
          }
        }
        val gid = (id % 400).toInt
        // reset the timestamp every once in a while
        if (gid == 0) {
          // seconds < 59 so that millis+gid does not overflow into next hour
          cal = ZonedDateTime.of(2016, 6, day + 6, rnd.nextInt() & 0x07,
            math.abs(rnd.nextInt() % 60), math.abs(rnd.nextInt() % 59),
            math.abs(rnd.nextInt() % 1000000000), zoneId)
          millisTime = cal.toInstant.toEpochMilli
        }
        val time = new Timestamp(millisTime + gid)
        val dec = Decimal(math.abs(rnd.nextInt() % 100000000), 10, 4)
        val c1 = Array(sym, ex, sym)
        val bid = rnd.nextDouble() * 1000
        val c2 = Map(sym -> bid, ex -> bid)
        Trade(sym, ex, dec, time, date, rnd.nextDouble() * 1000, c1, c2)
      }
    }

    val quoteDF = spark.createDataset(quoteRDD)
    val quoteDataDF = spark.internalCreateDataFrame(
      quoteDF.queryExecution.toRdd,
      StructType(quoteDF.schema.fields.map(_.copy(nullable = false))))
    val tradeDF = spark.createDataset(tradeRDD)
    val tradeDataDF = spark.internalCreateDataFrame(
      tradeDF.queryExecution.toRdd,
      StructType(tradeDF.schema.fields.map {
        case f if f.dataType.isInstanceOf[DecimalType] =>
          f.copy(dataType = DecimalType(10, 4), nullable = false)
        case f => f.copy(nullable = false)
      }))

    val qDF = session.createDataset(quoteRDD)
    val tDF = session.createDataset(tradeRDD)
    val sDF = session.createDataset(SYMBOLS)
    val symDF = spark.internalCreateDataFrame(
      spark.createDataset(SYMBOLS).queryExecution.toRdd,
      StructType(Array(StructField("SYM", StringType, nullable = false))))

    quoteDataDF.createOrReplaceTempView("cQuote")
    tradeDataDF.createOrReplaceTempView("cTrade")
    symDF.createOrReplaceTempView("cS")

    def cacheTable(spark: SparkSession, table: String): Unit = {
      spark.catalog.cacheTable(table)
      spark.sql(s"select count(*) from $table").collect()
    }

    /**
     * Add a benchmark case, optionally specifying whether to cache the DataSet.
     */
    def addBenchmark(name: String, cache: Boolean,
        params: Map[String, String] = Map(), query: String,
        expectedNumResults: Int, snappy: Boolean, init: Boolean): Unit = {
      val defaults = params.keys.flatMap {
        k => session.conf.getOption(k).map((k, _))
      }

      def prepare(): Unit = {
        params.foreach { case (k, v) =>
          session.conf.set(k, v); spark.conf.set(k, v)
        }
        doGC()
        if (cache) {
          spark.catalog.clearCache()
          cacheTable(spark, "cQuote")
          cacheTable(spark, "cTrade")
          cacheTable(spark, "cS")
          spark.sql(query).collect()
        } else {
          assert(snappy, "Only cache=T or snappy=T supported")
          if (init) {
            session.sql("drop table if exists quote")
            session.sql("drop table if exists trade")
            session.sql("drop table if exists S")
            val partitioning = if (op == CreateOp.Read) {
              " options (partition_by 'sym')"
            } else ""
            if (COLUMN_TABLE) {
              session.sql(s"$sqlQuote using column$partitioning")
              session.sql(s"$sqlTrade using column$partitioning")
            } else {
              session.sql(s"$sqlQuote using row$partitioning")
              session.sql(s"$sqlTrade using row$partitioning")
            }
            session.sql(s"CREATE TABLE S (sym CHAR(4) NOT NULL)")
            qDF.write.insertInto("quote")
            tDF.write.insertInto("trade")
            sDF.write.insertInto("S")
          }
          session.sql(query).collect()
        }
        testCleanup()
        doGC()
      }

      def cleanup(): Unit = {
        SnappySession.clearAllCache()
        defaults.foreach { case (k, v) =>
          session.conf.set(k, v); spark.conf.set(k, v)
        }
        doGC()
      }

      def testCleanup(): Unit = {
        if (op != CreateOp.Read) {
          if (snappy) {
            session.sql("truncate table quote")
            session.sql("truncate table trade")
          } else {
            spark.catalog.clearCache()
          }
          doGC()
        }
      }

      ColumnCacheBenchmark.addCaseWithCleanup(benchmark, name, numIters,
        prepare, cleanup, testCleanup) { _ =>
        op match {
          case CreateOp.Read =>
            if (snappy) {
              collect(session.sql(query), expectedNumResults)
            } else {
              collect(spark.sql(query), expectedNumResults)
            }
          case CreateOp.Quote if snappy =>
            qDF.write.insertInto("quote")
          case CreateOp.Quote =>
            cacheTable(spark, "cQuote")
          case CreateOp.Trade if snappy =>
            tDF.write.insertInto("trade")
          case CreateOp.Trade =>
            cacheTable(spark, "cTrade")
        }
      }
    }

    session.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    session.conf.set(SQLConf.WHOLESTAGE_FALLBACK.key, "false")
    spark.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    spark.conf.set(SQLConf.WHOLESTAGE_FALLBACK.key, "false")

    // Benchmark cases:
    //   (1) Spark caching with column batch compression
    //   (2) SnappyData Column table with plan optimizations

    var init = doInit

    if (runSparkCaching) {
      addBenchmark(s"Q$queryNumber: cache = T", cache = true,
        Map.empty, query = cacheQueries(queryNumber - 1),
        expectedNumResults = expectedResultSizes(queryNumber - 1),
        snappy = false, init)
    }

    addBenchmark(s"Q$queryNumber: cache = F snappy = T", cache = false,
      Map.empty, query = queries(queryNumber - 1),
      expectedNumResults = expectedResultSizes(queryNumber - 1),
      snappy = true, init)
    init = false

    benchmark.run()
  }
}
