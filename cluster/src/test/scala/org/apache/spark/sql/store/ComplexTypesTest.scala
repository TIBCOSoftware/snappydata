/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

import java.sql.{Date, Timestamp}
import java.util.{Calendar, GregorianCalendar}

import io.snappydata.SnappyFunSuite

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.execution.benchmark.{ColumnCacheBenchmark, TAQTest}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.random.XORShiftRandom

class ComplexTypesTest extends SnappyFunSuite {

  test("check complex types insert/select") {
    val tradeSize = 50000L
    val numDays = 1
    val session = new SnappySession(sc)

    import session.implicits._

    val tradeRDD = ComplexTypesTest.createTradeRDD(sc, tradeSize, numDays)

    session.sql("drop table if exists trade")
    if (COLUMN_TABLE) {
      session.sql(s"${ComplexTypesTest.sqlTrade} using column " +
          s"options (buckets '8')")
    } else {
      session.sql(s"${ComplexTypesTest.sqlTrade} using row options " +
          s"(partition_by 'sym', buckets '8', overflow 'true')")
    }

    val tradeDF = session.createDataset(tradeRDD)
    tradeDF.cache()
    tradeDF.count()
    tradeDF.write.insertInto("trade")

    val query = "select * from trade order by id"
    val result = session.sql(query)
    // val tDF = tradeDF.toDF().as(RowEncoder(session.table("trade").schema))
    val expected = tradeDF.toDF().sort("id").collect().toSeq

    ColumnCacheBenchmark.collect(result, expected)

    session.sql("drop table trade")
    session.catalog.clearCache()
  }

  private[sql] var COLUMN_TABLE = true
}

object ComplexTypesTest {

  def createTradeRDD(sc: SparkContext, tradeSize: Long,
      numDays: Int): RDD[TradeData] = {
    sc.range(0, tradeSize).mapPartitions { itr =>
      val rnd = new XORShiftRandom
      val syms = TAQTest.ALL_SYMBOLS.map(UTF8String.fromString)
      val numSyms = syms.length
      val exs = TAQTest.EXCHANGES.map(UTF8String.fromString)
      val numExs = exs.length
      var day = 0
      // month is 0 based
      var cal = new GregorianCalendar(2016, 5, day + 6)
      var date = new Date(cal.getTimeInMillis)
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
            cal = new GregorianCalendar(2016, 5, day + 6)
            date = new Date(cal.getTimeInMillis)
            dayCounter = 0
          }
        }
        val gid = (id % 400).toInt
        // reset the timestamp every once in a while
        if (gid == 0) {
          cal.set(Calendar.HOUR, rnd.nextInt() & 0x07)
          cal.set(Calendar.MINUTE, math.abs(rnd.nextInt() % 60))
          cal.set(Calendar.SECOND, math.abs(rnd.nextInt() % 60))
          cal.set(Calendar.MILLISECOND, math.abs(rnd.nextInt() % 1000))
        }
        val time = new Timestamp(cal.getTimeInMillis + gid)
        val dec = Decimal(math.abs(rnd.nextInt() % 1000000))
        val dec2 = Decimal(math.abs(rnd.nextInt() % 1000000000))
        val dec3 = Decimal(math.abs(rnd.nextInt() % 100000000))
        val c1 = Array(id, id + 1, id + 2, id + 3)
        val c2 = Array(sym, ex, sym)
        val c3 = Map(sym -> dec2, ex -> dec2)
        val tradeB = TradeB(dec3, c2)
        val c4 = Map(sym -> tradeB, ex -> tradeB)
        val idInt = id.toInt
        val c5 = TradeC(sym, gid, dec, Map(sym -> idInt, ex -> idInt))
        TradeData(id, sym, ex, dec, time, date, rnd.nextDouble() * 1000,
          c1, c2, c3, c4, c5)
      }
    }
  }

  val sqlTrade: String =
    s"""
       |CREATE TABLE trade (
       |   id BIGINT NOT NULL,
       |   sym CHAR(4) NOT NULL,
       |   ex VARCHAR(64) NOT NULL,
       |   price DECIMAL(10,4) NOT NULL,
       |   time TIMESTAMP NOT NULL,
       |   date DATE NOT NULL,
       |   size DOUBLE NOT NULL,
       |   c1 ARRAY<LONG>,
       |   c2 ARRAY<STRING> NOT NULL,
       |   c3 MAP<STRING, DECIMAL(28,10)> NOT NULL,
       |   c4 MAP<STRING, STRUCT<dec: DECIMAL(20,8), arr: Array<CHAR(4)>>>,
       |   c5 STRUCT<sym: STRING, gid: Int, dec: DECIMAL(10,4), map: Map<String, Int>>
       |)
     """.stripMargin
}

case class TradeB(dec: Decimal, syms: Array[UTF8String])

case class TradeC(sym: UTF8String, gid: Int, dec: Decimal,
    map: Map[UTF8String, Int])

case class TradeData(id: Long, sym: UTF8String, ex: UTF8String, price: Decimal,
    time: Timestamp, date: Date, size: Double, c1: Array[Long],
    c2: Array[UTF8String], c3: Map[UTF8String, Decimal],
    c4: Map[UTF8String, TradeB], c5: TradeC)
