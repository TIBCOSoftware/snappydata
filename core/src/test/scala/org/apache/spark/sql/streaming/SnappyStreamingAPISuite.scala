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

package org.apache.spark.sql.streaming

import scala.concurrent.duration._
import scala.language.postfixOps

import io.snappydata.SnappyFunSuite
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SnappyContext}
import org.apache.spark.streaming.{Duration, Seconds, SnappyStreamingContext}

class SnappyStreamingAPISuite extends SnappyFunSuite with Eventually
  with BeforeAndAfter with BeforeAndAfterAll {

  def framework: String = this.getClass.getSimpleName

  protected var ssnc: SnappyStreamingContext = _

  private lazy val session: SnappyContext = snc

  def batchDuration: Duration = Seconds(1)

  def creatingFunc(): SnappyStreamingContext = {
    val context = new SnappyStreamingContext(sc, batchDuration)
    context
  }

  before {
    SnappyStreamingContext.getActive.foreach {
      _.stop(stopSparkContext = false, stopGracefully = true)
    }
    ssnc = SnappyStreamingContext.getActiveOrCreate(creatingFunc)
  }

  after {
    baseCleanup(false)
    SnappyStreamingContext.getActive.foreach {
      _.stop(stopSparkContext = false, stopGracefully = true)
    }
  }
  val empty = Map.empty[String, String]

  test("tumbling sliding default window") {
    val q = new scala.collection.mutable.Queue[RDD[Tweet]]
    for (i <- 1 until 112 by 10) {
      q.enqueue(sc.parallelize(i to i + 9).map(i => Tweet(i, s"Text$i")))
    }
    val stream = ssnc.createSchemaDStream(ssnc.queueStream[Tweet](q))
    stream.registerAsTable("tweetStream")

    snc.dropTable("defaultWin", ifExists = true)
    snc.createTable("defaultWin", "column", stream.schema, empty)
    stream.foreachDataFrame(_.write.insertInto("defaultWin"))

    snc.dropTable("tumblingWin", ifExists = true)
    snc.createTable("tumblingWin", "column", stream.schema, empty)
    ssnc.registerCQ(
      "SELECT * from tweetStream window (duration 3 seconds, slide 3 seconds)")
      .foreachDataFrame(_.write.insertInto("tumblingWin"))

    snc.dropTable("slidingWin", ifExists = true)
    snc.createTable("slidingWin", "column", stream.schema, empty)
    ssnc.registerCQ(
      "SELECT * from tweetStream window (duration 2 seconds, slide 1 seconds)")
      .foreachDataFrame(_.write.insertInto("slidingWin"))

    ssnc.start()
    eventually(timeout(100000.milliseconds), interval(1000.milliseconds)) {
      val defaultCnt = ssnc.sql("select * from defaultWin").count
      val tumblingCnt = ssnc.sql("select * from tumblingWin").count
      val slidingCnt = ssnc.sql("select * from slidingWin").count / 2

      assert(defaultCnt == 120, s"Actual count is $defaultCnt")
      assert(tumblingCnt == 120, s"Actual count is $tumblingCnt")
      assert(slidingCnt == 120, s"Actual count is $slidingCnt")
    }
    ssnc.stop(stopSparkContext = false)

    snc.dropTable("defaultWin", ifExists = true)
    snc.dropTable("tumblingWin", ifExists = true)
    snc.dropTable("slidingWin", ifExists = true)

  }

  test("stream to stream and stream to table join") {
    val queue1 = new scala.collection.mutable.Queue[RDD[Tweet]]
    for (i <- 1 until 31 by 10) {
      queue1.enqueue(sc.parallelize(i to i + 9).map(i => Tweet(i, s"Text$i")))
    }
    val stream1 = ssnc.createSchemaDStream(ssnc.queueStream[Tweet](queue1))
    stream1.registerAsTable("tweetStream1")
    snc.dropTable("totalRows", ifExists = true)
    snc.createTable("totalRows", "column", stream1.schema, empty)
    stream1.foreachDataFrame(_.write.insertInto("totalRows"))

    val queue2 = new scala.collection.mutable.Queue[RDD[Tweet]]
    for (i <- 9 until 30 by 10) {
      queue2.enqueue(sc.parallelize(i to i + 6).map(i => Tweet(i, s"Text$i")))
    }
    val stream2 = ssnc.createSchemaDStream(ssnc.queueStream[Tweet](queue2))
    stream2.registerAsTable("tweetStream2")

    snc.dropTable("streamJoinResult", ifExists = true)
    snc.createTable("streamJoinResult", "column", stream2.schema, empty)
    val streamToStream = ssnc.registerCQ("SELECT t1.id, t2.text FROM " +
      "tweetStream1 window (duration 1 seconds, slide 1 seconds) t1 " +
      "JOIN tweetStream2 t2 ON t1.id = t2.id ")
    streamToStream.foreachDataFrame(_.write.insertInto("streamJoinResult"))

    snc.dropTable("refTable", ifExists = true)
    snc.createTable("refTable", "column", stream1.schema, empty)
    snc.createDataFrame(Seq(Tweet(5, "Text5"), Tweet(15, "Text10"), Tweet(25, "Text25")))
      .write.insertInto("refTable")
    val streamToTable = ssnc.registerCQ("SELECT t2.id, t2.text FROM tweetStream1 window " +
      "(duration 1 seconds, slide 1 seconds) " +
      "t1 JOIN refTable t2 ON t1.id = t2.id")
    snc.dropTable("tableJoinResult", ifExists = true)
    snc.createTable("tableJoinResult", "column", stream1.schema, empty)
    streamToTable.foreachDataFrame(_.write.insertInto("tableJoinResult"))

    ssnc.start()
    eventually(timeout(100000.milliseconds), interval(1000.milliseconds)) {
      var actual = ssnc.sql("select id from totalRows").collect() map (_.getInt(0))
      assert(actual.length === 30)
      (1 to 30).foreach(v => assert(actual.contains(v)))

      var expected = Seq(9, 10, 19, 20, 29, 30)
      actual = ssnc.sql("select id from streamJoinResult").collect() map (_.getInt(0))
      expected.foreach(v => assert(actual.contains(v)))
      assert(expected.length == actual.length)

      expected = Seq(5, 15, 25)
      actual = ssnc.sql("select id from tableJoinResult").collect() map (_.getInt(0))
      expected.foreach(v => assert(actual.contains(v)))
      assert(expected.length == actual.length)
    }
    ssnc.stop(stopSparkContext = false)

    snc.dropTable("refTable", ifExists = true)
    snc.dropTable("totalRows", ifExists = true)
    snc.dropTable("streamJoinResult", ifExists = true)
    snc.dropTable("tableJoinResult", ifExists = true)
  }

  test("avoid shuffle reuse in CQs") {
    val queue = new scala.collection.mutable.Queue[RDD[Tweet]]
    for (i <- 1 until 31 by 10) {
      queue.enqueue(sc.parallelize(i to i + 9).map(i => Tweet(i, s"Text$i")))
    }
    ssnc.createSchemaDStream(ssnc.queueStream[Tweet](queue)).registerAsTable("tweetStream")
    val cqResults = ssnc.registerCQ("select text from" +
      " tweetStream window (duration 1 seconds, slide 1 seconds)" +
      " where text like '%7%' order by text")
    val result = new scala.collection.mutable.ArrayBuffer[String]()

    cqResults.foreachDataFrame {
      _.collect().foreach { row =>
        result.synchronized {
          result.append(s"${row.getString(0)}")
        }
      }
    }

    ssnc.start()
    val expected = Seq("Text7", "Text17", "Text27")
    eventually(timeout(5000 milliseconds), interval(200 milliseconds)) {
      assert(result.synchronized {
        expected === result
      })
    }
    ssnc.stop(stopSparkContext = false)
  }

  test("stream to big table join CQ using SnappyHashJoin") {
    val queue = new scala.collection.mutable.Queue[RDD[Tweet]]
    for (i <- 1 until 31 by 10) {
      queue.enqueue(sc.parallelize(i to i + 9).map(i => Tweet(i, s"Text$i")))
    }
    val stream = ssnc.createSchemaDStream(ssnc.queueStream[Tweet](queue))
    stream.registerAsTable("tweetStream")

    snc.dropTable("refTable", ifExists = true)
    snc.createTable("refTable", "column", stream.schema, empty)
    snc.createDataFrame(Seq(Tweet(5, "Text5"), Tweet(15, "Text10"), Tweet(25, "Text25")))
      .write.insertInto("refTable")
    import session.implicits._
    snc.sparkContext.range(100, 1000000).map(l => (l, l.toString))
      .toDF("id", "text").write.insertInto("refTable")

    val streamToBigTable = ssnc.registerCQ("SELECT t1.id, t2.text FROM tweetStream" +
      " window (duration 1 seconds, slide 1 seconds) t1 " +
      " JOIN refTable t2 ON t1.id = t2.id")

    snc.dropTable("tableJoinResult", ifExists = true)
    snc.createTable("tableJoinResult", "column", stream.schema, empty)
    streamToBigTable.foreachDataFrame(_.write.insertInto("tableJoinResult"))

    ssnc.start()
    eventually(timeout(200000.milliseconds), interval(1000.milliseconds)) {
      val expected = Seq(5)
      val actual = ssnc.sql("select id from tableJoinResult").collect() map (_.getInt(0))
      expected.foreach(v => assert(actual.contains(v)))
      assert(actual.length != 0)
    }
    ssnc.stop(stopSparkContext = false)

    snc.dropTable("refTable", ifExists = true)
    snc.dropTable("tableJoinResult", ifExists = true)
  }

  test("stream creation using Product or schema") {
    val queue1 = new scala.collection.mutable.Queue[RDD[Row]]
    for (i <- 1 until 17 by 4) {
      queue1.enqueue(sc.parallelize(i to i + 3).map(i => Row(i, s"Text$i")))
    }
    val schema = StructType(Seq(StructField("id", IntegerType, nullable = true),
      StructField("text", StringType, nullable = true)))

    val rowStream = ssnc.createSchemaDStream(ssnc.queueStream[Row](queue1), schema)
    rowStream.foreachDataFrame(df => assert(df.count == 4))

    val queue2 = new scala.collection.mutable.Queue[RDD[Tweet]]
    for (i <- 1 until 5) {
      queue2.enqueue(sc.parallelize(i to i).map(i => Tweet(i, s"Text$i")))
    }
    val tweetStream = ssnc.createSchemaDStream(ssnc.queueStream[Tweet](queue2))
    tweetStream.foreachDataFrame(df => assert(df.count == 1))

    ssnc.start()
    ssnc.awaitTerminationOrTimeout(3000)
  }
  //  test("stream adhoc query plan caching")
  //  test("window units syntax variations")
  //  test("tumbling window join")
  //  test("big window duration join ")
  //  test("stream adhoc sql")
}

case class Tweet(id: Int, text: String)
