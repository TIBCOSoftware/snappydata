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
package io.snappydata.app.streaming

import java.util

import scala.collection.mutable

import io.snappydata.SnappyFunSuite
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually
import twitter4j.{Status, TwitterObjectFactory}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.{SchemaDStream, StreamToRowsConverter}
import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Seconds, SnappyStreamingContext}

class StreamingSuite
    extends SnappyFunSuite with Eventually with BeforeAndAfter {

  protected var ssnc: SnappyStreamingContext = _

  def framework: String = this.getClass.getSimpleName

  def master: String = "local[2]"

  def batchDuration: Duration = Seconds(1)

  def creatingFunc(): SnappyStreamingContext = {
    val context = new SnappyStreamingContext(sc, batchDuration)
    context.remember(Duration(3000))
    context
  }

  before {
    SnappyStreamingContext.getActive().foreach {
      _.stop(stopSparkContext = false, stopGracefully = true)
    }
    ssnc = SnappyStreamingContext.getActiveOrCreate(creatingFunc)
  }

  after {
    baseCleanup()
    SnappyStreamingContext.getActive().foreach {
      _.stop(stopSparkContext = false, stopGracefully = true)
    }
  }

  test("SchemaDStream Creation") {

    def getRowDStream: DStream[Row] = {
      val rowRDD1 = sc.parallelize(Row("1", "one") :: Row("2", "two") ::
          Row("3", "three") :: Row("4", "four") :: Nil)
      val rowRDD2 = sc.parallelize(Row("11", "one") :: Row("22", "two") ::
          Row("33", "three") :: Row("44", "four") :: Nil)
      val rowRDD3 = sc.parallelize(Row("111", "one") :: Row("222", "two") ::
          Row("333", "three") :: Row("444", "four") :: Nil)
      ssnc.queueStream[Row](mutable.Queue(rowRDD1, rowRDD2, rowRDD3))
    }

    def getTweetDStream: DStream[Tweet] = {
      val rdd1 = sc.parallelize(Tweet(1, "one") :: Nil)
      val rdd2 = sc.parallelize(Tweet(2, "two") :: Nil)
      val rdd3 = sc.parallelize(Tweet(3, "three") :: Nil)
      val rdd4 = sc.parallelize(Tweet(4, "four") :: Nil)
      ssnc.queueStream[Tweet](mutable.Queue(rdd1, rdd2, rdd3, rdd4))
    }

    def getLogSchema: StructType = {
      val publisher = createStructField("publisher", StringType, true)
      val advertiser = createStructField("advertiser", StringType, true)
      val fields = util.Arrays.asList(publisher, advertiser)
      val schema = DataTypes.createStructType(fields)
      schema
    }

    val rowStream = ssnc.createSchemaDStream(getRowDStream, getLogSchema)
    rowStream.foreachDataFrame(df => {
      assert(df.count == 4)
    })

    val tweetStream = ssnc.createSchemaDStream(getTweetDStream)
    tweetStream.foreachDataFrame(df => {
      assert(df.count == 1)
    })

    ssnc.start()
    ssnc.awaitTerminationOrTimeout(3000)
  }

  test("SNAP-414") {
    ssnc.sql("create stream table tableStream " +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        "consumerKey '0Xo8rg3W0SOiqu14HZYeyFPZi', " +
        "consumerSecret 'gieTDrdzFS4b1g9mcvyyyadOkKoHqbVQALoxfZ19eHJzV9CpLR', " +
        "accessToken '43324358-0KiFugPFlZNfYfib5b6Ah7c2NdHs1524v7LM2qaUq', " +
        "accessTokenSecret 'aB1AXHaRiE3g2d7tLgyASdgIg9J7CzbPKBkNfvK8Y88bu', " +
        "rowConverter 'io.snappydata.app.streaming.TweetToRowsConverter')")

    ssnc.sql("create stream table tableStream2 " +
        "(text string) " +
        "using twitter_stream options (" +
        "consumerKey '0Xo8rg3W0SOiqu14HZYeyFPZi', " +
        "consumerSecret 'gieTDrdzFS4b1g9mcvyyyadOkKoHqbVQALoxfZ19eHJzV9CpLR', " +
        "accessToken '43324358-0KiFugPFlZNfYfib5b6Ah7c2NdHs1524v7LM2qaUq', " +
        "accessTokenSecret 'aB1AXHaRiE3g2d7tLgyASdgIg9J7CzbPKBkNfvK8Y88bu', " +
        "rowConverter 'org.apache.spark.sql.streaming.HashTagToRowsConverter')")

    ssnc.sql("STREAMING START")
    for (a <- 1 to 3) {
      Thread.sleep(2000)
      ssnc.sql("select id, text, fullName from tableStream where text like '%e%'").show()
    }
    for (a <- 1 to 3) {
      Thread.sleep(2000)
      ssnc.sql("select text from tableStream2 where text like '%e%'").show()
    }
    ssnc.sql("drop table tableStream")
    ssnc.sql("drop table tableStream2")
  }

  test("SNAP-408") {
    ssnc.sql("create stream table tableStream " +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        "consumerKey '0Xo8rg3W0SOiqu14HZYeyFPZi', " +
        "consumerSecret 'gieTDrdzFS4b1g9mcvyyyadOkKoHqbVQALoxfZ19eHJzV9CpLR', " +
        "accessToken '43324358-0KiFugPFlZNfYfib5b6Ah7c2NdHs1524v7LM2qaUq', " +
        "accessTokenSecret 'aB1AXHaRiE3g2d7tLgyASdgIg9J7CzbPKBkNfvK8Y88bu', " +
        "rowConverter 'io.snappydata.app.streaming.TweetToRowsConverter')")
    ssnc.sql("STREAMING START")
    for (a <- 1 to 3) {
      Thread.sleep(2000)
      ssnc.sql("select id, text, fullName from tableStream where text like '%e%'").count()
    }
    // try drop from another streaming context
    SnappyStreamingContext.getActive().foreach {
      _.stop(stopSparkContext = false, stopGracefully = true)
    }
    ssnc = new SnappyStreamingContext(snc.sparkContext, batchDuration)

    ssnc.sql("drop table tableStream")
    intercept[Exception] {
      ssnc.sql("select id, text, fullName from tableStream where text like '%e%'").count()
    }
    ssnc.sql("create stream table tableStream " +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        "consumerKey '0Xo8rg3W0SOiqu14HZYeyFPZi', " +
        "consumerSecret 'gieTDrdzFS4b1g9mcvyyyadOkKoHqbVQALoxfZ19eHJzV9CpLR', " +
        "accessToken '43324358-0KiFugPFlZNfYfib5b6Ah7c2NdHs1524v7LM2qaUq', " +
        "accessTokenSecret 'aB1AXHaRiE3g2d7tLgyASdgIg9J7CzbPKBkNfvK8Y88bu', " +
        "rowConverter 'io.snappydata.app.streaming.TweetToRowsConverter')")
    for (a <- 1 to 3) {
      Thread.sleep(1000)
      ssnc.sql("select id, text, fullName from tableStream where text like '%e%'").count()
    }
    ssnc.sql("drop table tableStream")
  }

  test("stream ad-hoc sql") {
    ssnc.sql("create stream table tweetsTable " +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        "consumerKey '0Xo8rg3W0SOiqu14HZYeyFPZi', " +
        "consumerSecret 'gieTDrdzFS4b1g9mcvyyyadOkKoHqbVQALoxfZ19eHJzV9CpLR', " +
        "accessToken '43324358-0KiFugPFlZNfYfib5b6Ah7c2NdHs1524v7LM2qaUq', " +
        "accessTokenSecret 'aB1AXHaRiE3g2d7tLgyASdgIg9J7CzbPKBkNfvK8Y88bu', " +
        "rowConverter 'io.snappydata.app.streaming.TweetToRowsConverter')")

    val cqResult = ssnc.registerCQ("SELECT text FROM tweetsTable " +
        "window (duration 10 seconds, slide 10 seconds) where text like '%e%'")
    cqResult.foreachDataFrame(df => df.count())

    val dynamicCQResult = ssnc.registerCQ("SELECT text, fullName FROM tweetsTable " +
        "window (duration 4 seconds, slide 4 seconds) where text like '%e%'")
    dynamicCQResult.foreachDataFrame(df => df.count())

    ssnc.sql("STREAMING START")

    for (a <- 1 to 5) {
      Thread.sleep(2000)
      ssnc.sql("select text, fullName from tweetsTable where text like '%e%'").count()
    }
    ssnc.sql("drop table tweetsTable")
    ssnc.awaitTerminationOrTimeout(10 * 1000)
  }

  test("save stream to external table using forEachDataFrame") {
    def getQueueOfRDDs1: mutable.Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(1 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(11 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(21 to 30).map(i => Tweet(i, s"Text$i"))
      mutable.Queue(distData1, distData2, distData3)
    }

    val dStream1 = ssnc.queueStream[Tweet](getQueueOfRDDs1)
    val schemaStream1 = ssnc.createSchemaDStream(dStream1)
    ssnc.snappyContext.dropTable("gemxdColumnTable1", ifExists = true)
    schemaStream1.foreachDataFrame(df => {
      df.write.format("column").mode(SaveMode.Append)
          .options(Map.empty[String, String]).saveAsTable("gemxdColumnTable1")
    })

    ssnc.snappyContext.dropTable("gemxdColumnTable2", ifExists = true)
    schemaStream1.foreachDataFrame((df, time) => {
      df.write.format("column").mode(SaveMode.Append)
          .options(Map.empty[String, String]).saveAsTable("gemxdColumnTable2")
    })

    ssnc.start()
    ssnc.awaitTerminationOrTimeout(20 * 1000)

    val result1 = ssnc.sql("select * from gemxdColumnTable1")
    val r1 = result1.collect()
    assert(r1.length == 30)

    val result2 = ssnc.sql("select * from gemxdColumnTable2")
    val r2 = result2.collect()
    assert(r2.length == 30)
  }


  test("SNAP-240 NotSerializableException with checkpoint") {

    def getQueueOfRDDs: mutable.Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(1 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(11 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(21 to 30).map(i => Tweet(i, s"Text$i"))
      mutable.Queue(distData1, distData2, distData3)
    }

    val dStream = ssnc.queueStream[Tweet](getQueueOfRDDs)

    val schemaDStream = ssnc.createSchemaDStream(dStream)
    schemaDStream.foreachRDD(rdd => {
      // schemaDStream.createDataFrame (rdd).show() //NotSerializableException
      // println(rdd) // scalastyle:ignore
    })

    ssnc.start()
    ssnc.awaitTerminationOrTimeout(5 * 1000)
  }

  test("api stream to stream and stream to table join") {
    def getQueueOfRDDs1: mutable.Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(1 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(11 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(21 to 30).map(i => Tweet(i, s"Text$i"))
      mutable.Queue(distData1, distData2, distData3)
    }

    val dStream1 = ssnc.queueStream[Tweet](getQueueOfRDDs1)

    val schemaStream1 = ssnc.createSchemaDStream(dStream1)
    schemaStream1.foreachDataFrame(df => {
      df.count()
    })
    schemaStream1.registerAsTable("tweetStream1")

    def getQueueOfRDDs2: mutable.Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(9 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(19 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(29 to 30).map(i => Tweet(i, s"Text$i"))
      mutable.Queue(distData1, distData2, distData3)
    }

    val dStream2 = ssnc.queueStream[Tweet](getQueueOfRDDs2)

    val schemaStream2 = ssnc.createSchemaDStream(dStream2)
    schemaStream2.foreachDataFrame(df => {
      df.count()
    })
    schemaStream2.registerAsTable("tweetStream2")

    val resultStream: SchemaDStream = ssnc.registerCQ("SELECT t1.id, t1.text FROM " +
        "tweetStream1 window (duration 2 seconds, slide 2 seconds) t1 JOIN " +
        "tweetStream2 t2 ON t1.id = t2.id ")

    ssnc.snappyContext.dropTable("gemxdColumnTable", ifExists = true)
    ssnc.snappyContext.createTable("gemxdColumnTable", "column", schemaStream1.schema,
      Map.empty[String, String])

    resultStream.foreachDataFrame(df => {
      df.write.format("column").mode(SaveMode.Append).options(Map.empty[String, String])
          .saveAsTable("gemxdColumnTable")
    })

    val df = ssnc.snappyContext.createDataFrame(
      sc.parallelize(1 to 10).map(i => Tweet(i / 2, s"Text${i / 2}")))
    df.registerTempTable("tweetTable")

    val resultSet = ssnc.registerCQ("SELECT t2.id, t2.text FROM tweetStream1 window " +
        "(duration 4 seconds, slide 4 seconds) " +
        "t1 JOIN tweetTable t2 ON t1.id = t2.id")
    resultSet.foreachDataFrame(df => {
      df.write.format("column").mode(SaveMode.Append).options(Map.empty[String, String])
          .saveAsTable("gemxdColumnTable")
    })

    ssnc.start()
    ssnc.awaitTerminationOrTimeout(20 * 1000)

    val result = ssnc.sql("select * from gemxdColumnTable")
    val r = result.collect()
    assert(r.length > 0)
    ssnc.sql("drop table gemxdColumnTable")
  }

  test("sql on kafka streams") {

    ssnc.sql("create stream table kafkaStreamTable (name string, age int)" +
        " using kafka_stream options " +
        "(storagelevel 'MEMORY_AND_DISK_SER_2', " +
        "rowConverter 'io.snappydata.app.streaming.KafkaStreamToRowsConverter', " +
        "zkQuorum 'localhost:2181', " +
        "groupId 'streamSQLConsumer', " +
        "topics 'tweets:01')")

    /* val tableDStream: SchemaDStream = ssnc.getSchemaDStream("directKafkaStreamTable")
    import org.apache.spark.sql.streaming.snappy._
    tableDStream.saveToExternalTable("kafkaStreamGemXdTable", tableDStream.schema,
   Map.empty[String, String]) */

    // val thrown = intercept[Exception] {
    ssnc.sql("STREAMING START")
    // }
    // assert(thrown.getMessage === "requirement failed: No output operations " +
    //    "registered, so nothing to execute")
    ssnc.sql("drop table kafkaStreamTable")
  }
}

case class Tweet(id: Int, text: String)

class TweetToRowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[Row] = {
    val status: Status = message.asInstanceOf[Status]
    Seq(Row.fromSeq(Seq(status.getId,
      status.getText,
      status.getUser.getName,
      status.getUser.getLang,
      status.getRetweetCount,
      status.getHashtagEntities.mkString(","))))
  }

}

class LineToRowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[Row] = {
    Seq(Row.fromSeq(Seq(message.toString)))
  }
}

class KafkaStreamToRowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[Row] = {
    val status: Status = TwitterObjectFactory.createStatus(message.asInstanceOf[String])
    TwitterObjectFactory.getRawJSON(message)
    Seq(Row.fromSeq(Seq(status.getId,
      status.getText,
      status.getUser.getName,
      status.getUser.getLang,
      status.getRetweetCount,
      status.getHashtagEntities.mkString(","))))
  }
}
