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

import scala.collection.mutable.Queue

import io.snappydata.SnappyFunSuite
import org.scalatest.BeforeAndAfter
import twitter4j.{Status, TwitterObjectFactory}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.streaming.{SchemaDStream, SnappyStreamingContext, StreamToRowsConverter}
import org.apache.spark.streaming._
import org.apache.spark.unsafe.types.UTF8String

/**
  * Created by ymahajan on 25/09/15.
  */
class StreamingSuite extends SnappyFunSuite with BeforeAndAfter {

  private var ssnc: SnappyStreamingContext = _

  def framework: String = this.getClass.getSimpleName

  def master: String = "local[2]"

  def batchDuration: Duration = Seconds(1)

  before {
    ssnc = SnappyStreamingContext(snc, batchDuration);
    // ssc.checkpoint("/tmp")
  }

  after {
    if (ssnc != null) {
      SnappyStreamingContext.stop()
    }
  }
  test("stream ad-hoc sql") {
    ssnc.sql("create stream table tweetsTable " +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        "consumerKey '***REMOVED***', " +
        "consumerSecret '***REMOVED***', " +
        "accessToken '***REMOVED***', " +
        "accessTokenSecret '***REMOVED***', " +
        "rowConverter 'io.snappydata.app.streaming.TweetToRowsConverter')")

    val cqResult = ssnc.registerCQ("SELECT text FROM tweetsTable " +
        "window (duration '10' seconds, slide '10' seconds) where text like '%e%'")
    cqResult.foreachDataFrame(df => df.count())

    ssnc.sql("STREAMING START")

    val dynamicCQResult = ssnc.registerCQ("SELECT text, fullName FROM tweetsTable " +
        "window (duration '4' seconds, slide '4' seconds) where text like '%e%'")
    dynamicCQResult.foreachDataFrame(df => df.count())

    for (a <- 1 to 5) {
      Thread.sleep(2000)
      ssnc.sql("select text, fullName from tweetsTable where text like '%e%'").count()
    }
    ssnc.sql("drop table tweetsTable")
    ssnc.awaitTerminationOrTimeout(10 * 1000)
  }

  test("save stream to external table using forEachDataFrame") {
    def getQueueOfRDDs1: Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(1 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(11 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(21 to 30).map(i => Tweet(i, s"Text$i"))
      Queue(distData1, distData2, distData3)
    }

    val dStream1 = ssnc.queueStream[Tweet](getQueueOfRDDs1)
    val map = Map.empty[String, String]
    val schemaStream1 = ssnc.createSchemaDStream(dStream1)
    ssnc.snappyContext.dropTable("gemxdColumnTable1", true)
    schemaStream1.foreachDataFrame(df => {
      df.write.format("column").mode(SaveMode.Append)
          .options(Map.empty[String, String]).saveAsTable("gemxdColumnTable1")
    })

    ssnc.snappyContext.dropTable("gemxdColumnTable2", true)
    schemaStream1.foreachDataFrame((df, time) => {
      df.write.format("column").mode(SaveMode.Append)
          .options(Map.empty[String, String]).saveAsTable("gemxdColumnTable2")
    })

    ssnc.start
    ssnc.awaitTerminationOrTimeout(20 * 1000)

    val result1 = ssnc.sql("select * from gemxdColumnTable1")
    val r1 = result1.collect
    assert(r1.length == 30)

    val result2 = ssnc.sql("select * from gemxdColumnTable2")
    val r2 = result2.collect
    assert(r2.length == 30)
  }


  test("SNAP-240 NotSerializableException with checkpoint") {

    def getQueueOfRDDs: Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(1 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(11 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(21 to 30).map(i => Tweet(i, s"Text$i"))
      Queue(distData1, distData2, distData3)
    }

    val dStream = ssnc.queueStream[Tweet](getQueueOfRDDs)

    val schemaDStream = ssnc.createSchemaDStream(dStream)
    schemaDStream.foreachRDD(rdd => {
      // schemaDStream.createDataFrame (rdd).show() //NotSerializableException
      // println(rdd) // scalastyle:ignore
    })

    SnappyStreamingContext.start
    ssnc.awaitTerminationOrTimeout(5 * 1000)
  }

  test("api stream to stream and stream to table join") {
    def getQueueOfRDDs1: Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(1 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(11 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(21 to 30).map(i => Tweet(i, s"Text$i"))
      Queue(distData1, distData2, distData3)
    }

    val dStream1 = ssnc.queueStream[Tweet](getQueueOfRDDs1)

    val schemaStream1 = ssnc.createSchemaDStream(dStream1)
    schemaStream1.foreachDataFrame(df => {
      df.count()
    })
    schemaStream1.registerAsTable("tweetStream1")

    def getQueueOfRDDs2: Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(9 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(19 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(29 to 30).map(i => Tweet(i, s"Text$i"))
      Queue(distData1, distData2, distData3)
    }

    val dStream2 = ssnc.queueStream[Tweet](getQueueOfRDDs2)

    val schemaStream2 = ssnc.createSchemaDStream(dStream2)
    schemaStream2.foreachDataFrame(df => {
      df.count()
    })
    schemaStream2.registerAsTable("tweetStream2")

    val resultStream: SchemaDStream = ssnc.registerCQ("SELECT t1.id, t1.text FROM " +
        "tweetStream1 window (duration '2' seconds, slide '2' seconds) t1 JOIN " +
        "tweetStream2 t2 ON t1.id = t2.id ")

    ssnc.snappyContext.dropTable("gemxdColumnTable", true)
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
        "(duration '4' seconds, slide '4' seconds) " +
        "t1 JOIN tweetTable t2 ON t1.id = t2.id")
    resultSet.foreachDataFrame(df => {
      df.write.format("column").mode(SaveMode.Append).options(Map.empty[String, String])
          .saveAsTable("gemxdColumnTable")
    })

    SnappyStreamingContext.start
    ssnc.awaitTerminationOrTimeout(20 * 1000)

    val result = ssnc.sql("select * from gemxdColumnTable")
    val r = result.collect
    assert(r.length > 0)
    ssnc.sql("drop table gemxdColumnTable")
  }

  test("dynamic CQ") {

    def getQueueOfRDDs1: Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(1 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(11 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(21 to 30).map(i => Tweet(i, s"Text$i"))
      Queue(distData1, distData2, distData3)
    }
    val dStream1 = ssnc.queueStream[Tweet](getQueueOfRDDs1)

    val schemaStream1 = ssnc.createSchemaDStream(dStream1)
    schemaStream1.foreachDataFrame(df => {
      df.count()
    })
    schemaStream1.registerAsTable("tweetStream1")

    def getQueueOfRDDs2: Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(9 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(19 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(29 to 30).map(i => Tweet(i, s"Text$i"))
      Queue(distData1, distData2, distData3)
    }

    val dStream2 = ssnc.queueStream[Tweet](getQueueOfRDDs2)

    val schemaStream2 = ssnc.createSchemaDStream(dStream2)
    schemaStream2.foreachDataFrame(df => {
      df.count()
    })
    schemaStream2.registerAsTable("tweetStream2")

    val resultStream: SchemaDStream = ssnc.registerCQ("SELECT t1.id, t1.text" +
        " FROM tweetStream1 window (duration '2' seconds, slide '2' seconds)" +
        "t1 JOIN tweetStream2 t2 ON t1.id = t2.id ")

    ssnc.snappyContext.dropTable("gemColumnTable", true)
    ssnc.snappyContext.createTable("gemColumnTable", "column", schemaStream1.schema,
      Map.empty[String, String])

    resultStream.foreachDataFrame(df => {
      df.write.format("column").mode(SaveMode.Append).options(Map.empty[String, String])
          .saveAsTable("gemColumnTable")
    })

    val df = ssnc.snappyContext.createDataFrame(
      sc.parallelize(1 to 10).map(i => Tweet(i / 2, s"Text${i / 2}")))
    df.registerTempTable("tweetTable")

    SnappyStreamingContext.start

    val resultSet = ssnc.registerCQ("SELECT t2.id, t2.text FROM tweetStream1 window" +
        " (duration '4' seconds, slide '4' seconds) " +
        "t1 JOIN tweetTable t2 ON t1.id = t2.id")

    resultSet.foreachDataFrame(df => {
      df.write.format("column").mode(SaveMode.Append).options(Map.empty[String, String])
          .saveAsTable("gemColumnTable")
    })
    ssnc.awaitTerminationOrTimeout(20 * 1000)

    val result = ssnc.sql("select * from gemColumnTable")
    val r = result.collect
    assert(r.length > 0)
    ssnc.sql("drop table gemColumnTable")

  }

  /* ignore("sql stream sampling") {

    ssnc.sql("create stream table tweetstreamtable " +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        "consumerKey '***REMOVED***', " +
        "consumerSecret '***REMOVED***', " +
        "accessToken '***REMOVED***', " +
        "accessTokenSecret '***REMOVED***', " +
        "rowConverter 'io.snappydata.app.streaming.TweetToRowsConverter')")

    val tableStream = ssnc.getSchemaDStream("tweetstreamtable")

    ssnc.snappyContext.registerSampleTable("tweetstreamtable_sampled", tableStream.schema, Map(
      "qcs" -> "hashtag",
      "fraction" -> "0.05",
      "strataReservoirSize" -> "300",
      "timeInterval" -> "3m"), Some("tweetstreamtable"))

    tableStream.saveStream(Seq("tweetstreamtable_sampled"))

    ssnc.sql("create table rawStreamColumnTable(id long, " +
        "text string, " +
        "fullName string, " +
        "country string, " +
        "retweets int, " +
        "hashtag string) " +
        "using column " +
        "options('PARTITION_BY','id')")

    var numTimes = 0
    tableStream.foreachDataFrame { df =>
      df.write.format("column").mode(SaveMode.Append).options(Map.empty[String, String])
          .saveAsTable("rawStreamColumnTable")

      val top10Tags = ssnc.sql("select count(*) as cnt, hashtag from " +
          "rawStreamColumnTable where length(hashtag) > 0 group by hashtag " +
          "order by cnt desc limit 10").collect()
      top10Tags.foreach(println) // scalastyle:ignore

      numTimes += 1
      if ((numTimes % 18) == 1) {
        ssnc.sql("SELECT count(*) FROM rawStreamColumnTable").count()
      }

      val stop10Tags = ssnc.sql("select count(*) as cnt, " +
          "hashtag from tweetstreamtable_sampled where length(hashtag) > 0 " +
          "group by hashtag order by cnt desc limit 10").collect()
      stop10Tags.foreach(println) // scalastyle:ignore
    }

    ssnc.sql("STREAMING START")
    ssnc.awaitTerminationOrTimeout(10 * 1000)
    ssnc.sql("drop table tweetstreamtable_sampled")
    ssnc.sql("drop table tweetstreamtable")
    ssnc.sql("drop table rawStreamColumnTable")
  }

  ignore("sql on socket streams") {

    ssnc.sql("create stream table socketStreamTable (name string) " +
        "using socket_stream options (" +
        "hostname 'localhost', " +
        "port '9998', " +
        "storagelevel 'MEMORY_AND_DISK_SER_2', " +
        "rowConverter 'io.snappydata.app.streaming.LineToRowsConverter') ")

    ssnc.registerCQ("SELECT * FROM socketStreamTable window " +
        "(duration '10' seconds, slide '10' seconds) ")

    ssnc.sql("STREAMING START")
    ssnc.sql("drop table socketStreamTable")
  }
*/

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


  test("sql on direct kafka streams") {

    intercept[Exception] {
      // java.nio.channels.ClosedChannelException since no kafka cluster
      ssnc.sql("create stream table directKafkaStreamTable (name string, age int) " +
          "using directkafka_stream options " +
          "(storagelevel 'MEMORY_AND_DISK_SER_2', " +
          "rowConverter 'io.snappydata.app.streaming.KafkastreamToRowsConverter', " +
          " kafkaParams 'metadata.broker.list->localhost:9092', " +
          "topics 'tweets')")
    }
    val ex = intercept[Exception] {
      ssnc.sql("STREAMING START")
    }
    assert(ex.getMessage === "requirement failed: No output operations" +
        " registered, so nothing to execute")
  }

  test("sql on file streams") {

    // var hfile: String = getClass.getResource("/2015.parquet").getPath
    ssnc.sql("create stream table fileStreamTable (name string, age int)" +
        " using file_stream options " +
        "(storagelevel 'MEMORY_AND_DISK_SER_2', " +
        "rowConverter 'io.snappydata.app.streaming.KafkaStreamToRowsConverter', " +
        " directory '/tmp')")
    ssnc.registerCQ("SELECT name FROM fileStreamTable window " +
        "(duration '10' seconds, slide '10' seconds) WHERE age >= 18")
    // val thrown = intercept[Exception] {
    ssnc.sql("STREAMING START")
    // }
    // assert(thrown.getMessage === "requirement failed: No output operations" +
    //    " registered, so nothing to execute")
    ssnc.sql("drop table fileStreamTable")
  }
}

case class Tweet(id: Int, text: String)

class TweetToRowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[InternalRow] = {
    val status: Status = message.asInstanceOf[Status]
    Seq(InternalRow.fromSeq(Seq(status.getId,
      UTF8String.fromString(status.getText),
      UTF8String.fromString(status.getUser().getName),
      UTF8String.fromString(status.getUser.getLang),
      status.getRetweetCount, UTF8String.fromString(
        status.getHashtagEntities.mkString(",")))))
  }

}

class LineToRowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[InternalRow] = {
    Seq(InternalRow.fromSeq(Seq(UTF8String.fromString(message.toString))))
  }
}

class KafkaStreamToRowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[InternalRow] = {
    val status: Status = TwitterObjectFactory.createStatus(message.asInstanceOf[String])
    TwitterObjectFactory.getRawJSON(message)
    Seq(InternalRow.fromSeq(Seq(status.getId,
      UTF8String.fromString(status.getText),
      UTF8String.fromString(status.getUser().getName),
      UTF8String.fromString(status.getUser.getLang),
      status.getRetweetCount, UTF8String.fromString(
        status.getHashtagEntities.mkString(",")))))
  }

  /* override def toRow(message: Any): Seq[InternalRow] = {
    // TODO Yogesh. convert this raw JSON string to twitter4j.SatusJSONImpl
    val status : Status = TwitterObjectFactory.createStatus(message.asInstanceOf[String])
    // val tweet = new JSONObject(message.asInstanceOf[String])
    val hashTags = status.getHashtagEntities

    val limit = KafkaMessageToRowConverter.rand.nextInt(20000)
    (0 until limit).flatMap { i =>
      val id = status.getId + (i.toLong *
        KafkaMessageToRowConverter.rand.nextInt(100000))
      if (hashTags.length <= 1) {
        Seq(InternalRow.fromSeq(Seq(id,
          UTF8String.fromString(status.getText),
          UTF8String.fromString(status.getUser().getName),
          UTF8String.fromString(status.getUser.getLang),
          status.getRetweetCount,
          UTF8String.fromString(if (hashTags.isEmpty) "" else hashTags(0).getText))))
      } else {
        hashTags.map { tag =>
          InternalRow.fromSeq(Seq(id,
            UTF8String.fromString(status.getText),
            UTF8String.fromString(status.getUser().getName),
            UTF8String.fromString(status.getUser.getLang),
            status.getRetweetCount, UTF8String.fromString(tag.getText)))
        }
      }
    }
    // Row.fromSeq
  }  */
}

/*
object KafkaMessageToRowConverter {
  private val rand = new Random
} */