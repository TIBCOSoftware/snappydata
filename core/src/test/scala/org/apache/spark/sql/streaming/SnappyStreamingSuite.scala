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
package org.apache.spark.sql.streaming

import java.io.File
import java.net.InetSocketAddress
import java.util.Properties
import java.util.concurrent.TimeoutException

import io.snappydata.SnappyFunSuite
import kafka.admin.AdminUtils
import kafka.api.Request
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.{StringDecoder, StringEncoder}
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{ZKStringSerializer, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.commons.lang3.RandomUtils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, Row, SaveMode}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.{KafkaCluster, KafkaUtils}
import org.apache.spark.streaming.{Duration, Seconds, SnappyStreamingContext, Time}
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkConf}
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import twitter4j.{Status, TwitterObjectFactory}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random
import scala.util.control.NonFatal


class SnappyStreamingSuite
    extends SnappyFunSuite with Eventually
    with BeforeAndAfter with BeforeAndAfterAll{

  private var kc: KafkaCluster = _

  protected var kafkaUtils: EmbeddedKafkaUtils = _

  override def beforeAll() {
    kafkaUtils = new EmbeddedKafkaUtils
    kafkaUtils.setup()
    kc = new KafkaCluster(Map("metadata.broker.list" -> kafkaUtils.brokerAddress))
  }

  override def afterAll() {
    if (kafkaUtils != null) {
      kafkaUtils.teardown()
      kafkaUtils = null
    }
  }

  protected var ssnc: SnappyStreamingContext = _

  def framework: String = this.getClass.getSimpleName

  def batchDuration: Duration = Seconds(1)

  def creatingFunc(): SnappyStreamingContext = {
    val context = new SnappyStreamingContext(sc, batchDuration)
    context.remember(Duration(3000))
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

  val consumerKey = "0Xo8rg3W0SOiqu14HZYeyFPZi"
  val consumerSecret = "gieTDrdzFS4b1g9mcvyyyadOkKoHqbVQALoxfZ19eHJzV9CpLR"
  val accessToken = "43324358-0KiFugPFlZNfYfib5b6Ah7c2NdHs1524v7LM2qaUq"
  val accessTokenSecret = "aB1AXHaRiE3g2d7tLgyASdgIg9J7CzbPKBkNfvK8Y88bu"

  test("Kafka input stream") {
    val topic = "topic1"
    val sent = Map("a" -> 5, "b" -> 3, "c" -> 10)
    kafkaUtils.createTopic(topic)
    kafkaUtils.sendMessages(topic, sent)

    val kafkaParams = Map(
      "zookeeper.connect" -> kafkaUtils.zkAddress,
      "group.id" -> s"test-consumer-${Random.nextInt(10000)}",
      "auto.offset.reset" -> "smallest")

    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssnc, kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_ONLY)

    val result = new mutable.HashMap[String, Long]()

    stream.map(_._2).countByValue().foreachRDD { r =>
      r.collect().foreach { kv =>
        result.synchronized {
          val count = result.getOrElseUpdate(kv._1, 0) + kv._2
          result.put(kv._1, count)
        }
      }
    }

    ssnc.start()

    eventually(timeout(10000 milliseconds), interval(100 milliseconds)) {
      assert(result.synchronized {
        sent === result
      })
    }

  }

  test("SnappyData Kafka Streaming") {
    val topic = "kafka_topic"
    var sent = Map("1" -> 1, "2" -> 1, "3" -> 1)
    kafkaUtils.createTopic(topic)
    kafkaUtils.sendMessages(topic, sent)

    val topic2 = "kafka_topic2"
    kafkaUtils.createTopic(topic2)

    val add = kafkaUtils.zkAddress
    ssnc.sql("create stream table if not exists kafkaStream (" +
        " publisher string) " +
        " using kafka_stream options(" +
        " rowConverter 'org.apache.spark.sql.streaming.RowsConverter'," +
        s" kafkaParams 'zookeeper.connect->$add;auto.offset.reset->smallest;group.id->myGroupId'," +
        s" topics '$topic:1,$topic2:1')")

    val stream = ssnc.getSchemaDStream("kafkaStream")

    val repartitioned = stream.repartition(2)
    val filtered = repartitioned.filter(row => row.getString(0).startsWith("2"))

    val result = new mutable.HashMap[String, Long]()

    filtered.foreachDataFrame(df => {
      df.collect().foreach(row => {
        result.synchronized {
          result.put(row.getString(0), 1)
        }
      })
    })

    // scalastyle:off println
    filtered.glom().foreachRDD(rdd => rdd.foreach(_.foreach(println)))
    val mapped = filtered.map(row => row.getString(0).toInt)
    mapped.foreachRDD(rdd => rdd.foreach(println))
    mapped.reduce(_ + _).foreachRDD(rdd => println(rdd.first()))
    mapped.count().foreachRDD(rdd => println(rdd.first()))
    // mapped.mapPartitions { _ => Seq.empty.toIterator }
    mapped.mapPartitions { x => Iterator(x.sum)}
    mapped.transform(rdd => rdd.map(_.toString))
    // scalastyle:on println

    ssnc.start()
    sent = Map("2" -> 1)
    eventually(timeout(10000 milliseconds), interval(100 milliseconds)) {
      assert(result.synchronized {
        sent === result
      })
    }
  }

  test("Test stream plan optimizations") {
    val topic1 = "direct_kafka_topic1"
    kafkaUtils.createTopic(topic1)

    val add = kafkaUtils.brokerAddress
    ssnc.sql("create stream table directKafkaStream (" +
        " publisher string, advertiser string)" +
        " using directkafka_stream options(" +
        " rowConverter 'org.apache.spark.sql.streaming.RowsConverter' ," +
        s" kafkaParams 'metadata.broker.list->$add;auto.offset.reset->smallest'," +
        s" topics '$topic1')")

    val result = new scala.collection.mutable.HashMap[String, Long]()

    val cqResults = ssnc.registerCQ("select publisher from" +
        " directKafkaStream window (duration 1 seconds, slide 1 seconds)" +
        " where publisher like '%77%' order by publisher")

    cqResults.foreachDataFrame { df =>
      df.collect().foreach { row =>
        result.synchronized {
          val key = s"${row.getString(0)}"
          result.put(key, result.getOrElse(key, 0L) + 1)
        }
      }
    }

    ssnc.start()
    val sent1 = Map("pub1,adv1,1" -> 1, "pub2,adv2,2" -> 1, "pub3,adv3,3" -> 1)
    kafkaUtils.sendMessages(topic1, sent1)
    val sent2 = Map("pub4,adv4,4" -> 1, "pub5,adv5,5" -> 1, "pub6,adv6,6" -> 1)
    kafkaUtils.sendMessages(topic1, sent2)
    val sent3 = Map("pub7,adv7,7" -> 1, "pub8,adv8,8" -> 1, "pub9,adv9,9" -> 1,
      "pub77,adv77,77" -> 1)
    kafkaUtils.sendMessages(topic1, sent3)

    val sent = Map("pub77" -> 1)
    eventually(timeout(4000 milliseconds), interval(200 milliseconds)) {
      assert(result.synchronized {
        sent === result
      })
    }
  }

  test("SNAP-2003 stream to big ref table join") {
    val topic = "snap2003Topic"
    kafkaUtils.createTopic(topic)

    val add = kafkaUtils.brokerAddress
    ssnc.sql("create stream table directKafkaStream (" +
      " publisher string, advertiser string)" +
      " using directkafka_stream options(" +
      " rowConverter 'org.apache.spark.sql.streaming.RowsConverter' ," +
      s" kafkaParams 'metadata.broker.list->$add;auto.offset.reset->smallest'," +
      s" topics '$topic')")
    val snappy = snc
    import snappy.implicits._
    val df = snc.sparkContext.parallelize(Seq(("pub2", "adv2"), ("pub4", "adv4"),
      ("pub6", "adv6"), ("pub8", "adv8"))).toDF("pub", "adv")

    ssnc.snappyContext.dropTable("refTable", ifExists = true)
    ssnc.snappyContext.createTable("refTable", "column", df.schema,
      Map.empty[String, String])

    df.write.insertInto("refTable")

    snc.sparkContext.range(1, 1000000).map(l => (l.toString, l.toString))
      .toDF("pub", "adv").write.insertInto("refTable")
    val resultSet = ssnc.registerCQ("SELECT t1.advertiser, t2.pub FROM directKafkaStream window " +
      "(duration 1 seconds, slide 1 seconds) " +
      "t1 JOIN refTable t2 ON t1.publisher = t2.pub")

    val result = new mutable.HashMap[String, String]()

    resultSet.foreachDataFrame(df => {
      df.collect().foreach(row => {
        result.synchronized {
          result.put(row.getString(0), row.getString(1))
        }
      })
    })

    ssnc.start()
    kafkaUtils.sendMessages(topic, Map("pub1,adv1,1" -> 1, "pub2,adv2,2" -> 1))
    Thread.sleep(1000)
    kafkaUtils.sendMessages(topic, Map("pub3,adv3,1" -> 1, "pub4,adv4,2" -> 1))
    Thread.sleep(1000)
    kafkaUtils.sendMessages(topic, Map("pub5,adv5,1" -> 1, "pub6,adv6,2" -> 1))
    Thread.sleep(1000)
    kafkaUtils.sendMessages(topic, Map("pub7,adv7,1" -> 1, "pub8,adv8,2" -> 1))
    eventually(timeout(10000 milliseconds), interval(100 milliseconds)) {
      assert(result.synchronized {
        result.size > 1
      })
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
      StructType(Seq(
        StructField("publisher", StringType, true),
        StructField("advertiser", StringType, true)))
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

  test("SNAP-408") {
    ssnc.sql("create stream table tableStream " +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        s"consumerKey '$consumerKey', " +
        s"consumerSecret '$consumerSecret', " +
        s"accessToken '$accessToken', " +
        s"accessTokenSecret '$accessTokenSecret', " +
        "rowConverter 'org.apache.spark.sql.streaming.TweetToRowsConverter')")
    ssnc.sql("STREAMING START")
    for (a <- 1 to 3) {
      Thread.sleep(2000)
      ssnc.sql("select id, text, fullName from tableStream where text like '%e%'").count()
    }
    // try drop from another streaming context
    SnappyStreamingContext.getActive.foreach {
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
        s"consumerKey '$consumerKey', " +
        s"consumerSecret '$consumerSecret', " +
        s"accessToken '$accessToken', " +
        s"accessTokenSecret '$accessTokenSecret', " +
        "rowConverter 'org.apache.spark.sql.streaming.TweetToRowsConverter')")
    for (a <- 1 to 3) {
      Thread.sleep(1000)
      ssnc.sql("select id, text, fullName from tableStream where text like '%e%'").count()
    }
    ssnc.sql("drop table tableStream")
  }

  test("stream ad-hoc sql") {
    // test SNAP-993
    ssnc.sql("create stream table if not exists tweetsTable" +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        s"consumerKey '$consumerKey', " +
        s"consumerSecret '$consumerSecret', " +
        s"accessToken '$accessToken', " +
        s"accessTokenSecret '$accessTokenSecret', " +
        "rowConverter 'org.apache.spark.sql.streaming.TweetToRowsConverter')")
    ssnc.sql("drop table tweetsTable")

    ssnc.sql("create stream table tweetsTable " +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        s"consumerKey '$consumerKey', " +
        s"consumerSecret '$consumerSecret', " +
        s"accessToken '$accessToken', " +
        s"accessTokenSecret '$accessTokenSecret', " +
        "rowConverter 'org.apache.spark.sql.streaming.TweetToRowsConverter')")

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

  test("save SchemaDStream to column table") {
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

    waitForCriterion(
      try {
        val result1 = ssnc.sql("select * from gemxdColumnTable1")
        result1.collect().length == 30
      } catch {
        case _: AnalysisException => false
      }, "data ingestion in gemxdColumnTable1", 60000, 500, throwOnTimeout = true)

    waitForCriterion(
      try {
        val result2 = ssnc.sql("select * from gemxdColumnTable2")
        result2.collect().length == 30
      } catch {
        case _: AnalysisException => false
      }, "data ingestion in gemxdColumnTable2", 60000, 500, throwOnTimeout = true)

    ssnc.awaitTerminationOrTimeout(20 * 1000)
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

    // Create Table to collect the data for schemaStream1
    ssnc.snappyContext.dropTable("dataTable", ifExists = true)
    ssnc.snappyContext.createTable("dataTable", "column", schemaStream1.schema,
      Map.empty[String, String])

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

    schemaStream1.foreachDataFrame(df => {

      df.write.format("column").mode(SaveMode.Append).options(Map.empty[String, String])
          .saveAsTable("dataTable")
    })


    val resultStream: SchemaDStream = ssnc.registerCQ("SELECT t1.id, t1.text FROM " +
        "tweetStream1 window (duration 2 seconds, slide 1 seconds) t1 " +
        "JOIN tweetStream2 t2 ON t1.id = t2.id ")


    ssnc.snappyContext.dropTable("joinDataColumnTable", ifExists = true)
    ssnc.snappyContext.createTable("joinDataColumnTable", "column", schemaStream2.schema,
      Map.empty[String, String])

    resultStream.foreachDataFrame(df => {
      df.write.format("column").mode(SaveMode.Append).options(Map.empty[String, String])
          .saveAsTable("joinDataColumnTable")
    })

    val df = ssnc.snappyContext.createDataFrame(
      sc.parallelize(1 to 10).map(i => Tweet(i / 2, s"Text${i / 2}")))
    df.createOrReplaceTempView("tweetTable")

    val resultSet = ssnc.registerCQ("SELECT t2.id, t2.text FROM tweetStream1 window " +
        "(duration 2 seconds, slide 1 seconds) " +
        "t1 JOIN tweetTable t2 ON t1.id = t2.id")
    ssnc.snappyContext.dropTable("tempColumnTable", ifExists = true)
    ssnc.snappyContext.createTable("tempColumnTable", "column", schemaStream1.schema,
      Map.empty[String, String])
    resultSet.foreachDataFrame(df => {
      df.write.format("column").mode(SaveMode.Append).options(Map.empty[String, String])
          .saveAsTable("tempColumnTable")
    })

    ssnc.start()


    // Assert all values that were inserted during streaming
    var listOfRows: Array[Int] = null
    waitForCriterion({
      listOfRows = snc.sql("select id from dataTable").collect() map {
        row => row.getInt(0)
      }
      listOfRows.length == 30
    }, "data ingestion in dataTable", 60000, 500, true)

    ssnc.awaitTerminationOrTimeout(20 * 1000)

    assert(listOfRows.length == 30)
    // Assert values
    val colValues = 1 to 30
    colValues.foreach(v => assert(listOfRows.contains(v)))
    ssnc.sql("drop table dataTable")

    val tempResult = ssnc.sql("select * from tempColumnTable")
    val records = tempResult.collect()
    assert(records.length > 0)
    ssnc.sql("drop table tempColumnTable")


    val result = ssnc.sql("select id from joinDataColumnTable")
    val expectedValues = Seq(9, 10, 19, 20, 29, 30)

    val r = result.collect()  map {
      row => row.getInt(0)
    }

    // TODO:  This currently fails as join results are not proper
    expectedValues.foreach(v => assert(r.contains(v)))
    assert(r.length > 0)
    ssnc.sql("drop table joinDataColumnTable")
  }

  test("SNAP-1411"){
    ssnc.sql("streaming init 2secs")
    ssnc.sql("create stream table if not exists tweetsTable" +
      "(id long, text string, fullName string, " +
      "country string, retweets int, hashtag string) " +
      "using twitter_stream options (" +
      s"consumerKey '$consumerKey', " +
      s"consumerSecret '$consumerSecret', " +
      s"accessToken '$accessToken', " +
      s"accessTokenSecret '$accessTokenSecret', " +
      "rowConverter 'org.apache.spark.sql.streaming.TweetToRowsConverter')")
    ssnc.sql("streaming start")
    for (a <- 1 to 3) {
      Thread.sleep(1000)
      ssnc.sql("select text, fullName from tweetsTable where text like '%e%'").count()
    }
    ssnc.sql("drop table tweetsTable")
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
class RowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[Row] = {
    val log = message.asInstanceOf[String]
    val rows = Seq(Row.fromSeq(log.split(",")))
    rows
  }
}
protected class EmbeddedKafkaUtils extends Logging {

  // Zookeeper related configurations
  private val zkHost = "localhost"
  private var zkPort: Int = 0
  private val zkConnectionTimeout = 60000
  private val zkSessionTimeout = 6000

  private var zookeeper: EmbeddedZookeeper = _

  private var zkClient: ZkClient = _

  // Kafka broker related configurations
  private val brokerHost = "localhost"
  // 0.8.2 server doesn't have a boundPort method, so can't use 0 for a random port
  private var brokerPort = RandomUtils.nextInt(1024, 65536)
  private var brokerConf: KafkaConfig = _

  // Kafka broker server
  private var server: KafkaServer = _

  // Kafka producer
  private var producer: Producer[String, String] = _

  // Flag to test whether the system is correctly started
  private var zkReady = false
  private var brokerReady = false

  def zkAddress: String = {
    assert(zkReady, "Zookeeper not setup yet or already torn down, cannot get zookeeper address")
    s"$zkHost:$zkPort"
  }

  def brokerAddress: String = {
    assert(brokerReady, "Kafka not setup yet or already torn down, cannot get broker address")
    s"$brokerHost:$brokerPort"
  }

  def zookeeperClient: ZkClient = {
    assert(zkReady, "Zookeeper not setup yet or already torn down, cannot get zookeeper client")
    Option(zkClient).getOrElse(
      throw new IllegalStateException("Zookeeper client is not yet initialized"))
  }

  // Set up the Embedded Zookeeper server and get the proper Zookeeper port
  private def setupEmbeddedZookeeper(): Unit = {
    // Zookeeper server startup
    zookeeper = new EmbeddedZookeeper(s"$zkHost:$zkPort")
    // Get the actual zookeeper binding port
    zkPort = zookeeper.actualPort
    zkClient = new ZkClient(s"$zkHost:$zkPort", zkSessionTimeout, zkConnectionTimeout,
      ZKStringSerializer)
    zkReady = true
  }

  // Set up the Embedded Kafka server
  private def setupEmbeddedKafkaServer(): Unit = {
    assert(zkReady, "Zookeeper should be set up beforehand")

    // Kafka broker startup
    Utils.startServiceOnPort(brokerPort, port => {
      brokerPort = port
      brokerConf = new KafkaConfig(brokerConfiguration)
      server = new KafkaServer(brokerConf)
      server.startup()
      (server, brokerPort)
    }, new SparkConf(), "KafkaBroker")

    brokerReady = true
  }

  /** setup the whole embedded servers, including Zookeeper and Kafka brokers */
  def setup(): Unit = {
    setupEmbeddedZookeeper()
    setupEmbeddedKafkaServer()
  }

  /** Teardown the whole servers, including Kafka broker and Zookeeper */
  def teardown(): Unit = {
    brokerReady = false
    zkReady = false

    if (producer != null) {
      producer.close()
      producer = null
    }

    if (server != null) {
      server.shutdown()
      server = null
    }

    brokerConf.logDirs.foreach { f => Utils.deleteRecursively(new File(f)) }

    if (zkClient != null) {
      zkClient.close()
      zkClient = null
    }

    if (zookeeper != null) {
      zookeeper.shutdown()
      zookeeper = null
    }
  }

  /** Create a Kafka topic and wait until it is propagated to the whole cluster */
  def createTopic(topic: String, partitions: Int): Unit = {
    AdminUtils.createTopic(zkClient, topic, partitions, 1)
    // wait until metadata is propagated
    (0 until partitions).foreach { p => waitUntilMetadataIsPropagated(topic, p) }
  }

  /** Single-argument version for backwards compatibility */
  def createTopic(topic: String): Unit = createTopic(topic, 1)

  /** Send the messages to the Kafka broker */
  def sendMessages(topic: String, messageToFreq: Map[String, Int]): Unit = {
    val messages = messageToFreq.flatMap { case (s, freq) => Seq.fill(freq)(s) }.toArray
    sendMessages(topic, messages)
  }

  /** Send the array of messages to the Kafka broker */
  def sendMessages(topic: String, messages: Array[String]): Unit = {
    producer = new Producer[String, String](new ProducerConfig(producerConfiguration))
    producer.send(messages.map {
      new KeyedMessage[String, String](topic, _)
    }: _*)
    producer.close()
    producer = null
  }

  private def brokerConfiguration: Properties = {
    val props = new Properties()
    props.put("broker.id", "0")
    props.put("host.name", "localhost")
    props.put("port", brokerPort.toString)
    props.put("log.dir", Utils.createTempDir().getAbsolutePath)
    props.put("zookeeper.connect", zkAddress)
    props.put("log.flush.interval.messages", "1")
    props.put("replica.socket.timeout.ms", "1500")
    props
  }

  private def producerConfiguration: Properties = {
    val props = new Properties()
    props.put("metadata.broker.list", brokerAddress)
    props.put("serializer.class", classOf[StringEncoder].getName)
    // wait for all in-sync replicas to ack sends
    props.put("request.required.acks", "-1")
    props
  }

  // A simplified version of scalatest eventually, rewritten here to avoid adding extra test
  // dependency
  def eventually[T](timeout: Time, interval: Time)(func: => T): T = {
    def makeAttempt(): Either[Throwable, T] = {
      try {
        Right(func)
      } catch {
        case e if NonFatal(e) => Left(e)
      }
    }

    val startTime = System.currentTimeMillis()
    @tailrec
    def tryAgain(attempt: Int): T = {
      makeAttempt() match {
        case Right(result) => result
        case Left(e) =>
          val duration = System.currentTimeMillis() - startTime
          if (duration < timeout.milliseconds) {
            Thread.sleep(interval.milliseconds)
          } else {
            throw new TimeoutException(e.getMessage)
          }

          tryAgain(attempt + 1)
      }
    }

    tryAgain(1)
  }

  private def waitUntilMetadataIsPropagated(topic: String, partition: Int): Unit = {
    def isPropagated = server.apis.metadataCache.getPartitionInfo(topic, partition) match {
      case Some(partitionState) =>
        val leaderAndInSyncReplicas = partitionState.leaderIsrAndControllerEpoch.leaderAndIsr

        ZkUtils.getLeaderForPartition(zkClient, topic, partition).isDefined &&
            Request.isValidBrokerId(leaderAndInSyncReplicas.leader) &&
            leaderAndInSyncReplicas.isr.nonEmpty

      case _ =>
        false
    }
    eventually(Time(10000), Time(100)) {
      assert(isPropagated, s"Partition [$topic, $partition] metadata not propagated after timeout")
    }
  }

  private class EmbeddedZookeeper(val zkConnect: String) {
    val snapshotDir = Utils.createTempDir()
    val logDir = Utils.createTempDir()

    val zookeeper = new ZooKeeperServer(snapshotDir, logDir, 500)
    val (ip, port) = {
      val splits = zkConnect.split(":")
      (splits(0), splits(1).toInt)
    }
    val factory = new NIOServerCnxnFactory()
    factory.configure(new InetSocketAddress(ip, port), 16)
    factory.startup(zookeeper)

    val actualPort = factory.getLocalPort

    def shutdown() {
      factory.shutdown()
      Utils.deleteRecursively(snapshotDir)
      Utils.deleteRecursively(logDir)
    }
  }

}