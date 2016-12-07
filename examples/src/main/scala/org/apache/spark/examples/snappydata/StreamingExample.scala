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

package org.apache.spark.examples.snappydata

import java.io.File
import java.net.InetSocketAddress
import java.util.Properties

import scala.language.postfixOps

import kafka.admin.AdminUtils
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.StringEncoder
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import org.apache.commons.lang3.RandomUtils
import org.apache.log4j.{Level, Logger}
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.jdbc.{ConnectionConfBuilder, ConnectionUtil}
import org.apache.spark.sql.streaming.{SchemaDStream, StreamToRowsConverter}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.{Seconds, SnappyStreamingContext}
import org.apache.spark.util.Utils

/**
 * An example showing usage of streaming with SnappyData
 *
 * <p></p>
 * To run the example in local mode go to your SnappyData product distribution
 * directory and type following command on the command prompt
 * <pre>
 * bin/run-example snappydata.StreamingExample
 * </pre>
 *
 * This example starts embedded Kafka and publishes messages(advertising bids)
 * on it to be processed in SnappyData as streaming data. In SnappyData, streams
 * are managed declaratively by creating a stream table(adImpressionStream). A
 * continuous query is executed on the stream and its result is ingested in a column
 * table(aggrAdImpressions).
 *
 * We also update a row table to maintain the no of distinct bids so far(example
 * of storing and updating a state of streaming data)
 *
 * For more details on streaming with SnappyData refer to:
 * http://snappydatainc.github.io/snappydata/streamingWithSQL/
 *
 */
object StreamingExample {

  def main(args: Array[String]) {
    // reducing the log level to minimize the messages on console
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    println("Initializing a SnappyStreamingContext")
    val spark: SparkSession = SparkSession
        .builder
        .appName(getClass.getSimpleName)
        .master("local[*]")
        .getOrCreate

    val snsc = new SnappyStreamingContext(spark.sparkContext, Seconds(1))

    println()
    println("Initializing a embedded Kafka")
    val utils = new EmbeddedKafkaUtils()
    utils.setup()
    val topic = "kafka_topic"
    utils.createTopic(topic)


    val add = utils.brokerAddress
    println()
    println("Creating a stream table to read data from Kafka")
    snsc.sql("drop table if exists adImpressionStream")
    // Streams can be managed as tables declaratively using SQL
    // statements.
    // rowConverter attribute specifies a class that converts a
    // stream message into a Row object
    // for more details on stream tables refer to
    // http://snappydatainc.github.io/snappydata/streamingWithSQL/
    snsc.sql(
      "create stream table adImpressionStream (" +
          " time_stamp timestamp," +
          " publisher string," +
          " advertiser string," +
          " website string," +
          " geo string," +
          " bid double," +
          " cookie string) " + " using directkafka_stream options(" +
          " rowConverter 'org.apache.spark.examples.snappydata.RowsConverter'," +
          s" kafkaParams 'metadata.broker.list->$add;auto.offset.reset->smallest'," +
          s" topics '$topic')"
    )

    println()
    // Next, create a column table to ingest aggregate data in one window
    println("Creating a column table to ingest aggregate data in one window")
    snsc.sql("create table aggrAdImpressions(time_stamp timestamp, publisher string," +
        " geo string, avg_bid double, imps long, uniques long) " +
        "using column ")

    println()
    println("Creating a row table to maintain total no of distinct bids received in streaming window")
    snsc.sql("create table totalBids(bidCount int) using row ")
    snsc.sql("insert into totalBids values(0)")

    println()
    // Execute this query once every second. Output is a SchemaDStream.
    println("Registering a continuous query to to be executed every second on the stream table")
    // you can register continuous queries that will be executed as per the mentioned time window
    val resultStream: SchemaDStream = snsc.registerCQ(
      "select min(time_stamp), publisher, geo, avg(bid) as avg_bid," +
          " count(*) as imps , count(distinct(cookie)) as uniques" +
          " from adImpressionStream window (duration 1 seconds, slide 1 seconds)" +
          " group by publisher, geo")

    // this conf is used to get a connection a JDBC connection
    val conf = new ConnectionConfBuilder(snsc.snappySession).build()

    println()
    // process the stream data received in a window
    resultStream.foreachDataFrame(df => {
      println("Data returned by the registered continuous query")
      df.show

      println("Ingesting the result of continuous query into aggrAdImpressions table")
      df.write.insertInto("aggrAdImpressions")

      val numBids = df.count()
      println("Number of distinct bids in this window=" + numBids)
      val conn = ConnectionUtil.getConnection(conf)
      val stmt = conn.prepareStatement("update totalBids set bidCount = bidCount + " + numBids)
      stmt.executeUpdate()
      conn.close()
    })

    snsc.start

    println("Publishing messages on Kafka")
    publishKafkaMessages(utils, topic)

    Thread.sleep(3000)
    println("Total no of distinct bids received in all windows so far")
    snsc.snappySession.sql("select bidCount from totalBids").show()


    println("****Exiting****")
    snsc.awaitTerminationOrTimeout(50)
  }

  def publishKafkaMessages(utils: EmbeddedKafkaUtils, topic: String): Unit = {
    for (i <- 1 until 10) {
      val currentTime = System.currentTimeMillis()

      // bids with comma separated fields
      //timestamp, publisher,advertiser,web,geo,bid,cookie
      val bid1 = currentTime + ",publisher1,advt1,pb1.web,US," + scala.util.Random.nextDouble() + ",23543"
      val bid2 = currentTime + ",publisher2,advt1,pb1.web,US," + scala.util.Random.nextDouble() + ",45445"
      val bid3 = currentTime + ",publisher3,advt2,pb1.web,US," + scala.util.Random.nextDouble() + ",13434"
      val bid4 = currentTime + ",publisher4,advt2,pb1.web,US," + scala.util.Random.nextDouble() + ",34324"
      val bid5 = currentTime + ",publisher2,advt1,pb1.web,US," + scala.util.Random.nextDouble() + ",23233"
      val bid6 = currentTime + ",publisher4,advt2,pb1.web,US," + scala.util.Random.nextDouble() + ",43545"

      // publish the bids as a Kafka message
      utils.sendMessages(topic, Array(bid1, bid2, bid3, bid4, bid5, bid6))
      println("Published message containing 6 rows")
      Thread.sleep(1000)
    }

  }
}


/**
 * Converts an input stream message into an org.apache.spark.sql.Row
 * instance
 */
class RowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[Row] = {
    val log = message.asInstanceOf[String]
    val fields = log.split(",")
    val rows = Seq(Row.fromSeq(Seq(new java.sql.Timestamp(fields(0).toLong),
      fields(1),
      fields(2),
      fields(3),
      fields(4),
      fields(5).toDouble,
      fields(6)
    )))

    rows
  }
}

/**
 * Utility class to start embedded Kafka and publish messages
 */
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

  /** Shutdown the whole servers, including Kafka broker and Zookeeper */
  def shutdown(): Unit = {
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
  }

  /** Single-argument version for backwards compatibility */
  def createTopic(topic: String): Unit = createTopic(topic, 1)

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