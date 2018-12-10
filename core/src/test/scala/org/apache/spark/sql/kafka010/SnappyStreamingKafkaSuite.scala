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

package org.apache.spark.sql.kafka010

import java.util
import java.util.Arrays
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

import io.snappydata.SnappyFunSuite
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization._
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.StreamToRowsConverter
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Milliseconds, SnappyStreamingContext}

class SnappyStreamingKafkaSuite extends SnappyFunSuite with Eventually
    with BeforeAndAfter with BeforeAndAfterAll {

  private lazy val session = snc.sparkSession

  private var kafkaTestUtils: KafkaTestUtils = _

  override def beforeAll() {
    super.beforeAll()
    kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()
  }

  override def afterAll() {
    super.afterAll()
    if (kafkaTestUtils != null) {
      kafkaTestUtils.teardown()
      kafkaTestUtils = null
    }
  }

  def framework: String = this.getClass.getSimpleName

  private val topicId = new AtomicInteger(0)

  private def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

  protected var ssnc: SnappyStreamingContext = _

  def batchDuration: Duration = Milliseconds(1000)

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


  test("basic kafka streaming pipeline") {
    val topics = List("pat1", "pat2", "pat3", "advanced3")
    // Should match 3 out of 4 topics
    val pat =
      """pat\d""".r.pattern
    val data = Map("a" -> 7, "b" -> 9)
    topics.foreach { t =>
      kafkaTestUtils.createTopic(t)
      kafkaTestUtils.sendMessages(t, data)
    }
    val offsets = Map(
      new TopicPartition("pat2", 0) -> 3L,
      new TopicPartition("pat3", 0) -> 4L)
    // 3 matching topics, two of which start a total of 7 messages later
    val expectedTotal = (data.values.sum * 3) - 7
    val kafkaParams = Map(
      "bootstrap.servers" -> kafkaTestUtils.brokerAddress,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> s"test-consumer-${Random.nextInt(10000)}",
      "auto.offset.reset" -> "earliest")
    val preferredHosts = LocationStrategies.PreferConsistent

    val allReceived = new ConcurrentLinkedQueue[(String, String)]()
    val stream = KafkaUtils.createDirectStream[String, String](
      ssnc, preferredHosts,
      ConsumerStrategies.SubscribePattern[String, String](pat, kafkaParams, offsets))

    stream.foreachRDD { rdd =>
      allReceived.addAll(Arrays.asList(rdd.map(r => (r.key, r.value)).collect(): _*))
    }

    ssnc.start()
    eventually(timeout(100000.milliseconds), interval(1000.milliseconds)) {
      assert(allReceived.size === expectedTotal,
        "didn't get expected number of messages, messages:\n" +
            allReceived.asScala.mkString("\n"))
    }
    ssnc.stop(stopSparkContext = false)
  }

  test("Snappy kafka streaming") {
    val topic = newTopic()
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val groupId = s"test-consumer-" + Random.nextInt(10000)
    val add = kafkaTestUtils.brokerAddress

    val partitions = Map(
      new TopicPartition(topic, 0) -> 0L,
      new TopicPartition(topic, 1) -> 0L,
      new TopicPartition(topic, 2) -> 0L
    )

    val startingOffsets = JsonUtils.partitionOffsets(partitions)

    ssnc.sql("create stream table kafkaStream (" +
        " publisher string)" +
        " using kafka_stream options(" +
        " rowConverter 'org.apache.spark.sql.kafka010.RowsConverter' ," +
        s" kafkaParams 'bootstrap.servers->$add;" +
        "key.deserializer->org.apache.kafka.common.serialization.StringDeserializer;" +
        "value.deserializer->org.apache.kafka.common.serialization.StringDeserializer;" +
        s"group.id->$groupId;auto.offset.reset->earliest'," +
        s"startingOffsets '$startingOffsets', " +
        s" subscribe '$topic')")

    snc.dropTable("snappyTable", ifExists = true)
    snc.createTable("snappyTable", "column",
      StructType(Seq(StructField("pubisher", StringType, nullable = false))),
      Map.empty[String, String])
    ssnc.registerCQ("select * from kafkaStream" +
        " window (duration 1 seconds, slide 1 seconds)")
        .foreachDataFrame(_.write.insertInto("snappyTable"))

    ssnc.start()
    kafkaTestUtils.sendMessages(topic, (100 to 200).map(_.toString).toArray, Some(0))
    kafkaTestUtils.sendMessages(topic, (10 to 20).map(_.toString).toArray, Some(1))
    kafkaTestUtils.sendMessages(topic, Array("1"), Some(2))
    eventually(timeout(100000.milliseconds), interval(1000.milliseconds)) {
      assert(113 == session.sql("select * from snappyTable").count)
    }
    ssnc.stop(stopSparkContext = false)
  }

  test("Snappy kafka streaming with custom deserializer") {

    val topic = newTopic()
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val groupId = s"test-consumer-" + Random.nextInt(10000)
    val add = kafkaTestUtils.brokerAddress

    val partitions = Map(
      new TopicPartition(topic, 0) -> 0L,
      new TopicPartition(topic, 1) -> 0L,
      new TopicPartition(topic, 2) -> 0L
    )

    kafkaTestUtils.sendMessages(topic,
      (100 to 200).map(i => s"$i,name$i,$i").toArray)
    ssnc.sql("create stream table kafkaStream (" +
        " id Long, name String, age int)" +
        " using kafka_stream options(" +
        " rowConverter 'org.apache.spark.sql.kafka010.UserRowsConverter' ," +
        s" kafkaParams 'bootstrap.servers->$add;" +
        "key.deserializer->org.apache.kafka.common.serialization.StringDeserializer;" +
        "value.deserializer->org.apache.spark.sql.kafka010.UserDeserializer;" +
        s"group.id->$groupId;auto.offset.reset->earliest'," +
        s" subscribe '$topic')")

    snc.dropTable("snappyTable", ifExists = true)
    snc.createTable("snappyTable", "column",
      StructType(Seq(StructField("id", LongType, nullable = false)
        , StructField("name", StringType, nullable = false)
        , StructField("age", IntegerType, nullable = false))),
      Map.empty[String, String])
    ssnc.registerCQ("select * from kafkaStream" +
        " window (duration 1 seconds, slide 1 seconds)")
        .foreachDataFrame(_.write.insertInto("snappyTable"))
    ssnc.start()
    eventually(timeout(100000.milliseconds), interval(1000.milliseconds)) {
      assert(101 == session.sql("select * from snappyTable").count)
    }
    ssnc.stop(stopSparkContext = false)
  }
}

case class User(id: Long, name: String, age: Integer)

class UserDeserializer extends Deserializer[User] {

  def deserialize(topic: String, data: Array[Byte]): User = {
    val s = new String(data)
    val split = s.split(",")
    User(split(0).toLong, split(1), split(2).toInt)
  }

  def close() {}

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}
}


class RowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[Row] = {
    val log = message.asInstanceOf[String]
    val rows = Seq(Row.fromSeq(log.split(",")))
    rows
  }
}

class UserRowsConverter extends StreamToRowsConverter with Serializable {

  override def toRows(message: Any): Seq[Row] = {
    val user = message.asInstanceOf[User]
    val rows = Seq(Row(user.id, user.name, user.age))
    rows
  }
}