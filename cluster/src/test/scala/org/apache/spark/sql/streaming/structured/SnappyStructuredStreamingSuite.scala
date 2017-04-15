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

package org.apache.spark.sql.streaming.structured

import java.util.concurrent.atomic.AtomicInteger

import io.snappydata.SnappyFunSuite
import org.apache.kafka.common.TopicPartition
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}
import org.apache.spark.streaming.{Duration, Seconds, SnappyStreamingContext}
import org.json4s.NoTypeHints
import org.json4s.jackson.Serialization
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import scala.collection.mutable.HashMap

class SnappyStructuredStreamingSuite extends SnappyFunSuite with Eventually
  with BeforeAndAfter with BeforeAndAfterAll {

  private implicit val formats = Serialization.formats(NoTypeHints)

  private var kafkaUtils: KafkaTestUtils = _

  override def beforeAll() {
    kafkaUtils = new KafkaTestUtils
    kafkaUtils.setup()
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

  private val topicId = new AtomicInteger(0)

  private def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

  test("SnappyData Structured Streaming with Kafka") {
    val topic = newTopic()
    kafkaUtils.createTopic(topic, partitions = 3)
    kafkaUtils.sendMessages(topic, (100 to 200).map(_.toString).toArray, Some(0))
    kafkaUtils.sendMessages(topic, (10 to 20).map(_.toString).toArray, Some(1))
    kafkaUtils.sendMessages(topic, Array("1"), Some(2))

    val spark = snc.sparkSession

    import spark.implicits._

    val reader = snc.sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("maxOffsetsPerTrigger", 10)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")

    /*
    root
    |-- key: binary (nullable = true)
    |-- value: binary (nullable = true)
    |-- topic: string (nullable = true)
    |-- partition: integer (nullable = true)
    |-- offset: long (nullable = true)
    |-- timestamp: timestamp (nullable = true)
    |-- timestampType: integer (nullable = true)
    */

    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped: org.apache.spark.sql.Dataset[_] = kafka.map(kv => kv._2.toInt)

    val query = mapped
      .writeStream
      .format("console")
      .outputMode("append")
      .trigger(ProcessingTime("3 seconds"))
      .start

    query.awaitTermination(timeoutMs = 15000)
  }

  def partitionOffsets(partitionOffsets: Map[TopicPartition, Long]): String = {
    val result = new HashMap[String, HashMap[Int, Long]]()
    implicit val ordering = new Ordering[TopicPartition] {
      override def compare(x: TopicPartition, y: TopicPartition): Int = {
        Ordering.Tuple2[String, Int].compare((x.topic, x.partition), (y.topic, y.partition))
      }
    }
    val partitions = partitionOffsets.keySet.toSeq.sorted  // sort for more determinism
    partitions.foreach { tp =>
      val off = partitionOffsets(tp)
      val parts = result.getOrElse(tp.topic, new HashMap[Int, Long])
      parts += tp.partition -> off
      result += tp.topic -> parts
    }
    Serialization.write(result)
  }

  test("SnappyData Structured Streaming with Kafka - Snappy sink") {
    val topic = newTopic()
    kafkaUtils.createTopic(topic, partitions = 3)

    val spark = snc.sparkSession

    import spark.implicits._

    val partitions = Map(
      new TopicPartition(topic, 0) -> 0L,
      new TopicPartition(topic, 1) -> 0L,
      new TopicPartition(topic, 2) -> 0L
    )

    val startingOffsets = partitionOffsets(partitions)

    println(startingOffsets)

    val reader = snc.sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("maxOffsetsPerTrigger", 10)
      .option("subscribe", topic)
      .option("startingOffsets", startingOffsets)
      .option("failOnDataLoss", "false")

    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped: org.apache.spark.sql.Dataset[_] = kafka.map(kv => kv._2.toInt)

    val query = mapped
      .writeStream
      .format("snappy")
      .option("checkpointLocation", "/tmp")
      .queryName("snappyTable")
      .outputMode(OutputMode.Append)
      .trigger(ProcessingTime("1 seconds"))
      .start

    kafkaUtils.sendMessages(topic, (100 to 200).map(_.toString).toArray, Some(0))
    kafkaUtils.sendMessages(topic, (10 to 20).map(_.toString).toArray, Some(1))
    kafkaUtils.sendMessages(topic, Array("1"), Some(2))

    query.awaitTermination(timeoutMs = 15000)
    spark.sql("select * from snappyTable").show
    assert(113 == spark.sql("select * from snappyTable").count)
  }

  test("SnappyData Structured Streaming with Kafka - SnappyForeachWriter sink") {
    val topic = newTopic()
    kafkaUtils.createTopic(topic, partitions = 3)
    kafkaUtils.sendMessages(topic, (100 to 200).map(_.toString).toArray, Some(0))
    kafkaUtils.sendMessages(topic, (10 to 20).map(_.toString).toArray, Some(1))
    kafkaUtils.sendMessages(topic, Array("1"), Some(2))

    val spark = snc.sparkSession

    import spark.implicits._

    val reader = snc.sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("maxOffsetsPerTrigger", 10)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")

    val kafka = reader.load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped: org.apache.spark.sql.Dataset[Int] = kafka.map(kv => kv._2.toInt)

    val query = mapped
      .writeStream
      .foreach(new SnappyForeachWriter())
      .outputMode("append")
      .trigger(ProcessingTime("3 seconds"))
      .start

    query.awaitTermination(timeoutMs = 15000)
  }
}