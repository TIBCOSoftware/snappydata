/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql


import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.io.Path

import io.snappydata.Property.SchedulerPool
import io.snappydata.SnappyFunSuite
import io.snappydata.core.LocalSparkConf
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.kafka010.KafkaTestUtils
import org.apache.spark.sql.streaming.{ProcessingTime, SnappySinkCallback}

class SchedulerPoolTest extends SnappyFunSuite with BeforeAndAfter with BeforeAndAfterAll {

  private var kafkaTestUtils: KafkaTestUtils = _

  private val checkPointDir = "/tmp/SchedulerPoolSuite"

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

  after {
    // CAUTION!! - recursively deleting checkpoint directory. handle with care.
    Path(checkPointDir).deleteRecursively()
  }

  protected override def newSparkConf(addOn: SparkConf => SparkConf): SparkConf = {
    val xmlPath = getClass.getClassLoader.getResource("testFairscheduler.xml").getFile
    LocalSparkConf.newConf(c => {
      c.set("spark.scheduler.allocation.file", xmlPath)
          .set("spark.scheduler.mode", "FAIR")
    })
  }

  private val topicId = new AtomicInteger(0)

  private def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

  test("custom scheduler pool") {
    SchedulerPool.set(snc.sessionState.conf, "custom")
    val topic = newTopic()

    startStreaming(snc.snappySession, topic, "org.apache.spark.sql.TestSinkCallbackCustomPool",
      "testQuery1")

    assert(snc.sparkContext.getLocalProperty("spark.scheduler.pool") == null)
  }

  test("default scheduler pool") {
    SchedulerPool.remove(snc.sessionState.conf)
    val topic = newTopic()
    startStreaming(snc.snappySession, topic,
      "org.apache.spark.sql.TestSinkCallbackStreamingPoolNotConfigured",
      "testQuery2")

    assert(snc.sparkContext.getLocalProperty("spark.scheduler.pool") == null)
  }

  private def startStreaming(snc: SnappySession, topic: String, callback: String,
      queryName: String): Unit = {
    kafkaTestUtils.createTopic(topic, partitions = 3)
    kafkaTestUtils.sendMessages(topic,
      (1 to 10).map(i => "record" + i).toArray, Some(0))

    val streamingDF = snc
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaTestUtils.brokerAddress)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()

    val streamingQuery = streamingDF.writeStream.format("snappysink")
        .queryName(queryName)
        .trigger(ProcessingTime("1 seconds"))
        .option("streamqueryid", queryName)
        .option("sinkcallback", callback)
        .option("checkpointLocation", checkPointDir)
        .start()

    streamingQuery.processAllAvailable()
  }
}

class TestSinkCallbackStreamingPoolNotConfigured extends SnappySinkCallback {
  override def process(snappySession: SnappySession, sinkProps: Map[String, String],
      batchId: Long, df: Dataset[Row], possibleDuplicate: Boolean): Unit = {
    assert(snappySession.sparkContext.getLocalProperty("spark.scheduler.pool") == "default")
  }
}

class TestSinkCallbackCustomPool extends SnappySinkCallback {
  override def process(snappySession: SnappySession, sinkProps: Map[String, String],
      batchId: Long, df: Dataset[Row], possibleDuplicate: Boolean): Unit = {
    assert(snappySession.sparkContext.getLocalProperty("spark.scheduler.pool") == "custom")
  }
}