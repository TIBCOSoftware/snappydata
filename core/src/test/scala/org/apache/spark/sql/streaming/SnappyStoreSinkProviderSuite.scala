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

import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.io.Path

import io.snappydata.SnappyFunSuite
import org.apache.log4j.LogManager
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.kafka010.KafkaTestUtils
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.streaming.{Milliseconds, SnappyStreamingContext}

class SnappyStoreSinkProviderSuite extends SnappyFunSuite
    with BeforeAndAfter with BeforeAndAfterAll {

  private val session = snc.sparkSession

  import session.implicits._

  private var kafkaTestUtils: KafkaTestUtils = _
  protected var ssnc: SnappyStreamingContext = _

  private val batchDurationMillis: Long = 1000
  private val batchWriteWaitMillis = 7000
  private val streamTimeoutMillis = 10000

  private val testIdGenerator = new AtomicInteger(0)

  override def beforeAll() {
    kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()
  }

  override def afterAll() {
    if (kafkaTestUtils != null) {
      kafkaTestUtils.teardown()
      kafkaTestUtils = null
    }
  }

  def creatingFunc(): SnappyStreamingContext = {
    val context = new SnappyStreamingContext(sc, Milliseconds(batchDurationMillis))
    context
  }

  before {
    SnappyStreamingContext.getActive.foreach {
      _.stop(stopSparkContext = false, stopGracefully = true)
    }
    ssnc = SnappyStreamingContext.getActiveOrCreate(creatingFunc)
  }

  private val checkpointDirectory = "/tmp/SnappyStoreSinkProviderSuite"

  after {
    baseCleanup(false)
    SnappyStreamingContext.getActive.foreach {
      _.stop(stopSparkContext = false, stopGracefully = true)
    }

    // CAUTION!! - recursively deleting checkpoint directory. handle with care.
    Path(checkpointDirectory).deleteRecursively()
  }


  test("_eventType column: absent, key columns: defined, table type: column") {
    val testId = testIdGenerator.getAndIncrement()
    createTable()
    val topic = getTopic(testId)

    kafkaTestUtils.createTopic(topic, partitions = 3)

    val dataBatch1 = Seq(Seq(1, "name1", 30), Seq(2, "name2", 10),
      Seq(3, "name3", 30))

    kafkaTestUtils.sendMessages(topic, dataBatch1.map(r => r.mkString(",")).toArray)

    val streamingQuery = createAndStartStreamingQuery(topic, testId, withEventTypeColumn = false)
    Thread.sleep(batchDurationMillis + batchWriteWaitMillis)

    val rows1 = Array(Row(1, "name1", 30), Row(2, "name2", 10), Row(3, "name3", 30))
    assertData(rows1)

    val dataBatch2 = Seq(Seq(1, "name11", 40), Seq(4, "name4", 50))
    kafkaTestUtils.sendMessages(topic, dataBatch2.map(r => r.mkString(",")).toArray)

    streamingQuery.awaitTermination(streamTimeoutMillis)

    val rows = Array(Row(1, "name11", 40), Row(2, "name2", 10),
      Row(3, "name3", 30), Row(4, "name4", 50))
    assertData(rows)
  }


  private def getTopic(id: Int) = s"topic-$id"


  test("_eventType column: absent, key columns: not-defined, table type: column") {
    val testId = testIdGenerator.getAndIncrement()
    createTable(withKeyColumn = false)
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val streamingQuery = createAndStartStreamingQuery(topic, testId, withEventTypeColumn = false)

    val dataBatch = Seq(Seq(1, "name1", 30), Seq(2, "name2", 10),
      Seq(3, "name3", 30), Seq(1, "name1", 30))

    kafkaTestUtils.sendMessages(topic, dataBatch.map(r => r.mkString(",")).toArray)

    streamingQuery.awaitTermination(streamTimeoutMillis)

    val rows = Array(Row(1, "name1", 30), Row(1, "name1", 30), Row(2, "name2", 10),
      Row(3, "name3", 30))
    assertData(rows)
  }

  test("_eventType column: present, key columns: defined, table type: column") {
    val testId = testIdGenerator.getAndIncrement()
    createTable()
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val dataBatch1 = Seq(Seq(1, "name1", 20, 0), Seq(2, "name2", 10, 0))
    kafkaTestUtils.sendMessages(topic, dataBatch1.map(r => r.mkString(",")).toArray)

    val streamingQuery: StreamingQuery = createAndStartStreamingQuery(topic, testId)

    Thread.sleep(batchDurationMillis + batchWriteWaitMillis)

    assertData(Array(Row(1, "name1", 20), Row(2, "name2", 10)))

    val dataBatch2 = Seq(Seq(1, "name11", 30, 1), Seq(2, "name2", 10, 2), Seq(3, "name3", 30, 0))
    kafkaTestUtils.sendMessages(topic, dataBatch2.map(r => r.mkString(",")).toArray)
    streamingQuery.awaitTermination(timeoutMs = streamTimeoutMillis)

    assertData(Array(Row(1, "name11", 30), Row(3, "name3", 30)))
  }

  test("_eventType column: present, key columns: not-defined, table type: column") {
    val testId = testIdGenerator.getAndIncrement()
    createTable(withKeyColumn = false)
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val dataBatch = Seq(Seq(1, "name1", 20, 0), Seq(2, "name2", 10, 0))
    kafkaTestUtils.sendMessages(topic, dataBatch.map(r => r.mkString(",")).toArray)


    val thrown = intercept[StreamingQueryException] {
      val streamingQuery = createAndStartStreamingQuery(topic, testId)
      streamingQuery.awaitTermination(timeoutMs = streamTimeoutMillis)
    }

    val errorMessage = "_eventType is present in data but key columns are not defined on table."
    assert(thrown.getCause.getMessage == errorMessage)

  }


  private def assertData(expectedData: Array[Row]) = {
    expectedData.equals()
    val actualData = session.sql("select * from APP.USERS order by id").collect()
    assertResult(expectedData)(actualData)
  }

  private def createTable(withKeyColumn: Boolean = true, isRowTable: Boolean = false) = {
    def provider = if (isRowTable) "row" else "column"

    def options = if (withKeyColumn) "options(key_columns 'id')" else ""

    snc.sql("drop table if exists users")
    val s = s"create table users (id long, name varchar(40), age int) using $provider $options"
    LogManager.getRootLogger.error(s)
    snc.sql(s)
  }

  private def createAndStartStreamingQuery(topic: String, testId: Int,
      withEventTypeColumn: Boolean = true) = {
    val streamingDF = session
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaTestUtils.brokerAddress)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()

    def structFields() = {
      StructField("id", LongType, nullable = false) ::
          StructField("name", StringType, nullable = true) ::
          StructField("age", IntegerType, nullable = true) ::
          (if (withEventTypeColumn) {
            StructField("_eventType", IntegerType, nullable = false) :: Nil
          }
          else {
            Nil
          })
    }

    val schema = StructType(structFields())

    implicit val encoder = RowEncoder(schema)

    streamingDF.selectExpr("CAST(value AS STRING)")
        .as[String]
        .map(_.split(",")).map(r => {
          if (r.length == 4) {
            Row(r(0).toLong, r(1), r(2).toInt, r(3).toInt)
          } else {
            Row(r(0).toLong, r(1), r(2).toInt)
          }
        })
        .writeStream
        .format("snappysink")
        .queryName(s"USERS_$testId")
        .outputMode("append")
        .trigger(ProcessingTime("1 seconds"))
        .option("tablename", "APP.USERS").option("SINKID", s"users_$testId")
        .option("checkpointLocation", checkpointDirectory)
        .start()
  }
}
