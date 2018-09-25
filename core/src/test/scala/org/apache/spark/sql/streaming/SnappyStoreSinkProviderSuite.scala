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

import io.snappydata.{SnappyFunSuite, StreamingConstants}
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
  private val testIdGenerator = new AtomicInteger(0)
  private val tableName = "APP.USERS"
  private val checkpointDirectory = "/tmp/SnappyStoreSinkProviderSuite"

  private def getTopic(id: Int) = s"topic-$id"

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

  private def creatingFunc() = new SnappyStreamingContext(sc, Milliseconds(batchDurationMillis))

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

    // CAUTION!! - recursively deleting checkpoint directory. handle with care.
    Path(checkpointDirectory).deleteRecursively()
  }

  test("_eventType column: absent, key columns: defined, table type: column") {
    val testId = testIdGenerator.getAndIncrement()
    createTable()()
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val dataBatch1 = Seq(Seq(1, "name1", 30), Seq(2, "name2", 10),
      Seq(3, "name3", 30))
    kafkaTestUtils.sendMessages(topic, dataBatch1.map(r => r.mkString(",")).toArray)

    val streamingQuery = createAndStartStreamingQuery(topic, testId, withEventTypeColumn = false)
    waitTillTheBatchIsPickedForProcessing(0, testId)

    val dataBatch2 = Seq(Seq(1, "name11", 40), Seq(4, "name4", 50))
    kafkaTestUtils.sendMessages(topic, dataBatch2.map(r => r.mkString(",")).toArray)

    streamingQuery.processAllAvailable()

    val rows = Array(Row(1, "name11", 40), Row(2, "name2", 10),
      Row(3, "name3", 30), Row(4, "name4", 50))
    assertData(rows)
  }

  test("_eventType column: absent, key columns: undefined, table type: column") {
    val testId = testIdGenerator.getAndIncrement()
    createTable(withKeyColumn = false)()
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val streamingQuery = createAndStartStreamingQuery(topic, testId, withEventTypeColumn = false)

    val dataBatch = Seq(Seq(1, "name1", 30), Seq(2, "name2", 10),
      Seq(3, "name3", 30), Seq(1, "name1", 30))
    kafkaTestUtils.sendMessages(topic, dataBatch.map(r => r.mkString(",")).toArray)

    streamingQuery.processAllAvailable()

    val rows = Array(Row(1, "name1", 30), Row(1, "name1", 30), Row(2, "name2", 10),
      Row(3, "name3", 30))
    assertData(rows)
  }

  test("_eventType column: present, key columns: defined, table type: column") {
    val testId = testIdGenerator.getAndIncrement()
    createTable()()
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val dataBatch1 = Seq(Seq(1, "name1", 20, 0), Seq(2, "name2", 10, 0))
    kafkaTestUtils.sendMessages(topic, dataBatch1.map(r => r.mkString(",")).toArray)

    val streamingQuery: StreamingQuery = createAndStartStreamingQuery(topic, testId)
    waitTillTheBatchIsPickedForProcessing(0, testId)

    val dataBatch2 = Seq(Seq(1, "name11", 30, 1), Seq(2, "name2", 10, 2), Seq(3, "name3", 30, 0))
    kafkaTestUtils.sendMessages(topic, dataBatch2.map(r => r.mkString(",")).toArray)
    streamingQuery.processAllAvailable()

    assertData(Array(Row(1, "name11", 30), Row(3, "name3", 30)))
  }

  test("_eventType column: present, key columns: undefined, table type: column") {
    val testId = testIdGenerator.getAndIncrement()
    createTable(withKeyColumn = false)()
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val dataBatch = Seq(Seq(1, "name1", 20, 0), Seq(2, "name2", 10, 0))
    kafkaTestUtils.sendMessages(topic, dataBatch.map(r => r.mkString(",")).toArray)

    val thrown = intercept[StreamingQueryException] {
      val streamingQuery = createAndStartStreamingQuery(topic, testId)
      streamingQuery.processAllAvailable()
    }

    val errorMessage = "_eventType is present in data but key columns are not defined on table."
    assert(thrown.getCause.getMessage == errorMessage)
  }

  test("_eventType column: absent, key columns: defined, table type: row") {
    val testId = testIdGenerator.getAndIncrement()
    createTable()(isRowTable = true)
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val dataBatch1 = Seq(Seq(1, "name1", 30), Seq(2, "name2", 10),
      Seq(3, "name3", 30))
    kafkaTestUtils.sendMessages(topic, dataBatch1.map(r => r.mkString(",")).toArray)

    val streamingQuery = createAndStartStreamingQuery(topic, testId, withEventTypeColumn = false)
    waitTillTheBatchIsPickedForProcessing(0, testId)

    val dataBatch2 = Seq(Seq(1, "name11", 40), Seq(4, "name4", 50))
    kafkaTestUtils.sendMessages(topic, dataBatch2.map(r => r.mkString(",")).toArray)

    streamingQuery.processAllAvailable()

    val rows = Array(Row(1, "name11", 40), Row(2, "name2", 10),
      Row(3, "name3", 30), Row(4, "name4", 50))
    assertData(rows)
  }

  test("_eventType column: absent, key columns: undefined, table type: row") {
    val testId = testIdGenerator.getAndIncrement()
    createTable(withKeyColumn = false)(isRowTable = true)
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val streamingQuery = createAndStartStreamingQuery(topic, testId, withEventTypeColumn = false)

    val dataBatch = Seq(Seq(1, "name1", 30), Seq(2, "name2", 10),
      Seq(3, "name3", 30), Seq(1, "name1", 30))
    kafkaTestUtils.sendMessages(topic, dataBatch.map(r => r.mkString(",")).toArray)

    streamingQuery.processAllAvailable()

    val rows = Array(Row(1, "name1", 30), Row(1, "name1", 30), Row(2, "name2", 10),
      Row(3, "name3", 30))
    assertData(rows)
  }

  test("_eventType column: present, key columns: defined, table type: row") {
    val testId = testIdGenerator.getAndIncrement()
    createTable()(isRowTable = true)
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val dataBatch1 = Seq(Seq(1, "name1", 20, 0), Seq(2, "name2", 10, 0))
    kafkaTestUtils.sendMessages(topic, dataBatch1.map(r => r.mkString(",")).toArray)

    val streamingQuery: StreamingQuery = createAndStartStreamingQuery(topic, testId)
    waitTillTheBatchIsPickedForProcessing(0, testId)

    val dataBatch2 = Seq(Seq(1, "name11", 30, 1), Seq(2, "name2", 10, 2), Seq(3, "name3", 30, 0))
    kafkaTestUtils.sendMessages(topic, dataBatch2.map(r => r.mkString(",")).toArray)
    streamingQuery.processAllAvailable()

    assertData(Array(Row(1, "name11", 30), Row(3, "name3", 30)))
  }

  test("_eventType column: present, key columns: undefined, table type: row") {
    val testId = testIdGenerator.getAndIncrement()
    createTable(withKeyColumn = false)(isRowTable = true)
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val dataBatch = Seq(Seq(1, "name1", 20, 0), Seq(2, "name2", 10, 0))
    kafkaTestUtils.sendMessages(topic, dataBatch.map(r => r.mkString(",")).toArray)

    val thrown = intercept[StreamingQueryException] {
      val streamingQuery = createAndStartStreamingQuery(topic, testId)
      streamingQuery.processAllAvailable()
    }

    val errorMessage = "_eventType is present in data but key columns are not defined on table."
    assert(thrown.getCause.getMessage == errorMessage)
  }

  test("test idempotency") {
    val testId = testIdGenerator.getAndIncrement()
    createTable()()
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    kafkaTestUtils.sendMessages(topic, (0 to 10).map(i => s"$i,name$i,$i,0").toArray)

    val streamingQuery: StreamingQuery = createAndStartStreamingQuery(topic, testId)
    waitTillTheBatchIsPickedForProcessing(0, testId)
    streamingQuery.stop()

    val streamingQuery1: StreamingQuery = createAndStartStreamingQuery(topic, testId, true, true)
    kafkaTestUtils.sendMessages(topic, (11 to 20).map(i => s"$i,name$i,$i,0").toArray)
    try {
      streamingQuery1.processAllAvailable()
    } catch {
      case ex: StreamingQueryException if ex.cause.getMessage == "dummy failure for test" =>
        streamingQuery1.stop()
    }

    val streamingQuery2: StreamingQuery = createAndStartStreamingQuery(topic, testId)

    kafkaTestUtils.sendMessages(topic, (21 to 30).map(i => s"$i,name$i,$i,0").toArray)
    waitTillTheBatchIsPickedForProcessing(1, testId)
    streamingQuery2.processAllAvailable()

    assertData((0 to 30).map(i => Row(i, s"name$i", i)).toArray)
  }

  private def waitTillTheBatchIsPickedForProcessing(batchId: Int, testId: Int,
      retries: Int = 15): Unit = {
    if (retries == 0) {
      throw new RuntimeException(s"Batch id $batchId not found in sink status table")
    }
    val sql = s"select batch_id from ${StreamingConstants.SINK_STATE_TABLE} " +
        s"where stream_query_id = '${streamQueryId(testId)}'"
    val batchIdFromTable = snc.sql(sql).collect()

    if (batchIdFromTable.isEmpty || batchIdFromTable(0)(0) != batchId) {
      Thread.sleep(1000)
      waitTillTheBatchIsPickedForProcessing(batchId, testId, retries - 1)
    }
  }

  private def assertData(expectedData: Array[Row]) = {
    val actualData = session.sql("select * from " + tableName + " order by id").collect()
    assertResult(expectedData)(actualData)
  }

  private def createTable(withKeyColumn: Boolean = true)(isRowTable: Boolean = false) = {
    snc.sql("drop table if exists users")
    def provider = if (isRowTable) "row" else "column"
    def options = if (!isRowTable && withKeyColumn) "options(key_columns 'id')" else ""
    def primaryKey = if (isRowTable && withKeyColumn) "primary key" else ""

    val s = s"create table users (id long $primaryKey, name varchar(40), age int) " +
        s"using $provider $options"
    LogManager.getRootLogger.error(s)
    snc.sql(s)
  }

  private def createAndStartStreamingQuery(topic: String, testId: Int,
      withEventTypeColumn: Boolean = true, failBatch: Boolean = false) = {
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

    val streamWriter = streamingDF.selectExpr("CAST(value AS STRING)")
        .as[String]
        .map(_.split(","))
        .map(r => {
          if (r.length == 4) {
            Row(r(0).toLong, r(1), r(2).toInt, r(3).toInt)
          } else {
            Row(r(0).toLong, r(1), r(2).toInt)
          }
        })
        .writeStream
        .format("snappysink")
        .queryName(s"USERS_$testId")
        .trigger(ProcessingTime("1 seconds"))
        .option("tableName", tableName)
        .option("streamQueryId", streamQueryId(testId))
        .option("checkpointLocation", checkpointDirectory)
    if (failBatch) {
      streamWriter.option("internal___failBatch", "true").start()
    }
    else {
      streamWriter.start()
    }
  }

  private def streamQueryId(testId: Int) = {
    s"USERS_$testId"
  }
}
