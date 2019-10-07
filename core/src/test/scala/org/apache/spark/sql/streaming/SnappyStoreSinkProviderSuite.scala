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

import java.sql.SQLException
import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.io.Path

import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState.SNAPPY_CATALOG_SCHEMA_VERSION_MISMATCH
import io.snappydata.SnappyFunSuite
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.sql.{Dataset, Row, SnappyContext, SnappySession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.CatalogStaleException
import org.apache.spark.sql.kafka010.KafkaTestUtils
import org.apache.spark.sql.types._

class SnappyStoreSinkProviderSuite extends SnappyFunSuite
    with BeforeAndAfter with BeforeAndAfterAll {
  private val session = snc.sparkSession

  import session.implicits._

  private var kafkaTestUtils: KafkaTestUtils = _

  private val testIdGenerator = new AtomicInteger(0)
  private val tableName = "APP.USERS"
  private val checkpointDirectory = "/tmp/SnappyStoreSinkProviderSuite"

  private def getTopic(id: Int) = s"topic-$id"

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
    baseCleanup(false)

    // CAUTION!! - recursively deleting checkpoint directory. handle with care.
    Path(checkpointDirectory).deleteRecursively()
  }

  test("_eventType column: absent, key columns: defined, table type: column") {
    val testId = testIdGenerator.getAndIncrement()
    createTable()()
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val dataBatch1 = Seq(Seq(1, "name1", 30, "lname1"), Seq(2, "name2", 10, "lname2"),
      Seq(3, "name3", 30, "lname3"))
    kafkaTestUtils.sendMessages(topic, dataBatch1.map(r => r.mkString(",")).toArray)

    val streamingQuery = createAndStartStreamingQuery(topic, testId, withEventTypeColumn = false)
    waitTillTheBatchIsPickedForProcessing(0, testId)

    val dataBatch2 = Seq(Seq(1, "name11", 40, "lname1"), Seq(4, "name4", 50, "lname4"))
    kafkaTestUtils.sendMessages(topic, dataBatch2.map(r => r.mkString(",")).toArray)

    streamingQuery.processAllAvailable()

    val rows = Array(Row(1, "name11", 40, "lname1"), Row(2, "name2", 10, "lname2"),
      Row(3, "name3", 30, "lname3"), Row(4, "name4", 50, "lname4"))
    assertData(rows)
  }

  test("_eventType column: absent, key columns: undefined, table type: column") {
    val testId = testIdGenerator.getAndIncrement()
    createTable(withKeyColumn = false)()
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val streamingQuery = createAndStartStreamingQuery(topic, testId, withEventTypeColumn = false)

    val dataBatch = Seq(Seq(1, "name1", 30, "lname1"), Seq(2, "name2", 10, "lname2"),
      Seq(3, "name3", 30, "lname3"), Seq(1, "name1", 30, "lname1"))
    kafkaTestUtils.sendMessages(topic, dataBatch.map(r => r.mkString(",")).toArray)

    streamingQuery.processAllAvailable()

    val rows = Array(Row(1, "name1", 30, "lname1"), Row(1, "name1", 30, "lname1"),
      Row(2, "name2", 10, "lname2"), Row(3, "name3", 30, "lname3"))
    assertData(rows)
  }

  test("_eventType column: present, key columns: defined, table type: column") {
    val testId = testIdGenerator.getAndIncrement()
    createTable()()
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val dataBatch1 = Seq(Seq(1, "name1", 20, "lname1", 0), Seq(2, "name2", 10, "lname2", 0))
    kafkaTestUtils.sendMessages(topic, dataBatch1.map(r => r.mkString(",")).toArray)

    val streamingQuery: StreamingQuery = createAndStartStreamingQuery(topic, testId)
    waitTillTheBatchIsPickedForProcessing(0, testId)

    val dataBatch2 = Seq(Seq(1, "name11", 30, "lname1", 1), Seq(2, "name2", 13, "lname2", 2),
      Seq(3, "name3", 30, "lname3", 0), Seq(4, "name4", 10, "lname4", 2))
    kafkaTestUtils.sendMessages(topic, dataBatch2.map(r => r.mkString(",")).toArray)
    streamingQuery.processAllAvailable()

    assertData(Array(Row(1, "name11", 30, "lname1"), Row(3, "name3", 30, "lname3")))
  }

  test("_eventType column: present, key columns: undefined, table type: column") {
    val testId = testIdGenerator.getAndIncrement()
    createTable(withKeyColumn = false)()
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val dataBatch = Seq(Seq(1, "name1", 20, "lname1", 0), Seq(2, "name2", 10, "lname2", 0))
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

    val dataBatch1 = Seq(Seq(1, "name1", 30, "lname1"), Seq(2, "name2", 10, "lname2"),
      Seq(3, "name3", 30, "lname3"))
    kafkaTestUtils.sendMessages(topic, dataBatch1.map(r => r.mkString(",")).toArray)

    val streamingQuery = createAndStartStreamingQuery(topic, testId, withEventTypeColumn = false)
    waitTillTheBatchIsPickedForProcessing(0, testId)

    val dataBatch2 = Seq(Seq(1, "name11", 40, "lname1"), Seq(4, "name4", 50, "lname4"))
    kafkaTestUtils.sendMessages(topic, dataBatch2.map(r => r.mkString(",")).toArray)

    streamingQuery.processAllAvailable()

    val rows = Array(Row(1, "name11", 40, "lname1"), Row(2, "name2", 10, "lname2"),
      Row(3, "name3", 30, "lname3"), Row(4, "name4", 50, "lname4"))
    assertData(rows)
  }

  test("_eventType column: absent, key columns: undefined, table type: row") {
    val testId = testIdGenerator.getAndIncrement()
    createTable(withKeyColumn = false)(isRowTable = true)
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val streamingQuery = createAndStartStreamingQuery(topic, testId, withEventTypeColumn = false)

    val dataBatch = Seq(Seq(1, "name1", 30, "lname1"), Seq(2, "name2", 10, "lname2"),
      Seq(3, "name3", 30, "lname3"), Seq(1, "name1", 30, "lname1"))
    kafkaTestUtils.sendMessages(topic, dataBatch.map(r => r.mkString(",")).toArray)

    streamingQuery.processAllAvailable()

    val rows = Array(Row(1, "name1", 30, "lname1"), Row(1, "name1", 30, "lname1"),
      Row(2, "name2", 10, "lname2"), Row(3, "name3", 30, "lname3"))
    assertData(rows)
  }

  test("_eventType column: present, key columns: defined, table type: row") {
    val testId = testIdGenerator.getAndIncrement()
    createTable()(isRowTable = true)
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val dataBatch1 = Seq(Seq(1, "name1", 20, "lname1", 0), Seq(2, "name2", 10, "lname2", 0))
    kafkaTestUtils.sendMessages(topic, dataBatch1.map(r => r.mkString(",")).toArray)

    val streamingQuery: StreamingQuery = createAndStartStreamingQuery(topic, testId)
    waitTillTheBatchIsPickedForProcessing(0, testId)

    val dataBatch2 = Seq(Seq(1, "name11", 30, "lname1", 1), Seq(2, "name2", 13, "lname2", 2),
      Seq(3, "name3", 30, "lname3", 0), Seq(4, "name4", 10, "lname4", 2))
    kafkaTestUtils.sendMessages(topic, dataBatch2.map(r => r.mkString(",")).toArray)
    streamingQuery.processAllAvailable()

    assertData(Array(Row(1, "name11", 30, "lname1"), Row(3, "name3", 30, "lname3")))
  }

  test("_eventType column: present, key columns: undefined, table type: row") {
    val testId = testIdGenerator.getAndIncrement()
    createTable(withKeyColumn = false)(isRowTable = true)
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val dataBatch = Seq(Seq(1, "name1", 20, "lname1", 0), Seq(2, "name2", 10, "lname2", 0))
    kafkaTestUtils.sendMessages(topic, dataBatch.map(r => r.mkString(",")).toArray)
    val streamingQuery = createAndStartStreamingQuery(topic, testId)
    val thrown = intercept[StreamingQueryException] {
      streamingQuery.processAllAvailable()
    }

    val errorMessage = "_eventType is present in data but key columns are not defined on table."
    assert(thrown.getCause.getMessage == errorMessage)
    streamingQuery.stop()
  }

  test("test idempotency") {
    val testId = testIdGenerator.getAndIncrement()
    createTable()()
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    kafkaTestUtils.sendMessages(topic, (0 to 10).map(i => s"$i,name$i,$i,lname$i,0").toArray)

    val streamingQuery: StreamingQuery = createAndStartStreamingQuery(topic, testId)
    waitTillTheBatchIsPickedForProcessing(0, testId)
    streamingQuery.stop()

    val streamingQuery1: StreamingQuery = createAndStartStreamingQuery(topic, testId
      , options = Map("internal___failBatch" -> "true"))
    kafkaTestUtils.sendMessages(topic, (11 to 20).map(i => s"$i,name$i,$i,lname$i,0").toArray)
    try {
      streamingQuery1.processAllAvailable()
      fail("StreamingQueryException expected.")
    } catch {
      case ex: StreamingQueryException if ex.cause.getMessage == "dummy failure for test" =>
        streamingQuery1.stop()
    }

    val streamingQuery2: StreamingQuery = createAndStartStreamingQuery(topic, testId)

    kafkaTestUtils.sendMessages(topic, (21 to 30).map(i => s"$i,name$i,$i,lname$i,0").toArray)
    waitTillTheBatchIsPickedForProcessing(1, testId)
    streamingQuery2.processAllAvailable()

    assertData((0 to 30).map(i => Row(i, s"name$i", i, s"lname$i")).toArray)
  }

  test("test conflation enabled") {
    val testId = testIdGenerator.getAndIncrement()
    createTable()()
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 1)

    // producing all records with same key `1` on partition 0.
    kafkaTestUtils.sendMessages(topic, (0 to 999)
        .map(i => s"1,name$i,$i,lname1,${i % 3}").toArray, Some(0))

    // producing records with keh `1` on multiple partitions. This may not lead to expected result
    // kafkaTestUtils.sendMessages(topic, (0 to 999).map(i => s"1,name$i,$i,${i%3}").toArray)

    val streamingQuery = createAndStartStreamingQuery(topic, testId,
      options = Map("conflation" -> "true"))

    streamingQuery.processAllAvailable()

    assertData(Array(Row(1, "name999", 999, "lname1")))
  }

  test("conflation enabled, _eventType column: absent") {
    val testId = testIdGenerator.getAndIncrement()
    createTable()()
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 1)

    val batch2 = Seq(Seq(1, "name2", 30, "lname1"), Seq(1, "name3", 30, "lname1"))
    kafkaTestUtils.sendMessages(topic, batch2.map(r => r.mkString(",")).toArray)

    val streamingQuery = createAndStartStreamingQuery(topic, testId,
      withEventTypeColumn = false, options = Map("conflation" -> "true"))

    streamingQuery.processAllAvailable()

    assertData(Array(Row(1, "name3", 30, "lname1")))
  }

  test("conflation enabled, key columns : undefined") {
    val testId = testIdGenerator.getAndIncrement()
    createTable(withKeyColumn = false)()
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 1)

    val batch2 = Seq(Seq(1, "name2", 30, "lname1"), Seq(1, "name3", 30, "lname1"))
    kafkaTestUtils.sendMessages(topic, batch2.map(r => r.mkString(",")).toArray)

    val thrown = intercept[StreamingQueryException] {
      val streamingQuery = createAndStartStreamingQuery(topic, testId,
        withEventTypeColumn = false, options = Map("conflation" -> "true"))
      streamingQuery.processAllAvailable()
    }
    val errorMessage = "Key column(s) or primary key must be defined on table in order " +
        "to perform conflation."
    assert(thrown.getCause.isInstanceOf[IllegalStateException])
    assert(thrown.getCause.getMessage == errorMessage)
  }

  test("[SNAP-2745]-conflation: delete,insert") {
    val testId = testIdGenerator.getAndIncrement()
    createTable()()
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 1)

    val batch1 = Seq(Seq(1, "name1", 30, "lname1", 0))
    kafkaTestUtils.sendMessages(topic, batch1.map(r => r.mkString(",")).toArray)
    val streamingQuery = createAndStartStreamingQuery(topic, testId,
      options = Map("conflation" -> "true"))

    waitTillTheBatchIsPickedForProcessing(0, testId)
    val batch2 = Seq(Seq(1, "name1", 30, "lname1", 2), Seq(1, "name1", 30, "lname1", 0))
    kafkaTestUtils.sendMessages(topic, batch2.map(r => r.mkString(",")).toArray)

    streamingQuery.processAllAvailable()

    assertData(Array(Row(1, "name1", 30, "lname1")))
  }

  test("test conflation disabled") {
    val testId = testIdGenerator.getAndIncrement()
    createTable()()
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 1)

    val dataBatch = Seq(Seq(1, "name1", 1, "lname1", 0), Seq(1, "name1", 1, "lname1", 2))
    kafkaTestUtils.sendMessages(topic, dataBatch.map(r => r.mkString(",")).toArray, Some(0))

    val streamingQuery: StreamingQuery = createAndStartStreamingQuery(topic, testId)

    streamingQuery.processAllAvailable()
    // The delete will be processed prior to insert event irrespective of their order or arrival.
    // Hence when conflation is disabled, both the events are processed resulting into one record.
    assertData(Array(Row(1, "name1", 1, "lname1")))
  }

  test("queryName not specified") {
    val testId = testIdGenerator.getAndIncrement()
    createTable()()
    val topic = getTopic(testId)
    val streamingQuery = createAndStartStreamingQuery(topic, testId, withQueryName = false)

    try {
      streamingQuery.processAllAvailable()
      fail("StreamingQueryException expected.")
    } catch {
      case x: StreamingQueryException =>
        val expectedMessage = s"queryName must be specified for ${SnappyContext.SNAPPY_SINK_NAME}."
        assert(x.getCause.isInstanceOf[IllegalStateException])
        assert(x.getCause.getMessage.equals(expectedMessage))
    }
  }

  test("Streaming query fails after attempts exhausted") {
    val testId = testIdGenerator.getAndIncrement()
    createTable()()
    val topic = getTopic(testId)
    val streamingQuery = createAndStartStreamingQuery(topic, testId,
      options = Map("withQueryName" -> "false",
        "sinkCallback" -> "org.apache.spark.sql.streaming.TestSinkCallback",
        "internal___attempts" -> "3", "attempts" -> "4"))

    try {
      streamingQuery.processAllAvailable()
      fail("StreamingQueryException expected.")
    } catch {
      case x: StreamingQueryException =>
        assert(x.getCause.getCause.isInstanceOf[SQLException] ||
            x.getCause.getCause.isInstanceOf[CatalogStaleException])
    }
  }

  test("Streaming query passes is attempts are not exhausted") {
    val testId = testIdGenerator.getAndIncrement()
    createTable()()
    val topic = getTopic(testId)
    val streamingQuery = createAndStartStreamingQuery(topic, testId,
      options = Map("withQueryName" -> "false",
        "sinkCallback" -> "org.apache.spark.sql.streaming.TestSinkCallback",
        "internal___attempts" -> "3", "attempts" -> "3"))
    streamingQuery.processAllAvailable()
  }

  test("Streaming query fails on the first attempt itself when failure is not due" +
      " to stale catalog") {
    val testId = testIdGenerator.getAndIncrement()
    createTable()()
    val topic = getTopic(testId)
    val streamingQuery = createAndStartStreamingQuery(topic, testId,
      options = Map("withQueryName" -> "false",
        "sinkCallback" -> "org.apache.spark.sql.streaming.TestSinkCallback",
        "attempts" -> "1", "catalogNotStale" -> ""))

    try {
      streamingQuery.processAllAvailable()
      fail("StreamingQueryException expected.")
    } catch {
      case x: StreamingQueryException =>
        assert(x.getCause.isInstanceOf[RuntimeException]
            && x.getCause.getMessage.equals("catalogNotStale"))
    }
  }

  private def waitTillTheBatchIsPickedForProcessing(batchId: Int, testId: Int,
      retries: Int = 15): Unit = {
    if (retries == 0) {
      throw new RuntimeException(s"Batch id $batchId not found in sink status table")
    }
    val sqlString = s"select batch_id from APP.${SnappyStoreSinkProvider.SINK_STATE_TABLE} " +
        s"where stream_query_id = '${streamName(testId)}'"
    val batchIdFromTable = session.sql(sqlString).collect()

    if (batchIdFromTable.isEmpty || batchIdFromTable(0)(0) != batchId) {
      Thread.sleep(1000)
      waitTillTheBatchIsPickedForProcessing(batchId, testId, retries - 1)
    }
  }

  private def assertData(expectedData: Array[Row]) = {
    val actualData = session.sql(s"select * from $tableName order by id, last_name").collect()
    assertResult(expectedData)(actualData)
  }

  private def createTable(withKeyColumn: Boolean = true)(isRowTable: Boolean = false) = {
    def provider = if (isRowTable) "row" else "column"

    def options = if (!isRowTable && withKeyColumn) "options(key_columns 'id,last_name')" else ""

    def primaryKey = if (isRowTable && withKeyColumn) ", primary key (id,last_name)" else ""

    val s = s"create table IF NOT EXISTS $tableName  (id long , first_name varchar(40), age int, " +
        s"last_name varchar(40) $primaryKey) using $provider $options "
    session.sql(s)
    session.sql(s"truncate table $tableName")
  }

  private def createAndStartStreamingQuery(topic: String, testId: Int,
      withEventTypeColumn: Boolean = true, withQueryName: Boolean = true,
      options: Map[String, String] = Map.empty) = {
    val streamingDF = session
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaTestUtils.brokerAddress)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()

    def structFields() = {
      StructField("id", LongType, nullable = false) ::
          StructField("firstName", StringType, nullable = true) ::
          StructField("age", IntegerType, nullable = true) ::
          StructField("last_name", StringType, nullable = true) ::
          (if (withEventTypeColumn) {
            StructField("_eventType", IntegerType, nullable = false) :: Nil
          }
          else {
            Nil
          })
    }

    val schema = StructType(structFields())

    implicit val encoder = RowEncoder(schema)


    var streamWriter = streamingDF.selectExpr("CAST(value AS STRING)")
        .as[String]
        .map(_.split(","))
        .map(r => {
          if (r.length == 5) {
            Row(r(0).toLong, r(1), r(2).toInt, r(3), r(4).toInt)
          } else {
            Row(r(0).toLong, r(1), r(2).toInt, r(3))
          }
        })
        .writeStream
        .format("snappySink")
    if (withQueryName) {
      streamWriter = streamWriter.queryName(streamName(testId))
    }
    streamWriter.trigger(ProcessingTime("1 seconds"))
        .option("tableName", tableName)
        .option("checkpointLocation", checkpointDirectory)
    streamWriter.options(options)
    streamWriter.start()
  }

  private def streamName(testId: Int) = {
    s"users_$testId"
  }
}

class TestSinkCallback extends SnappySinkCallback {

  private var attempt = -1

  override def process(snappySession: SnappySession, sinkProps: Map[String, String], batchId: Long,
      df: Dataset[Row], possibleDuplicate: Boolean): Unit = {
    if (attempt == -1) attempt = sinkProps("attempts").toInt
    if (attempt < sinkProps("attempts").toInt) {
      assert(possibleDuplicate, "Value of possibleDuplicate should be true for retry attempts")
    }
    attempt -= 1
    if (sinkProps.contains("catalogNotStale")) {
      throw new RuntimeException("catalogNotStale")
    }
    if (attempt == 0) {
    } else if (attempt % 2 == 0) {
      throw new RuntimeException(new CatalogStaleException("dummy", null))
    } else {
      throw new RuntimeException(new SQLException("dummy", SNAPPY_CATALOG_SCHEMA_VERSION_MISMATCH))
    }
  }
}