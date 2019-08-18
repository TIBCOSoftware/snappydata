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

import java.util.concurrent.atomic.AtomicInteger

import io.snappydata.{Property, SnappyFunSuite}
import org.apache.kafka.common.TopicPartition
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{count, window}
import org.apache.spark.sql.streaming.ProcessingTime

case class Account(accountName: String)

class SnappyStructuredKafkaSuite extends SnappyFunSuite with Eventually
  with BeforeAndAfter with BeforeAndAfterAll {

  private lazy val session = snc.sparkSession

  private var kafkaTestUtils: KafkaTestUtils = _

  protected override def newSparkConf(addOn: (SparkConf) => SparkConf): SparkConf = {
    super.newSparkConf((conf: SparkConf) => {
      // conf.set(Property.TestDisableCodeGenFlag.name , "false")
      conf
    })
  }
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

  test("SnappyData Structured Streaming with Kafka") {
    import session.implicits._

    snc.sql("drop table if exists users")
    snc.sql("create table users (id int, name string) using column options(key_columns 'id')")

    val topic = newTopic()
    kafkaTestUtils.createTopic(topic, partitions = 3)
    kafkaTestUtils.sendMessages(topic,
      (100 to 200).map(i => i.toString + ",name_" + i).toArray, Some(0))
    kafkaTestUtils.sendMessages(topic,
      (10 to 20).map(i => i.toString + ",name_" + i).toArray, Some(1))
    kafkaTestUtils.sendMessages(topic, Array("1,name_1"), Some(2))

    val streamingDF = session
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaTestUtils.brokerAddress)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load

    implicit val encoder = RowEncoder(snc.table("users").schema)

    val streamingQuery = streamingDF
        .selectExpr("CAST(value AS STRING)")
        .as[String]
        .map(_.split(","))
        .map(r => {
          Row(r(0).toInt, r(1))
        })
      .writeStream
      .format("snappysink")
      .queryName("simple")
      .outputMode("append")
      .trigger(ProcessingTime("1 seconds"))
      .option("tablename", "APP.USERS").option("streamqueryid", "abc")
      .option("checkpointLocation", "/tmp/snappyTable")
      .start

    streamingQuery.processAllAvailable()
    assert(113 == session.sql("select * from APP.USERS").count)
  }


  test("ETL Job") {
    import session.implicits._

    val topic = newTopic()
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val partitions = Map(
      new TopicPartition(topic, 0) -> 0L,
      new TopicPartition(topic, 1) -> 0L,
      new TopicPartition(topic, 2) -> 0L
    )

    val startingOffsets = JsonUtils.partitionOffsets(partitions)

    val streamingDF = session
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaTestUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("maxOffsetsPerTrigger", 10)
      .option("subscribe", topic)
      .option("startingOffsets", startingOffsets)
      .load

    val streamingQuery = streamingDF
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
      .writeStream
      .format("memory")
     // .option("checkpointLocation", "/tmp/etl")
      .queryName("snappyTable")
      .outputMode("append")
      .trigger(ProcessingTime("1 seconds"))
      .start

    kafkaTestUtils.sendMessages(topic, (100 to 200).map(_.toString).toArray, Some(0))
    kafkaTestUtils.sendMessages(topic, (10 to 20).map(_.toString).toArray, Some(1))
    kafkaTestUtils.sendMessages(topic, Array("1"), Some(2))

    streamingQuery.processAllAvailable()
    assert(113 == session.sql("select * from snappyTable").count)
  }

  test("infinite streaming aggregation") {
    import session.implicits._

    val topic = newTopic()
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val partitions = Map(
      new TopicPartition(topic, 0) -> 0L,
      new TopicPartition(topic, 1) -> 0L,
      new TopicPartition(topic, 2) -> 0L
    )

    val startingOffsets = JsonUtils.partitionOffsets(partitions)

    val streamingDF = session
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaTestUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("maxOffsetsPerTrigger", 10)
      .option("subscribe", topic)
      .option("startingOffsets", startingOffsets)
      .option("failOnDataLoss", "false")
      .load

    val streamingQuery = streamingDF
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").groupBy("value").count()
      .as[(String, String)]
      .writeStream
      .format("memory")
      .option("checkpointLocation", "/tmp/infinite-" + System.currentTimeMillis())
      .queryName("snappyAggrTable")
      .outputMode("complete")
      .trigger(ProcessingTime("1 seconds"))
      .start

    kafkaTestUtils.sendMessages(topic, (100 to 150).map(_.toString).toArray, Some(0))
    kafkaTestUtils.sendMessages(topic, (125 to 150).map(_.toString).toArray, Some(1))
    kafkaTestUtils.sendMessages(topic, (100 to 124).map(_.toString).toArray, Some(2))

    streamingQuery.processAllAvailable()

    assert(51 == session.sql("select * from snappyAggrTable").count)
    assert(2.0 == session.sql("select avg(count) from snappyAggrTable").collect()(0).getDouble(0))
  }

  test("sliding window aggregation") {
    import session.implicits._

    val topic = newTopic()
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val partitions = Map(
      new TopicPartition(topic, 0) -> 0L,
      new TopicPartition(topic, 1) -> 0L,
      new TopicPartition(topic, 2) -> 0L
    )

    val startingOffsets = JsonUtils.partitionOffsets(partitions)

    kafkaTestUtils.sendMessages(topic, (100 to 150).map(_.toString).toArray, Some(0))
    kafkaTestUtils.sendMessages(topic, (125 to 150).map(_.toString).toArray, Some(1))
    kafkaTestUtils.sendMessages(topic, (100 to 124).map(_.toString).toArray, Some(2))

    val streamingDF = session
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaTestUtils.brokerAddress)
      .option("kafka.metadata.max.age.ms", "1")
      .option("maxOffsetsPerTrigger", 10)
      .option("subscribe", topic)
      .option("startingOffsets", startingOffsets)
      .option("failOnDataLoss", "false")
      .load

    val windowedAggregation = streamingDF
      .groupBy(window($"timestamp", "1 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"window".getField("start") as 'window, $"count")

    val streamingQuery = windowedAggregation
      .writeStream
      .format("memory")
      .option("checkpointLocation", "/tmp/snappyWindowAggrTable")
      .outputMode("complete")
      .queryName("snappyWindowAggrTable")
      .start()

    streamingQuery.processAllAvailable()
    logInfo(session.sql("select * from snappyWindowAggrTable").limit(200).collect().mkString("\n"))
    streamingQuery.stop()
  }

  test("streaming join to snappy table") {
    import session.implicits._

    val rdd = snc.sparkContext.parallelize((15 to 25).map(i => Account(i.toString)))
    val dfBlackList = snc.createDataFrame(rdd)
    // create a SnappyData table
    snc.createTable("blacklist", "row", dfBlackList.schema, Map.empty[String, String])

    import org.apache.spark.sql.snappy._
    dfBlackList.write.putInto("blacklist") // populate the table 'blacklist'.

    val topic = newTopic()
    kafkaTestUtils.createTopic(topic, partitions = 3)

    // Read the accounts from Kafka source
    val acctStreamingDF = session
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaTestUtils.brokerAddress)
      .option("subscribe", topic)
      .option("startingOffsets", "earliest").load
      .selectExpr("CAST(value AS STRING) accountName").as[(String)]

    val streamingQuery = acctStreamingDF.join(session.table("blacklist"), "accountName")
      .writeStream
      .outputMode("append")
      .format("memory")
      .queryName("snappyResultTable")
      .trigger(ProcessingTime("1 seconds"))
      .start

    kafkaTestUtils.sendMessages(topic, (10 to 18).map(_.toString).toArray, Some(1))
    kafkaTestUtils.sendMessages(topic, (20 to 30).map(_.toString).toArray, Some(2))

    streamingQuery.processAllAvailable()
    assert(10 == session.sql("select * from snappyResultTable").count)
  }

  // Unsupported operations with streaming DataFrames/Datasets -

  // Multiple streaming aggregations (i.e. a chain of aggregations on a
  // streaming DF) are not yet supported on streaming Datasets.
  // Limit and take first N rows are not supported on streaming Datasets.
  // Distinct operations on streaming Datasets are not supported.
  // Sorting operations are supported on streaming Datasets only after
  // an aggregation and in Complete Output Mode.
  // Outer joins between a streaming and a static Datasets are conditionally supported.
  // Full outer join with a streaming Dataset is not supported
  // Left outer join with a streaming Dataset on the right is not supported
  // Right outer join with a streaming Dataset on the left is not supported
  // Any kind of joins between two streaming Datasets are not yet supported.
  // They are actions that will immediately run queries and return results,
  // which does not make sense on a streaming Dataset.

  // count() - Cannot return a single count from a streaming Dataset.
  // Instead, use ds.groupBy.count() which returns a streaming Dataset containing a running count.
  // foreach() - Instead use ds.writeStream.foreach(...).
  // show() - Instead use the console sink.

  // sorting on the input stream is not supported, as it requires keeping
  // track of all the data received in the stream.
  // This is therefore fundamentally hard to execute efficiently.
}
