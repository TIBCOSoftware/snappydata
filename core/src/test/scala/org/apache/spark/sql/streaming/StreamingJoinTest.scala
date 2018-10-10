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

class StreamingJoinTest extends SnappyFunSuite
    with BeforeAndAfter with BeforeAndAfterAll {
  private val session = snc.sparkSession

  private var kafkaTestUtils: KafkaTestUtils = _

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

  after {
    baseCleanup(false)

    // CAUTION!! - recursively deleting checkpoint directory. handle with care.
    Path(checkpointDirectory).deleteRecursively()
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

  test("test join") {
    val testId = testIdGenerator.getAndIncrement()
    createTable()()
    val rdd = snc.sparkContext.parallelize((0 to 25)
        .map(i => Row(i.toLong, (i * 2).toLong)))
    val schema = StructType(Seq(StructField("bl_id", LongType, false),
      StructField("bl_age", LongType, false)))
    val dfBlackList = snc.createDataFrame(rdd, schema)

    // create a SnappyData table
    snc.createTable("blacklist", "row", dfBlackList.schema, Map.empty[String, String])

    import org.apache.spark.sql.snappy._
    dfBlackList.write.putInto("blacklist") // populate the table 'blacklist'.
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val dataBatch1 = Seq(Seq(1, "name1", 30), Seq(2, "name2", 10),
      Seq(3, "name3", 30))
    kafkaTestUtils.sendMessages(topic, dataBatch1.map(r => r.mkString(",")).toArray)

    val streamingQuery = getStream(topic, testId)

    streamingQuery.processAllAvailable()

  }

  private def getStream(topic: String, testId: Int) = {
    val streamingDF = snc
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
          StructField("_eventType", IntegerType, nullable = false) :: Nil
    }

    val schema = StructType(structFields())

    implicit val encoder = RowEncoder(schema)
    val session = snc.sparkSession
    import session.implicits._
    streamingDF.selectExpr("CAST(value AS STRING)")
        .as[String]
        .map(_.split(","))
        .map(r => {
          Row(r(0).toLong, r(1), r(2).toInt, r(3).toInt)
        })
        .join(snc.table("blacklist")).where("id = bl_id").groupBy("id").count()
        .writeStream
        .outputMode("complete")
        .format("console")
        .trigger(ProcessingTime("1 seconds"))
        .start()

  }

  private def streamQueryId(testId: Int) = {
    s"USERS_$testId"
  }
}
