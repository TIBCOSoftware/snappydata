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
package org.apache.spark.examples.snappydata.structuredstreaming

import org.apache.log4j.{Level, Logger}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SnappySession, SparkSession}

/**
 * An example of structured streaming depicting processing of JSON coming from kafka source
 * using snappy sink.
 *
 * Example input data:
 * key: {"country" : "USA"}, value: {"name":"Adam", "age":21, "address":{"city":"Columbus","state":"Ohio"}}
 * key: {"country" : "England"}, value: {"name":"John", "age":44, "address":{"city":"London"}}
 * key: {"country" : "USA"}, value: {"name":"Carol", "age":37, "address":{"city":"San Diego", "state":"California"}}
 *
 * Usage: JSONKafkaSourceExampleWithSnappySink <kafka-brokers> <topics> [checkpoint-directory]
 *
 *    <kafka-brokers> Compulsory argument providing comma separate list of kafka brokers
 *    <topics> Compulsory argument providing comma separated list of kafka topics to subscribe
 *    [checkpoint-directory] Optional argument providing checkpoint directory where the state of
 *                           the steaming query will be stored. Note that this directory needs to
 *                           be deleted manually to reset the state of the streaming query.
 *                           Default: `JSONKafkaSourceExampleWithSnappySink` directory under
 *                           working directory.
 *
 * Example:
 *    $ bin/run-example snappydata.structuredstreaming.JSONKafkaSourceExampleWithSnappySink \
 *    "broker-1:9092,broker-2:9092" "topic1,topic2" "checkpoint_dir"
 */
// scalastyle:off println
object JSONKafkaSourceExampleWithSnappySink extends Logging {

  def main(args: Array[String]) {

    if (args.length < 2) {
      println("Usage: JSONKafkaSourceExampleWithSnappySink <kafka-brokers> <topics>" +
          " [checkpoint-directory]")
      System.exit(1)
    }

    // reducing the log level to minimize the messages on console
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val checkpointDirectory = if (args.length >= 3) args(2) else getClass.getSimpleName
    println("Initializing SnappySession ... ")
    val spark: SparkSession = SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .master("local[*]")
        .getOrCreate()
    val snappy = new SnappySession(spark.sparkContext)
    println("Initializing SnappySession ... Done.")
    try {

      snappy.sql("create table people(name string , " +
          "city string, state string, country string)")

      val keySchema = StructType(Seq(StructField("country", StringType, nullable = false)))
      val addressSchema = StructType(Seq(StructField("city", StringType, nullable = false),
        StructField("state", StringType, nullable = false)))
      val valueSchema = StructType(Seq(StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false),
        StructField("address", addressSchema, nullable = false)
      ))

      val df = snappy.readStream.
          format("kafka").
          option("kafka.bootstrap.servers", args(0)).
          option("startingOffsets", "earliest").
          option("subscribe", args(1)).
          option("maxOffsetsPerTrigger", 100). // to restrict the batch size
          load()

      import org.apache.spark.sql.functions.from_json
      val streamingQuery = df.
          select(from_json(df.col("key").cast("string"), keySchema).alias("key"),
            from_json(df.col("value").cast("string"), valueSchema).alias("value")).
          select("value.name", "value.address.*", "key.country").
          writeStream.
          format("snappysink").
          queryName(getClass.getSimpleName). // must be unique across the Snappydata cluster
          trigger(ProcessingTime("1 seconds")).
          option("tableName", "people").
          option("checkpointLocation", checkpointDirectory).
          start()

      println("Streaming started. Will wait for termination.")
      // Following line will make streaming query terminate after 15 seconds.
      // This can be replaced by streamingQuery.awaitTermination() to keep the streaming query
      // running.
      streamingQuery.awaitTermination(15000)

      println("Data loaded in table:")
      snappy.sql("select * from people").show()
    } finally {
      snappy.sql("drop table if exists people")
    }
    println("Exiting")
    System.exit(0)
  }
}
