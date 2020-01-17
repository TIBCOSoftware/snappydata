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
 *
 * Key: USA, Value: Yin,31,Columbus,Ohio
 * Key: USA, Value: John,44,"San Jose",California
 *
 * Usage: CSVKafkaSourceExampleWithSnappySink <kafka-brokers> <topics> [checkpoint-directory]
 *
 *    <kafka-brokers> Compulsory argument providing comma separate list of kafka brokers
 *    <topics> Compulsory argument providing comma separated list of kafka topics to subscribe
 *    [checkpoint-directory] Optional argument providing checkpoint directory where the state of
 *                           the steaming query will be stored. Note that this directory needs to
 *                           be deleted manually to reset the state of the streaming query.
 *                           Default: `CSVKafkaSourceExampleWithSnappySink` directory under
 *                           working directory.
 *
 * Example:
 *    $ bin/run-example snappydata.structuredstreaming.CSVKafkaSourceExampleWithSnappySink \
 *    "broker-1:9092,broker-2:9092" "topic1,topic2" "checkpoint_dir"
 */
// scalastyle:off println
object CSVKafkaSourceExampleWithSnappySink extends Logging {

  def main(args: Array[String]) {

    if (args.length < 2) {
      println("Usage: CSVKafkaSourceExampleWithSnappySink <kafka-brokers> <topics>" +
          " [checkpoint-directory]")
      System.exit(1)
    }

    // reducing the log level to minimize the messages on console
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val checkpointDirectory = if (args.length >= 3) args(2) else this.getClass.getSimpleName
    println("Initializing SnappySession ... ")
    val spark: SparkSession = SparkSession
        .builder()
        .appName(this.getClass.getSimpleName)
        .master("local[*]")
        .getOrCreate()
    val snappy = new SnappySession(spark.sparkContext)
    println("Initializing SnappySession ... Done.")
    try {

      snappy.sql("create table people(name string , age int," +
          "city string, state string, country string)")

      val keySchema = StructType(Seq(StructField("country", StringType, nullable = false)))
      val addressSchema = StructType(Seq(StructField("city", StringType, nullable = false),
        StructField("state", StringType, nullable = false)))
      val valueSchema = StructType(Seq(StructField("name", StringType, nullable = false),
        StructField("age", IntegerType, nullable = false),
        StructField("address", addressSchema, nullable = false)
      ))

      val df = snappy.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", args(0))
          .option("startingOffsets", "earliest")
          // more kafka consumer options can be provided here
          .option("subscribe", args(1))
          .option("maxOffsetsPerTrigger", 100) // to restrict the streaming batch size
          .load()

      import snappy.implicits._
      val structDF = df.select("key", "value").as[(String, String)].map(s => {
        // Note: this split won't handle CSV containing comma character as part of quotes values.
        val fields = s._2.split(",")
        Person(fields(0), fields(1).toInt, fields(2), fields(3), s._1)
      })

      val streamingQuery = structDF
          .writeStream
          .format("snappysink")
          .queryName(this.getClass.getSimpleName) // must be unique across the Snappydata cluster
          .trigger(ProcessingTime("1 seconds")) // streaming query trigger interval
          .option("tableName", "people")    // name of the target table
          .option("checkpointLocation", checkpointDirectory)
          .start()

      println("Streaming started.")
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
  case class Person(name: String, age : Int, city : String, state : String, country : String)
}

