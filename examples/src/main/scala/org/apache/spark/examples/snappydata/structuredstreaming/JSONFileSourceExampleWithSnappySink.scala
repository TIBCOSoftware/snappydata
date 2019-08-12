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

import scala.reflect.io.Path

import org.apache.log4j.{Level, Logger}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.{SnappySession, SparkSession}

/**
 * An example of structured streaming depicting JSON file processing with
 * Snappy sink.
 *
 * This example reads input data from `people` directory available under resources.
 * Alternatively you can change the `inputDirectory` va
 *
 * To run the example in local mode go to your SnappyData product distribution
 * directory and type following command on the command prompt
 * <pre>
 * bin/run-example snappydata.structuredstreaming.JSONFileSourceExampleWithSnappySink
 * </pre>
 */
object JSONFileSourceExampleWithSnappySink extends Logging {

  def main(args: Array[String]) {
    // reducing the log level to minimize the messages on console
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // input directory where input JSON files are being dumped.
    val inputDirectory = "quickstart/src/main/resources/people"
    val checkpointDirectory = "JSONFileSourceExampleWithSnappySink"
    println("Initializing a SnappySesion")
    val spark: SparkSession = SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .master("local[*]")
        .getOrCreate()
    val snappy = new SnappySession(spark.sparkContext)

    try {
      snappy.sql("create table people (name string , lane string," +
          " city string, district string, state string)")

      val schema = snappy.read.json(inputDirectory).schema
      val jsonDF = snappy.readStream
          .option("maxFilesPerTrigger", 1) // Controls number of files to be processed per batch
          .schema(schema)
          .json(inputDirectory)


      // A simple streaming query to filter signal value and load the output into devices table.
      val streamingQuery = jsonDF
          .select("name", "address.*")
          .writeStream
          .format("snappysink")
          .queryName("people")  // must be unique across a snappydata cluster
          .trigger(ProcessingTime("1 seconds"))
          .option("tableName", "people")
          .option("checkpointLocation", checkpointDirectory)
          .start()

      println("Streaming started. Will wait for termination.")
      // Following line will make streaming query terminate after 15 seconds.
      // This can be replaced by streamingQuery.awaitTermination() to keep the streaming query
      // running.
      streamingQuery.awaitTermination( 15000)

      println("Data loaded in table:")
      snappy.sql("select * from people").show()
    } finally {
      snappy.sql("drop table if exists people")

      // CAUTION: recursively deleting directory
      Path(checkpointDirectory).deleteRecursively()
    }
    println("Exiting")
    System.exit(0)
  }
}
