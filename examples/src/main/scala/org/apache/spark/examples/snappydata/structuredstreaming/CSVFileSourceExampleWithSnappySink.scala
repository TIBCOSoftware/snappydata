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
import org.apache.spark.sql.{SnappySession, SparkSession}

/**
 * An example of structured streaming depicting CSV file processing with Snappy sink.
 *
 * Example input data:
 *
 * Yin,31,Columbus,Ohio
 * Michael,38,"San Jose",California
 *
 * Usage: CSVFileSourceExampleWithSnappySink [checkpoint-directory] [input-directory]
 *
 *    [checkpoint-directory] Optional argument providing checkpoint directory where the
 *                           state of the steaming query will be stored. Note that this
 *                           directory needs to be deleted manually to reset the state
 *                           of the streaming query.
 *                           Default: `CSVFileSourceExampleWithSnappySink` directory
 *                           in working directory.
 *    [input-directory] Optional argument pointing to input directory path where incoming
 *                      CSV files should be dumped to get picked up for processing.
 *                      Default: `people.csv` directory under resources
 *
 * Example:
 *    $ bin/run-example snappydata.structuredstreaming.CSVFileSourceExampleWithSnappySink \
 *    "checkpoint_dir" "CSV_input_dir"
 */
// scalastyle:off println
object CSVFileSourceExampleWithSnappySink extends Logging {

  def main(args: Array[String]) {
    // reducing the log level to minimize the messages on console
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val checkpointDirectory = if (args.length >= 1) args(0)
    else getClass.getSimpleName
    val inputDirectory = if (args.length >= 2) args(1)
    else "quickstart/src/main/resources/people.csv"
    println("Initializing SnappySession ...")
    val spark: SparkSession = SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .master("local[*]")
        .getOrCreate()
    val snappy = new SnappySession(spark.sparkContext)
    println("Initializing SnappySession ... Done.")

    try {
      snappy.sql("create table people (name string , age int," +
          " city string, state string)")

      val schema = snappy.read.csv(inputDirectory).schema
      val df = snappy.readStream
          .option("maxFilesPerTrigger", 1) // Controls number of files to be processed per batch
          .schema(schema)
          .csv(inputDirectory)

      val streamingQuery = df
          .writeStream
          .format("snappysink")
          .queryName(getClass.getSimpleName)  // must be unique across a snappydata cluster
          .trigger(ProcessingTime("1 seconds"))
          .option("tableName", "people")
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
}
