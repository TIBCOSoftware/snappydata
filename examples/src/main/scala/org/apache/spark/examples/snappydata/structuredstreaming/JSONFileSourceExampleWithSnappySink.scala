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

import com.typesafe.config.Config
import org.apache.log4j.{Level, Logger}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.streaming.{ProcessingTime, StreamingQuery}
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession, SparkSession}

/**
 * An example of structured streaming depicting JSON file processing with Snappy sink.
 *
 * This example can be run either in local mode (in which case the example runs
 * collocated with Spark+SnappyData Store in the same JVM) or can be submitted as a job
 * to an already running SnappyData cluster.
 *
 * Example input data:
 *
 * {"name":"Yin", "age":31, "address":{"city":"Columbus","state":"Ohio", "district" :"Cincinnati"}}
 * {"name":"Michael", "age":38, "address":{"city":"San Jose", "state":"California", "lane" :"15"}}
 *
 * Running locally:
 *
 * Usage: JSONFileSourceExampleWithSnappySink [checkpoint-directory] [input-directory]
 *
 *    [checkpoint-directory] Optional argument providing checkpoint directory where the
 *                           state of the steaming query will be stored. Note that this
 *                           directory needs to be deleted manually to reset the state
 *                           of the streaming query.
 *                           Default: `JSONFileSourceExampleWithSnappySink` directory
 *                           in working directory.
 *    [input-directory] Optional argument pointing to input directory path where incoming
 *                      JSON files should be dumped to get picked up for processing.
 *                      Default: `people.json` directory under resources
 *
 * Example:
 *    $ bin/run-example snappydata.structuredstreaming.JSONFileSourceExampleWithSnappySink \
 *    "checkpoint_dir" "JSON_input_dir"
 *
 * Submitting as a snappy job to already running cluster:
 *   cd $SNAPPY_HOME
 *  bin/snappy-job.sh submit \
 *    --app-name JSONFileSourceExampleWsithSnappySink \
 *    --class org.apache.spark.examples.snappydata.structuredstreaming.JSONFileSourceExampleWithSnappySink \
 *    --app-jar examples/jars/quickstart.jar \
 *    --conf checkpoint-directory=<checkpoint directory> \
 *    --conf input-directory=<input directory path>
 *
 * Note that the checkpoint directory and input directory are mandatory options while submitting snappy job.
 * Check the status of your job id
 *   bin/snappy-job.sh status --lead [leadHost:port] --job-id [job-id]
 *
 * To stop the job:
 *   bin/snappy-job.sh stop --lead [leadHost:port] --job-id [job-id]
 *
 * The content of the sink table can be checked from snappy-sql using a select query:
 *  select * from people;
 *
 * Resetting the streaming query:
 * To reset streaming query progress delete the checkpoint directory.
 * While running this example from as snappy job, you will also need to clear the state from state
 * table using following query:
 *
 *  delete from app.snappysys_internal____sink_state_table where stream_query_id = 'query1';
 *
 */
// scalastyle:off println
object JSONFileSourceExampleWithSnappySink extends SnappySQLJob with Logging {

  // Entry point for local mode
  def main(args: Array[String]) {
    // reducing the log level to minimize the messages on console
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val checkpointDirectory = if (args.length >= 1) args(0)
    else getClass.getSimpleName
    val inputDirectory = if (args.length >= 2) args(1)
    else "quickstart/src/main/resources/people.json"
    println("Initializing SnappySession ...")
    val spark: SparkSession = SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .master("local[*]")
        .getOrCreate()
    val snappy = new SnappySession(spark.sparkContext)
    println("Initializing SnappySession ... Done.")
    try {
      snappy.sql("create table people (name string , age int, lane string," +
          " city string, district string, state string)")

      val streamingQuery: StreamingQuery = startStreaming(snappy, checkpointDirectory,
        inputDirectory)
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

  // Entry point for snappy job submission
  override def runSnappyJob(sc: SnappySession, jobConfig: Config): Any = {
    sc.sql("create table if not exists people (name string , age int, lane string," +
        " city string, district string, state string)")
    val streamingQuery = startStreaming(sc, jobConfig.getString("checkpoint-directory"),
      jobConfig.getString("input-directory"))
    streamingQuery.awaitTermination()
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  private def startStreaming(snappy: SnappySession, checkpointDirectory: String,
      inputDirectory: String) = {
    val schema = snappy.read.json(inputDirectory).schema

    // Create DataFrame representing the stream of JSON
    val jsonDF = snappy.readStream
        .option("maxFilesPerTrigger", 1) // Controls number of files to be processed per batch
        .schema(schema)
        .json(inputDirectory)

    val streamingQuery = jsonDF
        .select("name", "age", "address.lane", "address.city", "address.district",
          "address.state")
        .writeStream
        .format("snappysink")
        .queryName("query1")       // must be unique across a snappydata cluster
        .trigger(ProcessingTime("1 seconds")) // streaming query trigger interval
        .option("tableName", "people")        // name of the target table
        .option("checkpointLocation", checkpointDirectory)
        .start()

    println("Streaming started.")
    streamingQuery
  }
}
