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
 * An example showing usage of structured streaming with SnappyData.<br>
 * To run this on your local machine, you need to first start a Netcat server: <br>
 * `$ nc -lk 9999`
 * <p>
 * Sample input data:
 * <pre>
 * device1,45
 * device2,67
 * device3,35
 * </pre>
 * To run the example in local mode go to your SnappyData product distribution
 * directory and run the following command:
 * <pre>
 * bin/run-example snappydata.structuredstreaming.SocketSourceExampleWithSnappySink
 * </pre>
 *
 */
// scalastyle:off println
object SocketSourceExampleWithSnappySink extends Logging {

  def main(args: Array[String]) {
    // reducing the log level to minimize the messages on console
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    println("Initializing SnappySession ... ")
    val checkpointDirectory = "/tmp/StructuredStreamingWithSnappySink"
    val spark: SparkSession = SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .master("local[*]")
        .getOrCreate()

    import spark.implicits._
    val snappy = new SnappySession(spark.sparkContext)
    println("Initializing SnappySession ... Done.")
    try {
      snappy.sql("create table devices (device varchar(30) , signal int)")

      // Create DataFrame representing the stream of input lines from connection to host:port
      val socketDF = snappy
          .readStream
          .format("socket")
          .option("host", "localhost")
          .option("port", 9999)
          .load()

      // Creating a typed DeviceData from raw string received on socket.
      val structDF = socketDF.as[String].map(s => {
        val fields = s.split(",")
        DeviceData(fields(0), fields(1).toInt)
      })

      // A simple streaming query to filter signal value and load the output into devices table.
      val streamingQuery = structDF
          .filter(_.signal > 10)
          .writeStream
          .format("snappysink")
          .outputMode("append")
          .queryName("Devices")  // must be unique across a snappydata cluster
          .trigger(ProcessingTime("1 seconds"))
          .option("tableName", "devices")
          .option("checkpointLocation", checkpointDirectory)
          .start()

      streamingQuery.awaitTermination( 15000)

      println("Data loaded in table: ")
      snappy.sql("select * from devices").show()
    } finally {
      snappy.sql("drop table if exists devices")

      // CAUTION: recursively deleting directory
      Path(checkpointDirectory).deleteRecursively()
    }
    println("Exiting")
    System.exit(0)
  }

  case class DeviceData(device: String, signal: Int)

}
