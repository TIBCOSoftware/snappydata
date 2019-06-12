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

package org.apache.spark.examples.snappydata

import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.{SnappySession, SparkSession}
import scala.language.postfixOps
import scala.reflect.io.Path

/**
 * An example showing CDC usage with SnappyData structured streaming
 *
 * <p></p>
 * To run the example in local mode go to your SnappyData product distribution
 * directory and type following command on the command prompt
 * <pre>
 * bin/run-example snappydata.StructuredStreamingCDCExample
 * </pre>
 * <p></p>
 * To run this on your local machine, you need to first run a Netcat server <br>
 * `$ nc -lk 9999`
 * <p>
 * Sample input data:
 * {{{
 * 1,user1,23,0
 * 2,user2,45,0
 * 1,user1,23,2
 * 2,user2,46,1
 * }}}
 *
 */
object StructuredStreamingCDCExample{

  def main(args: Array[String]) {
    // reducing the log level to minimize the messages on console
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    println("Initializing a SnappySesion")
    val checkpointDirectory = "/tmp/StructuredStreamingCDCExample"
    val spark: SparkSession = SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .master("local[*]")
        .getOrCreate()

    import spark.implicits._
    val snappy = new SnappySession(spark.sparkContext)


    snappy.sql("create table users (id long , name varchar(40), age int) using column options(key_columns 'id')")

    // Create DataFrame representing the stream of input lines from connection to host:port
    val socketDF = snappy
        .readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load()

    // Creating a typed User from raw string received on socket.
    val structDF = socketDF.as[String].map(s => {
      val fields = s.split(",")
      User(fields(0).toLong, fields(1), fields(2).toInt, fields(3).toInt)
    })

    // A simple streaming query to filter users by their age and load the data in users table
    val streamingQuery = structDF
        .filter( _.age >= 12)
        .writeStream
        .format("snappysink")
        .outputMode("append")
        .queryName("users")       // must be unique across the Snappydata cluster
        .trigger(ProcessingTime("1 seconds"))
        .option("tableName", "users")
        .option("checkpointLocation", checkpointDirectory)
        .start()

    streamingQuery.awaitTermination(timeoutMs = 15000)
    snappy.sql("select * from users").show()

    snappy.sql("drop table users")

    // CAUTION: recursively deleting directory
    Path(checkpointDirectory).deleteRecursively()

    println("Exiting")
    System.exit(0)
  }
}

case class User(is: Long, name: String, age: Int, _eventType: Int)

