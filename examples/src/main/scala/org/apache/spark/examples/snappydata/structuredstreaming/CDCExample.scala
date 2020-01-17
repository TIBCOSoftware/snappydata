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

import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.{SnappySession, SparkSession}
import scala.language.postfixOps
import scala.reflect.io.Path

/**
 * An example explaining CDC (change data capture) use case with SnappyData streaming sink.
 *
 * For CDC use case following two conditions should match:
 * 1) The target table must be defined with key columns (for column tables) or primary keys ( for
 * row table).
 * 2) The input dataset must have an numeric column with name `_eventType` indicating type of the
 * event. The value of this column is mapped with event type in the following manner:
 *
 *  0 - insert
 *  1 - putInto
 *  2 - delete
 *
 * Based on the key values in the incoming dataset and the value of `_eventType` column the sink
 * will decide which operation need to be performed for each record.
 *
 * To run this on your local machine, you need to first run a Netcat server:
 * `$ nc -lk 9999`
 *
 * Example input data. Note that the last value from CSV record indicates the `_eventType`:
 *
 * 1,user1,23,0
 * 2,user2,45,0
 * 1,user1,23,2
 * 2,user2,46,1
 *
 * To run the example in local mode go to your SnappyData product distribution
 * directory and execute the following command:
 * <p>
 * `bin/run-example snappydata.structuredstreaming.CDCExample`
 * <br><br>
 *
 */
// scalastyle:off println
object CDCExample{

  def main(args: Array[String]) {
    // reducing the log level to minimize the messages on console
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    println("Initializing a SnappySession")
    val checkpointDirectory = this.getClass.getSimpleName
    val spark: SparkSession = SparkSession
        .builder()
        .appName(getClass.getSimpleName)
        .master("local[*]")
        .getOrCreate()

    import spark.implicits._
    val snappy = new SnappySession(spark.sparkContext)

    // The target table is created with key columns (for column table) for CDC use case
    snappy.sql("create table users (id long , name varchar(40), age int) using" +
        " column options(key_columns 'id')")

    // Create DataFrame representing the stream of input lines from connection to host:port
    val socketDF = snappy
        .readStream
        .format("socket")
        .option("host", "localhost")
        .option("port", 9999)
        .load()

    // Creating a typed User from raw string received on socket
    val structDF = socketDF.as[String].map(s => {
      val fields = s.split(",")
      // Note that the fourth field of `User` class is `_eventType`. Value of this field indicates
      // the type of the DML operation to be performed.
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
