/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.{SnappyContext, SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession, SparkSession}

/**
 * This is a sample code snippet to work with JSON files and SnappyStore tables.
 * Run with
 * <pre>
 * bin/run-example snappydata.WorkingWithJson
 * </pre>
 */
object WorkingWithJson extends SnappySQLJob {

  override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(sc: SnappyContext, jobConfig: Config): Any = {

    // Read a JSON file using Spark API
    val people = sc.jsonFile("examples/src/resources/people.json")

    //Drop the table if it exists.
    sc.dropTable("people", ifExists = true)

    // Write the created DataFrame to a column table.
    people.write.format("column").saveAsTable("people")

    // Append more people to the column table
    val morePeople = sc.jsonFile("examples/src/resources/more_people.json")
    morePeople.write.insertInto("people")

    // Query it like any other table
    val nameAndAddress = sc.sql("SELECT name, address.city, address.state FROM people")

    val builder = new StringBuilder
    nameAndAddress.collect.map(row => {
      builder.append(s"${row(0)} ,")
      builder.append(s"${row(1)} ,")
      builder.append(s"${row(2)} \n")

    })
    builder.toString
  }

  def main(args: Array[String]) {
    // reducing the log level to minimize the messages on console
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark: SparkSession = SparkSession.builder.appName("WorkingWithJson").master("local[4]").getOrCreate
    val snSession = new SnappySession(spark.sparkContext, existingSharedState = None)
    val config = ConfigFactory.parseString("")
    val results = runSnappyJob(snSession.snappyContext, config)
    println("Printing All People \n################## \n" + results)
  }
}
