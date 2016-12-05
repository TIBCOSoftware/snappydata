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
 * This is a sample code snippet to work with domain objects and SnappyStore column tables.
 * Run with
 * <pre>
 * bin/run-example snappydata.WorkingWithObjects
 * </pre>
 */

case class Address(city: String, state: String)

case class Person(name: String, address: Address)

object WorkingWithObjects extends SnappySQLJob {

  override def isValidJob(snSession: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {

    //Import the implicits for automatic conversion between Objects to DataSets.
    import snSession.implicits._
    // Create a Dataset using Spark APIs
    val people = Seq(Person("Tom", Address("Columbus", "Ohio")), Person("Ned", Address("San Diego", "California"))).toDS()


    //Drop the table if it exists.
    snSession.dropTable("people", ifExists = true)

    //Create a columnar table with the DataFrame schema
    snSession.createTable(tableName = "people",
      provider = "column",
      schema = people.schema,
      options = Map.empty[String,String],
      allowExisting = false)

    // Write the created DataFrame to the columnar table.
    people.write.insertInto("people")

    //print schema of the table
    println("Print Schema of the table\n################")
    println(snSession.table("people").schema)
    println


    // Append more people to the column table
    val morePeople = Seq(Person("Jon Snow", Address("Columbus", "Ohio")),
      Person("Rob Stark", Address("San Diego", "California")),
      Person("Michael", Address("Null", "California"))).toDS()

    morePeople.write.insertInto("people")

    // Query it like any other table
    val nameAndAddress = snSession.sql("SELECT name, address.city, address.state FROM people")

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

    val spark: SparkSession = SparkSession
        .builder
        .appName("WorkingWithObjects")
        .master("local[4]")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)
    val config = ConfigFactory.parseString("")
    val results = runSnappyJob(snSession, config)
    println("Printing All People \n################## \n" + results)
  }
}
