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
package org.apache.spark.examples.snappydata

import java.io.File

import scala.collection.Map

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession, SparkSession}

/**
  * This is a sample code snippet to work with domain objects and SnappyStore
  * column tables.
  *
  * <p></p>
  * This example can be run either in local mode (in which case the example runs
  * collocated with Spark+SnappyData Store in the same JVM) or can be submitted as a job
  * to an already running SnappyData cluster.
  *
  * <p></p>
  * To run the example in local mode go to your SnappyData product distribution
  * directory and type following command on the command prompt
  * <pre>
  * bin/run-example snappydata.WorkingWithObjects
  * </pre>
  *
  * To submit this example as a job to an already running cluster
  * <pre>
  * cd $SNAPPY_HOME
  * bin/snappy-job.sh submit
  * --app-name WorkingWithObjects
  * --class org.apache.spark.examples.snappydata.WorkingWithObjects
  * --app-jar examples/jars/quickstart.jar
  * --lead [leadHost:port]
  *
  * Check the status of your job id
  * bin/snappy-job.sh status --lead [leadHost:port] --job-id [job-id]
  *
  */

case class Address(city: String, state: String)

case class Person(name: String, address: Address, emergencyContacts: Map[String, String])

object WorkingWithObjects extends SnappySQLJob {

  override def isValidJob(snSession: SnappySession,
      config: Config): SnappyJobValidation = SnappyJobValid()

  def main(args: Array[String]) {
    // reducing the log level to minimize the messages on console
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val dataDirAbsolutePath: String = createAndGetDataDir

    val spark: SparkSession = SparkSession
        .builder
        .appName("WorkingWithObjects")
        .master("local[*]")
        // sys-disk-dir attribute specifies the directory where persistent data is saved
        .config("snappydata.store.sys-disk-dir", dataDirAbsolutePath)
        .config("snappydata.store.log-file", dataDirAbsolutePath + "/SnappyDataExample.log")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)
    val config = ConfigFactory.parseString("")
    runSnappyJob(snSession, config)
  }

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {

    // Import the implicits for automatic conversion between Objects to DataSets.
    import snSession.implicits._

    // Create a Dataset using Spark APIs
    val people = Seq(Person("Tom", Address("Columbus", "Ohio"),
      Map("frnd1" -> "8998797979", "frnd2" -> "09878786886"))
      , Person("Ned", Address("San Diego", "California"),
        Map.empty[String, String])).toDS()

    // Drop the table if it exists.
    snSession.dropTable("Persons", ifExists = true)

    // Create a columnar table with the a Struct to store Address
    snSession.sql("CREATE table Persons(name String, address Struct<city: String, state:String>, " +
        "emergencyContacts Map<String,String>) using column options()")

    // Write the created DataFrame to the columnar table.
    people.write.insertInto("Persons")

    // print schema of the table
    // scalastyle:off println
    println("Print Schema of the table\n################")
    println(snSession.table("Persons").schema)
    println
    // scalastyle:on println

    // Append more people to the column table
    val morePeople = Seq(Person("Jon Snow", Address("Columbus", "Ohio"), Map.empty[String, String]),
      Person("Rob Stark", Address("San Diego", "California"), Map.empty[String, String]),
      Person("Michael", Address("Null", "California"), Map.empty[String, String])).toDS()

    morePeople.write.insertInto("Persons")

    // Query it like any other table
    val nameAndAddress = snSession.sql("SELECT name, address, emergencyContacts FROM Persons")
    val allPersons = nameAndAddress.as[Person]
    allPersons.show()
  }

  def createAndGetDataDir: String = {
    // creating a directory to save all persistent data
    val dataDir = "./" + "snappydata_examples_data"
    new File(dataDir).mkdir()
    val dataDirAbsolutePath = new File(dataDir).getAbsolutePath
    dataDirAbsolutePath
  }

}
