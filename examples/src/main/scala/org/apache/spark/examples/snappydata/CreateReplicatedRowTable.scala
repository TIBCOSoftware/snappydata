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

import java.io.{File, PrintWriter}

import com.typesafe.config.Config
import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.types.{StructField, StructType, IntegerType, StringType, DecimalType}
import org.apache.spark.sql.{SnappySession, SparkSession, Row, SnappyJobValid, SnappyJobValidation, SnappyContext, SnappySQLJob}

/**
 * An example that shows how to create replicated row tables in SnappyData
 * using SQL or APIs.
 *
 * <p></p>
 * This example can be run either in local mode (in which case the example runs
 * collocated with Spark+SnappyData Store in the same JVM) or can be submitted as a job
 * to an already running SnappyData cluster.
 *
 * <p></p>
 * To run the example in local mode go to you SnappyData product distribution
 * directory and type following command on the command prompt
 * <pre>
 * bin/run-example snappydata.CreateReplicatedRowTable
 * </pre>
 *
 * To submit this example as a job to an already running cluster
 * <pre>
 *   cd $SNAPPY_HOME
 *   bin/snappy-job.sh submit
 *   --app-name CreateReplicatedRowTable
 *   --class org.apache.spark.examples.snappydata.CreateReplicatedRowTable
 *   --app-jar examples/jars/quickstart.jar
 *   --lead [leadHost:port]
 *
 * Check the status of your job id
 * bin/snappy-job.sh status --lead [leadHost:port] --job-id [job-id]
 *
 * The output of the job will be redirected to a file named CreateReplicatedRowTable.out
 *
 */
object CreateReplicatedRowTable extends SnappySQLJob {

  case class Data(S_SUPPKEY: Int, S_NAME: String, S_ADDRESS: String,
      S_NATIONKEY: Int, S_PHONE: String, S_ACCTBAL: BigDecimal, S_COMMENT: String)

  def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {

    val pw = new PrintWriter("CreateReplicatedRowTable.out")

    createReplicatedRowTableUsingSQL(snappySession, pw)
    createReplicatedRowTableUsingAPI(snappySession, pw)
    pw.close()

    s"Check ${getCurrentDirectory}/CreateReplicatedRowTable.out file for output of this job"
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  /**
   * Creates row table and performs operations on it using APIs
   */
  def createReplicatedRowTableUsingAPI(snSession: SnappySession, pw: PrintWriter): Unit = {
    pw.println()

    pw.println("****Create a row table(SUPPLIER) using API****")

    pw.println()

    // drop the table if it exists
    snSession.dropTable("SUPPLIER", ifExists = true)

    val schema = StructType(Array(StructField("S_SUPPKEY", IntegerType, false),
      StructField("S_NAME", StringType, false),
      StructField("S_ADDRESS", StringType, false),
      StructField("S_NATIONKEY", IntegerType, false),
      StructField("S_PHONE", StringType, false),
      StructField("S_ACCTBAL", DecimalType(15, 2), false),
      StructField("S_COMMENT", StringType, false)
    ))

    // props1 map specifies the properties for the table to be created
    // "PERSISTENT" that the table data should be persisted to disk asynchronously
    // For complete list of attributes refer the documentation
    // http://snappydatainc.github.io/snappydata/programming_guide#tables-in-snappydata
    val props1 = Map("PERSISTENT" -> "asynchronous")
    // create a row table using createTable API
    snSession.createTable("SUPPLIER", "row", schema, props1)

    pw.println("Inserting data in SUPPLIER table")
    val data = Seq(Seq(1, "SUPPLIER1", "CHICAGO, IL", 0, "555-543-789", BigDecimal(10000), " "),
      Seq(2, "SUPPLIER2", "BOSTON, MA", 0, "555-234-489", BigDecimal(20000), " "),
      Seq(3, "SUPPLIER3", "NEWYORK, NY", 0, "555-743-785", BigDecimal(34000), " "),
      Seq(4, "SUPPLIER4", "SANHOSE, CA", 0, "555-321-098", BigDecimal(1000), " ")
    )
    val rdd = snSession.sparkContext.parallelize(data,
      data.length).map(s => new Data(s(0).asInstanceOf[Int],
      s(1).asInstanceOf[String],
      s(2).asInstanceOf[String],
      s(3).asInstanceOf[Int],
      s(4).asInstanceOf[String],
      s(5).asInstanceOf[BigDecimal],
      s(6).asInstanceOf[String]))

    val dataDF = snSession.createDataFrame(rdd)
    dataDF.write.insertInto("SUPPLIER")

    pw.println("Printing the contents of the SUPPLIER table")
    var tableData = snSession.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(pw.println)

    pw.println()
    pw.println("Update the table account balance for SUPPLIER4")
    snSession.update("SUPPLIER", "S_NAME = 'SUPPLIER4'", Row(BigDecimal(50000)), "S_ACCTBAL")

    pw.println("Printing the contents of the SUPPLIER table after update")
    tableData = snSession.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(pw.println)

    pw.println()
    pw.println("Delete the records for SUPPLIER2 and SUPPLIER3")
    snSession.delete("SUPPLIER", "S_NAME = 'SUPPLIER2' OR S_NAME = 'SUPPLIER3'")

    pw.println("Printing the contents of the SUPPLIER table after delete")
    tableData = snSession.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(pw.println)

    pw.println("****Done****")
  }

  /**
   * Creates row table and performs operations on it using SQL queries thru
   * SnappyContext
   *
   * Other way to execute a SQL statement is thru JDBC or ODBC driver. Refer to
   * JDBCExample.scala for more details
   */
  def createReplicatedRowTableUsingSQL(snSession: SnappySession, pw: PrintWriter): Unit = {
    pw.println()

    pw.println("****Create a row table using SQL****")
    pw.println()
    pw.println("Creating a row table(SUPPLIER) using SQL")

    snSession.sql("DROP TABLE IF EXISTS SUPPLIER")

    // Create a row table using SQL
    // "PERSISTENT" that the table data should be persisted to disk asynchronously
    // For complete list of attributes refer the documentation
    // http://snappydatainc.github.io/snappydata/programming_guide#tables-in-snappydata
    snSession.sql(
      "CREATE TABLE SUPPLIER ( " +
          "S_SUPPKEY INTEGER NOT NULL PRIMARY KEY, " +
          "S_NAME STRING NOT NULL, " +
          "S_ADDRESS STRING NOT NULL, " +
          "S_NATIONKEY INTEGER NOT NULL, " +
          "S_PHONE STRING NOT NULL, " +
          "S_ACCTBAL DECIMAL(15, 2) NOT NULL, " +
          "S_COMMENT STRING NOT NULL " +
          ") USING ROW OPTIONS (PERSISTENT 'asynchronous')")

    // insert some data in it
    pw.println()
    pw.println("Inserting data in SUPPLIER table")
    snSession.sql("INSERT INTO SUPPLIER VALUES(1, 'SUPPLIER1', 'CHICAGO, IL', 0, '555-543-789', 10000, ' ')")
    snSession.sql("INSERT INTO SUPPLIER VALUES(2, 'SUPPLIER2', 'BOSTON, MA', 0, '555-234-489', 20000, ' ')")
    snSession.sql("INSERT INTO SUPPLIER VALUES(3, 'SUPPLIER3', 'NEWYORK, NY', 0, '555-743-785', 34000, ' ')")
    snSession.sql("INSERT INTO SUPPLIER VALUES(4, 'SUPPLIER4', 'SANHOSE, CA', 0, '555-321-098', 1000, ' ')")

    pw.println("Printing the contents of the SUPPLIER table")
    var tableData = snSession.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(pw.println)

    pw.println()
    pw.println("Update the table account balance for SUPPLIER4")
    snSession.sql("UPDATE SUPPLIER SET S_ACCTBAL = 50000 WHERE S_NAME = 'SUPPLIER4'")

    pw.println("Printing the contents of the SUPPLIER table after update")
    tableData = snSession.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(pw.println)

    pw.println()
    pw.println("Delete the records for SUPPLIER2 and SUPPLIER3")
    snSession.sql("DELETE FROM SUPPLIER WHERE S_NAME = 'SUPPLIER2' OR S_NAME = 'SUPPLIER3'")

    pw.println("Printing the contents of the SUPPLIER table after delete")
    tableData = snSession.sql("SELECT * FROM SUPPLIER").collect()
    tableData.foreach(pw.println)

    pw.println("****Done****")
  }

  def main(args: Array[String]): Unit = {
    // reducing the log level to minimize the messages on console
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val dataDirAbsolutePath: String = createAndGetDataDir

    println("Creating a SnappySession")
    val spark: SparkSession = SparkSession
        .builder
        .appName("CreateReplicatedRowTable")
        .master("local[*]")
        // sys-disk-dir attribute specifies the directory where persistent data is saved
        .config("snappydata.store.sys-disk-dir", dataDirAbsolutePath)
        .config("snappydata.store.log-file", dataDirAbsolutePath + "/SnappyDataExample.log")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)

    val pw = new PrintWriter(System.out, true)
    createReplicatedRowTableUsingSQL(snSession, pw)
    createReplicatedRowTableUsingAPI(snSession, pw)
    pw.close()
  }

  def createAndGetDataDir: String = {
    // creating a directory to save all persistent data
    val dataDir = "./" + "snappydata_examples_data"
    new File(dataDir).mkdir()
    val dataDirAbsolutePath = new File(dataDir).getAbsolutePath
    dataDirAbsolutePath
  }

}
