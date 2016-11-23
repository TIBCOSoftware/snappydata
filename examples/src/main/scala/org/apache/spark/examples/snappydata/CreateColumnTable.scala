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

import java.io.PrintWriter

import com.typesafe.config.Config
import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.types.{StringType, DecimalType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{SnappySession, SparkSession, SnappyContext, SnappyJobValid, SnappyJobValidation, SnappySQLJob}

/**
 * An example that shows how to create column tables in SnappyData
 * using SQL or APIs.
 *
 * <p></p>
 * This example can be run either in local mode(in which it will spawn a single
 * node SnappyData system) or can be submitted as a job to an already running
 * SnappyData cluster.
 *
 * To run the example in local mode go to you SnappyData product distribution
 * directory and type following command on the command prompt
 * <pre>
 * bin/run-example snappydata.CreateColumnTable
 * </pre>
 *
 */
object CreateColumnTable extends SnappySQLJob {

  case class CustomerRow (C_CUSTKEY: Int, C_NAME: String, C_ADDRESS: String,
                          C_NATIONKEY: Int, C_PHONE: String, C_ACCTBAL: Double,
                          C_MKTSEGMENT: String, C_COMMENT: String)

  override def runSnappyJob(snc: SnappyContext, jobConfig: Config): Any = {
    val pw = new PrintWriter("CreateColumnTable.out")
    createColumnTableUsingAPI(snc, pw)
    createColumnTableUsingSQL(snc, pw)
    pw.close()
  }

  override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = SnappyJobValid()

  def createColumnTableUsingAPI(snc: SnappyContext, pw: PrintWriter): Unit = {
    pw.println()

    pw.println("****Create a column table using API****")
    // create a partitioned row table using SQL
    pw.println()
    pw.println("Creating a column table(CUSTOMER) using API")

    snc.dropTable("CUSTOMER", ifExists = true)

    val schema = StructType(Array(StructField("C_CUSTKEY", IntegerType, false),
      StructField("C_NAME", StringType, false),
      StructField("C_ADDRESS", StringType, false),
      StructField("C_NATIONKEY", IntegerType, false),
      StructField("C_PHONE", StringType, false),
      StructField("C_ACCTBAL", DecimalType(15, 2), false),
      StructField("C_MKTSEGMENT", StringType, false),
      StructField("C_COMMENT", StringType, false)
    ))

    // props1 map specifies the properties for the table to be created
    val props1 = Map("PARTITION_BY" -> "C_CUSTKEY", "BUCKETS" -> "11")
    snc.createTable("CUSTOMER", "column", schema, props1)

    // insert some data in it
    pw.println()
    pw.println("Loading data in CUSTOMER table from a text file with delimited columns")
    val customerData = snc.sparkContext.
        textFile(s"quickstart/src/resources/customer.tbl")
    val customerReadings = customerData.map(s => s.split('|')).map(s => parseCustomerRow(s))
    val customerDF = snc.createDataFrame(customerReadings)
    customerDF.write.insertInto("CUSTOMER")

    pw.println()
    var result = snc.sql("SELECT COUNT(*) FROM CUSTOMER").collect()
    pw.println("Number of records in CUSTOMER table after loading data are " + result(0).get(0))

    pw.println()
    pw.println("Inserting a row using INSERT SQL")
    snc.sql("INSERT INTO CUSTOMER VALUES(20000, 'Customer20000', " +
        "'Chicago, IL', 1, '555-101-782', 3500, 'MKTSEGMENT', '')")

    pw.println()
    result = snc.sql("SELECT COUNT(*) FROM CUSTOMER").collect()
    pw.println("Number of records in CUSTOMER table are " + result(0).get(0))

    pw.println("****Done****")
  }

  def createColumnTableUsingSQL(snc: SnappyContext, pw: PrintWriter): Unit = {

    pw.println()

    pw.println("****Create a column table using SQL****")
    // create a partitioned row table using SQL
    pw.println()
    pw.println("Creating a column table(CUSTOMER) using SQL")

    snc.sql("DROP TABLE IF EXISTS CUSTOMER")

    snc.sql("CREATE TABLE CUSTOMER ( " +
        "C_CUSTKEY     INTEGER NOT NULL," +
        "C_NAME        VARCHAR(25) NOT NULL," +
        "C_ADDRESS     VARCHAR(40) NOT NULL," +
        "C_NATIONKEY   INTEGER NOT NULL," +
        "C_PHONE       VARCHAR(15) NOT NULL," +
        "C_ACCTBAL     DECIMAL(15,2)   NOT NULL," +
        "C_MKTSEGMENT  VARCHAR(10) NOT NULL," +
        "C_COMMENT     VARCHAR(117) NOT NULL)" +
        "USING COLUMN OPTIONS (PARTITION_BY 'C_CUSTKEY', BUCKETS '11' )")

    // insert some data in it
    pw.println()
    pw.println("Loading data in CUSTOMER table from a text file with delimited columns")
    val customerData = snc.sparkContext.
        textFile(s"quickstart/src/resources/customer.tbl")
    val customerReadings = customerData.map(s => s.split('|')).map(s => parseCustomerRow(s))
    val customerDF = snc.createDataFrame(customerReadings)
    customerDF.write.insertInto("CUSTOMER")

    pw.println()
    var result = snc.sql("SELECT COUNT(*) FROM CUSTOMER").collect()
    pw.println("Number of records in CUSTOMER table after loading data are " + result(0).get(0))

    pw.println()
    pw.println("Inserting a row using INSERT SQL")
    snc.sql("INSERT INTO CUSTOMER VALUES(20000, 'Customer20000', " +
        "'Chicago, IL', 1, '555-101-782', 3500, 'MKTSEGMENT', '')")

    pw.println()
    result = snc.sql("SELECT COUNT(*) FROM CUSTOMER").collect()
    pw.println("Number of records in CUSTOMER table are " + result(0).get(0))

    pw.println("****Done****")
  }

  def parseCustomerRow(s: Array[String]): CustomerRow = {
    CustomerRow(s(0).toInt, s(1), s(2), s(3).toInt, s(4), s(5).toDouble, s(6), s(7))
  }

  def main(args: Array[String]): Unit = {
    // reducing the log level to minimize the messages on console
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    println("Creating a SnappySession")
    val spark: SparkSession = SparkSession
        .builder
        .appName("CreateColumnTable")
        .master("local[4]")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext, existingSharedState = None)

    val pw = new PrintWriter(System.out, true)
    createColumnTableUsingAPI(snSession.snappyContext, pw)
    createColumnTableUsingSQL(snSession.snappyContext, pw)
    pw.close()
  }

}
