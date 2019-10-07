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

import java.io.{File, PrintWriter}

import com.typesafe.config.Config
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types._
import org.apache.spark.sql._

import scala.util.Try

/**
 * An example that shows how to create column tables in SnappyData
 * using SQL or APIs.
 *
 * <p></p>
 * This example can be run either in local mode (in which case the example runs
 * collocated with Spark+SnappyData Store in the same JVM) or can be submitted as a job
 * to an already running SnappyData cluster.
 *
 * To run the example in local mode go to you SnappyData product distribution
 * directory and type following command on the command prompt
 * <pre>
 * bin/run-example snappydata.CreateColumnTable quickstart/src/main/resources
 * </pre>
 *
 * To submit this example as a job to an already running cluster
 * <pre>
 *   cd $SNAPPY_HOME
 *   bin/snappy-job.sh submit
 *   --app-name CreateColumnTable
 *   --class org.apache.spark.examples.snappydata.CreateColumnTable
 *   --app-jar examples/jars/quickstart.jar
 *   --lead [leadHost:port]
 *   --conf data_resource_folder=../../quickstart/src/main/resources
 *
 * Check the status of your job id
 * bin/snappy-job.sh status --lead [leadHost:port] --job-id [job-id]
 *
 * The output of the job will be redirected to a file named CreateColumnTable.out
 */
object CreateColumnTable extends SnappySQLJob {

  private var dataFolder: String = ""

  def getCurrentDirectory = new java.io.File( "." ).getCanonicalPath

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    val pw = new PrintWriter("CreateColumnTable.out")
    dataFolder = s"${jobConfig.getString("data_resource_folder")}"
    createColumnTableUsingAPI(snappySession, pw)
    createColumnTableUsingSQL(snappySession, pw)
    createColumnTableInferredSchema(snappySession, pw)
    pw.close()
    s"Check ${getCurrentDirectory}/CreateColumnTable.out for output of this job"
  }

  override def isValidJob(snappySession: SnappySession, config: Config): SnappyJobValidation = {
    {
      Try(config.getString("data_resource_folder"))
          .map(x => SnappyJobValid())
          .getOrElse(SnappyJobInvalid("No data_resource_folder config param"))
    }
  }

  /**
   * Creates a column table using APIs
   */
  def createColumnTableUsingAPI(snSession: SnappySession, pw: PrintWriter): Unit = {
    pw.println()

    pw.println("****Create a column table using API****")
    // create a partitioned column table using SQL
    pw.println()
    pw.println("Creating a column table(CUSTOMER) using API")

    snSession.dropTable("CUSTOMER", ifExists = true)

    val tableSchema = StructType(Array(StructField("C_CUSTKEY", IntegerType, false),
      StructField("C_NAME", StringType, false),
      StructField("C_ADDRESS", StringType, false),
      StructField("C_NATIONKEY", IntegerType, false),
      StructField("C_PHONE", StringType, false),
      StructField("C_ACCTBAL", DecimalType(15, 2), false),
      StructField("C_MKTSEGMENT", StringType, false),
      StructField("C_COMMENT", StringType, false)
    ))

    // props1 map specifies the properties for the table to be created
    // "PARTITION_BY" attribute specifies partitioning key for CUSTOMER table(C_CUSTKEY)
    // For complete list of attributes refer the documentation
    // http://snappydatainc.github.io/snappydata/programming_guide#tables-in-snappydata
    val props1 = Map("PARTITION_BY" -> "C_CUSTKEY")
    snSession.createTable("CUSTOMER", "column", tableSchema, props1)

    // insert some data in it
    pw.println()
    pw.println("Loading data in CUSTOMER table from a text file with delimited columns")
    val customerDF = snSession.read
      .format("com.databricks.spark.csv")
      .option("maxCharsPerColumn", "4096")
      .schema(schema = tableSchema)
      .load(s"$dataFolder/customer.csv")
    customerDF.write.insertInto("CUSTOMER")

    pw.println()
    var result = snSession.sql("SELECT COUNT(*) FROM CUSTOMER").collect()
    pw.println("Number of records in CUSTOMER table after loading data are " + result(0).get(0))

    pw.println()
    pw.println("Inserting a row using INSERT SQL")
    snSession.sql("INSERT INTO CUSTOMER VALUES(20000, 'Customer20000', " +
        "'Chicago, IL', 1, '555-101-782', 3500, 'MKTSEGMENT', '')")

    pw.println()
    result = snSession.sql("SELECT COUNT(*) FROM CUSTOMER").collect()
    pw.println("Number of records in CUSTOMER table are " + result(0).get(0))

    pw.println("****Done****")
  }

  /**
   * Creates a column table by executing a SQL statement thru SnappyContext
   *
   * Other way to execute a SQL statement is thru JDBC or ODBC driver. Refer to
   * JDBCExample.scala for more details
   */
  def createColumnTableUsingSQL(snSession: SnappySession, pw: PrintWriter): Unit = {
    pw.println()

    pw.println("****Create a column table using SQL****")
    // create a partitioned column table using SQL
    pw.println()
    pw.println("Creating a column table(CUSTOMER) using SQL")

    snSession.sql("DROP TABLE IF EXISTS CUSTOMER")

    // Create the table using SQL command
    // "PARTITION_BY" attribute specifies partitioning key for CUSTOMER table(C_CUSTKEY)
    // For complete list of attributes refer the documentation
    // http://snappydatainc.github.io/snappydata/programming_guide#tables-in-snappydata
    snSession.sql("CREATE TABLE CUSTOMER ( " +
        "C_CUSTKEY     INTEGER NOT NULL," +
        "C_NAME        VARCHAR(25) NOT NULL," +
        "C_ADDRESS     VARCHAR(40) NOT NULL," +
        "C_NATIONKEY   INTEGER NOT NULL," +
        "C_PHONE       VARCHAR(15) NOT NULL," +
        "C_ACCTBAL     DECIMAL(15,2)   NOT NULL," +
        "C_MKTSEGMENT  VARCHAR(10) NOT NULL," +
        "C_COMMENT     VARCHAR(117) NOT NULL)" +
        "USING COLUMN OPTIONS (PARTITION_BY 'C_CUSTKEY')")

    // insert some data in it
    pw.println()
    pw.println("Loading data in CUSTOMER table from a text file with delimited columns")
    val tableSchema = snSession.table("CUSTOMER").schema
    val customerDF = snSession.read.schema(schema = tableSchema)
      .option("maxCharsPerColumn", "4096").csv(s"$dataFolder/customer.csv")
    customerDF.write.insertInto("CUSTOMER")

    pw.println()
    var result = snSession.sql("SELECT COUNT(*) FROM CUSTOMER").collect()
    pw.println("Number of records in CUSTOMER table after loading data are " + result(0).get(0))

    pw.println()
    pw.println("Inserting a row using INSERT SQL")
    snSession.sql("INSERT INTO CUSTOMER VALUES(20000, 'Customer20000', " +
        "'Chicago, IL', 1, '555-101-782', 3500, 'MKTSEGMENT', '')")

    pw.println()
    result = snSession.sql("SELECT COUNT(*) FROM CUSTOMER").collect()
    pw.println("Number of records in CUSTOMER table are " + result(0).get(0))

    pw.println("****Done****")
  }

  /**
   * Creates a column table where schema is inferred from Parquet/CSV data file
   */
  def createColumnTableInferredSchema(snSession: SnappySession, pw: PrintWriter): Unit = {
    pw.println()

    pw.println("****Create a column table using API where schema is inferred from parquet file****")
    // create a partitioned column table using SQL
    snSession.dropTable("CUSTOMER", ifExists = true)

    val customerDF = snSession.read.parquet(s"$dataFolder/customerparquet")

    // props1 map specifies the properties for the table to be created
    // "PARTITION_BY" attribute specifies partitioning key for CUSTOMER table(C_CUSTKEY),
    // For complete list of attributes refer the documentation
    val props1 = Map("PARTITION_BY" -> "C_CUSTKEY")
    customerDF.write.format("column").mode("append").options(props1).saveAsTable("CUSTOMER")

    pw.println()
    val result = snSession.sql("SELECT COUNT(*) FROM CUSTOMER").collect()
    pw.println("Number of records in CUSTOMER table after loading data are " + result(0).get(0))

    pw.println("****Create a column table using API where schema is inferred from CSV file****")
    snSession.dropTable("CUSTOMER", ifExists = true)

    val customer_csv_DF = snSession.read.option("header", "true")
        .option("inferSchema", "true").option("maxCharsPerColumn", "4096")
        .csv(s"$dataFolder/customer_with_headers.csv")

    // props1 map specifies the properties for the table to be created
    // "PARTITION_BY" attribute specifies partitioning key for CUSTOMER table(C_CUSTKEY),
    // For complete list of attributes refer the documentation
    customer_csv_DF.write.format("column").mode("append").options(props1).saveAsTable("CUSTOMER")

    pw.println()
    val result2 = snSession.sql("SELECT COUNT(*) FROM CUSTOMER").collect()
    pw.println("Number of records in CUSTOMER table after loading data are " + result2(0).get(0))

    pw.println("****Done****")
  }


  def main(args: Array[String]): Unit = {
    parseArgs(args)

    // reducing the log level to minimize the messages on console
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val dataDirAbsolutePath: String = createAndGetDataDir

    println("Creating a SnappySession")
    val spark: SparkSession = SparkSession
        .builder
        .appName("CreateColumnTable")
        .master("local[*]")
         // sys-disk-dir attribute specifies the directory where persistent data is saved
        .config("snappydata.store.sys-disk-dir", dataDirAbsolutePath)
        .config("snappydata.store.log-file", dataDirAbsolutePath + "/SnappyDataExample.log")
        .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)

    val pw = new PrintWriter(System.out, true)
    createColumnTableUsingAPI(snSession, pw)
    createColumnTableUsingSQL(snSession, pw)
    createColumnTableInferredSchema(snSession, pw)
    pw.close()
  }

  def createAndGetDataDir: String = {
    // creating a directory to save all persistent data
    val dataDir = "./" + "snappydata_examples_data"
    new File(dataDir).mkdir()
    val dataDirAbsolutePath = new File(dataDir).getAbsolutePath
    dataDirAbsolutePath
  }

  private def parseArgs(args: Array[String]): Unit = {
    if (args.length != 1) {
      printUsage()
      System.exit(1)
    }
    dataFolder = args(0)
  }

  private def printUsage(): Unit = {
    val usage: String =
      "Usage: CreateColumnTable <dataFolderPath> \n" +
          "\n" +
          "dataFolderPath - (string) local folder where customer.csv is located\n"
    println(usage)
  }

}
