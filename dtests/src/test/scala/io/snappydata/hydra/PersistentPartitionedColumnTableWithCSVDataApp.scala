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
package io.snappydata.hydra

import org.apache.spark.sql._

/**
 * Created by swati on 29/7/16.
 */
object PersistentPartitionedColumnTableWithCSVDataApp {
    var debug: Boolean = false
  var setJars, executorExtraClassPath: String = null

  var executorExtraJavaOptions: String = null
  var tableName: String = "airline"
  val conf = new org.apache.spark.SparkConf()
    .setAppName("PersistentPartitionedColumnTableWithCSVDataApp")
    .set("spark.logConf", "true")
    .set("spark.scheduler.mode", "FAIR")

  val sc = new org.apache.spark.SparkContext(conf)
  val snContext = org.apache.spark.sql.SnappyContext(sc)
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)


  def main(args: Array[String]) {
    println(" Started spark context = " + SnappyContext.globalSparkContext)
    snContext.sql("set spark.sql.shuffle.partitions=6")
    createTableLoadData(snContext, tableName)
    runQueries(tableName)
  }

  def createTableLoadData(snContext: SnappyContext, tableName: String) = {
    var airlineDataFrame: DataFrame = null
    var hfile: String = "/home/swati/2015.csv"
    val codetableFile = "/home/swati/Downloads/airportCode.csv"
        // All these properties will default when using snappyContext in the release
    val props = Map("PERSISTENT" -> "")

    // Create the Airline columnar table
    airlineDataFrame = snContext.read
            .format("com.databricks.spark.csv") // CSV to DF package
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .load(hfile)


    println(s"The $tableName table schema ..")
    airlineDataFrame.schema.printTreeString()
    // airlineDataFrame.show(10) // expensive extract of all partitions?

    snContext.dropTable(tableName, ifExists = true)

    // This will do the real work. Load the data.
    snContext.createTable(tableName, "column",
      airlineDataFrame.schema, props)
    airlineDataFrame.write.mode(SaveMode.Append).saveAsTable(tableName)

    println(s"Done with loading data into airline ..")

    //Now create the airline code row replicated table
    val codeTabledf = snContext.read
            .format("com.databricks.spark.csv") // CSV to DF package
            .option("header", "true") // Use first line of all files as header
            .option("inferSchema", "true") // Automatically infer data types
            .load(codetableFile)
    println("The airline code table schema ..")
    codeTabledf.schema.printTreeString()
    codeTabledf.show(10)

    snContext.dropTable("airlineref", ifExists = true)

    codeTabledf.write.format("row").options(props).saveAsTable("airlineref")

    // finally creates some samples
    snContext.dropTable("airline_sample", ifExists = true)
    snContext.sql("create sample table airline_sample on airline " +
      "options(qcs 'UniqueCarrier,YearI,MonthI', fraction '0.03'," +
      "  strataReservoirSize '50')")

  }


  def runQueries(tableName: String): Unit = {
    var start: Long = 0
    var end: Long = 0
    var results: DataFrame = null
    val snContext = this.snContext
    for (i <- 0 until 1) {
      start = System.currentTimeMillis
      results = snContext.sql(s"SELECT count(*) FROM $tableName")
      results.map(t => "Count: " + t(0)).collect().foreach(msg)
      end = System.currentTimeMillis
      msg("Time taken for count(*): " + (end - start) + "ms")

      start = System.currentTimeMillis
      results = snContext.sql(s"SELECT count(DepTime) FROM $tableName")
      results.map(t => "Count: " + t(0)).collect().foreach(msg)
      end = System.currentTimeMillis
      msg("Time taken for count(DepTime): " + (end - start) + "ms")

      start = System.currentTimeMillis
      results = snContext.sql(
        s"""SELECT AVG(ArrDelay), count(*), UniqueCarrier,
	        YearI, MonthI FROM $tableName GROUP BY UniqueCarrier, YearI, MonthI""")
      results.collect() //.foreach(msg)
      end = System.currentTimeMillis
      msg("Time taken for AVG+count(*) with GROUP BY: " + (end - start) + "ms")

      start = System.currentTimeMillis
      results = snContext.sql(
        s"""SELECT AVG(ArrDelay), count(*), UniqueCarrier, t2.DESCRIPTION,
	        YearI, MonthI FROM $tableName t1, airlineref t2 where t1.UniqueCarrier = t2.CODE
	        GROUP BY UniqueCarrier, DESCRIPTION, YearI,MonthI""")
      results.collect()
      end = System.currentTimeMillis
      msg("Time taken for AVG+count(*) with JOIN + GROUP BY: " + (end - start) + "ms")

      Thread.sleep(3000)

      start = System.currentTimeMillis
      results = snContext.sql(
        s"""SELECT AVG(ArrDelay), UniqueCarrier, YearI,
	        MonthI FROM $tableName GROUP BY UniqueCarrier, YearI, MonthI ORDER BY
	        UniqueCarrier, YearI, MonthI""")
      //results.explain(true)
      results.collect() //.foreach(msg)
      end = System.currentTimeMillis
      msg("Time taken for AVG on GROUP BY with ORDER BY: " +
        (end - start) + "ms")

      Thread.sleep(3000)

      start = System.currentTimeMillis
      results = snContext.sql(
        s"""SELECT AVG(ArrDelay), UniqueCarrier, YearI,
	        MonthI FROM $tableName GROUP BY UniqueCarrier, YearI, MonthI
	        ORDER BY YearI, MonthI""")
      results.collect()
      end = System.currentTimeMillis
      msg("Time taken for worst carrier processing: " + (end - start) + "ms")

      Thread.sleep(3000)
    }

    println(s"Done with running queries...")
  }

  def msg(s: String) = {
    println("Thread :: " + Thread.currentThread().getName + " :: " + s)
  }
}
