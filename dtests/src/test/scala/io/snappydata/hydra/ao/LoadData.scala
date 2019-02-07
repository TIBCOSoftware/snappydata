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

package io.snappydata.hydra.ao

import java.io.{File, PrintWriter}
import java.sql.Timestamp

import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config

import org.apache.spark.sql._


object LoadData extends SnappySQLJob {

  val currDir: String = new java.io.File(".").getCanonicalPath
  val fileSep: String = File.separator
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val outputFileName = s"LoadTimeTest_${System.currentTimeMillis()}.out"
    val pw = new PrintWriter(outputFileName)
    Try {
      // scalastyle:off println
      val snc = snSession.sqlContext
      val tableName = jobConfig.getString("tableName")
      val dataLocation = jobConfig.getString("dataLocation")
      val genDupData: Boolean = jobConfig.getBoolean("genDupData")
      val numIter: Int = jobConfig.getInt("iterations")
      val insertType: String = jobConfig.getString("insertUsing") // external table for dataframe
      val insertFrom: String = jobConfig.getString("insertFrom") // csv or parquet

      val parquetFileLoc = dataLocation + fileSep + numIter + "_iters" + fileSep + "parquetFiles"
      val csvFileLoc = dataLocation + fileSep + numIter + "_iters" + fileSep + "csvFiles"

      if (genDupData) {
        generateDupData(tableName, pw, numIter, snc, dataLocation, csvFileLoc, parquetFileLoc)
      }

      var fileLocation: String = ""
      if(insertFrom.equals("csv")) {
        fileLocation = csvFileLoc
      } else {
        fileLocation = parquetFileLoc
      }
      insertData(tableName, pw, snc, insertType, insertFrom, fileLocation)

    } match {
      case Success(v) => pw.close()
        s"See ${currDir}/$outputFileName"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  def insertData(tableName: String, pw: PrintWriter, snc: SQLContext, insertType: String,
      insertFrom: String, fileLocation: String): Any = {
    pw.println(s"Loading using $insertType from $insertFrom...")

    snc.sql(s"drop table if exists STAGING_$tableName")

    var externalTableDDL: String = s"CREATE EXTERNAL TABLE STAGING_$tableName USING "
    if(insertFrom.equals("csv")){
      externalTableDDL = externalTableDDL + s" com.databricks.spark.csv OPTIONS(path " +
          s"'$fileLocation/$tableName', header 'false', inferSchema 'false', " +
          s"nullValue 'NULL', maxCharsPerColumn '4096')"
    } else { // parquet
      externalTableDDL = externalTableDDL + s" parquet OPTIONS(path " +
          s"'$fileLocation/$tableName')"
    }

    // loading data
    var startTime, endTime: Long = 0L
    pw.println(s"[${new Timestamp(System.currentTimeMillis())}] Start loading data...")
    if(insertType.equals("externalTable")){
      startTime = System.currentTimeMillis()
      snc.sql(externalTableDDL)
      snc.sql(s"insert into ${tableName} select * from STAGING_$tableName" )
      endTime = System.currentTimeMillis()
    } else { // dataframe.insertinto
      var meterReadings_DF: DataFrame = null
      startTime = System.currentTimeMillis()
      if (insertFrom.equals("csv")) {
        meterReadings_DF =
            snc.read.format("com.databricks.spark.csv")
                .option("header", "false")
                .option("inferSchema", "false")
                .option("maxCharsPerColumn", "4096")
                .option("nullValue", "")
                .csv(s"${fileLocation}/$tableName")
      } else { // parquet
        meterReadings_DF =
            snc.read.load(s"${fileLocation}/$tableName")
      }
      meterReadings_DF.write.insertInto(s"${tableName}")
      endTime = System.currentTimeMillis()
    }
    val totalTime = (endTime - startTime)/1000
    pw.println(s"[${new Timestamp(endTime)}] Loading data using $insertType from $insertFrom took" +
        s" $totalTime secs")
    pw.flush()
  }

  /*
  Generate duplicate data from csv.
   */
  def generateDupData(tableName: String, pw: PrintWriter, numIter: Int, snc: SQLContext,
  dataLocation: String, csvFileLocation: String, parquetFileLocation: String): Any
  = {
    def temp_DF: DataFrame =
      snc.read.format("com.databricks.spark.csv")
          .option("header", "false")
          .option("inferSchema", "false")
          .option("maxCharsPerColumn", "4096")
          .option("nullValue", "")
          .csv(s"${dataLocation}/${tableName}.csv")

    // generating 10 million records from 2 lac records
    pw.println("Duplicating data to temp table...")
    for (i <- 0 until numIter) {
      temp_DF.write.format("column").mode(SaveMode.Append).saveAsTable(s"${tableName}Temp")
    }

    // writing dat to parquet and csv
    writeToCsvAndParquet(tableName + "Temp", pw, snc, csvFileLocation, parquetFileLocation)

    pw.println("Dropping temp table")
    snc.sql(s"drop table if exists ${tableName}Temp")
  }

  def writeToCsvAndParquet(tableName: String, pw: PrintWriter, snc: SQLContext, csvFileLocation:
  String, parquetFileLocation: String): Unit = {
    pw.println("creating df with duplicate records and writing it to csv and parquet..")
    val df = snc.sql(s"select * from ${tableName}")
    df.write.parquet(s"${parquetFileLocation}/${tableName}")
    df.write.csv(s"${csvFileLocation}/$tableName")
    pw.flush()
  }
  /**
    * Validate if the data files are available, else throw SparkJobInvalid
    *
    */
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

}