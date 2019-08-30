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


object ConvertToParquet extends SnappySQLJob {

  val currDir: String = new java.io.File(".").getCanonicalPath
  val fileSep: String = File.separator
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val outputFileName = s"ConvertToParquet_${System.currentTimeMillis()}.out"
    val pw = new PrintWriter(outputFileName)
    Try {
      // scalastyle:off println
      val snc = snSession.sqlContext
      val tableName = jobConfig.getString("tableName")
      // where to store the parquet file and read from csv
      val dataFileLocation = jobConfig.getString("dataFileLocation")
      val fromType: String = jobConfig.getString("fromType")
      writeToParquetAndCsv(tableName, pw, snc, dataFileLocation, fromType)
    } match {
      case Success(v) => pw.close()
        s"See ${currDir}/$outputFileName"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  def writeToParquetAndCsv(tableName: String, pw: PrintWriter, snc: SQLContext, dataFileLocation:
  String, fromType: String): Unit = {
    pw.println("Writing data to parquet and/or csv..")
    val parquetFileLoc = dataFileLocation  + fileSep + "parquetFiles"
    val csvFileLoc = dataFileLocation + fileSep + "csvFiles"
    var df: DataFrame = null
    if (fromType.equals("csv")) {
      df = snc.read.format("com.databricks.spark.csv")
          .option("header", "false")
          .option("inferSchema", "false")
          .option("maxCharsPerColumn", "4096")
          .option("nullValue", "")
          .csv(s"${dataFileLocation}/${tableName}.csv")
      df.write.parquet(s"${parquetFileLoc}/${tableName}")
    } else if (fromType.equals("table")) {
      df = snc.sql(s"select * from ${tableName}")
      df.write.parquet(s"${parquetFileLoc}/${tableName}")
      df.write.csv(s"${csvFileLoc}/$tableName")
    }
    pw.flush()
  }

  /**
    * Validate if the data files are available, else throw SparkJobInvalid
    *
    */
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

}