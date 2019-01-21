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

package io.snappydata.benchmark.LoadPerformance

import java.io.{File, PrintWriter}

import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql._

/**
 * Created by kishor on 29/8/16.
 */
object ParquetLoad extends  SnappySQLJob{

  var parquetFilePath: String = _
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {

    val snc = snSession.sqlContext
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    val pw = new PrintWriter("ParquetLoadPerformance.out")
    Try {

      // Drop tables if already exists
      snc.dropTable("AIRLINE_SNAPPY_ROW", ifExists = true)
      snc.dropTable("AIRLINE_SNAPPY_COLUMN", ifExists = true)

      pw.println(s"****** ParquetLoadPerformance ******")

      //Read Parquet File
      val parquetFile = snc.read.parquet(parquetFilePath)
      val updatedSchema = replaceReservedWords(parquetFile.schema)

      pw.println(s"Row Count : " + parquetFile.count())

      pw.println("loading spark table" )
      var start = System.currentTimeMillis()
      parquetFile.registerTempTable("AIRLINE_SPARK")
      parquetFile.cache()
      parquetFile.count()
      var end = System.currentTimeMillis()
      val sparkTableLoadTime = end-start
      pw.println(s"Time taken to load spark table : $sparkTableLoadTime ")

      //Create Snappy tables
      //row table
      snc.createTable("AIRLINE_SNAPPY_ROW", "row",
        updatedSchema, Map.empty[String, String])

      pw.println( "Created table AIRLINE_SNAPPY_ROW " )

      //column table
      snc.createTable("AIRLINE_SNAPPY_COLUMN", "column",
        updatedSchema, Map("buckets" -> "16"))

      pw.println( "Created table AIRLINE_SNAPPY_COLUMN " )


      // Save Parquet to snappy table
      pw.println("loading row table" )
      start = System.currentTimeMillis()
      parquetFile.write.format("row").mode(SaveMode.Append).saveAsTable("AIRLINE_SNAPPY_ROW")
      //airlineDF.write.format("row").mode(SaveMode.Append).saveAsTable("AIRLINE_SNAPPY_ROW")
      end = System.currentTimeMillis()
      val snappyRowTableLoadTime = end-start
      pw.println(s"Time taken to load row table : $snappyRowTableLoadTime" )


      pw.println("loading column table" )
      start = System.currentTimeMillis()
      parquetFile.write.format("column").mode(SaveMode.Append).saveAsTable("AIRLINE_SNAPPY_COLUMN")
      //airlineDF.write.format("column").mode(SaveMode.Append).saveAsTable("AIRLINE_SNAPPY_COLUMN")
      end = System.currentTimeMillis()
      val snappyColumnTableLoadTime = end-start
      pw.println(s"Time taken to load column table : $snappyColumnTableLoadTime")

      val slownessRowTable = ((snappyRowTableLoadTime - sparkTableLoadTime) / sparkTableLoadTime) * 100
      val slownessColumnTable = ((snappyColumnTableLoadTime - sparkTableLoadTime) / sparkTableLoadTime) * 100

      pw. println(s"Parquet loading to row table compare to spark table is slow by $slownessRowTable %")
      pw. println(s"Parquet loading to column table compare to spark table is slow by $slownessColumnTable %")

    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/CreateAndLoadAirlineDataJob.out"
      case Failure(e) => pw.close();
        throw e;
    }
    // scalastyle:on println
  }

  /**
   * Validate if the data files are available, else throw SparkJobInvalid
   *
   */
  override def isValidJob(snSession: SnappySession, config: Config): SnappyJobValidation = {
    parquetFilePath = if (config.hasPath("airline_file")) {
      config.getString("airline_file")
    } else {
      "../../../../../../../../examples/quickstart/data/airlineParquetData"
    }

    if (!(new File(parquetFilePath)).exists()) {
      return new SnappyJobInvalid("Incorrect airline path. " +
          "Specify airline_file property in APP_PROPS")
    }

    SnappyJobValid()
  }

  /**
   * Replace the words that are reserved in Snappy store
   * @param airlineSchema schema with reserved words
   * @return updated schema
   */
  private def replaceReservedWords(airlineSchema : StructType) : StructType = {
    new StructType( airlineSchema.map( s => {
      if (s.name.equals("Year")) {
        new StructField("Year_", s.dataType, s.nullable, s.metadata)
      }
      else if (s.name.equals("Month")) {
        new StructField("Month_", s.dataType, s.nullable, s.metadata)
      }
      else {
        s
      }}).toArray)
  }
}
