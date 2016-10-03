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

package io.snappydata.examples

import java.io.{File, PrintWriter}

import com.typesafe.config.Config

import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{SnappyContext, SnappyJobInvalid, SnappyJobValid, SnappyJobValidation, SnappySQLJob}

/**
 * Creates and loads Airline data from parquet files in row and column
 * tables. Also samples the data and stores it in a column table.
 */
object CreateAndLoadAirlineDataJob extends SnappySQLJob {

  var airlinefilePath: String = _
  var airlinereftablefilePath: String = _
  val colTable = "AIRLINE"
  val rowTable = "AIRLINEREF"
  val sampleTable = "AIRLINE_SAMPLE"
  val stagingAirline = "STAGING_AIRLINE"

  override def runSnappyJob(snc: SnappyContext, jobConfig: Config): Any = {
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    val pw = new PrintWriter("CreateAndLoadAirlineDataJob.out")
      // scalastyle:off println

      // Drop tables if already exists
      snc.dropTable(sampleTable, ifExists = true)
      snc.dropTable(colTable, ifExists = true)
      snc.dropTable(rowTable, ifExists = true)
      snc.dropTable(stagingAirline, ifExists = true)

    // scalastyle:on println
  }

  /**
   * Validate if the data files are available, else throw SparkJobInvalid
   *
   */
  override def isValidJob(snc: SnappyContext, config: Config): SnappyJobValidation = {

    airlinefilePath = if (config.hasPath("airline_file")) {
      config.getString("airline_file")
    } else {
      "../../quickstart/data/airlineParquetData"
    }

    if (!(new File(airlinefilePath)).exists()) {
      return new SnappyJobInvalid("Incorrect airline path. " +
          "Specify airline_file property in APP_PROPS")
    }

    airlinereftablefilePath = if (config.hasPath("airlineref_file")) {
      config.getString("airlineref_file")
    } else {
      "../../quickstart/data/airportcodeParquetData"
    }
    if (!(new File(airlinereftablefilePath)).exists()) {
      return new SnappyJobInvalid("Incorrect airline ref path. " +
          "Specify airlineref_file property in APP_PROPS")
    }

    SnappyJobValid()
  }

  /**
   * Replace the words that are reserved in Snappy store
   * @param airlineSchema schema with reserved words
   * @return updated schema
   */
  private def replaceReservedWords(airlineSchema: StructType): StructType = {
    new StructType(airlineSchema.map(s => {
      if (s.name.equals("Year")) {
        new StructField("Year_", s.dataType, s.nullable, s.metadata)
      }
      else if (s.name.equals("Month")) {
        new StructField("Month_", s.dataType, s.nullable, s.metadata)
      }
      else {
        s
      }
    }).toArray)
  }
}
