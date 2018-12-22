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
package io.snappydata.hydra

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}


object AirlineCleanedParquetDataJob extends SnappySQLJob {
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val airlineParquetTable = "STAGING_AIRLINE"
    val airlineRefParquetTable = "STAGING_AIRLINEREF"
    val snc = snSession.sqlContext

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    // scalastyle:off println
    val pw = new PrintWriter(new FileOutputStream(new File(jobConfig.getString("logFileName")),
      true))
    val airlineParquetDir = jobConfig.getString("airlineParquetDir")
    val airlineRefParquetDir = jobConfig.getString("airlineRefParquetDir")
    Try {
      // Get the already created tables
      val airlineParquetDF: DataFrame = snc.table(airlineParquetTable)
        val airlineRefParquetDF: DataFrame = snc.table(airlineRefParquetTable)

      airlineParquetDF.write.parquet(airlineParquetDir)
      airlineRefParquetDF.write.parquet(airlineRefParquetDir)
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/${jobConfig.getString("logFileName")}"
      case Failure(e) => pw.close();
        throw e;
    }

  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}
