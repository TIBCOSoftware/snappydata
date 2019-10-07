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
package io.snappydata.hydra

import com.typesafe.config.Config
import org.apache.spark.sql.{SnappySQLJob, _}
import spark.jobserver.{SparkJobValid, SparkJobValidation}

class LoadCSVDataJob extends SnappySQLJob {
  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {

    val snContext = snappySession.sqlContext

    def createTable(tableName: String, mode: String): Unit = {
      val dataLocation = jobConfig.getString("dataLocation")
      val path = s"$dataLocation/${tableName}.dat"
      val colTblDataFrame = snContext.read
          .format("com.databricks.spark.csv") // CSV to DF package
          .option("header", "true") // Use first line of all files as header
          .load(path)
      // scalastyle:off println
      println(s" Loading table $tableName from $path")

      colTblDataFrame.write.format(mode).mode(SaveMode.Append).saveAsTable(tableName)

      println(s" Loaded table $tableName from $path")
    }

    val columnTableList: String = jobConfig.getString("columnTableList")
    val rowTableList: String = jobConfig.getString("rowTableList")

    val columnTables = Map(
      0 -> "MVP_HC__MEMBER",
      1 -> "MVP_HC__PROFESSIONALCLAIM",
      2 -> "MVP_HC__PHYSICIAN")
    val rowTables = Map(
      0 -> "MVP_HC__MEMBERMEMBERSHIP",
      1 -> "MVP_HC__OTHERSUBSCRIBERINFO4CLAIM",
      2 -> "RDF_TYPES",
      3 -> "RDV_ORG__ADDRESS",
      4 -> "RDV_ORG__BUSINESSPHONE",
      5 -> "RDV_ORG__EMAILADDRESS",
      6 -> "RDV_ORG__HOMEPHONE",
      7 -> "RDV_ORG__LOCATION",
      8 -> "RDV_ORG__MAILINGLOCATION",
      9 -> "RDV_ORG__MAINLOCATION",
      10 -> "RDV_ORG__ORGANIZATION",
      11 -> "RDV_ORG__PHONE")

    val columnIndexes = columnTableList.split("-")
    val rowIndexes = rowTableList.split("-")

    columnIndexes.foreach(index => createTable(columnTables.get(index.toInt).get, "column"))
    rowIndexes.foreach(index => createTable(rowTables.get(index.toInt).get, "row"))
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }
}