/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

package io.snappydata.hydra.dataExtractorTool

import com.typesafe.config.Config
import org.apache.spark.sql._
import org.apache.spark.sql.collection.Utils

object CreateAndLoadTablesDE extends SnappySQLJob {
  // scalastyle:off println
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val ddlPath = jobConfig.getString("extractedDDLPath")
    val dataPath = jobConfig.getString("extractedDataPath")
    val tableQry = "SELECT TABLENAME FROM SYS.SYSTABLES WHERE TABLESCHEMANAME = 'APP' " +
      "AND TABLENAME NOT LIKE 'SNAPPYSYS_INTERNA%'"
    val snc = snSession.sqlContext
    val tableList = snc.sql(tableQry).select("tablename").collect() // AsList()
    // tableDF.show()
    var dataDF: DataFrame = null

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    // println("SP: The total number of tables are " + tableList.size())

    tableList.foreach { r => val actualDataPath = dataPath + "_*" + "/APP." + r.getString(0)
       println("SP: the actualPath is " + actualDataPath)
      if (dataPath.contains("parquet")) {
        println("SP: dataFile path is that of parquet.")
        dataDF = snc.read.load(actualDataPath)
      }
      else {
        dataDF = snc.read.format("com.databricks.spark.csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("maxCharsPerColumn", "4096")
          .load(actualDataPath + "/part-*.csv")
      }

      dataDF.write.mode(SaveMode.Overwrite).saveAsTable(r.getString(0))
    }
/*   for (i <- 0 until tableList.length)
    {
      println("SP: the table name is " +  tableList.)
      val actualDataPath = dataPath + "_*" + "/APP." +  tableList.get(i).toString
      println("SP: the actualPath is " + actualDataPath)
     if (dataPath.contains("parquet")) {
        dataDF = snc.read.load(dataPath + "_*" + "/APP.")
      }
      else {
        dataDF = snc.read.format("com.databricks.spark.csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .option("maxCharsPerColumn", "4096")
          .load(dataPath)
      }

    dataDF.write.mode(SaveMode.Overwrite).saveAsTable("")
   } */
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}

