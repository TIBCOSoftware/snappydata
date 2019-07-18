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
package io.snappydata.hydra.preparedStmt

import java.io.PrintWriter

import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

object PopulateDataJob extends SnappySQLJob {
  override def runSnappyJob(snc: SnappySession, jobConfig: Config): Any = {
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val tables = Seq("catalog_page", "catalog_returns", "customer", "customer_address",
      "customer_demographics", "date_dim", "household_demographics", "inventory", "item",
      "promotion", "store", "store_returns", "catalog_sales", "web_sales", "store_sales",
      "web_returns", "web_site", "reason", "call_center", "warehouse", "ship_mode", "income_band",
      "time_dim", "web_page")

    val dataLocation = jobConfig.getString("dataFilesLocation")
    // "/export/shared/QA_DATA/TPCDS/data"
    val pw = new PrintWriter("PopulateDataJob.out")
    Try {
      tables.map { tableName =>
        val df = snc.read.parquet(s"$dataLocation/$tableName")
        val props = Map("BUCKETS" -> "7")
        snc.createTable(tableName, "column",
          new StructType(df.schema.map(_.copy(nullable = false)).toArray), props)
        df.write.insertInto(tableName)
        // scalastyle:off println
        pw.println("Table Created..." + tableName)
        tableName -> snc.table(tableName).count()
      }.toMap
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/PopulateDataJob.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation
  = SnappyJobValid()

}

