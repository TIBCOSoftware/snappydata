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
package io.snappydata.hydra.concurrency

import com.typesafe.config.Config
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

object ExportTable extends SnappySQLJob {
  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()

  override def runSnappyJob(sn: SnappySession, jobConfig: Config): Any = {
    val op_path = jobConfig.getString("output_path")
    val input_table = jobConfig.getString("input_table")
    val limit = jobConfig.getString("limit")
    val op_format = "csv"

    val tableDF = sn.sql(s"select * from $input_table limit $limit")

    op_format match {
      case "parquet" => tableDF.write.parquet(op_path)
      case "csv" => tableDF.write.format("com.databricks.spark.csv").save(op_path)
    }
  }
}
