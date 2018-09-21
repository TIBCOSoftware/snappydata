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

package io.snappydata.hydra.slackIssues

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}

class CreateTablesSlackIssue extends SnappySQLJob {

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val outputFile = "CreateTableJob_" + jobConfig.getString("logFileName")
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    Try {
      snappySession.sql("create table if not exists users(user_id INTEGER NOT NULL, " +
          "product_id INTEGER NOT NULL, " +
          "name VARCHAR(120) NOT NULL, " +
          "name_b VARCHAR(150) NOT NULL, " +
          "social_id INTEGER NOT NULL, " +
          "property_id INTEGER NOT NULL, " +
          "address VARCHAR(350) NOT NULL, " +
          "channel_id INTEGER NOT NULL, " +
          "network_id INTEGER NOT NULL " +
          ") " +
          "USING column " +
          "OPTIONS ( " +
          // " COLOCATE_WITH 'products', " +
          " PARTITION_BY 'product_id', " +
          " BUCKETS  '180' )")
      pw.close()

    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/${outputFile}"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }

}
