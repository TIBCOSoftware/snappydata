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
package io.snappydata.hydra.putInto

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import io.snappydata.hydra.northwind.NWTestJob
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

import scala.util.{Failure, Success, Try}

object LoadDataFromJson extends SnappySQLJob {
  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    // snSession.sql("set snappydata.cache.putIntoInnerJoinResultSize=10GB")
    val tableName = jobConfig.getString("tableName")
    val fromVal = jobConfig.getString("fromVal").toInt
    val untilVal = jobConfig.getString("untilVal").toInt
    val filePath = jobConfig.getString("jsonFile")
    val pw = new PrintWriter(new FileOutputStream(new File("ConcurrentPutIntoJob.out"), true))
    Try {
      loadDataFromJsonFile(snSession, filePath, tableName, fromVal, untilVal)
      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${NWTestJob.getCurrentDirectory}/ConcurrentPutIntoJob.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  def loadDataFromJsonFile(snSession: SnappySession, file: String, tableName: String,
                           fromVal: Int, untilVal: Int): Unit = {
    for (i <- fromVal to untilVal) {
      val jsonDF = snSession.read.json(file)
      jsonDF.write.putInto(tableName + i)
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}



