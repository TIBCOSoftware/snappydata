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

package io.snappydata.hydra.ct

import java.io.{File, FileOutputStream, PrintWriter}
import scala.util.{Failure, Success, Try}

import com.typesafe.config.Config
import util.TestException

import org.apache.spark.SparkContext
import org.apache.spark.sql.{SQLContext, SnappyContext, SnappyJobValid, SnappyJobValidation, SnappySQLJob}

object ValidateCTQueriesJob extends SnappySQLJob {

  override def runSnappyJob(snc: SnappyContext, jobConfig: Config): Any = {
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    val outputFile = "ValidateCTQueries_" + jobConfig.getString("logFileName")
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    val tableType = jobConfig.getString("tableType")

    Try {
      snc.sql("set spark.sql.shuffle.partitions=23")
      val dataFilesLocation = jobConfig.getString("dataFilesLocation")
      snc.setConf("dataFilesLocation", dataFilesLocation)
      CTQueries.snc = snc
      pw.println(s"Validation for $tableType tables started in snappy Job")
      val fullResultSetValidation: Boolean = jobConfig.getString("fullResultSetValidation").toBoolean
      val sc = SparkContext.getOrCreate()
      val sqlContext = SQLContext.getOrCreate(sc)
      if (fullResultSetValidation)
        pw.println(s"Test will perform fullResultSetValidation")
      else
        pw.println(s"Test will not perform fullResultSetValidation")
      val hasValidationFailed = CTTestUtil.executeQueries(snc, tableType, pw, fullResultSetValidation, sqlContext)
      if(hasValidationFailed) {
        pw.println(s"Validation failed for ${tableType}")
        throw new TestException(s"Validation task failed for ${tableType}. Please check logs.")
      }
      pw.println(s"Validation for $tableType tables completed.")
      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/${outputFile}"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappyContext, config: Config): SnappyJobValidation = SnappyJobValid()
}
