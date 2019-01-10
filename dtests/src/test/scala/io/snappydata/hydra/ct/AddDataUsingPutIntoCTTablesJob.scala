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
package io.snappydata.hydra.ct

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}

import scala.util.{Failure, Success, Try}

class AddDataUsingPutIntoCTTablesJob extends SnappySQLJob {

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val pw = new PrintWriter(new FileOutputStream(new File("AddDataUsingPutIntoCTTablesJob.out"), true));
    Try {
      val snc = snSession.sqlContext
      val dataFilesLocation = jobConfig.getString("dataFilesLocation")
      pw.println(s"Data files are at : ${dataFilesLocation}")
      snc.setConf("dataFilesLocation", dataFilesLocation)
      CTQueries.snc = snc
      pw.println(s"AddDataUsingPutIntoCTTablesJob has started...")
      pw.flush()
      CTTestUtil.addDataUsingPutInto(snc);
      println(s"AddDataUsingPutIntoCTTablesJob tables has completed successfully. " +
          s"See ${CTTestUtil.getCurrentDirectory}/AddDataUsingPutIntoCTTablesJob.out")
      pw.println(s"AddDataUsingPutIntoCTTablesJob has completed successfully")
      pw.close()
    } match {
      case Success(v) => pw.close()
        s"See ${CTTestUtil.getCurrentDirectory}/AddDataUsingPutIntoCTTablesJob.out"
      case Failure(e) => pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = SnappyJobValid()
}