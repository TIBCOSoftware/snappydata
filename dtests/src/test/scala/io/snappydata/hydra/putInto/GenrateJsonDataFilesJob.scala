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

package io.snappydata.hydra.putInto

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import org.apache.commons.lang.RandomStringUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql._

import scala.util.{Failure, Success, Try}

case class Test_Table(ID: String, data: String, data2: BigDecimal, APPLICATION_ID: String,
                      ORDERGROUPID: String, PAYMENTADDRESS1: String, PAYMENTADDRESS2: String, PAYMENTCOUNTRY: String,
                      PAYMENTSTATUS: String, PAYMENTRESULT: String, PAYMENTZIP: String, PAYMENTSETUP: String,
                      PROVIDER_RESPONSE_DETAILS: String, PAYMENTAMOUNT: String, PAYMENTCHANNEL: String,
                      PAYMENTCITY: String, PAYMENTSTATECODE: String, PAYMENTSETDOWN: String, PAYMENTREFNUMBER: String,
                      PAYMENTST: String, PAYMENTAUTHCODE: String, PAYMENTID: String, PAYMENTMERCHID: String,
                      PAYMENTHOSTRESPONSECODE: String, PAYMENTNAME: String, PAYMENTOUTLETID: String,
                      PAYMENTTRANSTYPE: String, PAYMENTDATE: String, CLIENT_ID: String, CUSTOMERID: String)

object GenerateJsonDataFilesJob extends SnappySQLJob {

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    val snc = snappySession.sqlContext

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val outputFile = "LoadDataAndValidateQueriesJob_" + jobConfig.getString("logFileName")
    val fileCnt = jobConfig.getString("fileCount").toInt
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    val sc = SparkContext.getOrCreate()
    Try {
      var numRows = jobConfig.getString("numRows").toLong
      var idNum = jobConfig.getString("idNum").toLong
      import snappySession.implicits._
      val range = numRows - idNum
      val sc = snappySession.sparkContext
        for (j <- 1 to fileCnt) {
        val dataRDD = sc.range(idNum, numRows).mapPartitions { itr =>
          itr.map { id =>

            Test_Table((id + 1).toString, RandomStringUtils.random(30, true, false), id * 10.2,
              (id + 1).toString, RandomStringUtils.random(30, true, false),
              RandomStringUtils.random(30, true, false), RandomStringUtils.random(30, true, false),
              RandomStringUtils.random(30, true, false), RandomStringUtils.random(30, true, false),
              RandomStringUtils.random(30, true, false),
              RandomStringUtils.random(30, true, false), RandomStringUtils.random(30, true, false),
              RandomStringUtils.random(30, true, false), RandomStringUtils.random(30, true, false),
              RandomStringUtils.random(30, true, false), RandomStringUtils.random(30, true, false),
              RandomStringUtils.random(30, true, false), RandomStringUtils.random(30, true, false),
              RandomStringUtils.random(30, true, false), RandomStringUtils.random(30, true, false),
              "", "", "", "", "", "", "", "", "", "")
          }
        }
        var qDF = snappySession.createDataset(dataRDD)
        var cacheDF = qDF.cache()
        cacheDF.write.insertInto("testL")
        cacheDF.repartition(1).write.json("/export/shared/QA_DATA/kr/jsonFilesToInsert/" +
            range + "/" + range + "_" + j)
        pw.flush()
        var oldID = idNum
        idNum = numRows + 1
        numRows = numRows + numRows - oldID
        // scalastyle:off println
        println("idNum: " + idNum + " numRows: " +
            numRows + " filecnt = " + j)
        pw.flush()
      }
      pw.close()


    } match {
      case Success(v) =>
        s"success"
      case Failure(e) =>
        throw e;
    }
  }

  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }

}