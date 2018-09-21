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
import org.apache.commons.lang.RandomStringUtils

import org.apache.spark.SparkContext
import org.apache.spark.sql._
import scala.util.{Failure, Random, Success, Try}

case class users(USER_ID: BigInt, PRODUCT_ID: BigInt, Name: String, NAME_B: String, SOCIAL_ID: Integer,
                 PROPERTY_ID: Integer, ADDRESS: String, CHANNEL_ID: Integer, NETWORK_ID: Integer)

class LoadDataSlackTablesJob extends SnappySQLJob {

  override def runSnappyJob(snappySession: SnappySession, jobConfig: Config): Any = {
    val snc = snappySession.sqlContext

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    val outputFile = "LoadDataAndValidateQueriesJob_" + jobConfig.getString("logFileName")
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    val sc = SparkContext.getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sc)
    Try {
      val numRows = jobConfig.getString("numRows").toLong
      val performDMLOPs = jobConfig.getString("performDMLOps").toBoolean
      val numTimesToExecute = jobConfig.getString("numTimesToExecute").toInt

      import snappySession.implicits._

      val sc = snappySession.sparkContext
      var idNum: Long = 0
      var rows = numRows
      val dataRDD = sc.range(idNum, rows).mapPartitions { itr =>
        val rnd = new Random()
        itr.map { id =>
          users((id + 1).toInt, (id + 1).toInt, RandomStringUtils.random(30, true, false),
            RandomStringUtils.random(30, true, false),
            rnd.nextInt(Integer.MAX_VALUE), rnd.nextInt(Integer.MAX_VALUE),
            RandomStringUtils.random(30, true, false), rnd.nextInt(Integer.MAX_VALUE),
            rnd.nextInt(Integer.MAX_VALUE))
        }
      }
      val qDF = snappySession.createDataset(dataRDD)
      val cacheDF = qDF.cache();
      cacheDF.write.insertInto("users")

      if (performDMLOPs) {
        for (i <- 1 to numTimesToExecute) {
          idNum = numRows * i + 1
          rows = numRows
          val qDF1 = snappySession.createDataset(dataRDD)
          val cacheDF1 = qDF1.cache();
          cacheDF1.write.insertInto("users")
        }
      }
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

