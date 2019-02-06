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
package io.snappydata.cluster

import java.io.{FileOutputStream, PrintWriter}

import com.pivotal.gemfirexd.Attribute
import com.typesafe.config.{Config, ConfigException}
import io.snappydata.{Constant, ServiceManager}
import io.snappydata.impl.LeadImpl

import org.apache.spark.SparkCallbacks
import org.apache.spark.sql.types.{DecimalType, IntegerType, StructField, StructType}
import org.apache.spark.sql._
import org.apache.spark.sql.hive.HiveClientUtil
import org.apache.spark.sql.streaming.SnappyStreamingJob
import org.apache.spark.streaming.SnappyStreamingContext
import org.apache.spark.ui.SnappyBasicAuthenticator

// scalastyle:off println
class RecoverModeJob extends SnappySQLJob {

  // Job config names
  val outputFile = "output.file"

  private var pw: PrintWriter = _
  def getCurrentDirectory: String = new java.io.File(".").getCanonicalPath

  override def runSnappyJob(snSession: SnappySession, jobConfig: Config): Any = {
    val file = jobConfig.getString(outputFile)
    val msg = s"\nCheck ${getCurrentDirectory}/$file file for output of this job"
    pw = new PrintWriter(new FileOutputStream(file), true)
    try {
      // scalastyle:off println
      println("inside runsnappyjob RecoverModeJob opopopopopop")

      val snExtHiveCat = HiveClientUtil
          .getOrCreateExternalCatalog(snSession.sparkContext, snSession.sparkContext.getConf)
      pw.println("--- >> *^^* here *^^* " + snExtHiveCat.getAllTables())
      pw.println("*^^* n here *^^* " +
          snExtHiveCat.hadoopConf.get("javax.jdo.option.ConnectionURL"))
      val someData = Seq(snExtHiveCat.getAllTables().toString())
      val someRdd = snSession.sparkContext.parallelize(someData, 2)
      someRdd.saveAsTextFile("/home/ppatil/tempfolder/temp_output_dir/out1_"
          + System.currentTimeMillis())

      pw.println(msg)
      println(msg)
      // scalastyle:on println

    } finally {
      pw.close()
    }
    msg
  }


  override def isValidJob(sc: SnappySession, config: Config): SnappyJobValidation = {
    //    SnappyStreamingSecureJob.verifySessionAndConfig(sc, config)
    SnappyJobValid()
  }

}
