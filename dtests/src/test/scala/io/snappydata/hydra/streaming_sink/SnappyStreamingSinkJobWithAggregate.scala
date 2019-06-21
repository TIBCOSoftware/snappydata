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

package io.snappydata.hydra.streaming_sink

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config

import org.apache.spark.sql.{SnappyJobValid, SnappyJobValidation, SnappySQLJob, SnappySession}


class SnappyStreamingSinkJobWithAggregate extends SnappySQLJob {

  override def runSnappyJob(snc: SnappySession, jobConfig: Config): Any = {

    val tid: Int = jobConfig.getString("tid").toInt
    var brokerList: String = jobConfig.getString("brokerList")
    brokerList = brokerList.replace("--", ":")
    val kafkaTopic: String = jobConfig.getString("kafkaTopic")
    val tableName: String = jobConfig.getString("tableName")
    val isConflationTest: Boolean = jobConfig.getBoolean("isConflation")
    val useCustomCallback: Boolean = false // jobConfig.getBoolean("useCustomCallback")
    val outputMode: String = jobConfig.getString("outputMode")
    val outputFile = "KafkaStreamingJob_output" + tid + "_" + System.currentTimeMillis() + ".txt"
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));

    // scalastyle:off println

    pw.println("Starting stream query...")
    pw.flush()

    StructuredStreamingTestUtil.createAndStartAggStreamingQuery(snc, tableName, brokerList,
      kafkaTopic, tid, pw, isConflationTest, true, useCustomCallback, outputMode)

    pw.println("started streaming query")
    pw.flush()

  }

  override def isValidJob(snsc: SnappySession, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }
}

