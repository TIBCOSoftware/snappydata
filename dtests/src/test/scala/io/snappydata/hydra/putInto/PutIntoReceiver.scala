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

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.streaming.{ProcessingTime, SchemaDStream, SnappyStreamingJob}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SnappyJobValid, SnappyJobValidation}
import org.apache.spark.streaming.SnappyStreamingContext

class PutIntoReceiver extends SnappyStreamingJob {

  override def runSnappyJob(snsc: SnappyStreamingContext, jobConfig: Config): Any = {
    val tid: Int = jobConfig.getString("tid").toInt
    var brokerList: String = jobConfig.getString("brokerList")
    brokerList = brokerList.replace("--", ":")
    val kafkaTopic: String = jobConfig.getString("kafkaTopic")
    val tableName: String = jobConfig.getString("tableName")

    val checkpointDirectory: String = (new File("..")).getCanonicalPath +
        File.separator + "checkpointDirectory_" + tableName + tid
    val outputFile = "KafkaStreamingJob_output" + tid + "_" + System.currentTimeMillis() + ".txt"
    val pw = new PrintWriter(new FileOutputStream(new File(outputFile), true));
    // scalastyle:off println
    pw.println("Starting stream query...")
    pw.flush()
    import snsc.snappySession.implicits._

    createAndStartStreamingQuery(kafkaTopic, tid, true)

    pw.println("started streaming query")
    pw.flush()

    def createAndStartStreamingQuery(topic: String, testId: Int,
        withEventTypeColumn: Boolean = true, failBatch: Boolean = false) = {

      val session = snsc.snappySession
      val streamingDF = session
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", brokerList)
          .option("subscribe", topic)
          .option("startingOffsets", "earliest")
          .load()

      def structFields() = {
        StructField("id", StringType, nullable = false) ::
            StructField("data1", StringType) ::
            StructField("data2", DoubleType) ::
            StructField("APPLICATION_ID", StringType, nullable = false) ::
            StructField("ORDERGROUPID", StringType) ::
            StructField("PAYMENTADDRESS1", StringType) ::
            StructField("PAYMENTADDRESS2", StringType) ::
            StructField("PAYMENTCOUNTRY", StringType) ::
            StructField("PAYMENTSTATUS", StringType) ::
            StructField("PAYMENTRESULT", StringType) ::
            StructField("PAYMENTZIP", StringType) ::
            StructField("PAYMENTSETUP", StringType) ::
            StructField("PROVIDER_RESPONSE_DETAILS", StringType) ::
            StructField("PAYMENTAMOUNT", StringType) ::
            StructField("PAYMENTCHANNEL", StringType) ::
            StructField("PAYMENTCITY", StringType) ::
            StructField("PAYMENTSTATECODE", StringType) ::
            StructField("PAYMENTSETDOWN", StringType) ::
            StructField("PAYMENTREFNUMBER", StringType) ::
            StructField("PAYMENTST", StringType) ::
            StructField("PAYMENTAUTHCODE", StringType) ::
            StructField("PAYMENTID", StringType) ::
            StructField("PAYMENTMERCHID", StringType) ::
            StructField("PAYMENTHOSTRESPONSECODE", StringType) ::
            StructField("PAYMENTNAME", StringType) ::
            StructField("PAYMENTOUTLETID", StringType) ::
            StructField("PAYMENTTRANSTYPE", StringType) ::
            StructField("PAYMENTDATE", StringType) ::
            StructField("CLIENT_ID", StringType) ::
            StructField("CUSTOMERID", StringType) ::
            StructField("_eventType", IntegerType, nullable = false) :: Nil
      }

      val schema = StructType(structFields())
      implicit val encoder = RowEncoder(schema)

        streamingDF.selectExpr("CAST(value AS STRING)")
            .as[String]
            .map(_.split(","))
            .map(r => {
                Row(r(0), r(1), r(2).toDouble, r(3), r(4), r(5), r(6), r(7), r(8),
                  r(9), r(10), r(11), r(12), r(13), r(14), r(15), r(16), r(17),
                  r(18), r(19), r(20), r(21), r(22), r(23), r(24), r(25), r(26),
                  r(27), r(28), r(29), r(30).toInt)
            })
            .writeStream
            .format("snappysink")
            .queryName(s"USERS_$testId")
            .trigger(ProcessingTime("1 seconds"))
            .option("tableName", tableName)
            .option("streamQueryId", "Query_" + tableName)
            .option("checkpointLocation", checkpointDirectory).start

    }
  }

  override def isValidJob(snsc: SnappyStreamingContext, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }
}


