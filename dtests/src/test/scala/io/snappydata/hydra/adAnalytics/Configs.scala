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

package io.snappydata.hydra.adAnalytics

import org.apache.spark.sql.types._
import org.apache.spark.streaming.Seconds

object Configs {

  val snappyMasterURL = "snappydata://localhost:10334"

  val sparkMasterURL = "spark://127.0.0.1:7077"

  val cassandraHost = "127.0.0.1"

  val snappyLocators = "localhost:10334"

  val maxRatePerPartition = 1000

  val kafkaTopic = "adImpressionsTopic"

  val brokerList = "localhost:9092"

  val kafkaParams: Map[String, String] = Map(
    "metadata.broker.list" -> brokerList
  )

  val hostname = "localhost"

  val socketPort = 9000

  val numPublishers = 50

  val numAdvertisers = 30

  val publishers = (0 to numPublishers).map("publisher" +)

  val advertisers = (0 to numAdvertisers).map("advertiser" +)

  val numProducerThreads = 1

  val UnknownGeo = "un"

  val geos = Seq("AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL",
    "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM",
    "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", "SD", "TN",
    "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", UnknownGeo)

  val numGeos = geos.size

  val numWebsites = 999

  val numCookies = 999

  val websites = (0 to numWebsites).map("website" +)

  val cookies = (0 to numCookies).map("cookie" +)

  val numLogsPerThread = 20000000

  val batchDuration = Seconds(1)

  val topics = Set(kafkaTopic)

  val maxLogsPerSecPerThread = 5000

  def getAdImpressionSchema: StructType = {
    StructType(Array(
      StructField("timestamp", TimestampType, true),
      StructField("publisher", StringType, true),
      StructField("advertiser", StringType, true),
      StructField("website", StringType, true),
      StructField("geo", StringType, true),
      StructField("bid", DoubleType, true),
      StructField("cookie", StringType, true)))
  }

}
