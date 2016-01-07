/*
 * Copyright (c) 2010-2016 SnappyData, Inc. All rights reserved.
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
package io.snappydata.app.streaming.kafka

/**
 * Created by ymahajan on 28/10/15.
 */

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.streaming.SnappyStreamingContext
import org.apache.spark.sql.{Row, SaveMode, SnappyContext}
import org.apache.spark.streaming._


object KafkaConsumer {

  def main(args: Array[String]) {
    val sparkConf = new org.apache.spark.SparkConf()
    .setAppName("kafkaconsumer")
    // .set("snappydata.store.locators", "localhost:10101")
    .setMaster("local[2]")
    // .set("snappydata.embedded", "true")

    val sc = new SparkContext(sparkConf)
    val ssnc = SnappyStreamingContext(SnappyContext.getOrCreate(sc), Milliseconds(30000))


    val urlString = "jdbc:snappydata:;locators=localhost:10101;persist-dd=false;" +
      "member-timeout=600000;jmx-manager-start=false;enable-time-statistics=false;" +
      "statistic-sampling-enabled=false"

    val props = Map(
      "url" -> urlString,
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "poolImpl" -> "tomcat",
      "poolProperties" -> "maxActive=300",
      "user" -> "app",
      "password" -> "app"
    )

    ssnc.sql("create stream table tweetstreamtable (id long, text string, fullName string, " +
      "country string, retweets int, hashtag string) " +
      "using kafka_stream options " +
      "(storagelevel 'MEMORY_AND_DISK_SER_2', " +
      "streamToRow 'io.snappydata.app.streaming.KafkaMessageToRowConverter' ," +
      // " kafkaParams 'metadata.broker.list->rdu-w28:9092,rdu-w29:9092,
      // rdu-w30:9092,rdu-w31:9092,rdu-w32:9092'," +
      " kafkaParams 'metadata.broker.list->localhost:9092'," +
      " topics 'tweetstream')")


    val tableStream = ssnc.getSchemaDStream("tweetstreamtable")


    ssnc.snappyContext.registerSampleTable("tweetstreamtable_sampled", tableStream.schema, Map(
      "qcs" -> "hashtag",
      "fraction" -> "0.05",
      "strataReservoirSize" -> "300",
      "timeInterval" -> "3m"), Some("tweetstreamtable"))

    tableStream.saveStream(Seq("tweetstreamtable_sampled"))

    var numTimes = 0
    tableStream.foreachDataFrame { df =>
      println("Evaluating new batch") // scalastyle:ignore
      var start: Long = 0
      var end: Long = 0
      df.write.format("column").mode(SaveMode.Append).options(props)
        .saveAsTable("rawStreamColumnTable")

      println("Top 10 hash tags from exact table") // scalastyle:ignore
      start = System.nanoTime()

      val top10Tags = ssnc.sql("select count(*) as cnt, hashtag from rawStreamColumnTable " +
        "where length(hashtag) > 0 group by hashtag order by cnt desc limit 10").collect()
      end = System.nanoTime()
      top10Tags.foreach(println) // scalastyle:ignore
      println("\n\nTime taken: " + ((end - start) / 1000000L) + "ms") // scalastyle:ignore

      numTimes += 1
      if ((numTimes % 18) == 1) {
        ssnc.sql("SELECT count(*) FROM rawStreamColumnTable").show()
      }

      println("Top 10 hash tags from sample table") // scalastyle:ignore
      start = System.nanoTime()
      val stop10Tags = ssnc.sql("select count(*) as cnt, hashtag from tweetstreamtable_sampled " +
          "where length(hashtag) > 0 group by hashtag order by cnt desc limit 10").collect()
      end = System.nanoTime()
      stop10Tags.foreach(println) // scalastyle:ignore
      println("\n\nTime taken: " + ((end - start) / 1000000L) + "ms") // scalastyle:ignore

    }

/*
        val resultSet2: SchemaDStream = ssnc.registerCQ("SELECT * FROM tweetStreamTable
        window (duration '2' seconds, slide '2' seconds) where text like '%the%'")
        resultSet2.foreachRDD(rdd =>
          println(s"Received Twitter stream results. Count:" +
              s" ${if(!rdd.isEmpty()) {
                val df = ssnc.createDataFrame(rdd, resultSet2.schema)
                //SnappyContext.createTopKRDD()
                //df.show
                //df.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("kafkaExtTable")
                //ssnc.appendToCacheRDD(rdd, streamTable, resultSet.schema)
              }
              }")
        )
*/

    ssnc.sql("STREAMING START")

    ssnc.awaitTerminationOrTimeout(1800* 1000)
    ssnc.sql( "select count(*) from rawStreamColumnTable").show

    ssnc.sql("STREAMING STOP")
  }

}

