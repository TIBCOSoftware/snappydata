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
package org.apache.spark.sql.streaming

import scala.collection.mutable
import scala.language.postfixOps

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

/**
  * Inherit all tests of SnappyStreamingSuite to run with snappy-spark
  * (instead of stock spark that SnappyStreamingSuite is run with).
  */
class ClusterSnappyStreamingSuite
    extends SnappyStreamingSuite {

  /** same test in core does not test for dynamic CQ registration */
  test("stream ad-hoc sql with dynamic CQ") {
    ssnc.sql("create stream table tweetsTable " +
        "(id long, text string, fullName string, " +
        "country string, retweets int, hashtag string) " +
        "using twitter_stream options (" +
        "consumerKey '0Xo8rg3W0SOiqu14HZYeyFPZi', " +
        "consumerSecret 'gieTDrdzFS4b1g9mcvyyyadOkKoHqbVQALoxfZ19eHJzV9CpLR', " +
        "accessToken '43324358-0KiFugPFlZNfYfib5b6Ah7c2NdHs1524v7LM2qaUq', " +
        "accessTokenSecret 'aB1AXHaRiE3g2d7tLgyASdgIg9J7CzbPKBkNfvK8Y88bu', " +
        "rowConverter 'org.apache.spark.sql.streaming.TweetToRowsConverter')")

    val cqResult = ssnc.registerCQ("SELECT text FROM tweetsTable " +
        "window (duration 10 seconds, slide 10 seconds) where text like '%e%'")
    cqResult.foreachDataFrame(df => df.count())

    ssnc.sql("STREAMING START")

    val dynamicCQResult = ssnc.registerCQ("SELECT text, fullName FROM tweetsTable " +
        "window (duration 4 seconds, slide 4 seconds) where text like '%e%'")
    dynamicCQResult.foreachDataFrame(df => df.count())

    for (a <- 1 to 5) {
      Thread.sleep(2000)
      ssnc.sql("select text, fullName from tweetsTable where text like '%e%'").count()
    }
    ssnc.sql("drop table tweetsTable")
    ssnc.awaitTerminationOrTimeout(10 * 1000)
  }

  test("dynamic CQ") {

    def getQueueOfRDDs1: mutable.Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(1 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(11 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(21 to 30).map(i => Tweet(i, s"Text$i"))
      mutable.Queue(distData1, distData2, distData3)
    }
    val dStream1 = ssnc.queueStream[Tweet](getQueueOfRDDs1)

    val schemaStream1 = ssnc.createSchemaDStream(dStream1)
    schemaStream1.foreachDataFrame(df => {
      df.count()
    })
    schemaStream1.registerAsTable("tweetStream1")

    def getQueueOfRDDs2: mutable.Queue[RDD[Tweet]] = {
      val distData1: RDD[Tweet] = sc.parallelize(9 to 10).map(i => Tweet(i, s"Text$i"))
      val distData2: RDD[Tweet] = sc.parallelize(19 to 20).map(i => Tweet(i, s"Text$i"))
      val distData3: RDD[Tweet] = sc.parallelize(29 to 30).map(i => Tweet(i, s"Text$i"))
      mutable.Queue(distData1, distData2, distData3)
    }

    val dStream2 = ssnc.queueStream[Tweet](getQueueOfRDDs2)

    val schemaStream2 = ssnc.createSchemaDStream(dStream2)
    schemaStream2.foreachDataFrame(df => {
      df.count()
    })
    schemaStream2.registerAsTable("tweetStream2")

    val resultStream: SchemaDStream = ssnc.registerCQ("SELECT t1.id, t1.text" +
        " FROM tweetStream1 window (duration 2 seconds, slide 2 seconds)" +
        "t1 JOIN tweetStream2 t2 ON t1.id = t2.id ")

    ssnc.snappyContext.dropTable("gemColumnTable", ifExists = true)
    ssnc.snappyContext.createTable("gemColumnTable", "column", schemaStream1.schema,
      Map.empty[String, String])

    resultStream.foreachDataFrame(df => {
      df.write.format("column").mode(SaveMode.Append).options(Map.empty[String, String])
          .saveAsTable("gemColumnTable")
    })

    val df = ssnc.snappyContext.createDataFrame(
      sc.parallelize(1 to 10).map(i => Tweet(i / 2, s"Text${i / 2}")))
    df.createOrReplaceTempView("tweetTable")

    ssnc.start()

    val resultSet = ssnc.registerCQ("SELECT t2.id, t2.text FROM tweetStream1 window" +
        " (duration 4 seconds, slide 4 seconds) " +
        "t1 JOIN tweetTable t2 ON t1.id = t2.id")

    resultSet.foreachDataFrame(df => {
      df.write.format("column").mode(SaveMode.Append).options(Map.empty[String, String])
          .saveAsTable("gemColumnTable")
    })
    ssnc.awaitTerminationOrTimeout(20 * 1000)

    val result = ssnc.sql("select * from gemColumnTable")
    val r = result.collect()
    assert(r.length > 0)
    ssnc.sql("drop table gemColumnTable")
  }
}
