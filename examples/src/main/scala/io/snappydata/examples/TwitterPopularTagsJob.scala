/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

package io.snappydata.examples

import java.io.PrintWriter

import com.typesafe.config.Config

import org.apache.spark.sql.streaming.{SchemaDStream, SnappyStreamingJob}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SnappyJobValid, SnappyJobValidation}
import org.apache.spark.streaming.SnappyStreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * Run this on your local machine:
 * <p/>
 * `$ sbin/snappy-start-all.sh`
 * <p/>
 * To run with live twitter streaming, export twitter credentials
 *
 * `$ export APP_PROPS="consumerKey=<consumerKey>,consumerSecret=<consumerSecret>, \
 * accessToken=<accessToken>,accessTokenSecret=<accessTokenSecret>"`
 *
 *  <p/>
 * `$ ./bin/snappy-job.sh submit --lead localhost:8090 \
 * --app-name TwitterPopularTagsJob --class io.snappydata.examples.TwitterPopularTagsJob \
 * --app-jar $SNAPPY_HOME/examples/jars/quickstart.jar --stream`
 * <p/>
 * To run with stored twitter data, run simulateTwitterStream after the Job is submitted:
 *
 * `$ ./quickstart/scripts/simulateTwitterStream`
 */

object TwitterPopularTagsJob extends SnappyStreamingJob {

  override def runSnappyJob(snsc: C, jobConfig: Config): Any = {

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    // scalastyle:off println
    var stream: DStream[_] = null
    var outFileName = s"TwitterPopularTagsJob-${System.currentTimeMillis}.out"
    val pw = new PrintWriter(outFileName)

    val schema = StructType(List(StructField("hashtag", StringType)))

    snsc.snappyContext.sql("DROP TABLE IF EXISTS topktable")
    snsc.snappyContext.sql("DROP TABLE IF EXISTS hashtagtable")
    snsc.snappyContext.sql("DROP TABLE IF EXISTS retweettable")

    if (jobConfig.hasPath("consumerKey") && jobConfig.hasPath("consumerKey")
        && jobConfig.hasPath("accessToken") && jobConfig.hasPath("accessTokenSecret")) {
      pw.println("##### Running example with live twitter stream #####")

      // Create twitter stream table
      snsc.sql("CREATE STREAM TABLE hashtagtable (hashtag STRING) USING " +
          "twitter_stream OPTIONS (" +
          s"consumerKey '${jobConfig.getString("consumerKey")}', " +
          s"consumerSecret '${jobConfig.getString("consumerSecret")}', " +
          s"accessToken '${jobConfig.getString("accessToken")}', " +
          s"accessTokenSecret '${jobConfig.getString("accessTokenSecret")}', " +
          "rowConverter 'org.apache.spark.sql.streaming.TweetToHashtagRow')")

      snsc.sql("CREATE STREAM TABLE retweettable (retweetId LONG, retweetCnt INT, " +
          "retweetTxt STRING) USING twitter_stream OPTIONS (" +
          s"consumerKey '${jobConfig.getString("consumerKey")}', " +
          s"consumerSecret '${jobConfig.getString("consumerSecret")}', " +
          s"accessToken '${jobConfig.getString("accessToken")}', " +
          s"accessTokenSecret '${jobConfig.getString("accessTokenSecret")}', " +
          "rowConverter 'org.apache.spark.sql.streaming.TweetToRetweetRow')")


    } else {
      // Create file stream table
      pw.println("##### Running example with stored tweet data #####")
      snsc.sql("CREATE STREAM TABLE hashtagtable (hashtag STRING) USING file_stream " +
          "OPTIONS (storagelevel 'MEMORY_AND_DISK_SER_2', " +
          "rowConverter 'org.apache.spark.sql.streaming.TweetToHashtagRow'," +
          "directory '/tmp/copiedtwitterdata')")

      snsc.sql("CREATE STREAM TABLE retweettable (retweetId LONG, retweetCnt INT, " +
          "retweetTxt STRING) USING file_stream " +
          "OPTIONS (storagelevel 'MEMORY_AND_DISK_SER_2', " +
          "rowConverter 'org.apache.spark.sql.streaming.TweetToRetweetRow'," +
          "directory '/tmp/copiedtwitterdata')")

    }

    // Register continuous queries on the tables and specify window clauses
    val retweetStream: SchemaDStream = snsc.registerCQ("SELECT * FROM retweettable " +
        "WINDOW (DURATION 2 SECONDS, SLIDE 2 SECONDS)")

    val topKOption = Map(
      "epoch" -> System.currentTimeMillis().toString,
      "timeInterval" -> "2000ms",
      "size" -> "10"
    )

    // Create TopK table on the base stream table which is hashtagtable
    // TopK object is automatically populated from the stream table
    snsc.snappyContext.createApproxTSTopK("topktable", Some("hashtagTable"),
      "hashtag", schema, topKOption)

    val tableName = "retweetStore"

    snsc.snappyContext.dropTable(tableName, true)

    // Create row table to insert retweets based on retweetId as Primary key
    // When a tweet is retweeted multiple times, the previous entry of the tweet
    // is over written by the new retweet count.
    snsc.snappyContext.sql(s"CREATE TABLE $tableName (retweetId BIGINT PRIMARY KEY, " +
        s"retweetCnt INT, retweetTxt STRING) USING row OPTIONS ()")

    // Save data in snappy store
    retweetStream.foreachDataFrame(df => {
      df.write.mode(SaveMode.Append).saveAsTable(tableName)
    })

    snsc.start()

    // Iterate over the streaming data for twitter data and publish the results to a file.
    try {

      val runTime = if (jobConfig.hasPath("streamRunTime")) {
        jobConfig.getString("streamRunTime").toInt * 1000
      } else {
        120 * 1000
      }

      val end = System.currentTimeMillis + runTime
      while (end > System.currentTimeMillis()) {
        Thread.sleep(2000)
        pw.println("\n******** Top 10 hash tags of last two seconds *******\n")

        // Query the topk structure for the popular hashtags of last two seconds
        snsc.snappyContext.queryApproxTSTopK("topktable",
          System.currentTimeMillis - 2000,
          System.currentTimeMillis).collect().foreach {
          result => pw.println(result.toString())
        }

      }
      pw.println("\n************ Top 10 hash tags until now ***************\n")

      // Query the topk structure for the popular hashtags of until now
      snsc.sql("SELECT * FROM topktable").collect().foreach {
        result => pw.println(result.toString())
      }

      // Query the snappystore Row table to find out the top retweets
      pw.println("\n####### Top 10 popular tweets - Query Row table #######\n")
      snsc.snappyContext.sql(s"SELECT retweetId AS RetweetId, " +
          s"retweetCnt AS RetweetsCount, retweetTxt AS Text FROM ${tableName}" +
          s" ORDER BY RetweetsCount DESC LIMIT 10")
          .collect.foreach(row => {
        pw.println(row.toString())
      })

      pw.println("\n#######################################################")

    } finally {
      pw.close()

      snsc.stop(false, true)
    }
    // Return the output file name
    s"See ${getCurrentDirectory}/$outFileName"

    // scalastyle:on println
  }

  override def isValidJob(snsc: SnappyStreamingContext, config: Config): SnappyJobValidation = {
    SnappyJobValid()
  }


}


