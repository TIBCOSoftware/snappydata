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
package io.snappydata.hydra

import java.io.PrintWriter

import com.typesafe.config.Config

import org.apache.spark.sql.streaming.{SchemaDStream, SnappyStreamingJob}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{SnappyJobValid, SaveMode, SnappyJobValidation}
import org.apache.spark.streaming.SnappyStreamingContext
import org.apache.spark.streaming.dstream.DStream

object FileStreamingJob extends SnappyStreamingJob {

  override def runSnappyJob(snsc: SnappyStreamingContext, jobConfig: Config): Any = {

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    // scalastyle:off println
    var stream: DStream[_] = null
    val outFileName = s"FileStreamingJob-${System.currentTimeMillis}.out"
    val pw = new PrintWriter(outFileName)
    val schema = StructType(List(StructField("hashtag", StringType)))
    val dataDir = jobConfig.getString("dataDirName")
    snsc.snappyContext.sql("DROP TABLE IF EXISTS hashtagtable")
    snsc.snappyContext.sql("DROP TABLE IF EXISTS retweettable")

    // Create file stream table
    pw.println("##### Running example with stored tweet data #####")

    snsc.sql("CREATE STREAM TABLE hashtagtable (hashtag STRING) USING file_stream " +
        "OPTIONS (storagelevel 'MEMORY_AND_DISK_SER_2', " +
        "rowConverter 'org.apache.spark.sql.streaming.TweetToHashtagRow'," +
        "directory '" + dataDir + "/copiedtwitterdata')")

    snsc.sql("CREATE STREAM TABLE retweettable (retweetId LONG, retweetCnt INT, " +
        "retweetTxt STRING) USING file_stream " +
        "OPTIONS (storagelevel 'MEMORY_AND_DISK_SER_2', " +
        "rowConverter 'org.apache.spark.sql.streaming.TweetToRetweetRow'," +
        "directory '" + dataDir + "/copiedtwitterdata')")

    // Register continuous queries on the tables and specify window clauses
    val retweetStream: SchemaDStream = snsc.registerCQ("SELECT * FROM retweettable " +
        "WINDOW (DURATION 2 SECONDS, SLIDE 2 SECONDS)")

    val tableName = "retweetStore"

    snsc.snappyContext.dropTable(tableName, true)

    // Create row table to insert retweets based on retweetId
    // When a tweet is retweeted multiple times, the previous entry of the tweet
    // is over written by the new retweet count.
    snsc.snappyContext.sql(s"CREATE TABLE $tableName (retweetId BIGINT, " +
        s"retweetCnt INT, retweetTxt STRING) USING row OPTIONS ()")

    var totalSize: Long = 0;
    // Save data in snappy store
    retweetStream.foreachDataFrame(df => {
      val size = df.count
      df.write.mode(SaveMode.Append).saveAsTable(tableName)
      totalSize += size
      pw.println(s"\n Total size of data we are writing to file is :: ${totalSize} \n")
    })
    snsc.start()

    // Iterate over the streaming data for twitter data and publish the results to a file.
    try {

      val runTime = if (jobConfig.hasPath("streamRunTime")) {
        jobConfig.getString("streamRunTime").toInt * 1000
      } else {
        600 * 1000
      }
      val end = System.currentTimeMillis + runTime
      while (end > System.currentTimeMillis()) {
        // Query the snappystore Row table to find out the top retweets
        pw.println("\n Top 10 popular tweets - Query Row table \n")
        snsc.snappyContext.sql(s"SELECT retweetId AS RetweetId, " +
            s"retweetCnt AS RetweetsCount, retweetTxt AS Text FROM ${tableName}" +
            s" ORDER BY RetweetsCount DESC LIMIT 10")
            .collect.foreach(row => {
          pw.println(row.toString())
        })
        pw.println("\n#######################################################")
        pw.println("\n Select count(*) query to get incremented counter as " +
            "the time progresses  - Query Row table \n")
        snsc.snappyContext.sql(s"SELECT count(*) FROM ${tableName}")
            .collect.foreach(row => {
          pw.println(row.toString())
        })
        Thread.sleep(2000)
      }

    } finally {
      pw.close()
      snsc.stop(false, true)
    }
    // Return the output file name
    s"See ${getCurrentDirectory}/$outFileName"

  }

  override def isValidJob(sc: SnappyStreamingContext, config: Config): SnappyJobValidation =
    SnappyJobValid()
}

