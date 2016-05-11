package io.snappydata.hydra

import java.io.PrintWriter

import com.typesafe.config.Config
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.streaming.{SchemaDStream, SnappyStreamingJob}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.DStream
import spark.jobserver.{SparkJobValid, SparkJobValidation}

/**
 * Created by swati on 7/4/16.
 */

object FileStreamingJob extends SnappyStreamingJob {

  override def runJob(snsc: C, jobConfig: Config): Any = {

    def getCurrentDirectory = new java.io.File(".").getCanonicalPath

    // scalastyle:off println
    var stream: DStream[_] = null
    val outFileName = s"FileStreamingJob-${System.currentTimeMillis}.out"
    val pw = new PrintWriter(outFileName)
    val schema = StructType(List(StructField("hashtag", StringType)))

    snsc.snappyContext.sql("DROP TABLE IF EXISTS hashtagtable")
    snsc.snappyContext.sql("DROP TABLE IF EXISTS retweettable")

    // Create file stream table
    pw.println("##### Running example with stored tweet data #####")
    snsc.sql("CREATE STREAM TABLE hashtagtable (hashtag STRING) USING file_stream " +
      "OPTIONS (storagelevel 'MEMORY_AND_DISK_SER_2', " +
      "rowConverter 'org.apache.spark.sql.streaming.TweetToHashtagRow'," +
      "directory '/home/swati/copiedtwitterdata')")

    snsc.sql("CREATE STREAM TABLE retweettable (retweetId LONG, retweetCnt INT, " +
      "retweetTxt STRING) USING file_stream " +
      "OPTIONS (storagelevel 'MEMORY_AND_DISK_SER_2', " +
      "rowConverter 'org.apache.spark.sql.streaming.TweetToRetweetRow'," +
      "directory '/home/swati/copiedtwitterdata')")

    // Register continuous queries on the tables and specify window clauses
    val retweetStream: SchemaDStream = snsc.registerCQ("SELECT * FROM retweettable " +
      "WINDOW (DURATION 2 SECONDS, SLIDE 2 SECONDS)")

    val tableName = "retweetStore"

    snsc.snappyContext.dropTable(tableName, true)

    // Create row table to insert retweets based on retweetId as Primary key
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
        pw.println("\n Select count(*) query to get incremented counter as the time progresses  - Query Row table \n")
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

  override def validate(snsc: C, config: Config): SparkJobValidation = {
    SparkJobValid
  }
}

