package io.snappydata.examples

import java.io.PrintWriter

import com.typesafe.config.Config
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.streaming.{SchemaDStream, SnappyStreamingJob}
import org.apache.spark.sql.types._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds}
import org.apache.spark.streaming.twitter._
import spark.jobserver.{SparkJobValid, SparkJobValidation}

/**
 * Run this on your local machine:
 *
 * `$ sbin/snappy-start-all.sh`
 *
 * To run with live twitter streaming, export twitter credentials
 * `$ export APP_PROPS="consumerKey=<consumerKey>,consumerSecret=<consumerSecret>, \
 * accessToken=<accessToken>,accessTokenSecret=<accessTokenSecret>"`
 *
 * `$ ./bin/snappy-job.sh submit --lead localhost:8090 \
 * --app-name TwitterPopularTagsJob --class io.snappydata.examples.TwitterPopularTagsJob \
 * --app-jar $SNAPPY_HOME/lib/quickstart-0.1.0-SNAPSHOT.jar --context snappyStreamingContext `
 *
 * To run with stored twitter data, run simulateTwitterStream after the Job is submitted:
 * `$ ./quickstart/scripts/simulateTwitterStream`
 */

object TwitterPopularTagsJob extends SnappyStreamingJob {

  override def runJob(snsc: C, jobConfig: Config): Any = {


    var stream: DStream[_] = null
    val pw = new PrintWriter(s"TwitterPopularTagsJob-${System.currentTimeMillis}.out")

    val schema = StructType(List(StructField("hashtag", StringType)))

    snsc.snappyContext.sql("DROP TABLE IF EXISTS HASHTAGTABLE")
    snsc.snappyContext.sql("DROP TABLE IF EXISTS RETWEETTABLE")

    if (jobConfig.hasPath("consumerKey") && jobConfig.hasPath("consumerKey")
      && jobConfig.hasPath("accessToken")  && jobConfig.hasPath("accessTokenSecret") ) {
      pw.println("##### Running example with live twitter stream #####")

      snsc.sql("CREATE STREAM TABLE HASHTAGTABLE (hashtag string) using " +
        "twitter_stream options (" +
        s"consumerKey '${jobConfig.getString("consumerKey")}', " +
        s"consumerSecret '${jobConfig.getString("consumerSecret")}', " +
        s"accessToken '${jobConfig.getString("accessToken")}', " +
        s"accessTokenSecret '${jobConfig.getString("accessTokenSecret")}', " +
        "rowConverter 'org.apache.spark.sql.streaming.TweetToHashtagRow')")

      snsc.sql("CREATE STREAM TABLE RETWEETTABLE (retweetId long, retweetCnt int, retweetTxt string) using " +
        "twitter_stream options (" +
        s"consumerKey '${jobConfig.getString("consumerKey")}', " +
        s"consumerSecret '${jobConfig.getString("consumerSecret")}', " +
        s"accessToken '${jobConfig.getString("accessToken")}', " +
        s"accessTokenSecret '${jobConfig.getString("accessTokenSecret")}', " +
        "rowConverter 'org.apache.spark.sql.streaming.TweetToRetweetRow')")


    } else {
      // Create file stream
      pw.println("##### Running example with stored tweet data #####")
      snsc.sql("CREATE STREAM TABLE HASHTAGTABLE (hashtag string) USING file_stream " +
        "OPTIONS (storagelevel 'MEMORY_AND_DISK_SER_2', rowConverter 'org.apache.spark.sql.streaming.TweetToHashtagRow'," +
        "directory '/tmp/copiedtwitterdata')");

      snsc.sql("CREATE STREAM TABLE RETWEETTABLE (retweetId long, retweetCnt int, retweetTxt string) USING file_stream " +
        "OPTIONS (storagelevel 'MEMORY_AND_DISK_SER_2', rowConverter 'org.apache.spark.sql.streaming.TweetToRetweetRow'," +
        "directory '/tmp/copiedtwitterdata')");

    }
    val retweetStream: SchemaDStream = snsc.registerCQ("SELECT * FROM RETWEETTABLE window (duration '2' seconds, slide '2' seconds)")
    val hashtagStream: SchemaDStream = snsc.registerCQ("SELECT * FROM HASHTAGTABLE window (duration '2' seconds, slide '2' seconds)")


    val topKOption = Map(
        "epoch" -> System.currentTimeMillis(),
        "timeInterval" -> 2000,
        "size" -> 10
      )

    snsc.snappyContext.createTopK("topktable", "hashtag", schema, topKOption, false)

    snsc.snappyContext.saveStream(hashtagStream,
      Seq("topktable"),
      None
    )

    val tableName = "retweetStore"

    snsc.snappyContext.dropTable(tableName,true )

    snsc.snappyContext.sql(s"CREATE TABLE $tableName (retweetId bigint, " +
      s"retweetCnt int, retweetTxt string) USING row OPTIONS ()")

    retweetStream.foreachDataFrame(df => {
      df.write.mode(SaveMode.Append).saveAsTable(tableName)
    })

    snsc.start()

    // Iterate over the streaming data for sometime and publish the results to a file.
    try {
      val end = System.currentTimeMillis + 90000
      while (end > System.currentTimeMillis()) {
        Thread.sleep(2000)
        pw.println("\n******** Top 10 hash tags for the last interval *******\n")

        snsc.snappyContext.queryTopK("topktable",System.currentTimeMillis - 2000,
          System.currentTimeMillis).collect.foreach(result => {
          pw.println(result.toString)
        })
      }
      pw.println("\n************ Top 10 hash tags until now ***************\n")

      snsc.snappyContext.queryTopK("topktable").collect.foreach(result => {
        pw.println(result.toString)
      })

      pw.println("\n####### Top 10 popular tweets using gemxd query #######\n")
      snsc.snappyContext.sql(s"select retweetId as RetweetId, retweetCnt as RetweetsCount, " +
        s"retweetTxt as Text from ${tableName} order by RetweetsCount desc limit 10")
        .collect.foreach(row => {
        pw.println(row.toString())
      })

      pw.println("\n#######################################################")

    } finally {
      pw.close()

      snsc.stop(false, true)
    }

  }

  override def validate(snsc: C, config: Config): SparkJobValidation = {
    SparkJobValid
  }


}


