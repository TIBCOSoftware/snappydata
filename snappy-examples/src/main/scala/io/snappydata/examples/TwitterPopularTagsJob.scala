package io.snappydata.examples

import java.io.PrintWriter

import com.typesafe.config.Config
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.streaming.{SnappyStreamingJob}
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

    if (jobConfig.hasPath("consumerKey") && jobConfig.hasPath("consumerKey")
      && jobConfig.hasPath("accessToken")  && jobConfig.hasPath("accessTokenSecret") ) {
      pw.println("##### Running example with live twitter stream #####")

      stream = TwitterUtils.createStream(snsc, Some(StreamingUtils.getTwitterAuth(jobConfig)))

    } else {
      // Create file stream
      pw.println("##### Running example with stored tweet data #####")
      stream = snsc.textFileStream("/tmp/copiedtwitterdata")

    }

    // Create window of 1 second on the stream and apply schema to it
    val rowStream: DStream[Row] =
      stream.window(Seconds(1), Seconds(1)).flatMap(
        StreamingUtils.convertTweetToRow(_, schema)
      )

    val tweetStream = stream.window(Seconds(1), Seconds(1)).flatMap(StreamingUtils.convertPopularTweetsToRow(_))

    val topKOption = Map(
        "epoch" -> System.currentTimeMillis(),
        "timeInterval" -> 2000,
        "size" -> 10
      )

    snsc.snappyContext.createApproxTSTopK("topktable", "hashtag", schema, topKOption)

    snsc.snappyContext.saveStream(rowStream,
      Seq("topktable"),
      None
    )

    val schemaDStream = snsc.createSchemaDStream(tweetStream)

    val tableName = "retweetTable"
    snsc.snappyContext.dropTable(tableName,true )
    snsc.snappyContext.createTable(tableName, "row", schemaDStream.schema , Map.empty[String, String])

    schemaDStream.foreachDataFrame(df => {
      df.write.mode(SaveMode.Append).saveAsTable(tableName)
    })

    snsc.start()

    // Iterate over the streaming data for sometime and publish the results to a file.
    try {
      val end = System.currentTimeMillis + 90000
      while (end > System.currentTimeMillis()) {
        Thread.sleep(2000)
        pw.println("\n******** Top 10 hash tags for the last interval *******\n")

        snsc.snappyContext.queryApproxTSTopK("topktable",System.currentTimeMillis - 2000, System.currentTimeMillis).collect.foreach(result => {
          pw.println(result.toString)
        })

        pw.println("\n************ Top 10 hash tags until now ***************\n")

        snsc.snappyContext.queryApproxTSTopK("topktable").collect.foreach(result => {
          pw.println(result.toString)
        })


      }
      pw.println("\n####### Top 10 popular tweets using gemxd query #######\n")
      snsc.snappyContext.sql(s"select retweetCnt as RetweetsCount, retweetTxt as Text from ${tableName} order by RetweetsCount desc limit 10").collect.foreach(row => {
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


