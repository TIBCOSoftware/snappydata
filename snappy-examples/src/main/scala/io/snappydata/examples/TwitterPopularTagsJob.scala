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
 * accessToken=<accessToken>,ccessTokenSecret=<accessTokenSecret>"`
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

    val schema = StructType(List(StructField("id", LongType),
      StructField("text", StringType),
      StructField("retweets", IntegerType),
      StructField("hashtag", StringType)))

    if (jobConfig.hasPath("consumerKey") && jobConfig.hasPath("consumerKey")
      && jobConfig.hasPath("accessToken")  && jobConfig.hasPath("accessTokenSecret") ) {
      pw.println("Running example with live twitter stream")

      stream = TwitterUtils.createStream(snsc, Some(StreamingUtils.getTwitterAuth(jobConfig)))

    } else {
      // Create the file stream
      pw.println("Running example with stored tweet data")
      stream = snsc.textFileStream("/tmp/copiedtwitterdata")

    }

    // Create window of 1 second on the stream and apply schema to it
    val rowStream: DStream[Row] =
      stream.window(Seconds(1), Seconds(1)).flatMap(
        StreamingUtils.convertTweetToRow(_, schema)
      )

    /*
    Will be used when we use registerTopK API

    val topKOption = Map(
        "key" -> "hashtag",
        "frequencyCol" -> "retweets",
        "epoch" -> System.currentTimeMillis(),
        "timeInterval" -> -1
      )
    snsc.registerTopK("topktable","filestreamtable",topKOption,false)
    */
    val tableName = "StreamGemXdTable"
    snsc.snappyContext.dropExternalTable(tableName,true )
    snsc.snappyContext.createExternalTable(tableName, "column", schema, Map.empty[String, String])


    rowStream.foreachRDD(rdd => {
      snsc.snappyContext.createDataFrame(rdd, schema).
        write.mode(SaveMode.Append).saveAsTable(tableName)

      rdd.foreach(row => {
        val hashtag = row.get(row.fieldIndex("hashtag"))
        if (hashtag != null && hashtag != "")
          temporarytopkcms.cms.add(hashtag.toString, 1L)

      })

    })

    snsc.start()

    // Iterate over the streaming data for sometime and publish the results to a file.
    try {
      val end = System.currentTimeMillis + 60000
      while (end > System.currentTimeMillis()) {
        Thread.sleep(2000)
        pw.println("Top 10 hash tags using sketches for the last interval")
        /*
        snc.queryTopK("topktable", System.currentTimeMillis - 2000, System.currentTimeMillis).show()
        */
        pw.println("Top 10 hash tags until now using sketches ")

        temporarytopkcms.cms.getTopK.foreach(topktuple => {
          pw.println(topktuple.toString())
        })

        pw.println("Top 10 hash tags until now")

        snsc.snappyContext.sql("select hashtag, count(*) as tagcount from  StreamGemXdTable group " +
          " by hashtag order by tagcount desc limit 10").collect.foreach(row => {
          pw.println(row.toString())
        })
      }
    } finally {
      pw.close()

      snsc.stop(false, true)
    }

  }

  override def validate(snsc: C, config: Config): SparkJobValidation = {
    SparkJobValid
  }


}


