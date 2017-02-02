package org.apache.spark.streaming

import java.io.{File, FileOutputStream, PrintWriter}

import com.typesafe.config.Config
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{StreamToRowsConverter, SnappyStreamingJob}

import scala.util.{Failure, Success, Try}

class HitachiHealthcareDemo extends SnappyStreamingJob {

  override def runSnappyJob(snsc: SnappyStreamingContext, jobConfig: Config): Any = {
    def getCurrentDirectory = new java.io.File(".").getCanonicalPath
    val pw: PrintWriter = new PrintWriter(new FileOutputStream(new File("HitachiHealthcareDemo.out")),
      true)
    Try {
      pw.println("****** HitachiHealthcareDemo started ******")
      val currentDirectory: String = new File(".").getCanonicalPath
      /*val spark: SparkSession = SparkSession
        .builder
        .appName("HitachiHealthcareDemo")
        .master("local[*]")
        .getOrCreate

      val sc = spark.sparkContext
      val session = new SnappySession(sc)

      val snsc = new SnappyStreamingContext(sc, Seconds(1))*/

      snsc.sql("drop table if exists accStreamTable")
      snsc.sql("create stream table " +
        "accStreamTable (user_id string, ts timestamp, ax double, ay double, az double) " +
        "using file_stream options (directory '/tmp/ACC/', " +
        "rowConverter 'org.apache.spark.streaming.AccFileToRowsConverter')")

      snsc.sql("drop table if exists bioStreamTable")
      snsc.sql("create stream table " +
        "bioStreamTable (user_id string, ts timestamp, heart_rate int, breath_rate int) " +
        "using file_stream options (directory '/tmp/BIO/', " +
        "rowConverter 'org.apache.spark.streaming.BioFileToRowsConverter')")

      snsc.sql("drop table if exists envStreamTable")
      snsc.sql("create stream table " +
        "envStreamTable (user_id string, ts timestamp, temperature double, humidity double) " +
        "using file_stream options (directory '/tmp/ENV/', " +
        "rowConverter 'org.apache.spark.streaming.EnvFileToRowsConverter')")

      snsc.registerCQ("select * from accStreamTable window " +
        "(duration 1 seconds, slide 1 seconds)").foreachDataFrame(_.show)

      snsc.registerCQ("select * from bioStreamTable window " +
        "(duration 1 seconds, slide 1 seconds)").foreachDataFrame(_.show)

      snsc.registerCQ("select * from envStreamTable window " +
        "(duration 1 seconds, slide 1 seconds)").foreachDataFrame(_.show)

      /*val streamToStreamJoin = "select t1.USER_ID, t1.TS, t2.HEART_RATE, " +
        "t2.BREATH_RATE, t3.TEMPERATURE, t3.HUMIDITY from " +
        "accStreamTable window (duration 2 seconds, slide 1 seconds) as t1 " +
        "JOIN bioStreamTable window (duration 2 seconds, slide 1 seconds) as t2 " +
        "JOIN envStreamTable window (duration 2 seconds, slide 1 seconds) as t3 " +
        "ON t1.USER_ID=t2.USER_ID and t2.USER_ID=t3.USER_ID"*/

      // snsc.registerCQ(streamToStreamJoin).foreachDataFrame(_.show)

      snsc.start()

      try {

        val runTime = if (jobConfig.hasPath("streamRunTime")) {
          jobConfig.getString("streamRunTime").toInt * 1000
        } else {
          100 * 1000
        }
        val end = System.currentTimeMillis + runTime
        while (end > System.currentTimeMillis()) {
          // Query the snappystore Row table to find out the top retweets
          pw.println("\n Running  HitachiHealthcareDemo job ....\n")
          Thread.sleep(2000)
        }

      } finally {
        snsc.stop(stopSparkContext=false, stopGracefully=true)
        pw.println("STOP!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        pw.close()
      }

     // snsc.awaitTerminationOrTimeout(10 * 1000) // run for 10 seconds

      pw.println("****** HitachiHealthcareDemo finished ******")
      return String.format("See %s/" + jobConfig.getString("logFileName"), currentDirectory)
    } match {
      case Success(v) => pw.close()
        s"See ${getCurrentDirectory}/${jobConfig.getString("logFileName")}"
      case Failure(e) =>
        pw.println("Exception occurred while executing the job " + "\nError Message:" + e.getMessage)
        pw.close();
        throw e;
    }
  }

  override def isValidJob(sc: SnappyStreamingContext, config: Config): SnappyJobValidation =
    SnappyJobValid()
}
