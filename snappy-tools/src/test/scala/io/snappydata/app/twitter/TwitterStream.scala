package io.snappydata.app.twitter

import org.apache.spark.SparkConf
import org.apache.spark.sql.streaming.{MessageToRowConverter, StreamUtils, StreamingSnappyContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by ymahajan on 28/10/15.
 */
object TwitterStream {

  def main(args: Array[String]) {

    val sparkConf = new SparkConf().setAppName("StreamingSql")
      .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val snsc = StreamingSnappyContext(ssc)

    snsc.sql("create stream table twitterstreamtable (name string) using " +
      "twitter_stream options (" +
      "consumerKey '***REMOVED***', " +
      "consumerSecret '***REMOVED***', " +
      "accessToken '***REMOVED***', " +
      "accessTokenSecret '***REMOVED***', " +
      "storagelevel 'MEMORY_AND_DISK_SER', " +
      "streamToRow 'io.snappydata.app.twitter.SocketToRowConverter')")

    val resultSet = snsc.registerCQ("SELECT name FROM twitterstreamtable window " +
      "(duration '1' seconds, slide '1' seconds)")

    //val tableStream = snsc.getSchemaDStream("twitterstreamtable")

    resultSet.foreachRDD(rdd =>{ rdd.foreach(row => println("Tweet --->" +row))})

    ssc.start()
    ssc.awaitTerminationOrTimeout(30 * 1000)
    ssc.stop()
    println("ok")
  }
}