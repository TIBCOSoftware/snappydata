package io.snappydata.app.twitter

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream

//import org.apache.spark.examples.streaming.StreamingExamples
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SaveMode
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status
/**
 * Created by ymahajan on 28/10/15.
 */
object Twitter {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: Twitter <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generat OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TwitterPopularTags")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    val stream : ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None, filters)

    stream.foreachRDD( rdd => {
      for(item <- rdd.collect()) {
        println(item.getText);
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}



//  def main(args: Array[String]) {
//
//    val filters = Array("***REMOVED***",
//      "***REMOVED***", "***REMOVED***",
//      "***REMOVED***")
//
//    // Set the system properties so that Twitter4j library used by twitter stream
//    // can use them to generate OAuth credentials
//    System.setProperty("twitter4j.oauth.consumerKey", "***REMOVED***")
//    System.setProperty("twitter4j.oauth.consumerSecret", "***REMOVED***")
//    System.setProperty("twitter4j.oauth.accessToken", "***REMOVED***")
//    System.setProperty("twitter4j.oauth.accessTokenSecret", "***REMOVED***")
//
//    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SnappyDataTwitterAnalytics")
//
//    val ssc = new StreamingContext(sparkConf, Seconds(1))
//
//    val tweets = TwitterUtils.createStream(ssc, None, filters)
//
//    tweets.print()
//
//    tweets.foreachRDD( rdd : RDD[Status] => {
//      for(item <- rdd.collect()) {
//        println("YOGS" ,item);
//      }
//    })
//
//    //val tweets = stream.map(r => r.getText)
//
//    /*tweets.foreachRDD{rdd =>
//      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)
//      import sqlContext.implicits._
//      val df = rdd.map(t => Record(t)).toDF()
//      df.save("com.databricks.spark.csv",SaveMode.Append,Map("path"->"tweetstream.csv")
//    }*/
//
////    val hashTags = tweets.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))
////
////    val topCounts60 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(60))
////      .map{case (topic, count) => (count, topic)}
////      .transform(_.sortByKey(false))
////
////    val topCounts10 = hashTags.map((_, 1)).reduceByKeyAndWindow(_ + _, Seconds(10))
////      .map{case (topic, count) => (count, topic)}
////      .transform(_.sortByKey(false))
////
////
////    // Print popular hashtags
////    topCounts60.foreachRDD(rdd => {
////      val topList = rdd.take(10)
////      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
////      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
////    })
////
////    topCounts10.foreachRDD(rdd => {
////      val topList = rdd.take(10)
////      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
////      topList.foreach{case (count, tag) => println("%s (%s tweets)".format(tag, count))}
////    })
//
////    tweets.foreachRDD { (rdd, time) =>
////      print(rdd.first())
////    }
////
////    tweets.foreachRDD { (rdd, time) =>
////      rdd.map(t => t.getText) {
////        print("YOGS" + t)
////      }
////    }
////        print("YOGS", t)
////        print("YOGS", t.getUser.getScreenName)
////        print("YOGS", t.getText)
////        Map(
////          "user" -> t.getUser.getScreenName,
////          "text" -> t.getText,
////          "hashtags" -> t.getHashtagEntities.map(_.getText),
////          "retweet" -> t.getRetweetCount
////        )
//     // }
//    //)
//        //.saveAsTextFile("twitter/tweet")
////    }
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}
