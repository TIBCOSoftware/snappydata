package io.snappydata.app.twitter

import com.typesafe.config.ConfigFactory
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark._
import org.apache.spark.sql.streaming.{SchemaDStream, StreamingSnappyContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

/**
 * Created by ymahajan on 28/10/15.
 */
object KafkaConsumer {

  //private val conf = ConfigFactory.load()

//  val sparkConf = new SparkConf().setAppName("kafka-twitter-spark-example").setMaster("local[*]")
//  val sc = new StreamingContext(sparkConf, Seconds(5))

  def main (args: Array[String]) {

    val sc = new org.apache.spark.SparkConf().
      setMaster("local[2]").
      setAppName("streamingsql")
    val ssc = new StreamingContext(new SparkContext(sc), Duration(10000))
    val snsc = StreamingSnappyContext(ssc);

    //  val encTweets = {
    //    val topics = Map(KafkaProducerApp.KafkaTopic -> 1)
    //    val kafkaParams = Map(
    //      "zookeeper.connect" -> conf.getString("kafka.zookeeper.quorum"),
    //      "group.id" -> "1")
    //    KafkaUtils.createStream[String, Array[Byte], StringDecoder, DefaultDecoder](
    //      sc, kafkaParams, topics, StorageLevel.MEMORY_ONLY)
    //  }
    //  encTweets.print()

    //snc.sql( """STREAMING CONTEXT  INIT 10""")

    snsc.sql("create stream table kafkaStreamTable (name string, text string) using kafka options (storagelevel 'MEMORY_AND_DISK_SER_2', formatter 'org.apache.spark.sql.streaming.MyStreamFormatter', " +
      " zkQuorum '10.112.195.65:2181', groupId 'streamSQLConsumer', topics 'tweets:01')")

    snsc.sql("create stream table directKafkaStreamTable (name string, text string) using kafka options (storagelevel 'MEMORY_AND_DISK_SER_2', formatter 'org.apache.spark.sql.streaming.MyStreamFormatter', " +
      " kafkaParams 'metadata.broker.list -> localhost:9092', topics 'tweets')")

//    val resultSet1: SchemaDStream = snsc.registerCQ("SELECT text FROM kafkaStreamTable window (duration '10' seconds, slide '10' seconds)")// WHERE age >= 18")
//
//    val resultSet2: SchemaDStream = snsc.registerCQ("SELECT text FROM directKafkaStreamTable window (duration '10' seconds, slide '10' seconds)")// WHERE age >= 18")
//
//    resultSet1.foreachRDD {
//      r => r.foreach(print)
//    }
//    resultSet2.foreachRDD {
//      r => r.foreach(print)
//    }
    snsc.sql( """STREAMING CONTEXT START """)
    ssc.awaitTerminationOrTimeout(20000)
    snsc.sql( """STREAMING CONTEXT STOP """)

    //  val tweets = encTweets.flatMap(x => SpecificAvroCodecs.toBinary[Tweet].invert(x._2).toOption)
    //
    //  val wordCounts = tweets.flatMap(_.getText.split(" ")).map((_,1)).reduceByKey(_ + _)
    //  val countsSorted = wordCounts.transform(_.sortBy(_._2, ascending = false))
    //
    //  countsSorted.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
