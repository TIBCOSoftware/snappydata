package io.snappydata.app.twitter

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import io.snappydata.app.twitter.TwitterStream.OnTweetPosted
import org.apache.spark.sql.streaming.Tweet
import twitter4j.{TwitterObjectFactory, Status, FilterQuery, TwitterFactory}

/**
 * Created by ymahajan on 28/10/15.
 */

object KafkaProducer {

  val KafkaTopic = "tweetstream"

  val kafkaProducer = {
    val props = new Properties()
    props.put("metadata.broker.list", "localhost:9092")//,localhost:9093")//conf.getString("kafka.brokers"))
    props.put("request.required.acks", "1")
    val config = new ProducerConfig(props)
    new Producer[String,  Array[Byte]](config)
  }


  val filterUsOnly = new FilterQuery().locations(
    Array(-126.562500,30.448674),
    Array(-61.171875,44.087585))


  def main (args: Array[String]) {
    val twitterStream = TwitterStream.getStream
    twitterStream.addListener(new OnTweetPosted(s => sendToKafka(s)))
    twitterStream.filter(filterUsOnly)
    //twitterStream.sample()
  }

  private def sendToKafka(s: Status) {
    val msg = new KeyedMessage[String, Array[Byte]](KafkaTopic, TwitterObjectFactory.getRawJSON(s).getBytes)
    kafkaProducer.send(msg)
  }
}
