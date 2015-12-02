package io.snappydata.app.twitter

import java.util.Properties

import twitter4j.conf.{ConfigurationBuilder, Configuration}

import scala.util.Random

import io.snappydata.app.twitter.twitter.OnTweetPosted
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import twitter4j._

/**
 * Created by ymahajan on 28/10/15.
 */

object KafkaProducer {

  val KafkaTopic = "streamtweet"

  val kafkaProducer = {
    val props = new Properties()
    props.put("metadata.broker.list", "rdu-w28:9092,rdu-w29:9092,rdu-w30:9092,rdu-w31:9092,rdu-w32:9092")
    //,localhost:9093")//conf.getString("kafka.brokers"))
    props.put("request.required.acks", "1")
    val config = new ProducerConfig(props)
    println(" XXXXXXXXXXXXX Starting KafkaProducer")
    new Producer[String,  Array[Byte]](config)
  }

  def main (args: Array[String]) {
    val twitterStream = twitter.getStream
    twitterStream.addListener(new OnTweetPosted(s => sendToKafka(s)))
    twitterStream.filter(new FilterQuery().locations(
      Array(-180,-90),
      Array(180,90))
    )
    //twitterStream.sample()
  }

  private def sendToKafka(s: Status) {
    if (!PrintJSON.printed) {
      println("XXXXXXXXXX: " + s.toString)
      PrintJSON.printed = true
    }
    val msg = new KeyedMessage[String, Array[Byte]](KafkaTopic,
      TwitterObjectFactory.getRawJSON(s).getBytes)
    kafkaProducer.send(msg)
  }
}

object PrintJSON {
  var printed = false
}


object twitter {

  private val getTwitterConf: Configuration = {
    val twitterConf = new ConfigurationBuilder()
      .setOAuthConsumerKey("***REMOVED***")
      .setOAuthConsumerSecret("***REMOVED***")
      .setOAuthAccessToken("***REMOVED***")
      .setOAuthAccessTokenSecret("***REMOVED***")
      .setJSONStoreEnabled(true)
      .build()
    twitterConf
  }

  def getStream = new TwitterStreamFactory(getTwitterConf).getInstance()

  class OnTweetPosted(cb: Status => Unit) extends StatusListener {

    override def onStatus(status: Status): Unit = {
      cb(status)}
    override def onException(ex: Exception): Unit = throw ex

    // no-op for the following events
    override def onStallWarning(warning: StallWarning): Unit = {}
    override def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice): Unit = {}
    override def onScrubGeo(userId: Long, upToStatusId: Long): Unit = {}
    override def onTrackLimitationNotice(numberOfLimitedStatuses: Int): Unit = {}
  }
}
