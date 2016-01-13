/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package io.snappydata.app.streaming.kafka

import java.util.Properties

import twitter4j.conf.{ConfigurationBuilder, Configuration}

import scala.util.Random

import twitter.OnTweetPosted
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import twitter4j._

/**
 * Created by ymahajan on 28/10/15.
 */

object KafkaProducer {

  //val KafkaTopic = "tweetstream"
  val KafkaTopic = "tweets"

  val kafkaProducer = {
    val props = new Properties()
    //props.put("metadata.broker.list", "rdu-w28:9092,rdu-w29:9092,rdu-w30:9092,rdu-w31:9092,rdu-w32:9092")
    props.put("metadata.broker.list","localhost:9092")//conf.getString("kafka.brokers"))
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
      .setOAuthConsumerKey("0Xo8rg3W0SOiqu14HZYeyFPZi")
      .setOAuthConsumerSecret("gieTDrdzFS4b1g9mcvyyyadOkKoHqbVQALoxfZ19eHJzV9CpLR")
      .setOAuthAccessToken("43324358-0KiFugPFlZNfYfib5b6Ah7c2NdHs1524v7LM2qaUq")
      .setOAuthAccessTokenSecret("aB1AXHaRiE3g2d7tLgyASdgIg9J7CzbPKBkNfvK8Y88bu")
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
