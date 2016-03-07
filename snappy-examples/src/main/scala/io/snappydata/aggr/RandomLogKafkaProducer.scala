// scalastyle:ignore

package io.snappydata.aggr

import java.util.Properties

import scala.util.Random

import io.snappydata.aggr.Constants._
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

object RandomLogKafkaProducer extends App {

  val props = new Properties()
  props.put("serializer.class", "io.snappydata.aggr.ImpressionLogAvroEncoder")
  props.put("metadata.broker.list", "localhost:9092")
  props.put("request.required.acks", "1")

  val config = new ProducerConfig(props)
  val producer = new Producer[String, ImpressionLog](config)

  println("Sending messages...") // scalastyle:ignore
  val random = new Random()
  var i = 0
  // infinite loop
  while (true) {
    val timestamp = System.currentTimeMillis()
    val publisher = Publishers(random.nextInt(NumPublishers))
    val advertiser = Advertisers(random.nextInt(NumAdvertisers))
    val website = s"website_${random.nextInt(Constants.NumWebsites)}.com"
    val cookie = s"cookie_${random.nextInt(Constants.NumCookies)}"
    val geo = Geos(random.nextInt(Geos.size))
    val bid = math.abs(random.nextDouble()) % 1
    val log = new ImpressionLog()
    log.setTimestamp(timestamp)
    log.setPublisher(publisher)
    log.setAdvertiser(advertiser)
    log.setWebsite(website)
    log.setGeo(geo)
    log.setBid(bid)
    log.setCookie(cookie)

    producer.send(new KeyedMessage[String, ImpressionLog](Constants.KafkaTopic, log))
    i = i + 1
    if (i % 10000 == 0) {
      println(s"Sent $i messages!") // scalastyle:ignore
    }
  }
}
