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
package io.snappydata.hydra.streaming_sink

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}
import scala.util.Random

object StringMessageProducer {

  /* Schema

  */

  def properties(brokers: String): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put("key.serializer", classOf[LongSerializer].getName)
    props
  }

  def main(args: Array[String]) {
    val eventCount: Int = args {0}.toInt
    val topic: String = args {1}
    val numThreads: Int = args{2}.toInt
    var brokers: String = args {4}
    val startRange: Int = args {3}.toInt
    brokers = brokers.replace("--", ":")
    // scalastyle:off println
    println("Sending Kafka messages of topic " + topic + " to brokers " + brokers)
    val producer = new KafkaProducer[Long, String](properties(brokers))
    val noOfPartitions = producer.partitionsFor(topic).size()
    val threads = new Array[Thread](numThreads)
    for (i <- 0 until numThreads) {
      val thread = new Thread(new RecordCreator(topic, eventCount, startRange, producer))
      thread.start()
      threads(i) = thread
    }
    threads.foreach(_.join())
    println(s"Done sending $eventCount Kafka messages of topic $topic")
    producer.close()
    System.exit(0)
  }

}

final class RecordCreator(topic: String, eventCount: Int, startRange: Int,
    producer: KafkaProducer[Long, String])
extends Runnable {
  def randomAlphanumeric(length: Int): String = Random.alphanumeric.take(length).mkString
  def run() {
    var id = 0
    (startRange to startRange + eventCount).foreach(i => {
      // val currtime = System.currentTimeMillis()
      // val r = scala.util.Random
      // val id = r.nextInt(1000)
      val data = new ProducerRecord[Long, String](topic, i, s"$i,name$i,$i,0")
      producer.send(data)
      })
    println(s"Done producing records...")
  }

}

