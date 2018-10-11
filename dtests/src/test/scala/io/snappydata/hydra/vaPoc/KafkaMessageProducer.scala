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
package io.snappydata.hydra.vaPoc

import java.util.Properties

import scala.io.Source
import scala.util.Random

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}

object KafkaMessageProducer {

  def properties(brokers: String): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put("key.serializer", classOf[LongSerializer].getName)
    props
  }

  def main(args: Array[String]) {
    val fileName: String = args {0}
    val topic: String = args {1}
    val columnIndexToBeModified = args{2}.toInt
    var brokers: String = args {3}
    val opType: Int = 1
    brokers = brokers.replace("--", ":")
    // scalastyle:off println
    println("Sending Kafka messages of topic " + topic + " to brokers " + brokers)
    val producer = new KafkaProducer[Long, String](properties(brokers))
    val noOfPartitions = producer.partitionsFor(topic).size()
    val thread = new Thread(new RecordCreator(topic, producer, fileName, columnIndexToBeModified))
    thread.start()
    println(s"Done sending events Kafka messages of topic $topic")
    producer.close()
    System.exit(0)
  }

}

final class RecordCreator(topic: String, producer: KafkaProducer[Long, String], fileName: String,
    colIndex: Int)
    extends Runnable {
  val opType: Int = 1
  var id: Int = 0
  val random = new Random()

  def run() {
    for (line <- Source.fromFile(fileName).getLines) {
      println(line)
      val columnArr = line.split(",")
      columnArr{colIndex} = columnArr{colIndex} +  randomString(3)
      val line1 = columnArr.mkString(",") + ",1"
      val data = new ProducerRecord[Long, String](topic, id, line1)
      producer.send(data)
      id += 1
    }
  }

  def randomString(length: Int): String = {
    val sb = new StringBuilder
    for (i <- 1 to length) {
      sb.append(random.nextPrintableChar)
    }
    sb.toString
  }

}

