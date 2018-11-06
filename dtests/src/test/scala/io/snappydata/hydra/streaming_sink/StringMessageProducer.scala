/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

import java.time.LocalDate
import java.util.Properties
import java.time.temporal.ChronoUnit.DAYS
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}
import scala.util.Random

import org.scalatest.time.Days

object StringMessageProducer {

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
    val opType: Int = args{4}.toInt
    var brokers: String = args {5}
    val startRange: Int = args {3}.toInt
    brokers = brokers.replace("--", ":")
    // scalastyle:off println
    println("Sending Kafka messages of topic " + topic + " to brokers " + brokers)
    val producer = new KafkaProducer[Long, String](properties(brokers))
    val noOfPartitions = producer.partitionsFor(topic).size()
    val threads = new Array[Thread](numThreads)
    for (i <- 0 until numThreads) {
      val thread = new Thread(new RecordCreator(topic, eventCount, startRange, producer, opType))
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
    producer: KafkaProducer[Long, String], opType: Int)
extends Runnable {
  val random = new Random()
  val range: Long = 9999999999L - 1000000000
  val statusArr = Array("Married", "Single")
  val titleArr = Array("Mr.", "Mrs.", "Miss")
  val occupationArr = Array("Employee", "Business")
  val educationArr = Array("Under Graduate", "Graduate", "PostGraduate")
  val countryArr = Array("US", "UK", "Canada", "India")

  def run() {
    val title: String = titleArr(random.nextInt(titleArr.length))
    val status: String = statusArr(random.nextInt(statusArr.length))
    val phone: Long = (range * random.nextDouble()).toLong + 1000000000
    val age: Int = random.nextInt(100) + 18
    val education: String = educationArr(random.nextInt(educationArr.length))
    val occupation: String = occupationArr(random.nextInt(occupationArr.length))
    val country: String = countryArr(random.nextInt(countryArr.length))
    val address: String = randomAlphanumeric(20)
    val email: String = randomAlphanumeric(10) + "@" + randomString(5) + "." + randomString(3)
    val dob: LocalDate = randomDate(LocalDate.of(1915, 1, 1), LocalDate.of(2000, 1, 1))
    (startRange to (startRange + eventCount - 1)).foreach(i => {
      // val currtime = System.currentTimeMillis()
      // val r = scala.util.Random
      // val id = r.nextInt(1000)
      val data = new ProducerRecord[Long, String](topic, i, s"$i,fName$i,mName$i,lName$i,$title," +
          s"$address,$country,$phone,$dob,$age,$status,$email,$education,$occupation,$opType")
      producer.send(data)
      })
    println(s"Done producing records...")
  }

  def randomAlphanumeric(length: Int): String = {
    Random.alphanumeric.take(length).mkString
  }

  def randomString(length: Int): String = {
    val sb = new StringBuilder
    for (i <- 1 to length) {
      sb.append(random.nextPrintableChar)
    }
    sb.toString
  }

  def randomDate(from: LocalDate, to: LocalDate): LocalDate = {
    val diff = DAYS.between(from, to)
    val random = new Random(System.nanoTime)
    from.plusDays(random.nextInt(diff.toInt))
  }

}

