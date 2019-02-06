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
package io.snappydata.hydra.putInto

import java.io.{File, FileOutputStream, PrintWriter}
import java.sql.{Connection, Timestamp, SQLException}
import java.time.LocalDate
import java.time.temporal.ChronoUnit.DAYS
import java.util.Properties

import scala.util.Random

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

object PutIntoProducer {

  val pw: PrintWriter = new PrintWriter(new FileOutputStream(new File("generatorAndPublisher.out"),
    true));

  def properties(brokers: String): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put("key.serializer", classOf[StringSerializer].getName)
    props
  }

  def getCurrTimeAsString: String = {
    "[" + new Timestamp(System.currentTimeMillis()).toString + "] "
  }

  def generateAndPublish(args: Array[String]) {
    val eventCount: Long = args {0}.toInt
    val topic: String = args {1}
    val startRange: Long = args {2}.toInt
    var brokers: String = args {args.length - 1}
    brokers = brokers.replace("--", ":")
    // scalastyle:off println
    pw.println(getCurrTimeAsString + s"Sending Kafka messages of topic $topic to brokers $brokers")
    pw.flush()
    val producer = new KafkaProducer[String, String](properties(brokers))
    val numThreads = 1;
    val threads = new Array[Thread](numThreads)
    val eventsPerThread = eventCount / numThreads;
    for (i <- 0 until numThreads) {
      val thrStartRange = startRange + (i * eventsPerThread)
      val thread = new Thread(new RecordCreator(topic, eventsPerThread, thrStartRange, producer))
      thread.start()
      threads(i) = thread
    }
    threads.foreach(_.join())
    pw.println(getCurrTimeAsString + s"Done sending $eventCount Kafka messages of topic $topic")
    pw.close()
    producer.close()
  }

  def main(args: Array[String]) {
    generateAndPublish(args)
  }

}

final class RecordCreator(topic: String, eventCount: Long, startRange: Long,
    producer: KafkaProducer[String, String])
    extends Runnable {

  val schema = Array ("id", "data1", "data2", "APPLICATION_ID", "ORDERGROUPID", "PAYMENTADDRESS1",
    "PAYMENTADDRESS2", " PAYMENTCOUNTRY", "PAYMENTSTATUS", " PAYMENTRESULT",
    "PAYMENTZIP", " PAYMENTSETUP", "PROVIDER_RESPONSE_DETAILS", "PAYMENTAMOUNT", "PAYMENTCHANNEL",
    "PAYMENTCITY", "PAYMENTSTATECODE", "PAYMENTSETDOWN", "PAYMENTREFNUMBER", "PAYMENTST",
    "PAYMENTAUTHCODE", "PAYMENTID", "PAYMENTMERCHID", "PAYMENTHOSTRESPONSECODE", "PAYMENTNAME",
    "PAYMENTOUTLETID", "PAYMENTTRANSTYPE", "PAYMENTDATE", "CLIENT_ID", "CUSTOMERID")
  val random = new Random()
  val range: Long = 9999999999L - 1000000000
  var conn: Connection = null
  def run() {
    PutIntoProducer.pw.println(PutIntoProducer.getCurrTimeAsString + s"start: " +
        s"$startRange and end: {$startRange + $eventCount}");

    (startRange until (startRange + eventCount)).foreach(i => {
      val id: String = i.toString
      val data1: String = "data " + i
      val data2: Double = i * 10.2
      val applicatoin_id: String = "APPLICATION_ID_" + i
      val orderGrp_id: String = "ORDERGROUPID" + i
      val payment_add1: String = "PAYMENTADDRESS1" + i
      val payment_add2: String = "PAYMENTADDRESS2" + i
      val payment_country: String = "PAYMENTCOUNTRY" + i
      val payment_status: String = "PAYMENTSTATUS" + i
      val payment_result: String = "PAYMENTRESULT" + i
      val payment_zip: String = "PAYMENTZIP" + i
      val payment_setup: String = "PAYMENTSETUP" + i
      val provider_details: String = "PROVIDER_RESPONSE_DETAILS" + i
      val payment_amount: String = "PAYMENTAMOUNT" + i
      val payment_channel: String = "PAYMENTCHANNEL" + i
      val payment_city: String = "PAYMENTCITY" + i
      val payment_state_code: String = "PAYMENTSTATECODE" + i
      val payment_setdown: String = "PAYMENTSETDOWN" + i
      val payment_refnumber: String = "PAYMENTREFNUMBER" + i
      val paymenttst: String = "PAYMENTST" + i
      val payment_auth_code: String = ""
      val payment_id: String = ""
      val payment_merchind: String = ""
      val payment_response_code: String = ""
      val payment_name: String = ""
      val payment_outled_id: String = ""
      val payment_transfer_type: String = ""
      val payment_date: String = ""
      val client_id: String = ""
      val customer_id: String = ""

      val row: String = s"$id,$data1,$data2,$applicatoin_id,$orderGrp_id,$payment_add1," +
          s"$payment_add2,$payment_country,$payment_status,$payment_result,$payment_zip," +
          s"$payment_setup,$provider_details,$payment_amount,$payment_channel,$payment_city," +
          s"$payment_state_code,$payment_setdown,$payment_refnumber,$paymenttst," +
          s"$payment_auth_code,$payment_id,$payment_merchind,$payment_response_code," +
          s"$payment_name,$payment_outled_id,$payment_transfer_type,$payment_date," +
          s"$client_id,$customer_id"
      PutIntoProducer.pw.println(PutIntoProducer.getCurrTimeAsString + s"row id : $id")
      val data = new ProducerRecord[String, String](topic, id, row + s",1")
      producer.send(data)
    })

    PutIntoProducer.pw.println(PutIntoProducer.getCurrTimeAsString + "Done producing " +
        "records...")
    PutIntoProducer.pw.flush()
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


