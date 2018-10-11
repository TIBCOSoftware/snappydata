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

import java.sql.Connection
import java.time.LocalDate
import java.time.temporal.ChronoUnit.DAYS
import java.util.Properties

import scala.util.Random

import io.snappydata.hydra.testDMLOps.{DerbyTestUtils, SnappySchemaPrms}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}

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
    val thread = new Thread(new RecordCreator(topic, eventCount, startRange, producer, opType))
    thread.start()
    thread.join()
    println(s"Done sending $eventCount Kafka messages of topic $topic")
    producer.close()
    System.exit(0)
  }

}

final class RecordCreator(topic: String, eventCount: Int, startRange: Int,
    producer: KafkaProducer[Long, String], opType: Int)
extends Runnable {

  val schema = Array ("id", "firstName", "middleName", "lastName", "title", "address", "country",
    "phone", "dateOfBirth", "age", "status", "email", "education", "occupation")
  val random = new Random()
  val range: Long = 9999999999L - 1000000000
  val statusArr = Array("Married", "Single")
  val titleArr = Array("Mr.", "Mrs.", "Miss")
  val occupationArr = Array("Employee", "Business")
  val educationArr = Array("Under Graduate", "Graduate", "PostGraduate")
  val countryArr = Array("US", "UK", "Canada", "India")
  var derbyTestUtils = new DerbyTestUtils

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
    val conn: Connection = derbyTestUtils.getDerbyConnection
    (startRange to (startRange + eventCount - 1)).foreach(i => {
      val row: String = s"$i,fName$i,mName$i,lName$i,$title,$address,$country,$phone,$dob,$age," +
          s"$status,$email,$education,$occupation,$opType"
      val data = new ProducerRecord[Long, String](topic, i, row)
      if(DerbyTestUtils.hasDerbyServer) {
        DerbyTestUtils.HydraTask_initialize
        performOpInDerby(conn, row, opType)
      }
      producer.send(data)
      })
    derbyTestUtils.closeDiscConnection(conn, true)
    println(s"Done producing records...")
  }

  def performOpInDerby(conn: Connection, row: String, eventType: Int): Unit = {
    var stmt: String = ""
    if (eventType == 0) { // insert
      stmt = getInsertStmt(row)
    } else if (eventType == 1) { // update
      stmt = getUpdateStmt(row)
    } else if (eventType == 2) { // delete
      getDeleteStmt(row)
    }
    println(s" derby stmt is : $stmt")
    val numRows: Int = conn.createStatement().executeUpdate(stmt)
    if (numRows == 0 && eventType == 1) {
      stmt = getInsertStmt(row)
      conn.createStatement().executeUpdate(stmt)
    }
    // write to file
  }

  def getInsertStmt(row: String): String = {
    var stmt: String = ""
    val columnVal = row.split(",")
    val id: Int = (columnVal {0}).toInt
    stmt = "insert into persoon values("
    for (i <- 0 to columnVal.length - 1) {
      if (i == 0 || i == 9) {
        stmt = stmt + columnVal {i}
      } else {
        stmt = stmt + "'" + columnVal {i} + "'"
      }
      if (i != (columnVal.length - 1)) {
        stmt = stmt + ","
      }
    }
    stmt = stmt + ")"
    stmt
  }

  def getUpdateStmt(row: String): String = {
    var stmt: String = ""
    val columnVal = row.split(",")
    val id: Int = (columnVal {0}).toInt
    stmt = stmt + "update table persoon set "
    for (i <- 0 to columnVal.length - 1) {
      if (i == 0 ) {
        // ignore id column
      }
      else if (i == 9) {
        stmt = stmt + schema{i} + "=" + columnVal {i}
      } else {
        stmt = stmt + schema{i} + "='" + columnVal {i} + "'"
      }
      if (i != (columnVal.length - 1)) {
        stmt = stmt + ","
      }
    }
    stmt = stmt + " where ID = " + id
    stmt
  }

  def getDeleteStmt(row: String): String = {
    var stmt: String = ""
    val id: Int = (row.split(",") {0}).toInt
    stmt = stmt + "delete from persoon where ID = " + id
    stmt
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

