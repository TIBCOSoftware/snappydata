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

import java.io.{File, FileOutputStream, PrintWriter}
import java.sql.{Connection, SQLException, Statement, Timestamp}
import java.time.LocalDate
import java.time.temporal.ChronoUnit.DAYS
import java.util.Properties

import scala.util.Random

import io.snappydata.hydra.cluster.SnappyTest
import io.snappydata.hydra.testDMLOps.DerbyTestUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}

object StringMessageProducer {

  var hasDerby: Boolean = false
  var isConflationTest: Boolean = false
  var pw: PrintWriter = null

  def properties(brokers: String): Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", brokers)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put("key.serializer", classOf[LongSerializer].getName)
    props
  }

  def getCurrTimeAsString: String = {
    "[" + new Timestamp(System.currentTimeMillis()).toString + "] "
  }

  def generateAndPublish(args: Array[String]) {
    val fileName = "generatorAndPublisher_" + System.currentTimeMillis() + ".out"
    pw = new PrintWriter(new FileOutputStream(new File(fileName), true));
    val eventCount: Int = args {0}.toInt
    val topic: String = args {1}
    val startRange: Int = args {2}.toInt
    val opType: Int = args{3}.toInt
    isConflationTest = args {args.length-3}.toBoolean
    hasDerby = args {args.length-2}.toBoolean
    var brokers: String = args {args.length - 1}
    brokers = brokers.replace("--", ":")
    // scalastyle:off println
    pw.println(getCurrTimeAsString + s"Sending Kafka messages of topic $topic to brokers $brokers")
    pw.println(getCurrTimeAsString + s"Conflation enabled $isConflationTest")
    pw.flush()
    val producer = new KafkaProducer[Long, String](properties(brokers))
    // val noOfPartitions = producer.partitionsFor(topic).size()
    var numThreads = 1;
    if (!isConflationTest) { numThreads = 5 }
    val threads = new Array[Thread](numThreads)
    val eventsPerThread = eventCount / numThreads;
    if (hasDerby) {
      DerbyTestUtils.HydraTask_initialize()
    }
    for (i <- 0 until numThreads) {
      val thrStartRange = startRange + (i * eventsPerThread)
      val thread = new Thread(new RecordCreator(topic, eventsPerThread, thrStartRange, producer,
        opType, hasDerby))
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

final class RecordCreator(topic: String, eventCount: Int, startRange: Int,
    producer: KafkaProducer[Long, String], opType: Int, hasDerby: Boolean)
extends Runnable {
  var eventType: Int = opType
  val schema = Array ("id", "firstName", "middleName", "lastName", "title", "address", "country",
    "phone", "dateOfBirth", "birthtime", "age", "status", "email", "education", "gender",
    "weight", "height", "bloodGrp", "occupation", "hasChildren", "numKids", "hasSiblings")
  val random = new Random()
  val range: Long = 9999999999L - 1000000000
  val statusArr = Array("Married", "Single", "Divorcee")
  val titleArr = Array("Mr.", "Mrs.", "Miss")
  val genderArr = Array("Male", "Female", "TransGender")
  val bloodGrpArr = Array("A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-")
  val occupationArr = Array("Employee", "Business")
  val educationArr = Array("Under Graduate", "Graduate", "PostGraduate")
  val countryArr = Array("US", "UK", "Canada", "India")
  var conn: Connection = null
  var stmt: Statement = null
  var batchSize: Int = 0
  var derbyTestUtils: DerbyTestUtils = null
  def run() {
    if (hasDerby) {
      derbyTestUtils = new DerbyTestUtils
      conn = derbyTestUtils.getDerbyConnection
    } else {
      conn = SnappyTest.getLocatorConnection
      stmt = conn.createStatement()
    }
    try {
      StringMessageProducer.pw.println(StringMessageProducer.getCurrTimeAsString + s"start: " +
          s"$startRange and end: {$startRange + $eventCount}");
      (startRange until (startRange + eventCount)).foreach(i => {
        var id: Int = i
        val title: String = titleArr(random.nextInt(titleArr.length))
        val status: String = statusArr(random.nextInt(statusArr.length))
        val phone: Long = (range * random.nextDouble()).toLong + 1000000000
        val age: Int = random.nextInt(100) + 18
        val education: String = educationArr(random.nextInt(educationArr.length))
        val gender: String = genderArr(random.nextInt(genderArr.length))
        val weight: Double = 40 + {60 * random.nextDouble()}
        val height: Double = 4 + {2 * random.nextDouble()}
        val bloodGrp: String = bloodGrpArr(random.nextInt(bloodGrpArr.length))
        val occupation: String = occupationArr(random.nextInt(occupationArr.length))
        val country: String = countryArr(random.nextInt(countryArr.length))
        val hasChildren = random.nextBoolean()
        val numKids = if (hasChildren) (random.nextInt(3) + 1) else 0
        val hasSiblings = random.nextBoolean()
        val address: String = randomAlphanumeric(20)
        val email: String = randomAlphanumeric(10) + "@" + randomAlphanumeric(5) + "." +
            randomAlphanumeric(3)
        val dob: LocalDate = randomDate(LocalDate.of(1915, 1, 1), LocalDate.of(2000, 1, 1))
        val birthtime: Timestamp = new Timestamp(System.currentTimeMillis())
        if (StringMessageProducer.isConflationTest) {
          id = random.nextInt(500)
        }
        val row: String = s"$id,fName$i,mName$i,lName$i,$title,$address,$country,$phone,$dob," +
            s"$birthtime," +
            s"$age,$status,$email,$education,$gender,$weight,$height,$bloodGrp,$occupation," +
            s"$hasChildren,$numKids,$hasSiblings"
        if (opType == 4) { // have more updates than deletes
          if (random.nextInt(3) != 0) eventType = 1
          else eventType = random.nextInt(2) + 1
        }
        StringMessageProducer.pw.println(StringMessageProducer.getCurrTimeAsString + s"row id:$id" +
            s" and eventType : $eventType")
        if (hasDerby) eventType = performOpInDerby(conn, row, eventType)
        else performOpOnTempTable(conn, row, eventType)
        val data = new ProducerRecord[Long, String](topic, id, row + s",$eventType")
        producer.send(data)
      })
    } catch {
      case e: Exception => StringMessageProducer.pw.println("Got exception while creating row " +
          "in kafka" + e.getMessage)
    }
    if (hasDerby) {
      derbyTestUtils.closeDiscConnection(conn, true)
    } else {
      if (stmt != null) {
        StringMessageProducer.pw.println(StringMessageProducer.getCurrTimeAsString + "Executing " +
            "remaining batch .. ")
        StringMessageProducer.pw.flush()
        stmt.executeBatch()
        StringMessageProducer.pw.println(StringMessageProducer.getCurrTimeAsString + "Executed " +
            "batch.")
      }
      conn.close()
    }
    StringMessageProducer.pw.println(StringMessageProducer.getCurrTimeAsString + "Done producing " +
        "records...")
      StringMessageProducer.pw.flush()
    }

  def performOpOnTempTable(conn: Connection, row: String, eventType: Int): Int = {
    var updateEventType: Int = eventType
    var query: String = ""
    var numRows: Int = 0
    if (eventType == 0) { // insert
      query = getInsertStmt(row, eventType)
    } else if (eventType == 1) { // update
      query = getInsertStmt(row, eventType)
    } else if (eventType == 2) { // delete
      query = getDeleteStmt(row)
    }
    if (stmt == null) stmt = conn.createStatement()
    stmt.addBatch(query)
    batchSize = batchSize + 1
    if (batchSize == 5000) {
      StringMessageProducer.pw.println(StringMessageProducer.getCurrTimeAsString + "Executing " +
          "batch .. ")
      StringMessageProducer.pw.flush()
      stmt.executeBatch()
      StringMessageProducer.pw.println(StringMessageProducer.getCurrTimeAsString + "Executed " +
          "batch.")
      batchSize = 0
      stmt = null
    }
    updateEventType
    // write to file
  }

  def performOpInDerby(conn: Connection, row: String, eventType: Int): Int = {
    var updateEventType: Int = eventType
    var stmt: String = ""
    var numRows: Int = 0
    if (eventType == 0) { // insert
      stmt = getInsertStmt(row, eventType)
    } else if (eventType == 1) { // update
      stmt = getUpdateStmt(row)
    } else if (eventType == 2) { // delete
      stmt = getDeleteStmt(row)
    }
    // StringMessageProducer.pw.println(StringMessageProducer.getCurrTimeAsString + s"derby stmt " +
    // s"is : $stmt")
    // StringMessageProducer.pw.flush()
    try {
      numRows = conn.createStatement().executeUpdate(stmt)
      if (numRows == 0 && eventType == 1) {
        stmt = getInsertStmt(row, 0 )
        conn.createStatement().executeUpdate(stmt)
      }
    } catch {
      case se: SQLException => if (se.getSQLState.equalsIgnoreCase("23505") && eventType == 0 ) {
        stmt = getUpdateStmt(row)
        conn.createStatement().executeUpdate(stmt)
        updateEventType = 1
      }
    }
    updateEventType
    // write to file
  }

  def getInsertStmt(row: String, eventType: Int): String = {
    var stmt: String = ""
    val columnVal = row.split(",")
    if (eventType == 0) stmt = "insert into temp_persoon values("
    else stmt = "put into temp_persoon values ("
    for (i <- 0 to columnVal.length - 1) {
      if (i == 0 || i == 10 || i == 15 || i == 16 || i > 18 ) {
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
    stmt = "update temp_persoon set "
    for (i <- 0 to columnVal.length - 1) {
      if (i == 0 ) {
        // ignore id column
      }
      else if (i == 10 || i == 15 || i == 16 || i > 18) {
        stmt = stmt + schema{i} + "=" + columnVal {i}
      } else {
        stmt = stmt + schema{i} + "='" + columnVal {i} + "'"
      }
      if (i != (columnVal.length - 1) && i != 0) {
        stmt = stmt + ","
      }
    }
    stmt = stmt + " where ID = " + id
    stmt
  }

  def getDeleteStmt(row: String): String = {
    var stmt: String = ""
    val id: Int = (row.split(",") {0}).toInt
    stmt = "delete from temp_persoon where ID = " + id
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

