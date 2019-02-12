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

package org.apache.spark.sql.streaming

import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.io.Path

import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.security.{LdapTestServer, SecurityTestUtils}
import io.snappydata.{Constant, SnappyFunSuite}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.kafka010.KafkaTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

class SnappyStoreSinkProviderSecuritySuite extends SnappyFunSuite
    with BeforeAndAfter with BeforeAndAfterAll {
  private val user1 = "gemfire1"
  private val user2 = "gemfire2"
  private val user5 = "gemfire5"
  private val sysUser = "gemfire10"
  private val ldapGroup = "gemGroup1"
  lazy val session: SparkSession = snc.newSession().snappySession

  private var kafkaTestUtils: KafkaTestUtils = _

  private val testIdGenerator = new AtomicInteger(0)
  private val tableName = "USERS"
  private val checkpointDirectory = "/tmp/SnappyStoreSinkProviderSecuritySuite"

  private def getTopic(id: Int) = s"topic-$id"

  override def beforeAll() {
    super.beforeAll()
    this.stopAll()
    kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()

    session.conf.set(Attribute.USERNAME_ATTR, sysUser)
    session.conf.set(Attribute.PASSWORD_ATTR, sysUser)

    session.sql(s"CREATE SCHEMA $ldapGroup AUTHORIZATION ldapgroup:$ldapGroup;")
  }

  override def afterAll() {
    super.afterAll()
    this.stopAll()
    if (kafkaTestUtils != null) {
      kafkaTestUtils.teardown()
      kafkaTestUtils = null
    }
    val ldapServer = LdapTestServer.getInstance()
    if (ldapServer.isServerStarted) {
      ldapServer.stopService()
    }
    import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SEARCH_BASE, AUTH_LDAP_SERVER}
    for (k <- List(Attribute.AUTH_PROVIDER, AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE)) {
      System.clearProperty(k)
      System.clearProperty("gemfirexd." + k)
      System.clearProperty(Constant.STORE_PROPERTY_PREFIX + k)
    }
    System.clearProperty(Constant.STORE_PROPERTY_PREFIX + Attribute.USERNAME_ATTR)
    System.clearProperty(Constant.STORE_PROPERTY_PREFIX + Attribute.PASSWORD_ATTR)
    System.setProperty("gemfirexd.authentication.required", "false")
  }

  after {

    session.conf.set(Attribute.USERNAME_ATTR, user1)
    session.conf.set(Attribute.PASSWORD_ATTR, user1)
    session.sql(s"DROP TABLE IF EXISTS $ldapGroup.${SnappyStoreSinkProvider.SINK_STATE_TABLE}")
    // CAUTION!! - recursively deleting checkpoint directory. handle with care.
    Path(checkpointDirectory).deleteRecursively()
  }

  protected override def newSparkConf(addOn: SparkConf => SparkConf): SparkConf = {
    val ldapProperties = SecurityTestUtils.startLdapServerAndGetBootProperties(0, 0, sysUser,
      getClass.getResource("/auth.ldif").getPath)
    import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SEARCH_BASE, AUTH_LDAP_SERVER}
    for (k <- List(Attribute.AUTH_PROVIDER, AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE)) {
      System.setProperty(k, ldapProperties.getProperty(k))
    }
    System.setProperty(Constant.STORE_PROPERTY_PREFIX + Attribute.USERNAME_ATTR, sysUser)
    System.setProperty(Constant.STORE_PROPERTY_PREFIX + Attribute.PASSWORD_ATTR, sysUser)
    val conf = new SparkConf()
        .setAppName("SnappySinkTest")
        .setMaster("local[3]")
        .set(Attribute.AUTH_PROVIDER, ldapProperties.getProperty(Attribute.AUTH_PROVIDER))
        .set(Constant.STORE_PROPERTY_PREFIX + Attribute.USERNAME_ATTR, sysUser)
        .set(Constant.STORE_PROPERTY_PREFIX + Attribute.PASSWORD_ATTR, sysUser)

    if (addOn != null) {
      addOn(conf)
    } else {
      conf
    }
  }


  test("state table schema not provided") {
    session.conf.set(Attribute.USERNAME_ATTR, user1)
    session.conf.set(Attribute.PASSWORD_ATTR, user1)

    val testId = testIdGenerator.getAndIncrement()
    createTable(user1)
    val topic = getTopic(testId)
    try{
      val streamingQuery: StreamingQuery = createAndStartStreamingQuery(topic, testId, false)
      streamingQuery.processAllAvailable()
      fail("IllegalStateException was expected")
    } catch {
      case x : IllegalStateException =>
        val expectedMessage = "stateTableSchema is a mandatory option when security is enabled."
        assert(x.getMessage.equals(expectedMessage))
    }
  }

  test("state table schema provided") {
    session.conf.set(Attribute.USERNAME_ATTR, user1)
    session.conf.set(Attribute.PASSWORD_ATTR, user1)

    val testId = testIdGenerator.getAndIncrement()
    createTable(user1)
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val dataBatch1 = Seq(Seq(1, "name1", 20, "lname1", 0), Seq(2, "name2", 10, "lname2", 0))
    kafkaTestUtils.sendMessages(topic, dataBatch1.map(r => r.mkString(",")).toArray)

    val streamingQuery = createAndStartStreamingQuery(topic, testId)
    waitTillTheBatchIsPickedForProcessing(0, testId)

    val dataBatch2 = Seq(Seq(1, "name11", 30, "lname1", 1), Seq(2, "name2", 13, "lname2", 2),
      Seq(3, "name3", 30, "lname3", 0), Seq(4, "name4", 10, "lname4", 2))
    kafkaTestUtils.sendMessages(topic, dataBatch2.map(r => r.mkString(",")).toArray)
    streamingQuery.processAllAvailable()
    streamingQuery.stop()
    assertData(Array(Row(1, "name11", 30, "lname1"), Row(3, "name3", 30, "lname3")))
  }

  test("query restart from different user in the same LDAP group") {
    session.conf.set(Attribute.USERNAME_ATTR, user1)
    session.conf.set(Attribute.PASSWORD_ATTR, user1)

    val testId = testIdGenerator.getAndIncrement()
    createTable(user1)
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val dataBatch1 = Seq(Seq(1, "name1", 20, "lname1", 0), Seq(2, "name2", 10, "lname2", 0))
    kafkaTestUtils.sendMessages(topic, dataBatch1.map(r => r.mkString(",")).toArray)

    val streamingQuery = createAndStartStreamingQuery(topic, testId)
    waitTillTheBatchIsPickedForProcessing(0, testId)
    streamingQuery.processAllAvailable()
    streamingQuery.stop()

    session.conf.set(Attribute.USERNAME_ATTR, user2)
    session.conf.set(Attribute.PASSWORD_ATTR, user2)

    val streamingQuery1 = createAndStartStreamingQuery(topic, testId)

    val dataBatch2 = Seq(Seq(1, "name11", 30, "lname1", 1), Seq(2, "name2", 13, "lname2", 2),
      Seq(3, "name3", 30, "lname3", 0), Seq(4, "name4", 10, "lname4", 2))
    kafkaTestUtils.sendMessages(topic, dataBatch2.map(r => r.mkString(",")).toArray)
    waitTillTheBatchIsPickedForProcessing(1, testId)
    streamingQuery1.processAllAvailable()
    streamingQuery1.stop()
    assertData(Array(Row(1, "name11", 30, "lname1"), Row(3, "name3", 30, "lname3")))
  }

  test("query restart from different user in a different LDAP group") {
    session.conf.set(Attribute.USERNAME_ATTR, user1)
    session.conf.set(Attribute.PASSWORD_ATTR, user1)

    val testId = testIdGenerator.getAndIncrement()
    createTable(user1)
    val topic = getTopic(testId)
    kafkaTestUtils.createTopic(topic, partitions = 3)

    val dataBatch1 = Seq(Seq(1, "name1", 20, "lname1", 0), Seq(2, "name2", 10, "lname2", 0))
    kafkaTestUtils.sendMessages(topic, dataBatch1.map(r => r.mkString(",")).toArray)

    val streamingQuery = createAndStartStreamingQuery(topic, testId)
    waitTillTheBatchIsPickedForProcessing(0, testId)
    streamingQuery.processAllAvailable()
    streamingQuery.stop()

    session.conf.set(Attribute.USERNAME_ATTR, user5)
    session.conf.set(Attribute.PASSWORD_ATTR, user5)

    val streamingQuery1 = createAndStartStreamingQuery(topic, testId)

    val dataBatch2 = Seq(Seq(1, "name11", 30, "lname1", 1), Seq(2, "name2", 13, "lname2", 2),
      Seq(3, "name3", 30, "lname3", 0), Seq(4, "name4", 10, "lname4", 2))
    kafkaTestUtils.sendMessages(topic, dataBatch2.map(r => r.mkString(",")).toArray)
    try{
      streamingQuery1.processAllAvailable()
      fail("StreamingQueryException was expected")
    } catch {
      case x: StreamingQueryException =>
        val expectedMessage = "User 'GEMFIRE5' does not have SELECT permission on column" +
            " 'STREAM_QUERY_ID' of table 'GEMGROUP1'.'SNAPPYSYS_INTERNAL____SINK_STATE_TABLE'."
        assert(x.getCause.getCause.getMessage.equals(expectedMessage))
    } finally {
      streamingQuery1.stop()
    }
  }

  private def waitTillTheBatchIsPickedForProcessing(batchId: Int, testId: Int,
      retries: Int = 15): Unit = {
    if (retries == 0) {
      throw new RuntimeException(s"Batch id $batchId not found in sink status table")
    }
    val sqlString = s"select batch_id from $ldapGroup.${SnappyStoreSinkProvider.SINK_STATE_TABLE}" +
        s" where stream_query_id = '${streamName(testId)}'"
    val batchIdFromTable = session.sql(sqlString).collect()

    if (batchIdFromTable.isEmpty || batchIdFromTable(0)(0) != batchId) {
      Thread.sleep(1000)
      waitTillTheBatchIsPickedForProcessing(batchId, testId , retries - 1)
    }
  }

  private def assertData(expectedData: Array[Row]) = {
    val actualData = session.sql(s"select * from $ldapGroup.$tableName " +
        s"order by id, last_name").collect()
    assertResult(expectedData)(actualData)
  }

  private def createTable(schema: String = null) = {
    val qualifiedTableName = s"$ldapGroup.$tableName"
    session.sql(s"drop table if exists $qualifiedTableName")
    val s = s"create table $qualifiedTableName " +
        s"(id long , first_name varchar(40), age int, " +
        s"last_name varchar(40)) using column options(key_columns 'id,last_name') "
    session.sql(s)
  }

  private def createAndStartStreamingQuery(topic: String, testId: Int,
      passStateTableSchema : Boolean = true) = {
    val streamingDF = session
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaTestUtils.brokerAddress)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()

    def structFields() = {
      StructField("id", LongType, nullable = false) ::
          StructField("firstName", StringType, nullable = true) ::
          StructField("age", IntegerType, nullable = true) ::
          StructField("last_name", StringType, nullable = true) ::
          StructField("_eventType", IntegerType, nullable = false) :: Nil
    }

    val schema = StructType(structFields())
    import session.implicits._

    implicit val encoder = RowEncoder(schema)

    val streamingQuery = streamingDF.selectExpr("CAST(value AS STRING)")
        .as[String]
        .map(_.split(","))
        .map(r => Row(r(0).toLong, r(1), r(2).toInt, r(3), r(4).toInt))
        .writeStream
        .format("snappySink")
        .queryName(streamName(testId))
        .trigger(ProcessingTime("1 seconds"))
        .option("tableName", s"$ldapGroup.$tableName")
        .option("checkpointLocation", checkpointDirectory)
        if(passStateTableSchema){
          streamingQuery.option("stateTableSchema", ldapGroup)
        }
        streamingQuery.start()
  }

  private def streamName(testId: Int) = {
    s"USERS_$testId"
  }
}