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

import java.io.PrintWriter
import java.net.InetAddress
import java.nio.file.{Files, Paths}
import java.sql.Connection
import java.util.Properties
import java.util.concurrent.atomic.AtomicInteger

import scala.reflect.io.Path
import scala.sys.process._
import scala.util.control.NonFatal

import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SEARCH_BASE, AUTH_LDAP_SERVER}
import com.pivotal.gemfirexd.security.{LdapTestServer, SecurityTestUtils}
import io.snappydata.Constant
import io.snappydata.cluster.SplitClusterDUnitTest
import io.snappydata.test.dunit.{AvailablePortHelper, DistributedTestBase, Host, VM}
import io.snappydata.test.util.TestException
import io.snappydata.util.TestUtils

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.kafka010.KafkaTestUtils
import org.apache.spark.sql.streaming.SnappySinkProviderDUnitTest.{adminUser, getConn, ldapGroup, locatorNetPort}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SnappyContext, SnappySession, ThinClientConnectorMode}
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * Contains tests for streaming sink in smart connector mode
 */
class SnappySinkProviderDUnitTest(s: String)
    extends DistributedTestBase(s)
        with Logging
        with Serializable {

  // reduce minimum compression size so that it happens for all the values for testing
  private def compressionMinSize = "128"

  private def compressionArg: String = s"-D${Constant.COMPRESSION_MIN_SIZE}=$compressionMinSize"

  System.setProperty(Constant.COMPRESSION_MIN_SIZE, compressionMinSize)

  private[this] var host: Host = _
  var vm: VM = _

  if (Host.getHostCount > 0) {
    host = Host.getHost(0)
    vm = host.getVM(0)
  }

  private[this] var ldapProperties: Properties = new Properties()

  def setSecurityProps(): Unit = {
    import com.pivotal.gemfirexd.Property.{AUTH_LDAP_SEARCH_BASE, AUTH_LDAP_SERVER}
    ldapProperties = SecurityTestUtils.startLdapServerAndGetBootProperties(0, 0,
      adminUser, getClass.getResource("/auth.ldif").getPath)
    for (k <- List(Attribute.AUTH_PROVIDER, AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE)) {
      System.setProperty(k, ldapProperties.getProperty(k))
    }
  }

  def getLdapConf: String = {
    var conf = ""
    for (k <- List(Attribute.AUTH_PROVIDER, Attribute.USERNAME_ATTR, Attribute.PASSWORD_ATTR)) {
      conf += s"-$k=${ldapProperties.getProperty(k)} "
    }
    for (k <- List(AUTH_LDAP_SERVER, AUTH_LDAP_SEARCH_BASE)) {
      conf += s"-J-D$k=${ldapProperties.getProperty(k)} "
    }
    conf
  }

  override def beforeClass(): Unit = {
    super.beforeClass()

    setSecurityProps()

    // create locators, leads and servers files
    val port = AvailablePortHelper.getRandomAvailableTCPPort
    val netPort = locatorNetPort
    val netPort2 = AvailablePortHelper.getRandomAvailableTCPPort
    val netPort3 = AvailablePortHelper.getRandomAvailableTCPPort

    logInfo(s"Starting snappy cluster in $snappyProductDir/work with locator client port $netPort")

    val compressionArg = this.compressionArg

    val waitForInit = "-jobserver.waitForInitialization=true"
    val confDir = s"$snappyProductDir/conf"
    val ldapConf = getLdapConf
    writeToFile(s"localhost  -peer-discovery-port=$port -client-port=$netPort $ldapConf",
      s"$confDir/locators")
    writeToFile(s"localhost  -locators=localhost[$port] $waitForInit $compressionArg $ldapConf",
      s"$confDir/leads")
    writeToFile(
      s"""localhost  -locators=localhost[$port] -client-port=$netPort2 $compressionArg $ldapConf
         |localhost  -locators=localhost[$port] -client-port=$netPort3 $compressionArg $ldapConf
         |""".stripMargin, s"$confDir/servers")

    val op = (snappyProductDir + "/sbin/snappy-start-all.sh").!!
    logInfo("snappy-start-all output:" + op)

    vm.invoke(getClass, "startSparkCluster", sparkProductDir)

    var connection: Connection = null
    try {
      connection = getConn(adminUser)
      val statement = connection.createStatement()
      statement.execute(s"CREATE SCHEMA ${ldapGroup}" +
          s" AUTHORIZATION ldapgroup:${ldapGroup};")
      statement.close()
    } finally {
      connection.close()
    }
  }

  def stopLdapTestServer(): Unit = {
    val ldapServer = LdapTestServer.getInstance()
    if (ldapServer.isServerStarted) {
      ldapServer.stopService()
    }
  }

  override def afterClass(): Unit = {
    super.afterClass()
    vm.invoke(getClass, "stopSparkCluster", sparkProductDir)
    stopLdapTestServer()
    logInfo(s"Stopping snappy cluster in $snappyProductDir/work")
    (snappyProductDir + "/sbin/snappy-stop-all.sh").!!
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "locators"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "leads"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "servers"))
  }

  private val snappyProductDir = getEnvironmentVariable("SNAPPY_HOME")

  private val sparkProductDir = getEnvironmentVariable("APACHE_SPARK_HOME")

  def testStructuredStreaming(): Unit = {
    vm.invoke(getClass, "doTestStructuredStreaming",
      Int.box(locatorNetPort))
  }

  def testIdempotency(): Unit = {
    vm.invoke(getClass, "doTestIdempotency",
      Int.box(locatorNetPort))
  }

  def testCustomCallback(): Unit = {
    vm.invoke(getClass, "doTestCustomCallback",
      Int.box(locatorNetPort))
  }

  def testStateTableSchemaNotProvided(): Unit = {
    vm.invoke(getClass, "doTestStateTableSchemaNotProvided",
      Int.box(locatorNetPort))
  }

  private def writeToFile(str: String, fileName: String): Unit = {
    val pw = new PrintWriter(fileName)
    try {
      pw.write(str)
      pw.flush()
    } finally {
      pw.close()
    }
    // wait until file becomes available (e.g. running on NFS)
    var matched = false
    while (!matched) {
      Thread.sleep(100)
      try {
        val source = scala.io.Source.fromFile(fileName)
        val lines = try {
          source.mkString
        } finally {
          source.close()
        }
        matched = lines == str
      } catch {
        case NonFatal(_) =>
      }
    }
  }

  def getEnvironmentVariable(env: String): String = {
    val value = scala.util.Properties.envOrElse(env, null)
    if (env == null) {
      throw new TestException(s"Environment variable $env is not defined")
    }
    value
  }
}

object SnappySinkProviderDUnitTest extends Logging {

  private val tableName = "USERS"
  private val checkpointDirectory = "/tmp/SnappyStoreSinkProviderDUnitTest"
  private val streamQueryId = s"USERS"
  private val locatorNetPort = AvailablePortHelper.getRandomAvailableTCPPort
  private val kafkaTestUtils = new KafkaTestUtils
  private var snc: SnappyContext = _
  private val testIdGenerator = new AtomicInteger(0)
  private val user = "gemfire1"
  private val adminUser = "gemfire10"
  val ldapGroup = "gemGroup1"

  private def setup(locatorClientPort: Int): Unit = {
    kafkaTestUtils.setup()
    val props = new Properties()
    props.setProperty(Attribute.USERNAME_ATTR, user)
    props.setProperty(Attribute.PASSWORD_ATTR, user)
    snc = getSnappyContextForConnector(locatorClientPort, props)
    createTable()
  }

  private def getConn(u: String): Connection = {
    val props = new Properties()
    props.setProperty(Attribute.USERNAME_ATTR, u)
    props.setProperty(Attribute.PASSWORD_ATTR, u)
    SplitClusterDUnitTest.getConnection(locatorNetPort, props)
  }

  private def teardown() = {
    Path(checkpointDirectory).deleteRecursively()

    kafkaTestUtils.teardown()
  }

  def getEnvironmentVariable(env: String): String = {
    val value = scala.util.Properties.envOrElse(env, null)
    if (env == null) {
      throw new TestException(s"Environment variable $env is not defined")
    }
    value
  }

  def startSparkCluster(productDir: String): Unit = {
    logInfo(s"Starting spark cluster in $productDir/work")
    (productDir + "/sbin/start-all.sh").!!
  }

  def stopSparkCluster(productDir: String): Unit = {
    val sparkContext = SnappyContext.globalSparkContext
    logInfo(s"Stopping spark cluster in $productDir/work")
    if (sparkContext != null) sparkContext.stop()
    (productDir + "/sbin/stop-all.sh").!!
  }

  def doTestStructuredStreaming(locatorClientPort: Int): Unit = {
    try {
      val testId = s"test_${testIdGenerator.getAndIncrement()}"
      setup(locatorClientPort)

      kafkaTestUtils.createTopic(testId, partitions = 3)

      val dataBatch1 = Seq(Seq(1, "name1", 20, 2), Seq(1, "name1", 20, 0), Seq(2, "name2", 10, 0))
      kafkaTestUtils.sendMessages(testId, dataBatch1.map(r => r.mkString(",")).toArray)

      val streamingQuery: StreamingQuery = createAndStartStreamingQuery(testId)
      snc.sql(s"select * from ${ldapGroup}.${SnappyStoreSinkProvider.SINK_STATE_TABLE}").show()
      waitTillTheBatchIsPickedForProcessing(0, testId)

      val dataBatch2 = Seq(Seq(1, "name11", 30, 1), Seq(2, "name2", 10, 2), Seq(3, "name3", 30, 0))
      kafkaTestUtils.sendMessages(testId, dataBatch2.map(r => r.mkString(",")).toArray)
      streamingQuery.processAllAvailable()

      assertData(Array(Row(1, "name11", 30), Row(3, "name3", 30)))
      streamingQuery.stop()
    } finally {
      teardown()
    }
  }

  def doTestIdempotency(locatorClientPort: Int): Unit = {
    try {
      val testId = s"test_${testIdGenerator.getAndIncrement()}"
      setup(locatorClientPort)
      kafkaTestUtils.createTopic(testId, partitions = 3)

      kafkaTestUtils.sendMessages(testId, (0 to 10).map(i => s"$i,name$i,$i,0").toArray)

      val streamingQuery: StreamingQuery = createAndStartStreamingQuery(testId)
      waitTillTheBatchIsPickedForProcessing(0, testId)
      streamingQuery.stop()

      val streamingQuery1 = createAndStartStreamingQuery(testId, true, true)
      kafkaTestUtils.sendMessages(testId, (11 to 20).map(i => s"$i,name$i,$i,0").toArray)
      try {
        streamingQuery1.processAllAvailable()
      } catch {
        case ex: StreamingQueryException if ex.cause.getMessage == "dummy failure for test" =>
          streamingQuery1.stop()
      }

      val streamingQuery2 = createAndStartStreamingQuery(testId)

      kafkaTestUtils.sendMessages(testId, (21 to 30).map(i => s"$i,name$i,$i,0").toArray)
      waitTillTheBatchIsPickedForProcessing(1, testId)
      streamingQuery2.processAllAvailable()

      assertData((0 to 30).map(i => Row(i, s"name$i", i)).toArray)
      streamingQuery.stop()
    } finally {
      teardown()
    }
  }


  def doTestCustomCallback(locatorClientPort: Int): Unit = {
    try {
      val testId = s"test_${testIdGenerator.getAndIncrement()}"
      setup(locatorClientPort)
      kafkaTestUtils.createTopic(testId, partitions = 3)

      val dataBatch = Seq(Seq(1, "name1", 20, 0), Seq(1, "name2", 10, 0))
      kafkaTestUtils.sendMessages(testId, dataBatch.map(_.mkString(",")).toArray)

      val streamingQuery = createAndStartStreamingQuery(testId,
        withEventTypeColumn = false, withCustomCallback = true)
      waitTillTheBatchIsPickedForProcessing(0, testId)

      streamingQuery.processAllAvailable()
      assertData(Array(Row(1, "name1", 20), Row(1, "name2", 10)))
      streamingQuery.stop()
    } finally {
      teardown()
    }
  }

  def doTestStateTableSchemaNotProvided(locatorClientPort: Int): Unit = {
    try {
      val testId = s"TEST_${testIdGenerator.getAndIncrement()}"
      setup(locatorClientPort)

      kafkaTestUtils.createTopic(testId, partitions = 3)

      val dataBatch1 = Seq(Seq(1, "name1", 20, 2), Seq(1, "name1", 20, 0), Seq(2, "name2", 10, 0))
      kafkaTestUtils.sendMessages(testId, dataBatch1.map(r => r.mkString(",")).toArray)

      try {
        val streamingQuery: StreamingQuery = createAndStartStreamingQuery(testId,
          provideStateTableSchema = false)
        streamingQuery.processAllAvailable()
        throw new RuntimeException("IllegalStateException was expected")
      } catch {
        case x: IllegalStateException =>
          val expectedMessage = "'stateTableSchema' is a mandatory option when security is enabled."
          assert(x.getMessage.equals(expectedMessage))
      }
    } finally {
      teardown()
    }
  }

  private def createAndStartStreamingQuery(testId: String,
      withEventTypeColumn: Boolean = true, failBatch: Boolean = false,
      withCustomCallback: Boolean = false, provideStateTableSchema: Boolean = true) = {
    val streamingDF = snc
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafkaTestUtils.brokerAddress)
        .option("subscribe", testId)
        .option("startingOffsets", "earliest")
        .load()

    def structFields() = {
      StructField("id", LongType, nullable = false) ::
          StructField("name", StringType, nullable = true) ::
          StructField("age", IntegerType, nullable = true) ::
          (if (withEventTypeColumn) {
            StructField("_eventType", IntegerType, nullable = false) :: Nil
          }
          else {
            Nil
          })
    }

    val schema = StructType(structFields())

    implicit val encoder = RowEncoder(schema)
    val session = snc.sparkSession
    import session.implicits._
    val streamWriter = streamingDF.selectExpr("CAST(value AS STRING)")
        .as[String]
        .map(_.split(","))
        .map(r => {
          if (r.length == 4) {
            Row(r(0).toLong, r(1), r(2).toInt, r(3).toInt)
          } else {
            Row(r(0).toLong, r(1), r(2).toInt)
          }
        })
        .writeStream
        .format("snappysink")
        .queryName(testId)
        .trigger(ProcessingTime("1 seconds"))
        .option("tableName", s"$ldapGroup.$tableName")
        .option("checkpointLocation", checkpointDirectory)
    if (provideStateTableSchema) {
      streamWriter.option("stateTableSchema", ldapGroup)
    }
    if (failBatch) {
      streamWriter.option("internal___failBatch", "true")
    }
    if (withCustomCallback) {
      streamWriter.option("sinkCallback", classOf[TestSinkCallback].getName)
    }
    streamWriter.start()
  }

  private def createTable() = {
    snc.sql(s"drop table if exists ${ldapGroup}.$tableName")
    snc.sql(
      s"""create table ${ldapGroup}.$tableName
       (id long , name varchar(40), age int) using column options(key_columns 'id')""")
  }

  private def assertData(expectedData: Array[Row]): Unit = {
    val actualData = snc.sql(s"select * from ${ldapGroup}.$tableName" +
        s" order by id, name, age")
        .collect()

    assert(expectedData sameElements actualData, "actual data:" +
        actualData.map(a => a.toString()).mkString(","))
  }

  private def waitTillTheBatchIsPickedForProcessing(batchId: Int, testId: String,
      retries: Int = 15): Unit = {
    if (retries == 0) {
      throw new RuntimeException(s"Batch id $batchId not found in sink status table")
    }
    val sql = s"select batch_id from ${ldapGroup}.${SnappyStoreSinkProvider.SINK_STATE_TABLE} " +
        s"where stream_query_id = '$testId'"
    val batchIdFromTable = snc.sql(sql).collect()

    if (batchIdFromTable.isEmpty || batchIdFromTable(0)(0) != batchId) {
      Thread.sleep(1000)
      waitTillTheBatchIsPickedForProcessing(batchId, testId, retries - 1)
    }
  }


  def getSnappyContextForConnector(locatorClientPort: Int, props: Properties = null):
  SnappyContext = {
    val hostName = InetAddress.getLocalHost.getHostName
    val connectionURL = s"localhost:$locatorClientPort"
    val conf = new SparkConf()
        .setAppName("test Application")
        .setMaster(s"spark://$hostName:7077")
        .set("spark.executor.cores", TestUtils.defaultCoresForSmartConnector)
        .set("spark.executor.extraClassPath",
          getEnvironmentVariable("SNAPPY_DIST_CLASSPATH"))
        .set("snappydata.connection", connectionURL)

    conf.set(Constant.SPARK_STORE_PREFIX + Attribute.USERNAME_ATTR, user)
    conf.set(Constant.SPARK_STORE_PREFIX + Attribute.PASSWORD_ATTR, user)

    val snc = SnappyContext(SparkContext.getOrCreate(conf))

    val mode = SnappyContext.getClusterMode(snc.sparkContext)
    mode match {
      case ThinClientConnectorMode(_, _) => // expected
      case _ => assert(assertion = false, "cluster mode is " + mode)
    }
    snc
  }

  class TestSinkCallback extends SnappySinkCallback {
    override def process(snappySession: SnappySession, sinkProps: Map[String, String],
        batchId: Long, df: Dataset[Row], possibleDuplicate: Boolean): Unit = {
      df.write.insertInto(s"${SnappySinkProviderDUnitTest.ldapGroup}.$tableName")
    }
  }

}