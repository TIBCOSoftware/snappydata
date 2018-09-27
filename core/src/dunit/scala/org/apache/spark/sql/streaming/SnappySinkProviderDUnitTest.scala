/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import scala.reflect.io.Path
import scala.sys.process._
import scala.util.control.NonFatal

import io.snappydata.test.dunit.{AvailablePortHelper, DistributedTestBase, Host, VM}
import io.snappydata.test.util.TestException
import io.snappydata.util.TestUtils
import io.snappydata.{Constant, StreamingConstants}

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.kafka010.KafkaTestUtils
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SnappyContext, ThinClientConnectorMode}
import org.apache.spark.{Logging, SparkConf, SparkContext}

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
    vm = host.getVM(3)
  }


  override def beforeClass(): Unit = {
    super.beforeClass()

    // create locators, leads and servers files
    val port = AvailablePortHelper.getRandomAvailableTCPPort
    val netPort = SnappySinkProviderDUnitTest.locatorNetPort
    val netPort2 = AvailablePortHelper.getRandomAvailableTCPPort
    val netPort3 = AvailablePortHelper.getRandomAvailableTCPPort

    logInfo(s"port: $port, netPort: $netPort,netPort2: $netPort2,netPort3: $netPort3")

    logInfo(s"Starting snappy cluster in $snappyProductDir/work with locator client port $netPort")

    val compressionArg = this.compressionArg

    val waitForInit = "-jobserver.waitForInitialization=true"
    val confDir = s"$snappyProductDir/conf"
    writeToFile(s"localhost  -peer-discovery-port=$port -client-port=$netPort",
      s"$confDir/locators")
    writeToFile(s"localhost  -locators=localhost[$port] $waitForInit $compressionArg",
      s"$confDir/leads")
    writeToFile(
      s"""localhost  -locators=localhost[$port] -client-port=$netPort2 $compressionArg
         |localhost  -locators=localhost[$port] -client-port=$netPort3 $compressionArg
         |""".stripMargin, s"$confDir/servers")

    (snappyProductDir + "/sbin/snappy-start-all.sh").!!

    vm.invoke(getClass, "startSparkCluster", productDir)

  }

  override def afterClass(): Unit = {
    super.afterClass()
    vm.invoke(getClass, "stopSparkCluster", productDir)

    logInfo(s"Stopping snappy cluster in $snappyProductDir/work")
    (snappyProductDir + "/sbin/snappy-stop-all.sh").!!
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "locators"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "leads"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "servers"))
  }

  private val snappyProductDir = getEnvironmentVariable("SNAPPY_HOME")

  private val productDir = getEnvironmentVariable("APACHE_SPARK_HOME")


  def testStructuredStreaming(): Unit = {
    vm.invoke(getClass, "doTestStructuredStreaming",
      Int.box(SnappySinkProviderDUnitTest.locatorNetPort))
  }

  def testIdempotency(): Unit = {
    vm.invoke(getClass, "doTestIdempotency",
      Int.box(SnappySinkProviderDUnitTest.locatorNetPort))
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

  private val tableName = "APP.USERS"
  private val checkpointDirectory = "/tmp/SnappyStoreSinkProviderDUnitTest"
  private val streamQueryId = s"USERS"
  private val locatorNetPort = AvailablePortHelper.getRandomAvailableTCPPort
  private val kafkaTestUtils = new KafkaTestUtils
  private var snc: SnappyContext = _

  private def setup(locatorClientPort: Int): Unit = {
    kafkaTestUtils.setup()
    snc = getSnappyContextForConnector(locatorClientPort)
    createTable(snc)
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
    setup(locatorClientPort)
    try {
      val topic = "test_topic"

      kafkaTestUtils.createTopic(topic, partitions = 3)

      val dataBatch1 = Seq(Seq(1, "name1", 20, 0), Seq(2, "name2", 10, 0))
      kafkaTestUtils.sendMessages(topic, dataBatch1.map(r => r.mkString(",")).toArray)

      val streamingQuery: StreamingQuery = createAndStartStreamingQuery(snc, topic,
        kafkaTestUtils.brokerAddress)
      waitTillTheBatchIsPickedForProcessing(snc, 0)

      val dataBatch2 = Seq(Seq(1, "name11", 30, 1), Seq(2, "name2", 10, 2), Seq(3, "name3", 30, 0))
      kafkaTestUtils.sendMessages(topic, dataBatch2.map(r => r.mkString(",")).toArray)
      streamingQuery.processAllAvailable()

      assertData(snc, Array(Row(1, "name11", 30), Row(3, "name3", 30)))
    } finally {
      teardown()
    }
  }

  private def teardown() = {
    Path(checkpointDirectory).deleteRecursively()
    kafkaTestUtils.teardown()
  }

  def doTestIdempotency(locatorClientPort: Int): Unit = {
    setup(locatorClientPort)
    try {
      val topic = "topic"
      kafkaTestUtils.createTopic(topic, partitions = 3)

      kafkaTestUtils.sendMessages(topic, (0 to 10).map(i => s"$i,name$i,$i,0").toArray)

      val streamingQuery: StreamingQuery = createAndStartStreamingQuery(snc, topic,
        kafkaTestUtils.brokerAddress)
      waitTillTheBatchIsPickedForProcessing(snc, 0)
      streamingQuery.stop()

      val streamingQuery1 = createAndStartStreamingQuery(snc, topic, kafkaTestUtils.brokerAddress,
        true, true)
      kafkaTestUtils.sendMessages(topic, (11 to 20).map(i => s"$i,name$i,$i,0").toArray)
      try {
        streamingQuery1.processAllAvailable()
      } catch {
        case ex: StreamingQueryException if ex.cause.getMessage == "dummy failure for test" =>
          streamingQuery1.stop()
      }

      val streamingQuery2 = createAndStartStreamingQuery(snc, topic, kafkaTestUtils.brokerAddress)

      kafkaTestUtils.sendMessages(topic, (21 to 30).map(i => s"$i,name$i,$i,0").toArray)
      waitTillTheBatchIsPickedForProcessing(snc, 1)
      streamingQuery2.processAllAvailable()

      assertData(snc, (0 to 30).map(i => Row(i, s"name$i", i)).toArray)
    } finally {
      teardown()
    }
  }

  private def createAndStartStreamingQuery(snc: SnappyContext, topic: String, brokerAddress: String,
      withEventTypeColumn: Boolean = true, failBatch: Boolean = false) = {
    val streamingDF = snc
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokerAddress)
        .option("subscribe", topic)
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
        .queryName("users")
        .trigger(ProcessingTime("1 seconds"))
        .option("tableName", tableName)
        .option("streamQueryId", streamQueryId)
        .option("checkpointLocation", checkpointDirectory)
    if (failBatch) {
      streamWriter.option("internal___failBatch", "true").start()
    }
    else {
      streamWriter.start()
    }
  }

  private def createTable(snc: SnappyContext) = {
    snc.sql(s"drop table if exists $tableName")
    snc.sql(
      s"""create table $tableName (id long , name varchar(40), age int)
        using column options(key_columns 'id')""")
  }

  private def assertData(snc: SnappyContext, expectedData: Array[Row]) = {
    val actualData = snc.sql(s"select * from $tableName order by id").collect()

    assert(expectedData sameElements actualData, "actual data:" +
        actualData.map(a => a.toString()).mkString(","))
  }

  private def waitTillTheBatchIsPickedForProcessing(snc: SnappyContext, batchId: Int,
      retries: Int = 15): Unit = {
    if (retries == 0) {
      throw new RuntimeException(s"Batch id $batchId not found in sink status table")
    }
    val sql = s"select batch_id from ${StreamingConstants.SINK_STATE_TABLE} " +
        s"where stream_query_id = '$streamQueryId'"
    val batchIdFromTable = snc.sql(sql).collect()

    if (batchIdFromTable.isEmpty || batchIdFromTable(0)(0) != batchId) {
      Thread.sleep(1000)
      waitTillTheBatchIsPickedForProcessing(snc, batchId, retries - 1)
    }
  }


  def getSnappyContextForConnector(locatorClientPort: Int): SnappyContext = {
    val hostName = InetAddress.getLocalHost.getHostName
    val connectionURL = s"localhost:$locatorClientPort"
    val conf = new SparkConf()
        .setAppName("test Application")
        .setMaster(s"spark://$hostName:7077")
        .set("spark.executor.cores", TestUtils.defaultCores.toString)
        .set("spark.executor.extraClassPath",
          getEnvironmentVariable("SNAPPY_DIST_CLASSPATH"))
        .set("snappydata.connection", connectionURL)


    val sc = SparkContext.getOrCreate(conf)
    //      sc.setLogLevel("DEBUG")
    //      Logger.getLogger("org").setLevel(Level.DEBUG)
    //      Logger.getLogger("akka").setLevel(Level.DEBUG)
    val snc = SnappyContext(sc)

    val mode = SnappyContext.getClusterMode(snc.sparkContext)
    mode match {
      case ThinClientConnectorMode(_, _) => // expected
      case _ => assert(assertion = false, "cluster mode is " + mode)
    }
    snc
  }
}
