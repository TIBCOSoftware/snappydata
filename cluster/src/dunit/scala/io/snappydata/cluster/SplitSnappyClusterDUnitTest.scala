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
package io.snappydata.cluster

import java.io.PrintWriter
import java.net.InetAddress
import java.nio.file.{Files, Paths}
import java.util.Properties

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.language.postfixOps
import scala.reflect.io.Path
import scala.util.{Failure, Success, Try}
import com.gemstone.gemfire.internal.cache.PartitionedRegion
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.core.{TestData, TestData2}
import io.snappydata.test.dunit.{AvailablePortHelper, SerializableRunnable}
import io.snappydata.util.TestUtils
import io.snappydata.{ColumnUpdateDeleteTests, ConcurrentOpsTests, Property, SnappyTableStatsProviderService}
import org.junit.Assert
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.execution.CatalogStaleException
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.sql.kafka010.KafkaTestUtils
import org.apache.spark.sql.store.{SnappyJoinSuite, StoreUtils}
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}
import org.apache.spark.sql.udf.UserDefinedFunctionsDUnitTest
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * Basic tests for non-embedded mode connections to an embedded cluster.
 */
class SplitSnappyClusterDUnitTest(s: String)
    extends ClusterManagerTestBase(s) with SplitClusterDUnitTestBase with Serializable {

  override val locatorNetPort: Int = testObject.locatorNetPort

  override val stopNetServersInTearDown = false

  val currentLocatorPort: Int = ClusterManagerTestBase.locPort

  override protected val sparkProductDir: String =
    testObject.getEnvironmentVariable("SNAPPY_HOME")

  override def beforeClass(): Unit = {
    // stop any existing SnappyContext to enable applying thrift-server properties
    val sc = SnappyContext.globalSparkContext
    if ((sc ne null) && !sc.isStopped) {
      ClusterManagerTestBase.stopSpark()
    }
    super.beforeClass()
    startNetworkServers()
    vm3.invoke(classOf[ClusterManagerTestBase], "startSparkCluster", sparkProductDir)
  }

  override def afterClass(): Unit = {
    Array(vm2, vm1, vm0).foreach(_.invoke(getClass, "stopNetworkServers"))
    ClusterManagerTestBase.stopNetworkServers()
    vm3.invoke(classOf[ClusterManagerTestBase], "stopSparkCluster", sparkProductDir)
    super.afterClass()
  }

  def testCreateTablesFromOtherTables(): Unit = {
    // stop a network server to test remote fetch
    vm0.invoke(classOf[ClusterManagerTestBase], "stopNetworkServers")
    vm3.invoke(getClass, "createTablesFromOtherTablesTest",
      startArgs :+
          Int.box(locatorClientPort))
  }

  override protected def locatorClientPort: Int = locatorNetPort

  override protected def startNetworkServers(): Unit = {
    startNetworkServersOnAllVMs()
  }

  override protected def testObject = SplitSnappyClusterDUnitTest

  def testCollocatedJoinInSplitModeRowTable(): Unit = {
    testObject.createRowTableForCollocatedJoin()
    vm3.invoke(getClass, "checkCollocatedJoins", startArgs :+
        "PR_TABLE1" :+ "PR_TABLE2" :+ Int.box(locatorClientPort))
  }

  def testCollocatedJoinInSplitModeColumnTable(): Unit = {
    testObject.createColumnTableForCollocatedJoin()
    vm3.invoke(getClass, "checkCollocatedJoins", startArgs :+
        "PR_TABLE3" :+ "PR_TABLE4" :+
        Int.box(locatorClientPort))
  }

  def testColumnTableStatsInSplitMode(): Unit = {
    vm3.invoke(getClass, "checkStatsForSplitMode", startArgs :+
        "1" :+ Int.box(locatorClientPort))
    vm3.invoke(getClass, "checkStatsForSplitMode", startArgs :+
        "8" :+ Int.box(locatorClientPort))
  }

  def testBatchSize(): Unit = {
    val snc = SnappyContext(sc)
    val tblBatchSizeSmall = "APP.tblBatchSizeSmall_embedded"
    val tblSizeBig = "APP.tblBatchSizeBig_embedded"
    val tblBatchSizeBig_split = "APP.tblBatchSizeBig_split"
    val tblBatchSizeSmall_split = "APP.tblBatchSizeSmall_split"

    snc.sql(s"drop table if exists $tblBatchSizeSmall")
    snc.sql(s"drop table if exists $tblSizeBig")
    snc.sql(s"drop table if exists $tblBatchSizeBig_split")
    snc.sql(s"drop table if exists $tblBatchSizeSmall_split")

    snc.sql(s"CREATE TABLE $tblBatchSizeSmall(Key1 INT ,Value STRING) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "BUCKETS '8', COLUMN_BATCH_SIZE '200')")

    snc.sql(s"CREATE TABLE $tblSizeBig (Key1 INT ,Value STRING) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "BUCKETS '8', COLUMN_BATCH_SIZE '200000')")

    val rdd = sc.parallelize(
      (1 to 100000).map(i => TestData(i, i.toString)))

    implicit val encoder: Encoder[TestData] = Encoders.product[TestData]
    val dataDF = snc.createDataset(rdd)

    dataDF.write.insertInto(tblBatchSizeSmall)
    dataDF.write.insertInto(tblSizeBig)

    // StandAlone Spark Cluster Operations
    vm3.invoke(getClass, "splitModeTableCreate",
      startArgs :+
          Int.box(locatorClientPort))

    assert(getShadowRegionSize(tblBatchSizeSmall) > 10,
      s"Expected batches should be greater than " +
          s"10 but are ${getShadowRegionSize(tblBatchSizeSmall)}")
    assert(getShadowRegionSize(tblSizeBig) > 0, s"Expected batches should be greater than " +
        s"0 but are ${getShadowRegionSize(tblSizeBig)}")
    assert(getShadowRegionSize(tblSizeBig) < 10, s"Expected batches should be less than " +
        s"10 but are ${getShadowRegionSize(tblSizeBig)}")

    assert(getShadowRegionSize(tblBatchSizeSmall_split) > 10,
      s"Expected batches should be greater than " +
          s"10 but are ${getShadowRegionSize(tblBatchSizeSmall_split)}")

    assert(getShadowRegionSize(tblBatchSizeBig_split) > 0,
      s"Expected batches should be greater than " +
          s"0 but are ${getShadowRegionSize(tblBatchSizeBig_split)}")

    assert(getShadowRegionSize(tblBatchSizeBig_split) < 10,
      s"Expected batches should be less than " +
          s"10 but are ${getShadowRegionSize(tblBatchSizeBig_split)}")

    logInfo("Test Completed Successfully")
  }

  def getRegionSize(tbl: String): Long = {
    Misc.getRegionForTable(tbl.toUpperCase,
      true).asInstanceOf[PartitionedRegion].size()

  }

  def getShadowRegionSize(tbl: String): Long = {
    // divide by three as 2 entries are for column and one is a base entry
    Misc.getRegionForTable(ColumnFormatRelation.
        columnBatchTableName(tbl).toUpperCase,
      true).asInstanceOf[PartitionedRegion].size() / 3
  }

  def testColumnTableStatsInSplitModeWithHA(): Unit = {
    vm3.invoke(getClass, "checkStatsForSplitMode", startArgs :+
        "1" :+ Int.box(locatorClientPort))
    val props = bootProps
    val port = currentLocatorPort

    val restartServer = new SerializableRunnable() {
      override def run(): Unit = ClusterManagerTestBase.startSnappyServer(port, props)
    }

    vm0.invoke(classOf[ClusterManagerTestBase], "stopAny")
    val stats = SnappyTableStatsProviderService.getService.
        getAggregatedStatsOnDemand._1("APP.SNAPPYTABLE")

    Assert.assertEquals(10000100, stats.getRowCount)
    vm0.invoke(restartServer)

    vm1.invoke(classOf[ClusterManagerTestBase], "stopAny")
    val stats1 = SnappyTableStatsProviderService.getService.
        getAggregatedStatsOnDemand._1("APP.SNAPPYTABLE")
    Assert.assertEquals(10000100, stats1.getRowCount)
    vm1.invoke(restartServer)

    // Test using using 5 buckets
    vm3.invoke(getClass, "checkStatsForSplitMode", startArgs :+
        "8" :+ Int.box(locatorClientPort))
    vm0.invoke(classOf[ClusterManagerTestBase], "stopAny")
    val stats2 = SnappyTableStatsProviderService.getService.
        getAggregatedStatsOnDemand._1("APP.SNAPPYTABLE")
    Assert.assertEquals(10000100, stats2.getRowCount)
    val snc = SnappyContext(sc)
    snc.sql("insert into snappyTable values(1,'Test')")
    SnappyTableStatsProviderService.getService.
        getAggregatedStatsOnDemand._1("APP.SNAPPYTABLE")
    vm0.invoke(restartServer)
  }

  def testCTAS(): Unit = {
    val snc = SnappyContext(sc)
    // StandAlone Spark Cluster Operations
    vm3.invoke(getClass, "splitModeCreateTableUsingCTAS",
      startArgs :+
          Int.box(locatorClientPort))

    val count = snc.sql("select * from customer").count()
    assert(count == 750, s"Expected 750 rows. Actual rows = $count")

    snc.sql("DROP TABLE CUSTOMER_STAGING")
    snc.sql("DROP TABLE CUSTOMER")

    val count2 = snc.sql("select * from customer_2").count()
    assert(count2 == 750, s"Expected 750 rows. Actual rows = $count2")
    snc.sql("DROP TABLE CUSTOMER_2")
  }

  def testUDF(): Unit = {
    doTestUDF(skewNetworkServers)
  }

  def doTestUDF(skewServerDistribution: Boolean): Unit = {
    testObject.createUDFInEmbeddedMode()

    // StandAlone Spark Cluster Operations
    vm3.invoke(getClass, "createUDFInSplitMode",
      startArgs :+ Int.box(locatorClientPort))

    testObject.verifyUDFInEmbeddedMode()

    // StandAlone Spark Cluster Operations
    vm3.invoke(getClass, "verifyUDFInSplitMode",
      startArgs :+ Int.box(locatorClientPort))
  }

  // frequently fails in precheckin
  def DISABLED_testDeployPackageNameFormat(): Unit = {
    val sns = new SnappySession(sc)
    try {
      val jarPath = s"$sparkProductDir/jars/hadoop-client-2.7.7.jar"
      sns.sql("deploy package  mongo_spark 'org.mongodb.spark:mongo-spark-connector_2.11:2.2.2'")
      sns.sql("deploy package mongo-spark_v1.0  'org.mongodb.spark:mongo-spark-" +
          "connector_2.11:2.2.2'")
      sns.sql("deploy package app.mongo-spark_v1.1  'org.mongodb.spark:mongo-spark-" +
          "connector_2.11:2.2.2'")
      sns.sql("deploy package testsch.mongo-spark_v1.2  'org.mongodb.spark:mongo-spark" +
          "-connector_2.11:2.2.2'")
      sns.sql(
        s"""deploy package "testsch"."mongo-spark_v1.3"  'org.mongodb.spark:mongo""" +
            "-spark-connector_2.11:2.2.2'")
      sns.sql(
        s"""deploy package testsch."mongo-spark_v1.4"  'org.mongodb.spark:mongo""" +
            "-spark-connector_2.11:2.2.2'")
      sns.sql(
        s"""deploy package "testsch".mongo-spark_v1.5  'org.mongodb.spark:mongo""" +
            "-spark-connector_2.11:2.2.2'")
      assert(sns.sql("list packages").count() == 7)

      sns.sql(s"""deploy jar avro-v_1.0 '$jarPath'""")
      sns.sql(s"""deploy jar app.avro-v_1.1 '$jarPath'""")
      sns.sql(s"""deploy jar testsch.avro-v_1.2 '$jarPath'""")
      sns.sql(s"""deploy jar "app".avro-v_1.3 '$jarPath'""")
      sns.sql(s"""deploy jar "testsch"."avro-v_1.4" '$jarPath'""")
      sns.sql(s"""deploy jar testsch."avro-v_1.5" '$jarPath'""")
      assert(sns.sql("list packages").count() == 13)
    }
    finally {
      sns.sql("undeploy  mongo_spark")
      sns.sql("undeploy  mongo-spark_v1.0")
      sns.sql("undeploy  app.mongo-spark_v1.1")
      sns.sql("undeploy  testsch.mongo-spark_v1.2")
      sns.sql(s"""undeploy  "testsch"."mongo-spark_v1.3" """)
      sns.sql(s"""undeploy  testsch."mongo-spark_v1.4" """)
      sns.sql(s"""undeploy "testsch".mongo-spark_v1.5 """)

      sns.sql("undeploy  avro-v_1.0 ")
      sns.sql("undeploy  app.avro-v_1.1")
      sns.sql("undeploy  testsch.avro-v_1.2")
      sns.sql(s"""undeploy "app".avro-v_1.3 """)
      sns.sql(s"""undeploy "testsch"."avro-v_1.4" """)
      sns.sql(s"""undeploy testsch."avro-v_1.5" """)
      assert(sns.sql("list packages").count() == 0)
    }
    import org.scalatest.Assertions._
    val thrown = intercept[Exception] {
      sns.sql("deploy package \"testsch\".mongo-###park_v1.5" +
          "  'org.mongodb.spark:mongo-spark-connector_2.11:2.2.2")
    }
    // scalastyle:off
    assert(thrown.getMessage === s"""Invalid input \"mongo-#\", expected packageIdentifierPart or stringLiteral (line 1, column 26):\ndeploy package \"testsch\".mongo-###park_v1.5  'org.mongodb.spark:mongo-spark-connector_2.11:2.2.2\n                         ^;""")
    // scalastyle:on
  }

  // always fails in precheckin
  def DISABLED_testDeployPackageDuplicateName(): Unit = {
    val sns = new SnappySession(sc)
    try {
      sns.sql("deploy package mongo-spark_v.1.5" +
          " 'org.mongodb.spark:mongo-spark-connector_2.11:2.2.2'")

      sns.sql("deploy package mongo-spark_v.1.5_dup" +
          "  'org.mongodb.spark:mongo-spark-connector_2.11:2.2.2'")

      assert(sns.sql("list packages").count() == 2)

      sns.sql("deploy package akka-v1 'com.typesafe.akka:akka-actor_2.11:2.5.8'")

      Try(sns.sql("deploy package akka-v1 'com.datastax.spark:" +
          "spark-cassandra-connector_2.11:2.3.2'")) match {
        case Success(_) => throw new AssertionError(
          "Deploy command should have failed because of the duplicate alias.")
        case Failure(error) => assert(error.getMessage == "Name 'akka-v1' specified in context" +
            " 'of deploying jars/packages' is not unique.")
      }
      assert(sns.sql("list packages").count() == 3)
    }
    finally {
      sns.sql("undeploy  mongo-spark_v.1.5")
      sns.sql("undeploy  mongo-spark_v.1.5_dup")
      sns.sql("undeploy  akka-v1")
      assert(sns.sql("list packages").count() == 0)
    }
  }

  override def testConcurrentOpsOnColumnTables(): Unit = {
    // check in embedded mode (connector mode tested in SplitClusterDUnitTest)
    val session = new SnappySession(sc)
    ConcurrentOpsTests.testSimpleLockInsert(session)
    ConcurrentOpsTests.testSimpleLockUpdate(session)
    ConcurrentOpsTests.testSimpleLockDeleteFrom(session)
    ConcurrentOpsTests.testSimpleLockPutInto(session)

    ConcurrentOpsTests.testConcurrentUpdate(session)
    ConcurrentOpsTests.testConcurrentPutInto(session)
    ConcurrentOpsTests.testConcurrentDelete(session)
    ConcurrentOpsTests.testConcurrentPutIntoUpdate(session)
    ConcurrentOpsTests.testAllOpsConcurrent(session)
    ConcurrentOpsTests.testConcurrentDeleteFromMultipleTables(session)
    ConcurrentOpsTests.testConcurrentPutIntoMultipleTables(session)
  }

  override def testUpdateDeleteOnColumnTables(): Unit = {
    // check in embedded mode (connector mode tested in SplitClusterDUnitTest)
    val session = new SnappySession(sc)
    // using random bucket assignment for this test to check remote iteration
    // added in SNAP-2012
    StoreUtils.TEST_RANDOM_BUCKETID_ASSIGNMENT = true
    try {
      ColumnUpdateDeleteTests.testBasicUpdate(session)
      ColumnUpdateDeleteTests.testDeltaStats(session)
      ColumnUpdateDeleteTests.testBasicDelete(session)
      ColumnUpdateDeleteTests.testSNAP1925(session)
      ColumnUpdateDeleteTests.testSNAP1926(session)
      ColumnUpdateDeleteTests.testConcurrentOps(session)
      ColumnUpdateDeleteTests.testSNAP2124(session)
    } finally {
      StoreUtils.TEST_RANDOM_BUCKETID_ASSIGNMENT = false
    }
  }

  def testStaleCatalog(): Unit = {

    val snc = SnappyContext(sc)
    snc.sql(s"CREATE TABLE T5(COL1 STRING, COL2 STRING) USING column OPTIONS" +
        s" (key_columns 'col1', PARTITION_BY 'COL1', COLUMN_MAX_DELTA_ROWS '1')")

    import scala.concurrent.ExecutionContext.Implicits.global
    val future = Future {
      vm3.invoke(getClass, "doTestStaleCatalog", startArgs :+ Int.box(locatorClientPort))
    }

    try {
      // wait till the smart connector job perform at-least one putInto operation
      var count = 0
      while (snc.table("T5").count() == 0 && count < 10) {
        Thread.sleep(4000)
        count += 1
      }
      assert(count != 10, "Smart connector application not performing putInto as expected.")

      // perform DDL
      snc.sql(s"CREATE TABLE T6(COL1 STRING, COL2 STRING) " +
          s"USING column OPTIONS (PARTITION_BY 'COL1', COLUMN_MAX_DELTA_ROWS '1')")

      Await.result(future, scala.concurrent.duration.Duration.apply(3, "min"))
    } finally {
      snc.sql("drop table if exists T6")
      snc.sql("drop table if exists T5")
    }
  }

  def testStaleCatalogRetryForStreamingSink(): Unit = {
    val snc = SnappyContext(sc)
    import scala.concurrent.ExecutionContext.Implicits.global
    val testTempDirectory = "/tmp/SplitSnappyClusterDUnitTest"

    def cleanUp(): Unit = {
      snc.sql("drop table if exists SYNC_TABLE")
      snc.sql("drop table if exists USERS")
      Path(testTempDirectory).deleteRecursively()
    }

    cleanUp()
    val future = Future {
      vm3.invoke(getClass, "doTestStaleCatalogRetryForStreamingSink",
        startArgs :+ Int.box(locatorClientPort) :+ testTempDirectory)
    }
    try {
      var attempts = 0
      while (!Files.exists(Paths.get(testTempDirectory, "file0")) && attempts < 15) {
        Thread.sleep(4000)
        attempts += 1
      }
      assert(attempts < 14, "No data ingested by streaming application.")

      // perform DDL leading to stale catalog in smart connector application
      snc.sql(s"CREATE TABLE SYNC_TABLE(COL1 STRING) " + s"USING column")

      new PrintWriter(s"$testTempDirectory/file1") {
        write("dummydata")
        close()
      }
      Await.result(future, Duration(2, "min"))
    } finally {
      cleanUp()
    }
  }

  def testSNAP3024(): Unit = {
    val snc = SnappyContext(sc)
    snc.sql(s"CREATE TABLE T5(COL1 STRING, COL2 STRING) USING column OPTIONS" +
        s" (key_columns 'col1', PARTITION_BY 'COL1', COLUMN_MAX_DELTA_ROWS '1')")
    snc.sql("insert into t5 values('1', '1')")
    snc.sql("insert into t5 values('2', '2')")
    snc.sql("insert into t5 values('3', '3')")

    import scala.concurrent.ExecutionContext.Implicits.global
    val future = Future {
      vm3.invoke(getClass, "doTestStaleCatalogForSNAP3024", startArgs :+ Int.box(locatorClientPort))
    }

    try {
      // wait till the smart connector job perform at-least one putInto operation
      var count = 0
      while (snc.table("T5").count() == 3 && count < 10) {
        Thread.sleep(4000)
        count += 1
      }
      assert(count != 10, "Smart connector application not performing putInto as expected.")

      // perform DDL
      snc.sql(s"CREATE TABLE T6(COL1 STRING, COL2 STRING) " +
          s"USING column OPTIONS (PARTITION_BY 'COL1', COLUMN_MAX_DELTA_ROWS '1')")

      Await.result(future, scala.concurrent.duration.Duration.apply(3, "min"))
    } finally {
      snc.sql("drop table if exists T6")
      snc.sql("drop table if exists T5")
    }
  }

  def testSmartConnectorAfterBucketRebalance(): Unit = {
    val snc = SnappyContext(sc)
    snc.sql(s"CREATE TABLE T5(COL1 STRING, COL2 STRING) USING column OPTIONS" +
        s" (key_columns 'col1', PARTITION_BY 'COL1', COLUMN_MAX_DELTA_ROWS '1')")
    snc.sql("insert into t5 values('1', '1')")
    snc.sql("insert into t5 values('2', '2')")
    snc.sql("insert into t5 values('3', '3')")

    import scala.concurrent.ExecutionContext.Implicits.global
    val future = Future {
      vm3.invoke(getClass,
        "doTestSmartConnectorForBucketRebalance", startArgs :+ Int.box(locatorClientPort))
    }

    try {
      // wait till the smart connector job perform at-least one putInto operation
      var count = 0
      while (snc.table("T5").count() == 3 && count < 10) {
        Thread.sleep(4000)
        count += 1
      }
      assert(count != 10, "Smart connector application not performing putInto as expected.")

      // rebalance the buckets
      snc.sql(s"CALL SYS.REBALANCE_ALL_BUCKETS()")

      Await.result(future, scala.concurrent.duration.Duration.apply(3, "min"))
    } finally {
      snc.sql("drop table if exists T6")
      snc.sql("drop table if exists T5")
    }
  }

  def testInsertIntoRowTableAfterStaleCatalog(): Unit = {
    insertDataAfterStaleCatalog("ROW")
  }

  def testInsertIntoColumnTableAfterStaleCatalog(): Unit = {
    insertDataAfterStaleCatalog("COLUMN")
  }

  private def insertDataAfterStaleCatalog(tableType: String) = {
    val snc = SnappyContext(sc)

    logInfo(s"insertDataAfterStaleCatalog: invoked for $tableType table")
    if (tableType == "COLUMN") {
      snc.sql(s"CREATE TABLE T5(COL1 STRING, COL2 STRING) USING column OPTIONS" +
          s" ( PARTITION_BY 'COL1', COLUMN_MAX_DELTA_ROWS '1')")
    } else {
      snc.sql(s"CREATE TABLE T5(COL1 STRING, COL2 STRING) USING row OPTIONS (partition_by 'col1')")
    }
    snc.sql("insert into t5 values('1', '1')")
    snc.sql("insert into t5 values('2', '2')")
    snc.sql("insert into t5 values('3', '3')")

    import scala.concurrent.ExecutionContext.Implicits.global
    val future = Future {
      vm3.invoke(getClass, "doTestInsertAfterStaleCatalog",
        startArgs :+ Int.box(locatorClientPort))
    }

    try {
      // wait till the smart connector job perform at-least one putInto operation
      var count = 0
      while (snc.table("T5").count() == 3 && count < 10) {
        Thread.sleep(4000)
        count += 1
      }
      assert(count != 10, "Smart connector application not performing insert as expected.")

      logInfo("testInsertQueryAfterStaleCatalog dropping table t5")
      // drop the table and create a table with same name and different schema
      // create a table with different schema
      snc.sql("drop table t5")
      if (tableType == "COLUMN") {
        snc.sql(s"CREATE TABLE T5(COL1 DATE, COL2 DATE) USING column OPTIONS" +
            s" ( PARTITION_BY 'COL1', COLUMN_MAX_DELTA_ROWS '1')")
      } else {
        snc.sql(s"CREATE TABLE T5(COL1 DATE, COL2 DATE) USING row OPTIONS (partition_by 'col1')")
      }

      Await.result(future, scala.concurrent.duration.Duration.apply(5, "min"))
    } finally {
      snc.sql("drop table if exists T5")
    }
  }

  def testDeleteAfterStaleCatalog(): Unit = {
    val snc = SnappyContext(sc)

    snc.sql(s"CREATE TABLE T6(COL1 STRING, COL2 STRING) USING column OPTIONS" +
        s" (key_columns 'COL1', PARTITION_BY 'COL1', COLUMN_MAX_DELTA_ROWS '1')")
    snc.sql("insert into t6 values('1', '1')")
    snc.sql("insert into t6 values('2', '2')")
    snc.sql("insert into t6 values('3', '3')")

    import scala.concurrent.ExecutionContext.Implicits.global
    val future = Future {
      vm3.invoke(getClass, "doTestDeleteAfterStaleCatalog",
        startArgs :+ Int.box(locatorClientPort))
    }

    try {
      // wait till the smart connector job perform at-least one putInto operation
      var count = 0
      while (snc.table("T6").count() == 3 && count < 10) {
        Thread.sleep(4000)
        count += 1
      }
      assert(count != 10, "Smart connector application not performing delete as expected.")

      logInfo("testDeleteAfterStaleCatalog dropping table t6")
      snc.sql("drop table t6")
      // create a table with different schema
      snc.sql(s"CREATE TABLE T6(COL1 DATE, COL2 DATE) USING column OPTIONS" +
          s" (key_columns 'COL1', PARTITION_BY 'COL1', COLUMN_MAX_DELTA_ROWS '1')")

      Await.result(future, scala.concurrent.duration.Duration.apply(5, "min"))
    } finally {
      snc.sql("drop table if exists T6")
    }
  }

  def testUpdateAfterStaleCatalog(): Unit = {
    val snc = SnappyContext(sc)

    snc.sql(s"CREATE TABLE T7(COL1 STRING, COL2 STRING) USING column OPTIONS" +
        s" (key_columns 'COL1', PARTITION_BY 'COL1', COLUMN_MAX_DELTA_ROWS '1')")
    snc.sql("insert into t7 values('1', '1')")
    snc.sql("insert into t7 values('2', '2')")
    snc.sql("insert into t7 values('3', '3')")

    import scala.concurrent.ExecutionContext.Implicits.global
    val future = Future {
      vm3.invoke(getClass, "doTestUpdateAfterStaleCatalog",
        startArgs :+ Int.box(locatorClientPort))
    }

    try {
      // wait till the smart connector job perform at-least one putInto operation
      var count = 0
      while (snc.table("T7").count() == 3 && count < 10) {
        Thread.sleep(4000)
        count += 1
      }
      assert(count != 10, "Smart connector application not performing delete as expected.")

      snc.sql(s"CREATE TABLE T8(COL1 DATE, COL2 DATE) USING column OPTIONS" +
          s" (key_columns 'COL1', PARTITION_BY 'COL1', COLUMN_MAX_DELTA_ROWS '1')")

      Await.result(future, scala.concurrent.duration.Duration.apply(5, "min"))
    } finally {
      snc.sql("drop table if exists T7")
      snc.sql("drop table if exists T8")
    }
  }
}

object SplitSnappyClusterDUnitTest
    extends SplitClusterDUnitTestObject with Logging {

  private val locatorNetPort = AvailablePortHelper.getRandomAvailableTCPPort

  def sc: SparkContext = {
    val context = ClusterManagerTestBase.sc
    context
  }

  def assertTableNotCachedInHiveCatalog(tableName: String): Unit = {
    val session = new SnappySession(SnappyContext.globalSparkContext)
    val catalog = session.sessionCatalog
    try {
      catalog.lookupRelation(session.tableIdentifier(tableName))
      assert(assertion = false, s"Table $tableName should not exist in the " +
          s"cached Hive catalog")
    } catch {
      // expected exception
      case _: org.apache.spark.sql.TableNotFoundException =>
    }
  }

  override def createTablesAndInsertData(tableType: String): Unit = {
    val snc = SnappyContext(sc)

    createTableUsingDataSourceAPI(snc, "embeddedModeTable1", tableType)
    selectFromTable(snc, "embeddedModeTable1", 1005)

    createTableUsingDataSourceAPI(snc, "embeddedModeTable2", tableType)
    selectFromTable(snc, "embeddedModeTable2", 1005)

    logInfo("Successful")
  }

  override def createComplexTablesAndInsertData(
      props: Map[String, String]): Unit = {
    val snc = SnappyContext(sc)

    createComplexTableUsingDataSourceAPI(snc, "embeddedModeTable1",
      "column", props)
    selectFromTable(snc, "embeddedModeTable1", 1005)

    createComplexTableUsingDataSourceAPI(snc, "embeddedModeTable2",
      "column", props)
    selectFromTable(snc, "embeddedModeTable2", 1005)

    logInfo("Successful")
  }

  def createUDFInEmbeddedMode(): Unit = {
    val snc = SnappyContext(sc)
    val rdd = sc.parallelize((1 to 5).map(i => OrderData(i, s"some $i", i)))
    val refDf = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS RR_TABLE")
    snc.sql("DROP TABLE IF EXISTS COL_TABLE")

    snc.sql("CREATE TABLE RR_TABLE(OrderRef INT NOT NULL, description String, price BIGINT)")
    snc.sql("CREATE TABLE COL_TABLE(OrderRef INT NOT NULL, " +
        "description String, price  LONG) using column options()")

    refDf.write.insertInto("RR_TABLE")
    refDf.write.insertInto("COL_TABLE")

    // create a udf in embedded mode
    val udfText: String = "public class IntegerUDF implements " +
        "    org.apache.spark.sql.api.java.UDF1<String,Integer> {" +
        " @Override public Integer call(String s){ " +
        "               return 6; " +
        "}" +
        "}"
    val file = UserDefinedFunctionsDUnitTest.createUDFClass("IntegerUDF", udfText)
    val jar = UserDefinedFunctionsDUnitTest.createJarFile(Seq(file))
    snc.sql(s"CREATE FUNCTION APP.intudf_embeddedmode AS IntegerUDF " +
        s"RETURNS Integer USING JAR " +
        s"'$jar'")
    val row = snc.sql("select intudf_embeddedmode(description) from col_table").collect()
    row.foreach(r => assert(r(0) == 6))
  }

  def createUDFInSplitMode(locatorPort: Int,
      prop: Properties,
      locatorClientPort: Int): Unit = {

    val snc: SnappyContext = getSnappyContextForConnector(locatorClientPort)

    // create a udf in split mode
    val udfText = "public class IntegerUDF2 implements org.apache.spark.sql.api.java.UDF1<String,Integer> {" +
        " @Override public Integer call(String s){ " +
        "               return 8; " +
        "}" +
        "}"

    val file2 = UserDefinedFunctionsDUnitTest.createUDFClass("IntegerUDF2", udfText)
    val jar = UserDefinedFunctionsDUnitTest.createJarFile(Seq(file2))
    snc.sql(s"CREATE FUNCTION APP.intudf_splitmode AS IntegerUDF2 " +
        s"RETURNS Integer USING JAR " +
        s"'$jar'")
    val row2 = snc.sql("select intudf_splitmode(description) from col_table").collect()
    row2.foreach(r => assert(r(0) == 8))

    // use function created in embedded mode
    val row = snc.sql("select intudf_embeddedmode(description) from col_table").collect()
    row.foreach(r => assert(r(0) == 6))
    snc.sql("drop function APP.intudf_embeddedmode")
    assert(snc.snappySession.sql(s"SHOW FUNCTIONS APP.intudf_embeddedmode").collect().length == 0)
  }

  def verifyUDFInEmbeddedMode(): Unit = {
    val snc = SnappyContext(sc)
    // use function created in splitmode
    val row2 = snc.sql("select intudf_splitmode(description) from col_table").collect()
    row2.foreach(r => assert(r(0) == 8))
    snc.sql("drop function APP.intudf_splitmode")
    assert(snc.snappySession.sql(s"SHOW FUNCTIONS APP.intudf_splitmode").collect().length == 0)
  }

  def verifyUDFInSplitMode(locatorPort: Int,
      prop: Properties,
      locatorClientPort: Int): Unit = {
    val snc: SnappyContext = getSnappyContextForConnector(locatorClientPort)

    // function that was dropped in embedded mode
    try {
      snc.sql("select intudf_splitmode(description) from col_table").collect()
    } catch {
      case e: AnalysisException if e.getMessage.contains("Undefined function") => // do nothing
    }
    assert(snc.snappySession.sql(s"SHOW FUNCTIONS APP.intudf_splitmode").collect().length == 0)
  }

  override def verifySplitModeOperations(tableType: String, isComplex: Boolean,
      props: Map[String, String]): Unit = {
    // embeddedModeTable1 is dropped in split mode. recreate it
    val snc = SnappyContext(sc)
    if (isComplex) {
      createComplexTableUsingDataSourceAPI(snc, "embeddedModeTable1",
        tableType, props)
    } else {
      createTableUsingDataSourceAPI(snc, "embeddedModeTable1",
        tableType, props)
    }
    selectFromTable(snc, "embeddedModeTable1", 1005)

    snc.dropTable("embeddedModeTable1", ifExists = true)

    // embeddedModeTable2 still exists drop it
    snc.dropTable("embeddedModeTable2", ifExists = true)

    // read data from splitModeTable1
    selectFromTable(snc, "splitModeTable1", 1005)

    // drop table created in split mode
    snc.dropTable("splitModeTable1", ifExists = true)

    // recreate the dropped table
    var expected: Seq[ComplexData] = Nil
    if (isComplex) {
      expected = createComplexTableUsingDataSourceAPI(snc, "splitModeTable1",
        tableType, props)
    } else {
      createTableUsingDataSourceAPI(snc, "splitModeTable1",
        tableType, props)
    }
    selectFromTable(snc, "splitModeTable1", 1005, expected)
    snc.dropTable("splitModeTable1", ifExists = true)

    logInfo("Successful")
  }

  def createRowTableForCollocatedJoin(): Unit = {

    val snc = SnappyContext(sc)
    val dimension1 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 10 + 1)))
    val refDf = snc.createDataFrame(dimension1)
    snc.sql("DROP TABLE IF EXISTS PR_TABLE1")

    snc.sql("CREATE TABLE PR_TABLE1(OrderId INT NOT NULL,description String, " +
        "OrderRef INT) USING row " +
        "options (" +
        "PARTITION_BY 'OrderId, OrderRef')")

    refDf.write.insertInto("PR_TABLE1")

    snc.sql("DROP TABLE IF EXISTS PR_TABLE2")

    snc.sql("CREATE TABLE PR_TABLE2(OrderId INT NOT NULL,description String, " +
        "OrderRef INT) USING row options (" +
        "PARTITION_BY 'OrderId,OrderRef'," +
        "COLOCATE_WITH 'PR_TABLE1')")

    val dimension2 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 5 + 1)))

    val dimensionDf = snc.createDataFrame(dimension2)
    dimensionDf.write.insertInto("PR_TABLE2")

    // force the stats to be populated
    SnappyTableStatsProviderService.getService.getTableStatsFromService("APP.PR_TABLE1")
    SnappyTableStatsProviderService.getService.getTableStatsFromService("APP.PR_TABLE2")
  }

  def createColumnTableForCollocatedJoin(): Unit = {

    val snc = SnappyContext(sc)
    val dimension1 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 10 + 1)))
    val refDf = snc.createDataFrame(dimension1)
    snc.sql("DROP TABLE IF EXISTS PR_TABLE3")

    snc.sql("CREATE TABLE PR_TABLE3(OrderId INT, description String, " +
        "OrderRef INT) USING column " +
        "options (" +
        "PARTITION_BY 'OrderId,OrderRef')")

    refDf.write.format("column").mode(SaveMode.Append).options(props)
        .saveAsTable("PR_TABLE3")

    val countdf = snc.sql("select * from PR_TABLE3")
    var count = countdf.count()
    assert(count == 1000, s"Unexpected count = $count, expected 1000")

    snc.sql("DROP TABLE IF EXISTS PR_TABLE4")

    snc.sql("CREATE TABLE PR_TABLE4(OrderId INT ,description String, " +
        "OrderRef INT) USING column options (" +
        "PARTITION_BY 'OrderId,OrderRef'," +
        "COLOCATE_WITH 'PR_TABLE3')")

    val dimension2 = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i % 5 + 1)))

    val dimensionDf = snc.createDataFrame(dimension2)
    dimensionDf.write.insertInto("PR_TABLE4")
    val countdf1 = snc.sql("select * from PR_TABLE4")
    count = countdf1.count()
    assert(count == 1000, s"Unexpected count = $count, expected 1000")

    // force the stats to be populated
    SnappyTableStatsProviderService.getService.getTableStatsFromService("APP.PR_TABLE3")
    SnappyTableStatsProviderService.getService.getTableStatsFromService("APP.PR_TABLE4")
  }


  def checkCollocatedJoins(locatorPort: Int, prop: Properties,
      table1: String, table2: String,
      locatorClientPort: Int): Unit = {
    val snc: SnappyContext = getSnappyContextForConnector(locatorClientPort)

    val testJoins = new SnappyJoinSuite()
    testJoins.partitionToPartitionJoinAssertions(snc, table1, table2)

    logInfo("Successful")
  }

  /**
   * Returns the SnappyContext for external(compute) Spark cluster connected to
   * SnappyData cluster using the locator property
   */
  override def getSnappyContextForConnector(locatorClientPort: Int, properties: Properties = null)
  : SnappyContext = {
    val hostName = InetAddress.getLocalHost.getHostName
    //      val connectionURL = "jdbc:snappydata://localhost:" + locatorClientPort + "/"
    val connectionURL = s"localhost:$locatorClientPort"
    logInfo(s"URL for connector is $connectionURL")
    val conf = new SparkConf()
        .setAppName("test Application")
        .setMaster(s"spark://$hostName:7077")
        .set("spark.executor.cores", TestUtils.defaultCores.toString)
        .set("spark.executor.extraClassPath",
          getEnvironmentVariable("SNAPPY_DIST_CLASSPATH"))
        .set("spark.testing.reservedMemory", "0")
        .set("spark.sql.autoBroadcastJoinThreshold", "-1")
        .set("snappydata.connection", connectionURL)
        .set("snapptdata.sql.planCaching", random.nextBoolean().toString)

    logInfo("Spark conf:" + conf.getAll.toString)

    val sc = SparkContext.getOrCreate(conf)
    //      sc.setLogLevel("DEBUG")
    //      Logger.getRootLogger.setLevel(Level.ALL)
    //      Logger.getLogger("org").setLevel(Level.DEBUG)
    //      Logger.getLogger("akka").setLevel(Level.DEBUG)
    //      val snc = SnappySession.getOrCreate(sc).sqlContext
    val snc = SnappyContext(sc)

    val mode = SnappyContext.getClusterMode(snc.sparkContext)
    mode match {
      case ThinClientConnectorMode(_, _) => // expected
      case _ => assert(assertion = false, "cluster mode is " + mode)
    }

    snc
  }

  def splitModeTableCreate(locatorPort: Int,
      prop: Properties,
      locatorClientPort: Int): Unit = {
    val tblBatchSize200K = "tblBatchSizeBig_split"

    val tblBatchSize200 = "tblBatchSizeSmall_split"

    val snc = getSnappyContextForConnector(locatorClientPort)
    snc.sql(s"CREATE TABLE $tblBatchSize200(Key1 INT ,Value STRING) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "BUCKETS '8', COLUMN_BATCH_SIZE '200')")

    snc.sql(s"CREATE TABLE $tblBatchSize200K (Key1 INT ,Value STRING) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "BUCKETS '8', COLUMN_BATCH_SIZE '200000')")

    val rdd = sc.parallelize(
      (1 to 100000).map(i => TestData(i, i.toString)))

    implicit val encoder: Encoder[TestData] = Encoders.product[TestData]
    val dataDF = snc.createDataset(rdd)

    dataDF.write.insertInto(tblBatchSize200)
    dataDF.write.insertInto(tblBatchSize200K)
  }

  def checkStatsForSplitMode(locatorPort: Int, prop: Properties,
      buckets: String,
      locatorClientPort: Int): Unit = {
    val snc: SnappyContext = getSnappyContextForConnector(locatorClientPort)
    snc.sql("drop table if exists snappyTable")
    snc.sql(s"create table snappyTable (id bigint not null, sym varchar(10) not null) using " +
        s"column options(redundancy '1', buckets '$buckets')")
    val testDF = snc.range(10000000).selectExpr("id", "concat('sym', cast((id % 100) as varchar" +
        "(10))) as sym")
    testDF.write.insertInto("snappyTable")
    // TODO: Fix this. wait added to make sure that stats are
    // generated on the embedded cluster and the smart connector
    // mode is able to get those. Ideally if table stats are not
    // present connector should send the table name and
    // get those from embedded side
    var expectedRowCount = 10000000

    def waitForStats: Boolean = {
      SnappyTableStatsProviderService.getService.
          getAggregatedStatsOnDemand._1.get("APP.SNAPPYTABLE") match {
        case Some(stats) => stats.getRowCount == expectedRowCount
        case _ => false
      }
    }

    ClusterManagerTestBase.waitForCriterion(waitForStats,
      s"Expected stats row count to be $expectedRowCount", 30000, 500, throwOnTimeout = true)
    for (i <- 1 to 100) {
      snc.sql(s"insert into snappyTable values($i,'Test$i')")
    }
    expectedRowCount = 10000100
    ClusterManagerTestBase.waitForCriterion(waitForStats,
      s"Expected stats row count to be $expectedRowCount", 30000, 500, throwOnTimeout = true)
    logInfo("Successful")
  }

  def splitModeCreateTableUsingCTAS(locatorPort: Int,
      prop: Properties,
      locatorClientPort: Int): Unit = {
    val snc = getSnappyContextForConnector(locatorClientPort)
    val customerFile: String = getClass.getResource("/customer.csv").getPath

    snc.sql(s"CREATE EXTERNAL TABLE CUSTOMER_STAGING ( " +
        "C_CUSTKEY     INTEGER NOT NULL," +
        "C_NAME        VARCHAR(25) NOT NULL," +
        "C_ADDRESS     VARCHAR(40) NOT NULL," +
        "C_NATIONKEY   INTEGER NOT NULL," +
        "C_PHONE       VARCHAR(15) NOT NULL," +
        "C_ACCTBAL     DECIMAL(15,2)   NOT NULL," +
        "C_MKTSEGMENT  VARCHAR(10) NOT NULL," +
        "C_COMMENT     VARCHAR(117) NOT NULL)" +
        s"USING csv OPTIONS (path '$customerFile', maxCharsPerColumn '4096')")

    snc.sql(s"CREATE TABLE CUSTOMER AS SELECT * FROM CUSTOMER_STAGING")
    val count = snc.sql("select * from customer").count()
    assert(count == 750, s"Expected 750 rows. Actual rows = $count")

    val customerWithHeadersFile: String = getClass.getResource("/customer_with_headers.csv").getPath
    val customer_csv_DF = snc.read.option("header", "true")
        .option("inferSchema", "true")
        .option("maxCharsPerColumn", "4096").csv(customerWithHeadersFile)
    val props1 = Map("PARTITION_BY" -> "C_CUSTKEY")
    customer_csv_DF.write.format("column").mode("append").options(props1).saveAsTable("CUSTOMER_2")
    val count2 = snc.sql("select * from customer_2").count()
    assert(count2 == 750, s"Expected 750 rows. Actual rows = $count2")

    // also test temp table
    snc.sql(s"CREATE TEMPORARY TABLE CUSTOMER_TEMP AS SELECT * FROM CUSTOMER_STAGING")
    val count3 = snc.sql("select * from CUSTOMER_TEMP").count()
    assert(count3 == 750, s"Expected 750 rows. Actual rows = $count3")
    val catalog = snc.snappySession.sessionCatalog
    assert(catalog.isTemporaryTable(snc.snappySession.tableIdentifier("CUSTOMER_TEMP")))
    snc.sql("DROP TABLE CUSTOMER_TEMP")
  }

  override def dropAndCreateTablesInEmbeddedMode(
      tableType: String): Unit = {
    val snc = SnappyContext(sc)
    val df = snc.table("APP.T1")
    assert(df.schema.fields.length == 3)
    snc.dropTable("APP.T1")

    snc.sql(s"CREATE TABLE T1(COL1 STRING, COL2 STRING) " +
        s"USING $tableType OPTIONS (PARTITION_BY 'COL1', COLUMN_MAX_DELTA_ROWS '1')")
    snc.sql("INSERT INTO T1 VALUES('AA', 'AA')")
    snc.sql("INSERT INTO T1 VALUES('BB', 'BB')")
    snc.sql("INSERT INTO T1 VALUES('CC', 'CC')")
    snc.sql("INSERT INTO T1 VALUES('DD', 'DD')")
    snc.sql("INSERT INTO T1 VALUES('EE', 'EE')")

    val rs = snc.sql("select * from t1").collect()
    assert(rs.length == 5)
  }

  var connectorSnc: SnappyContext = _

  override def createTablesInSplitMode(locatorPort: Int,
      prop: Properties,
      locatorClientPort: Int,
      tableType: String): Unit = {
    if (connectorSnc == null || connectorSnc.sparkContext.isStopped) {
      connectorSnc = getSnappyContextForConnector(locatorClientPort)
    }
    // row table
    connectorSnc.sql(s"CREATE TABLE T1(C1 INT, C2 INT, C3 INT) " +
        s"USING $tableType OPTIONS (PARTITION_BY 'C1', COLUMN_MAX_DELTA_ROWS '1')")
    connectorSnc.sql("INSERT INTO T1 VALUES(1, 1, 1)")
    connectorSnc.sql("INSERT INTO T1 VALUES(2, 2, 2)")
    connectorSnc.sql("INSERT INTO T1 VALUES(3, 3, 3)")
    connectorSnc.sql("INSERT INTO T1 VALUES(4, 4, 4)")
    connectorSnc.sql("INSERT INTO T1 VALUES(5, 5, 5)")

    val rs = connectorSnc.sql("select * from t1 order by c1").collect()

    assert(rs.length == 5)
    assert(rs(0).getAs[Int]("c1") == 1)
    assert(rs(0).getAs[Int]("c2") == 1)
    assert(rs(0).getAs[Int]("c3") == 1)
  }

  override def verifyTableFormInSplitMOde(locatorPort: Int,
      prop: Properties,
      locatorClientPort: Int): Unit = {
    var resultDF: org.apache.spark.sql.DataFrame = null
    try {
      resultDF = connectorSnc.sql("select * from t1 order by col1")
    } catch {
      case _: org.apache.spark.sql.AnalysisException =>
        resultDF = connectorSnc.sql("select * from t1 order by col1")
    }

    val rs = resultDF.collect()
    assert(rs.length == 5, s"Expected 5 but got ${rs.length}")
    assert(rs(0).getAs[String]("col1").equals("AA"))
    assert(rs(0).getAs[String]("col2").equals("AA"))

    connectorSnc.dropTable("APP.T1")
  }

  def createTablesFromOtherTablesTest(locatorPort: Int,
      prop: Properties,
      locatorClientPort: Int): Unit = {
    val tempRowTableProps = "BUCKETS '16', PARTITION_BY 'COL2'"

    executeTestWithOptions(locatorPort, locatorClientPort, Map("BUCKETS" -> "8",
      "PARTITION_BY" -> "COL1", "REDUNDANCY" -> "1"), Map.empty, tempRowTableProps)
    executeTestWithOptions(locatorPort, locatorClientPort, Map.empty, Map("BUCKETS" -> "16"),
      tempRowTableProps, "BUCKETS '8', PARTITION_BY 'COL1', REDUNDANCY '1'")
  }

  def executeTestWithOptions(locatorPort: Int, locatorClientPort: Int,
      rowTableOptios: Map[String, String] = Map.empty[String, String],
      colTableOptions: Map[String, String] = Map.empty[String, String],
      tempRowTableOptions: String = "",
      tempColTableOptions: String = ""): Unit = {

    val snc = getSnappyContextForConnector(locatorClientPort)
    val rowTable = "rowTable"
    val colTable = "colTable"


    snc.sql("DROP TABLE IF EXISTS " + rowTable)
    snc.sql("DROP TABLE IF EXISTS " + colTable)
    Property.ColumnBatchSize.set(snc.sessionState.conf, "30k")
    val rdd = sc.parallelize(
      (1 to 113999).map(i => TestRecord(i, i + 1, i + 2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable(rowTable, "row", dataDF.schema, rowTableOptios)
    dataDF.write.format("row").mode(SaveMode.Append).options(rowTableOptios).saveAsTable(rowTable)

    snc.createTable(colTable, "column", dataDF.schema, colTableOptions)
    dataDF.write.insertInto(colTable)

    val tempRowTableName = "testRowTable1"
    val tempColTableName = "testcolTable1"


    snc.sql("DROP TABLE IF EXISTS " + tempRowTableName)
    snc.sql(s"CREATE TABLE " + tempRowTableName + s" using row options($tempRowTableOptions)  AS" +
        s" (SELECT col1 ,col2  FROM " + rowTable + ")")
    val testResults1 = snc.sql("SELECT * FROM " + tempRowTableName).collect()
    assert(testResults1.length == 113999, s"Expected row count is 113999 while actual count is " +
        s"${testResults1.length}")


    snc.sql("DROP TABLE IF EXISTS " + tempRowTableName)
    snc.sql("CREATE TABLE " + tempRowTableName + s" using row options($tempRowTableOptions) AS " +
        s"(SELECT col1 ,col2  FROM " + colTable + ")")
    val testResults2 = snc.sql("SELECT * FROM " + tempRowTableName).collect()
    assert(testResults2.length == 113999, s"Expected row count is 113999 while actual count is " +
        s"${testResults2.length}")

    snc.sql("DROP TABLE IF EXISTS " + tempColTableName)
    snc.sql("CREATE TABLE " + tempColTableName + s" USING COLUMN OPTIONS($tempColTableOptions) " +
        s"AS (SELECT col1 ,col2 FROM " + tempRowTableName + ")")

    val testResults3 = snc.sql("SELECT * FROM " + tempColTableName).collect()
    assert(testResults3.length == 113999, s"Expected row count is 113999 while actual count is " +
        s"${testResults3.length}")

    snc.sql("DROP TABLE IF EXISTS " + tempColTableName)
    snc.sql("CREATE TABLE " + tempColTableName + s" USING COLUMN OPTIONS($tempColTableOptions) " +
        s"AS (SELECT col1 ,col2 FROM " + colTable + ")")

    val testResults4 = snc.sql("SELECT * FROM " + tempColTableName).collect()
    assert(testResults4.length == 113999, s"Expected row count is 113999 while actual count is" +
        s"${testResults4.length}")

    snc.sql("DROP TABLE IF EXISTS " + tempColTableName)

    snc.sql("CREATE TABLE " + tempColTableName + s" USING COLUMN OPTIONS($tempColTableOptions) " +
        s"AS (SELECT t1.col1 ,t1.col2 FROM " + colTable + " t1," + rowTable +
        " t2 where t1.col1=t2.col2)")
    // Expected count will be 113998 as first row will not match
    val testResults5 = snc.sql("SELECT * FROM " + tempColTableName).collect()

    assert(testResults5.length == 113998, s"Expected row count is 113998 while actual count is" +
        s"${testResults5.length}")

    snc.sql("DROP TABLE IF EXISTS " + tempColTableName)
    snc.sql("DROP TABLE IF EXISTS " + tempRowTableName)

    snc.sql("DROP TABLE IF EXISTS " + rowTable)
    snc.sql("DROP TABLE IF EXISTS " + colTable)
  }

  def doTestStaleCatalog(locatorPort: Int,
      prop: Properties,
      locatorClientPort: Int): Unit = {
    val snc: SnappyContext = getSnappyContextForConnector(locatorClientPort)

    val rdd: RDD[Row] = sc.parallelize(
      Seq(
        Row("val1", "val3"),
        Row("val2", "val4")
      )
    )
    val schema = new StructType()
        .add(StructField("col1", StringType))
        .add(StructField("col2", StringType))
    val dataFrame = snc.createDataFrame(rdd, schema)
    import org.apache.spark.sql.snappy._
    try {
      Thread.sleep(2000)
      for (_ <- 1 to 10) {
        dataFrame.write.putInto("T5")
      }
      Assert.fail("Should have thrown CatalogStaleException.")
    } catch {
      case _: CatalogStaleException =>
        // retrying putInto operation and it should pass
        dataFrame.write.putInto("T5")
    }
  }

  def doTestStaleCatalogForSNAP3024(locatorPort: Int,
      prop: Properties,
      locatorClientPort: Int): Unit = {
    performSmartConnectorOps(locatorClientPort)
  }

  private def performSmartConnectorOps(locatorClientPort: Int): Unit = {
    val snc: SnappyContext = getSnappyContextForConnector(locatorClientPort)

    snc.sql("select * from t5").collect()

    val rdd: RDD[Row] = sc.parallelize(
      Seq(
        Row("4", "4"),
        Row("5", "5")
      )
    )
    val schema = new StructType()
        .add(StructField("col1", StringType))
        .add(StructField("col2", StringType))
    val dataFrame = snc.createDataFrame(rdd, schema)

    dataFrame.write.insertInto("T5")
    // wait for the embedded mode to change the catalog or rebalance buckets
    Thread.sleep(6000)
    // should not throw an exception
    for (_ <- 1 to 5) {
      snc.sql("select * from t5").collect()
    }
  }

  def doTestSmartConnectorForBucketRebalance(locatorPort: Int,
      prop: Properties,
      locatorClientPort: Int): Unit = {
    performSmartConnectorOps(locatorClientPort)
  }

  def doTestInsertAfterStaleCatalog(locatorPort: Int,
      prop: Properties,
      locatorClientPort: Int): Unit = {
    val snc: SnappyContext = getSnappyContextForConnector(locatorClientPort)
    snc.sql("insert into t5 values('4', '4')")
    logInfo("1. schema is = " + snc.table("T5").schema)

    val schema2 = new StructType()
        .add(StructField("col1", DateType))
        .add(StructField("col2", DateType))
    val rdd2: RDD[Row] = sc.parallelize(
      Seq(
        Row(java.sql.Date.valueOf("2019-01-01"), java.sql.Date.valueOf("2019-01-01")),
        Row(java.sql.Date.valueOf("2019-02-02"), java.sql.Date.valueOf("2019-02-02"))
      )
    )
    val dataFrame2 = snc.createDataFrame(rdd2, schema2)

    logInfo("doTestInsertAfterStaleCatalog: Waiting 6 seconds to allow schema change")
    Thread.sleep(6000)
    try {
      for (_ <- 1 to 20) {
        Thread.sleep(500)
        logInfo("calling dataFrame.write.insertInto(\"T5\")")
        logInfo("2. schema is = " + snc.table("T5").schema)
        dataFrame2.write.insertInto("T5")
      }
      Assert.fail("Should have thrown CatalogStaleException.")
    } catch {
      case _: CatalogStaleException =>
        logInfo("doTestInsertAfterStaleCatalog: Caught expected CatalogStaleException")
        // retrying insertInto operation and it should pass
        retryOperation(5) {
          dataFrame2.write.insertInto("T5")
        }
    }
    logInfo("3. schema is = " + snc.table("T5").schema)
  }

  def retryOperation[T](maxRetryAttempts: Int)(f: => T): Unit = {
    var retryCount = 0
    var success = false
    while (!success) {
      try {
        f
        success = true
      } catch {
        // if table is not created yet on embedded cluster,
        // TableNotFoundException can be seen; retry in
        // such a case
        case t: TableNotFoundException =>
          retryCount = retryCount + 1
          if (retryCount == maxRetryAttempts) {
            throw t
          } else {
            Thread.sleep(200)
          }
      }
    }
  }

  def doTestDeleteAfterStaleCatalog(locatorPort: Int,
      prop: Properties,
      locatorClientPort: Int): Unit = {
    val snc: SnappyContext = getSnappyContextForConnector(locatorClientPort)
    snc.sql("delete from t6 where col1 like '1%'")

    logInfo("doTestDeleteAfterStaleCatalog: Waiting 6 seconds to allow schema change")
    Thread.sleep(6000)
    try {
      for (_ <- 1 to 20) {
        Thread.sleep(500)
        snc.sql("delete from t6 where col1 like '2%'")
      }
      Assert.fail("Should have thrown CatalogStaleException.")
    } catch {
      case _: CatalogStaleException =>
        logInfo("doTestDeleteAfterStaleCatalog: Caught expected CatalogStaleException")
        // retrying delete from operation and it should pass
        retryOperation(5) {
          snc.sql("delete from t6 where col1 like '2%'")
        }
    }
  }

  def doTestUpdateAfterStaleCatalog(locatorPort: Int,
      prop: Properties,
      locatorClientPort: Int): Unit = {
    val snc: SnappyContext = getSnappyContextForConnector(locatorClientPort)
    snc.sql("insert into t7 values('4', '4')")

    logInfo("doTestUpdateAfterStaleCatalog: Waiting 6 seconds to allow schema change")
    Thread.sleep(6000)
    try {
      for (_ <- 1 to 20) {
        Thread.sleep(500)
        snc.sql("update t7 set col2 = '22' where col1 = '2'")
      }
      Assert.fail("Should have thrown CatalogStaleException.")
    } catch {
      case _: CatalogStaleException =>
        logInfo("doTestUpdateAfterStaleCatalog: Caught expected CatalogStaleException")
        // retrying delete from operation and it should pass
        retryOperation(5) {
          snc.sql("update t7 set col2 = '22' where col1 = '2'")
        }
    }
  }

  def doTestStaleCatalogRetryForStreamingSink(locatorPort: Int,
      prop: Properties, locatorClientPort: Int, testTempDir: String): Unit = {
    val tableName = "users"
    val kafkaTestUtils = new KafkaTestUtils
    kafkaTestUtils.setup()
    kafkaTestUtils.createTopic(tableName, partitions = 3)
    try {
      val snc: SnappyContext = getSnappyContextForConnector(locatorClientPort)
      snc.sql(s"drop table if exists $tableName")
      snc.sql(
        s"""create table $tableName (id long , name varchar(40), age int)
           | using column options(key_columns 'id')""".stripMargin)

      val streamingDF = snc
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", kafkaTestUtils.brokerAddress)
          .option("subscribe", tableName)
          .option("startingOffsets", "earliest")
          .load()

      implicit val encoder: ExpressionEncoder[Row] = RowEncoder(snc.table(tableName).schema)
      val session = snc.sparkSession
      import session.implicits._
      val streamingQuery = streamingDF.selectExpr("CAST(value AS STRING)")
          .as[String]
          .map(_.split(","))
          .map(r => Row(r(0).toLong, r(1), r(2).toInt))
          .writeStream
          .format("snappysink")
          .queryName(tableName)
          .trigger(ProcessingTime("1 seconds"))
          .option("tableName", tableName)
          .option("checkpointLocation", s"$testTempDir/checkpoint")
          .start()

      // produce first batch of data
      val dataBatch1 = Seq(Seq(1, "name1", 20), Seq(2, "name2", 20))
      kafkaTestUtils.sendMessages(tableName, dataBatch1.map(r => r.mkString(",")).toArray)

      waitTillTheBatchIsPickedForProcessing(snc, 0, tableName)

      new PrintWriter(s"$testTempDir/file0") {
        write("dummyData")
        close()
      }

      // wait till DDL is fired on snappy cluster which will lead to stale smart-connector catalog
      var attempts = 0
      while (!Files.exists(Paths.get(testTempDir, "file1")) && attempts < 15) {
        Thread.sleep(4000)
        attempts += 1
      }

      assert(attempts < 14, "Waiting for stale catalog timed out")

      // produce second batch of data
      val dataBatch2 = Seq(Seq(3, "name3", 20))
      kafkaTestUtils.sendMessages(tableName, dataBatch2.map(r => r.mkString(",")).toArray)

      streamingQuery.processAllAvailable()

      assertData(Array(Row(1, "name1", 20), Row(2, "name2", 20), Row(3, "name3", 20)))

      def assertData(expectedData: Array[Row]): Unit = {
        val actualData = snc.sql(s"select * from $tableName" +
            s" order by id, name, age")
            .collect()

        assert(expectedData sameElements actualData, "actual data:" +
            actualData.map(a => a.toString()).mkString(","))
      }
    } finally {
      kafkaTestUtils.teardown()
    }
  }

  private def waitTillTheBatchIsPickedForProcessing(snc: SnappyContext, batchId: Int,
      queryName: String, retries: Int = 15): Unit = {
    if (retries == 0) {
      throw new RuntimeException(s"Batch id $batchId not found in sink status table")
    }
    val sql = s"select batch_id from snappysys_internal____sink_state_table " +
        s"where stream_query_id = '$queryName'"
    val batchIdFromTable = snc.sql(sql).collect()
    if (batchIdFromTable.isEmpty || batchIdFromTable(0)(0) != batchId) {
      Thread.sleep(1000)
      waitTillTheBatchIsPickedForProcessing(snc, batchId, queryName, retries - 1)
    }
  }
}
