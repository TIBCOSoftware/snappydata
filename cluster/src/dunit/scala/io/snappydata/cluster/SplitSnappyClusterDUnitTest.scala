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
package io.snappydata.cluster

import java.net.InetAddress
import java.sql.SQLException
import java.util.Properties

import scala.language.postfixOps

import com.gemstone.gemfire.internal.cache.PartitionedRegion
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import io.snappydata.SnappyTableStatsProviderService
import io.snappydata.core.{TestData, TestData2}
import io.snappydata.store.ClusterSnappyJoinSuite
import io.snappydata.test.dunit.{AvailablePortHelper, SerializableRunnable}
import org.junit.Assert

import org.apache.spark.sql._
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.sql.store.SnappyJoinSuite
import org.apache.spark.sql.udf.UserDefinedFunctionsDUnitTest
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * Basic tests for non-embedded mode connections to an embedded cluster.
 */
class SplitSnappyClusterDUnitTest(s: String)
    extends ClusterManagerTestBase(s)
    with SplitClusterDUnitTestBase
    with Serializable {

  override val locatorNetPort = testObject.locatorNetPort

  override val stopNetServersInTearDown = false

  val currenyLocatorPort = ClusterManagerTestBase.locPort

  override protected val productDir =
    testObject.getEnvironmentVariable("SNAPPY_HOME")

  override def beforeClass(): Unit = {
    super.beforeClass()
    startNetworkServers()
    vm3.invoke(classOf[ClusterManagerTestBase], "startSparkCluster", productDir)
  }

  override def afterClass(): Unit = {
    Array(vm2, vm1, vm0).foreach(_.invoke(getClass, "stopNetworkServers"))
    ClusterManagerTestBase.stopNetworkServers()
    vm3.invoke(classOf[ClusterManagerTestBase], "stopSparkCluster", productDir)
    super.afterClass()
  }

  override protected def locatorClientPort = { locatorNetPort }

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
        "5" :+ Int.box(locatorClientPort))
  }

  def testBatchSize(): Unit = {
    doTestBatchSize()
  }

  def doTestBatchSize(): Unit = {
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
        "BUCKETS '3', COLUMN_BATCH_SIZE '200')")

    snc.sql(s"CREATE TABLE $tblSizeBig (Key1 INT ,Value STRING) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "BUCKETS '3', COLUMN_BATCH_SIZE '200000')")

    val rdd = sc.parallelize(
      (1 to 100000).map(i => TestData(i, i.toString)))

    implicit val encoder = Encoders.product[TestData]
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

  def getRegionSize(tbl: String) : Long = {
    Misc.getRegionForTable(tbl.toUpperCase,
      true).asInstanceOf[PartitionedRegion].size()

  }

  def getShadowRegionSize(tbl: String) : Long = {
    // divide by three as 2 entries are for column and one is a base entry
    Misc.getRegionForTable(ColumnFormatRelation.
        columnBatchTableName(tbl).toUpperCase,
      true).asInstanceOf[PartitionedRegion].size() /3
  }

  def testColumnTableStatsInSplitModeWithHA(): Unit = {
    vm3.invoke(getClass, "checkStatsForSplitMode", startArgs :+
        "1" :+ Int.box(locatorClientPort))
    val props = bootProps
    val port = currenyLocatorPort

    val restartServer = new SerializableRunnable() {
      override def run(): Unit = ClusterManagerTestBase.startSnappyServer(port, props)
    }

    vm0.invoke(classOf[ClusterManagerTestBase], "stopAny")
    var stats = SnappyTableStatsProviderService.getService.
        getAggregatedStatsOnDemand._1("APP.SNAPPYTABLE")

    Assert.assertEquals(10000100, stats.getRowCount)
    vm0.invoke(restartServer)

    vm1.invoke(classOf[ClusterManagerTestBase], "stopAny")
    var stats1 = SnappyTableStatsProviderService.getService.
        getAggregatedStatsOnDemand._1("APP.SNAPPYTABLE")
    Assert.assertEquals(10000100, stats1.getRowCount)
    vm1.invoke(restartServer)

    // Test using using 5 buckets
    vm3.invoke(getClass, "checkStatsForSplitMode", startArgs :+
        "5" :+ Int.box(locatorClientPort))
    vm0.invoke(classOf[ClusterManagerTestBase], "stopAny")
    var stats2 = SnappyTableStatsProviderService.getService.
        getAggregatedStatsOnDemand._1("APP.SNAPPYTABLE")
    Assert.assertEquals(10000100, stats2.getRowCount)
    val snc = SnappyContext(sc)
    snc.sql("insert into snappyTable values(1,'Test')")
    var stats3 = SnappyTableStatsProviderService.getService.
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

}

object SplitSnappyClusterDUnitTest
    extends SplitClusterDUnitTestObject with Logging {

  private val locatorNetPort = AvailablePortHelper.getRandomAvailableTCPPort

  def sc: SparkContext = {
    val context = ClusterManagerTestBase.sc
    context
  }

  def assertTableNotCachedInHiveCatalog(tableName: String): Unit = {
    val catalog = SnappySession.getOrCreate(SnappyContext.globalSparkContext).
        sessionCatalog
    val t = catalog.newQualifiedTableName(tableName)
    try {
      catalog.getCachedHiveTable(t)
      assert(assertion = false, s"Table $tableName should not exist in the " +
          s"cached Hive catalog")
    } catch {
      // expected exception
      case e: org.apache.spark.sql.TableNotFoundException =>
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
//    row.foreach(r => println(r))
    row.foreach(r => assert(r(0) == 6))
  }

  // scalastyle:off println
  def createUDFInSplitMode(locatorPort: Int,
      prop: Properties,
      locatorClientPort: Int): Unit = {

    val snc: SnappyContext = getSnappyContextForConnector(locatorPort,
      locatorClientPort)

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
    row2.foreach(r => println(r))
    row2.foreach(r => assert(r(0) == 8))

    // use function created in embedded mode
    val row = snc.sql("select intudf_embeddedmode(description) from col_table").collect()
    row.foreach(r => assert(r(0) == 6))
    snc.sql("drop function APP.intudf_embeddedmode")
    assert(snc.snappySession.sql(s"SHOW FUNCTIONS APP.intudf_embeddedmode").collect().length == 0)
//    snc.sparkContext.stop()
  }

  def verifyUDFInEmbeddedMode(): Unit = {
    val snc = SnappyContext(sc)
    // use function created in splitmode
    val row2 = snc.sql("select intudf_splitmode(description) from col_table").collect()
    row2.foreach(r => println(r))
    row2.foreach(r => assert(r(0) == 8))
    snc.sql("drop function APP.intudf_splitmode")
    assert(snc.snappySession.sql(s"SHOW FUNCTIONS APP.intudf_splitmode").collect().length == 0)
  }

  def verifyUDFInSplitMode(locatorPort: Int,
      prop: Properties,
      locatorClientPort: Int): Unit = {
    val snc: SnappyContext = getSnappyContextForConnector(locatorPort,
      locatorClientPort)

    // function that was dropped in embedded mode
    try {
      val row2 = snc.sql("select intudf_splitmode(description) from col_table").collect()
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
    var expected = Seq.empty[ComplexData]
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
    val snc: SnappyContext = getSnappyContextForConnector(locatorPort,
      locatorClientPort)

    val testJoins = new ClusterSnappyJoinSuite()
    testJoins.partitionToPartitionJoinAssertions(snc, table1, table2)

    logInfo("Successful")
  }

  /**
   * Returns the SnappyContext for external(compute) Spark cluster connected to
   * SnappyData cluster using the locator property
   */
  override def getSnappyContextForConnector(locatorPort: Int,
      locatorClientPort: Int): SnappyContext = {
    val hostName = InetAddress.getLocalHost.getHostName
    //      val connectionURL = "jdbc:snappydata://localhost:" + locatorClientPort + "/"
    val connectionURL = s"localhost:$locatorClientPort"
    logInfo(s"URL for connector is $connectionURL")
    val conf = new SparkConf()
        .setAppName("test Application")
        .setMaster(s"spark://$hostName:7077")
        .set("spark.executor.extraClassPath",
          getEnvironmentVariable("SNAPPY_DIST_CLASSPATH"))
        .set("spark.testing.reservedMemory", "0")
        .set("spark.sql.autoBroadcastJoinThreshold", "-1")
        .set("snappydata.connection", connectionURL)
    conf.getAll.foreach(println)

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
      case _ => assert(false, "cluster mode is " + mode)
    }

    snc
  }
  // scalastyle:on println

  def splitModeTableCreate(locatorPort: Int,
      prop: Properties,
      locatorClientPort: Int): Unit = {
    val tblBatchSize200K = "tblBatchSizeBig_split"

    val tblBatchSize200 = "tblBatchSizeSmall_split"

    val snc = getSnappyContextForConnector(locatorPort,
      locatorClientPort)
    snc.sql(s"CREATE TABLE $tblBatchSize200(Key1 INT ,Value STRING) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "BUCKETS '3', COLUMN_BATCH_SIZE '200')")

    snc.sql(s"CREATE TABLE $tblBatchSize200K (Key1 INT ,Value STRING) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'Key1'," +
        "BUCKETS '3', COLUMN_BATCH_SIZE '200000')")

    val rdd = sc.parallelize(
      (1 to 100000).map(i => TestData(i, i.toString)))

    implicit val encoder = Encoders.product[TestData]
    val dataDF = snc.createDataset(rdd)

    dataDF.write.insertInto(tblBatchSize200)
    dataDF.write.insertInto(tblBatchSize200K)
  }

  def checkStatsForSplitMode(locatorPort: Int, prop: Properties,
       buckets: String,
      locatorClientPort: Int): Unit = {
    val snc: SnappyContext = getSnappyContextForConnector(locatorPort,
      locatorClientPort)
    snc.sql("drop table if exists snappyTable")
    snc.sql(s"create table snappyTable (id bigint not null, sym varchar(10) not null) using " +
        s"column options(redundancy '1', buckets '$buckets')")
    val testDF = snc.range(10000000).selectExpr("id", "concat('sym', cast((id % 100) as varchar" +
        "(10))) as sym")
    testDF.write.insertInto("snappyTable")
    // TODO: Fix this. Sleep added to make sure that stats are
    // generated on the embedded cluster and the smart connector
    // mode is able to get those. Ideally if table stats are not
    // present connector should send the table name and
    // get those from embedded side
    Thread.sleep(21000)
    val stats = SnappyTableStatsProviderService.getService.
        getAggregatedStatsOnDemand._1("APP.SNAPPYTABLE")
    Assert.assertEquals(10000000, stats.getRowCount)
    for (i <- 1 to 100) {
      snc.sql(s"insert into snappyTable values($i,'Test$i')")
    }
    // TODO: Fix this. See the above comment about fixing stats issue
    Thread.sleep(21000)
    val stats1 = SnappyTableStatsProviderService.getService.
        getAggregatedStatsOnDemand._1("APP.SNAPPYTABLE")
    Assert.assertEquals(10000100, stats1.getRowCount)
    logInfo("Successful")
  }

  def splitModeCreateTableUsingCTAS(locatorPort: Int,
      prop: Properties,
      locatorClientPort: Int): Unit = {
    val snc = getSnappyContextForConnector(locatorPort,
      locatorClientPort)
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
        s"USING csv OPTIONS (path '$customerFile')")

    snc.sql(s"CREATE TABLE CUSTOMER AS SELECT * FROM CUSTOMER_STAGING")
    val count = snc.sql("select * from customer").count()
    assert(count == 750, s"Expected 750 rows. Actual rows = $count")

    val customerWithHeadersFile: String = getClass.getResource("/customer_with_headers.csv").getPath
    val customer_csv_DF = snc.read.option("header", "true")
        .option("inferSchema", "true").csv(customerWithHeadersFile)
    val props1 = Map("PARTITION_BY" -> "C_CUSTKEY")
    customer_csv_DF.write.format("column").mode("append").options(props1).saveAsTable("CUSTOMER_2")
    val count2 = snc.sql("select * from customer_2").count()
    assert(count2 == 750, s"Expected 750 rows. Actual rows = $count2")

    // also test temp table
    snc.sql(s"CREATE TEMPORARY TABLE CUSTOMER_TEMP AS SELECT * FROM CUSTOMER_STAGING")
    val count3 = snc.sql("select * from CUSTOMER_TEMP").count()
    assert(count3 == 750, s"Expected 750 rows. Actual rows = $count3")
    val catalog = snc.snappySession.sessionCatalog
    assert(catalog.isTemporaryTable(catalog.newQualifiedTableName("CUSTOMER_TEMP")))
    snc.sql("DROP TABLE CUSTOMER_TEMP")
  }

  override def createDropEmbeddedModeTables(
      tableType: String): Unit = {
    val snc = SnappyContext(sc)
    val df = snc.table("APP.T1")
    assert(df.schema.fields.length == 3)
    snc.dropTable("APP.T1")

    snc.sql(s"CREATE TABLE T1(COL1 STRING, COL2 STRING) " +
        s"USING $tableType OPTIONS (PARTITION_BY 'COL1')")
    snc.sql("INSERT INTO T1 VALUES('AA', 'AA')")
    snc.sql("INSERT INTO T1 VALUES('BB', 'BB')")
    snc.sql("INSERT INTO T1 VALUES('CC', 'CC')")
    snc.sql("INSERT INTO T1 VALUES('DD', 'DD')")
    snc.sql("INSERT INTO T1 VALUES('EE', 'EE')")

  }

  var connectorSnc: SnappyContext = null
  override def createDropTablesInSplitMode(locatorPort: Int,
      prop: Properties,
      locatorClientPort: Int,
      tableType: String): Unit = {
    if (connectorSnc == null || connectorSnc.sparkContext.isStopped) {
      connectorSnc = getSnappyContextForConnector(locatorPort,
        locatorClientPort)
    }
    // row table
    connectorSnc.sql(s"CREATE TABLE T1(C1 INT, C2 INT, C3 INT) " +
        s"USING $tableType OPTIONS (PARTITION_BY 'C1')")
    connectorSnc.sql("INSERT INTO T1 VALUES(1, 1, 1)")
    connectorSnc.sql("INSERT INTO T1 VALUES(2, 2, 2)")
    connectorSnc.sql("INSERT INTO T1 VALUES(3, 3, 3)")
    connectorSnc.sql("INSERT INTO T1 VALUES(4, 4, 4)")
    connectorSnc.sql("INSERT INTO T1 VALUES(5, 5, 5)")

    var rs: Array[Row] = null
    try {
    rs = connectorSnc.sql("select * from t1 order by c1").collect()
  } catch {
    case sqle: SQLException
      if sqle.getSQLState.equals(SQLState.SNAPPY_RELATION_DESTROY_VERSION_MISMATCH) =>
      rs = connectorSnc.sql("select * from t1 order by c1").collect()
    case e: Exception => throw e
  }
    assert(rs.length == 5)
    assert(rs(0).getAs[Int]("C1") == 1)
    assert(rs(0).getAs[Int]("C2") == 1)
    assert(rs(0).getAs[Int]("C3") == 1)
  }

  override def verifyTableFormInSplitMOde(locatorPort: Int,
      prop: Properties,
      locatorClientPort: Int): Unit = {
    if (connectorSnc == null || connectorSnc.sparkContext.isStopped) {
      connectorSnc = getSnappyContextForConnector(locatorPort,
        locatorClientPort)
    }
    var resultDF = connectorSnc.sql("select * from t1 order by col1")
    var rs: Array[Row] = null
    try {
      rs = resultDF.collect()
    } catch {
      case sqle: SQLException
        if sqle.getSQLState.equals(SQLState.SNAPPY_RELATION_DESTROY_VERSION_MISMATCH) =>
        resultDF = connectorSnc.sql("select * from t1 order by col1")
        rs = resultDF.collect()
      case e: Exception => throw e
    }
    assert(rs.length == 5)
    assert(rs(0).getAs[String]("COL1").equals("AA"))
    assert(rs(0).getAs[String]("COL2").equals("AA"))

    connectorSnc.dropTable("APP.T1")
  }
}
