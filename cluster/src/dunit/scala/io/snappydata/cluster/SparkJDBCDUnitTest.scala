/*
 *
 */
package io.snappydata.cluster

import java.nio.file.{Files, Paths}
import java.util.Properties

import io.snappydata.test.dunit.AvailablePortHelper
import org.apache.spark.Logging
import org.apache.spark.sql.{SnappyContext, SparkSession}

import scala.sys.process._

/**
  * Basic tests for non-embedded mode connections to an embedded cluster.
  */
class SparkJDBCDUnitTest(s: String)
  extends ClusterManagerTestBase(s)
    with Serializable {

  override val locatorNetPort: Int = testObject.locatorNetPort

  override val stopNetServersInTearDown = false

  val currentLocatorPort: Int = ClusterManagerTestBase.locPort

  protected val productDir: String =
    testObject.getEnvironmentVariable("APACHE_SPARK_HOME")

  private val snappyProductDir =
    testObject.getEnvironmentVariable("SNAPPY_HOME")

  protected def testObject = SparkJDBCDUnitTest

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

  protected def locatorClientPort = { locatorNetPort }

  protected def startNetworkServers(): Unit = {
    startNetworkServersOnAllVMs()
  }

  def testSparkSubmit(): Unit = {
    val snContext = SnappyContext(sc)
    // Creating Snappy Table using snappy session
    testObject.createAirlineTable(productDir, snContext)

    // Executing spark driver application via spark-submit,
    // Which reads data from snappy table.
    testObject.invokeSparkSubmitForJDBC(snappyProductDir, locatorClientPort)

    // Creating Snappy Table using snappy session
    testObject.dropAirlineTable(snContext)
  }
}

object SparkJDBCDUnitTest extends SplitClusterDUnitTestObject
  with Logging {

  val locatorNetPort = AvailablePortHelper.getRandomAvailableTCPPort

  override def createTablesAndInsertData(tableType: String): Unit = {}

  override def createComplexTablesAndInsertData(props: Map[String, String]): Unit = {}

  override def verifySplitModeOperations(tableType: String, isComplex: Boolean,
                                         props: Map[String, String]): Unit = {}

  override def assertTableNotCachedInHiveCatalog(tableName: String): Unit = {}

  def invokeSparkSubmitForJDBC(productDir: String, locatorNetPort: Int): Unit = {

    logInfo(s"Testing JDBC Pool driver with stock spark, with spark-submit command")

    // perform some operation thru spark-shell
    val jars = Files.newDirectoryStream(Paths.get(s"$productDir/../distributions/"),
      "snappydata-client-*.jar")
    val snappyDataClientJar = jars.iterator().next().toAbsolutePath.toString

    val exampleJar = Files.newDirectoryStream(
      Paths.get(s"$productDir/../../../cluster/build-artifacts/scala-2.11/libs/"),
      "snappydata-cluster_*-tests.jar")

    val snappyTestClassJar = exampleJar.iterator().next().toAbsolutePath.toString

    // SparkSqlTestCode.txt file contains the commands executed on spark-shell
    val sparkSubmitCommand = productDir + "/bin/spark-submit  --master local[3]" +
      s""" --driver-java-options "-DlocatorPort=$locatorNetPort" """ +
      " --class io.snappydata.cluster.SparkJDBCTestJob" +
      s" --jars $snappyDataClientJar $snappyTestClassJar"

    logInfo(s"About to invoke spark-submit with command: $sparkSubmitCommand")

    var output = sparkSubmitCommand.!!
    logInfo(output)
    output = output.replaceAll("NoSuchObjectException", "NoSuchObject")
    assert(!output.contains("Exception"),
      s"Some exception stacktrace seen on spark-shell console: $output")
  }

  def createAirlineTable(productDir: String, snContext: SnappyContext): Unit = {
    val props = Map("PARTITION_BY" -> "YEAR", "buckets" -> "16")
    val airlinefilePath = s"$productDir/../../examples/quickstart/data/airlineParquetData"
    val airlineDF = snContext.read.parquet(airlinefilePath)
    snContext.dropTable("AIRLINE", ifExists = true)
    snContext.createTable("AIRLINE", "column", airlineDF.schema, props)
    airlineDF.write.insertInto("AIRLINE")
  }

  def dropAirlineTable(snContext: SnappyContext): Unit = {
    snContext.dropTable("AIRLINE", ifExists = true)
  }
}
