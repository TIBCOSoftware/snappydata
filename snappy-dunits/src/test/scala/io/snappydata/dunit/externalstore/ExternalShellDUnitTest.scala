package io.snappydata.dunit.externalstore

import java.net.InetAddress
import java.util.Properties
import com.pivotal.gemfirexd.internal.catalog.SystemProcedures
import dunit.AvailablePortHelper
import dunit.DistributedTestBase.ExpectedException
import io.snappydata.dunit.cluster.ClusterManagerTestBase
import io.snappydata.dunit.cluster.ClusterManagerTestUtils
import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.sql.{AnalysisException, SQLContext, SnappyContext, SaveMode}
import util.TestException
import scala.collection.Map
import sys.process._

/**
 * Created by nthanvi on 20/10/15.
 */
class ExternalShellDUnitTest(s: String) extends ClusterManagerTestBase(s) with Serializable {

  override val locatorNetPort = AvailablePortHelper.getRandomAvailableTCPPort


  def dummyTest(): Unit = {

  }

  def _testTableCreation(): Unit = {

    vm2.invoke(this.getClass, "startSnappyServer", startArgs)

    vm2.invoke(this.getClass, "startNetServer", AvailablePortHelper.getRandomAvailableTCPPort.asInstanceOf[AnyRef])

    val fullStartArgs = startArgs :+ true.asInstanceOf[AnyRef]

    vm1.invoke(this.getClass, "startSnappyLead", fullStartArgs)

    vm3.invoke(this.getClass, "startSparkCluster")

    //Embedded Cluster Operations
    vm1.invoke(this.getClass, "createTablesAndInsertData")

    //StandAlone Spark Cluster Operations
    vm3.invoke(this.getClass, "VerifyEmbeddedTablesAndCreateNewInShell", startArgs)

    //Embedded Cluster Verifying the Spark Cluster Operations
    vm1.invoke(this.getClass, "VerifyShellModeOperations")

    vm3.invoke(this.getClass, "stopSparkCluster")

    println("Test Completed Sucessfully")

  }

}

/**
 * Since this object derives from ClusterManagerTestUtils
 */
object ExternalShellDUnitTest extends ClusterManagerTestUtils with Serializable {
  private var tableName: String = "ColumnTable"

  val props = Map.empty[String, String]

  def createTablesAndInsertData(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    createTableUsingDataSourceAPI(snc, "embeddedModeTable1")
    selectFromTable(snc, "embeddedModeTable1", 5)

    createTableUsingDataSourceAPI(snc, "embeddedModeTable2")
    selectFromTable(snc, "embeddedModeTable2", 5)

    println("Successful")
  }


  def VerifyShellModeOperations(): Unit = {
    // embeddedModeTable1 is dropped in shell mode. recreate it
    val snc = org.apache.spark.sql.SnappyContext(sc)
    createTableUsingDataSourceAPI(snc, "embeddedModeTable1")
    selectFromTable(snc, "embeddedModeTable1", 5)

    snc.dropExternalTable("embeddedModeTable1", ifExists = true)

    // embeddedModeTable2 still exists drop it
    snc.dropExternalTable("embeddedModeTable2", ifExists = true)

    // read data from shellModeTable1
    selectFromTable(snc, "shellModeTable1", 5)

    //drop table created in shell mode
    snc.dropExternalTable("shellModeTable1", ifExists = true)

    //recreate the dropped table
    createTableUsingDataSourceAPI(snc, "shellModeTable1")
    selectFromTable(snc, "shellModeTable1", 5)
    snc.dropExternalTable("shellModeTable1", ifExists = true)
    println("Successful")
  }

  def VerifyEmbeddedTablesAndCreateNewInShell(locatorPort: Int, prop: Properties): Unit = {

    val hostName = InetAddress.getLocalHost.getHostName
    //This  should be created automatically in the same way for embedded and non embedded shell mode
    val snappydataurl = "jdbc:snappydata:;locators=localhost[" +
      locatorPort + "];route-query=false;user=HIVE_METASTORE;default-persistent=true"

    val conf = new SparkConf().
         setAppName("test Application")
        .setMaster(s"spark://$hostName:7077")
        .set("snappydata.locators", s"localhost:$locatorPort")
        .set("gemfirexd.db.url", snappydataurl)
        .set("gemfirexd.db.driver", "com.pivotal.gemfirexd.jdbc.EmbeddedDriver")

      //TODO - how to pass this class to spark executors??
        .set ("spark.executor.extraClassPath" , getEnvironmentVariable("SNAPPY_DIST_CLASSPATH"))


    val sc = new SparkContext(conf)
    val snc = org.apache.spark.sql.SnappyContext(sc)

    //try to create the table already created in embedded mode. it should throw the table exist exception.
    var tableAlreadyExistException: Exception = null
    try {
      createTableUsingDataSourceAPI(snc, "embeddedModeTable1")
    } catch {
      case e: AnalysisException => tableAlreadyExistException = e
    }
    assert(tableAlreadyExistException != null)
    assert(tableAlreadyExistException.getMessage.contains("Table embeddedModeTable1 already exists"))


    //select the data from table created in embedded mode
    selectFromTable(snc, "embeddedModeTable1", 5)

    // drop the table created in embedded mode
    snc.dropExternalTable("embeddedModeTable1", ifExists = true)

    //select the data from table created in embedded mode
    selectFromTable(snc, "embeddedModeTable2", 5)


    //create a table in shell mode
    createTableUsingDataSourceAPI(snc, "shellModeTable1")
    selectFromTable(snc, "shellModeTable1", 5)
    sc.stop()
    println("Successful")
  }


  def createTableUsingDataSourceAPI(sqlContext: SQLContext, tableName: String) = {
    val context = sqlContext.sparkContext
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = context.parallelize(data, data.length).map(s => Data(s(0), s(1), s(2)))

    val dataDF = sqlContext.createDataFrame(rdd)

    sqlContext.createExternalTable(tableName, "column", dataDF.schema, props)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    //sqlContext.createExternalTable(tableName, "column", dataDF.schema, props)
  }

  def selectFromTable(sqlContext: SQLContext, tableName: String, expectedLength: Int): Unit = {
    val result = sqlContext.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == expectedLength)
  }

  def getEnvironmentVariable(env:String):String =  {
    val value = scala.util.Properties.envOrElse(env, null)
    if (env == null )
      throw new TestException(s" Environment variable $env is not defined")
    value

  }
  def startSparkCluster = {
    (getEnvironmentVariable("SNAPPY_HOME") + "/sbin/start-all.sh") !!
  }

  def stopSparkCluster = {
    (getEnvironmentVariable("SNAPPY_HOME") + "/sbin/stop-all.sh") !!
  }
}