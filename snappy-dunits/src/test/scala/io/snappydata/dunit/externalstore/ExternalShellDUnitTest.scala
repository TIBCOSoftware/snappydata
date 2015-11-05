package io.snappydata.dunit.externalstore

import java.net.URL
import java.sql.DriverManager
import java.util.Properties
import io.snappydata.dunit.cluster.ClusterManagerTestBase
import io.snappydata.dunit.cluster.ClusterManagerTestUtils
import org.apache.spark.scheduler.cluster.DelegateClusterManager
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.{SparkContext, SparkConf}

import org.apache.spark.sql.SaveMode
import scala.collection.Map
import sys.process._
/**
 * Created by skumar on 20/10/15.
 */
class ExternalShellDUnitTest(s: String) extends ClusterManagerTestBase(s)  with Serializable{

  override val locatorNetPort =1527
  def testTableCreation(): Unit = {
      println ("Namrata locator port is " + startArgs(0));

    vm2.invoke(this.getClass, "startSnappyServer", startArgs)
    vm1.invoke(this.getClass, "startSnappyLead", startArgs)

    vm3.invoke(this.getClass, "startSparkCluster")

    vm1.invoke(this.getClass, "startEmbeddedClusterJob")

    vm3.invoke(this.getClass, "startSparkJob" , startArgs)

    vm1.invoke(this.getClass, "stopSpark")
    vm3.invoke(this.getClass, "stopSparkCluster")

  }
}

/**
 * Since this object derives from ClusterManagerTestUtils
 */
object ExternalShellDUnitTest extends ClusterManagerTestUtils with Serializable {
  private val tableName: String = "ColumnTable"

  val props = Map.empty[String, String]

  def startEmbeddedClusterJob(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))

    val rdd = sc.parallelize(data, data.length).map(s => Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createExternalTable(tableName, "column", dataDF.schema, props)
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect()
    assert(r.length == 0)

    //snc.dropExternalTable(tableName, ifExists = true)
    println("Successful")
  }

  def startSparkJob(locatorPort: Int, prop: Properties): Unit = {

    SparkContext.registerClusterManager(DelegateClusterManager)

    DriverRegistry.register("com.pivotal.gemfirexd.jdbc.ClientDriver")
    val conn = DriverManager.getConnection("jdbc:snappydata://localhost:1590")
    if (conn != null ) {
      conn.close()
    }


    val conf = new SparkConf().
        setAppName("test Application")
        .setMaster(s"external:snappy:spark://pnq-nthanvi02:$locatorPort")
        .set("snappy.locator", s"localhost[$locatorPort]")
      //switching to http broadcast as could not run jobs with TorrentBroadcast
      // .set("spark.broadcast.factory" , "org.apache.spark.broadcast.HttpBroadcastFactory")


    val sc = new SparkContext(conf)


    val snc = org.apache.spark.sql.SnappyContext(sc)

    var hfile: String = "/home/namrata/snappy-commons/snappy-dunits/src/test/resources/2015.parquet"

    val dataDF = snc.read.load(hfile)

    snc.createExternalTable(tableName, "column", dataDF.schema, props)


    dataDF.write.format("column").mode(SaveMode.Append)
        .options(props).saveAsTable(tableName)


    val result = snc.sql("SELECT * FROM " + tableName)

    val r = result.collect()

    assert(r.length == 5)

    snc.dropExternalTable(tableName, ifExists = true)
    println("Successful")
  }


  def startSparkCluster = {
    val currentDir = "pwd" !!

    "../../../../../snappy/sbin/start-all.sh" !!
  }

  def stopSparkCluster = {
    "../../../../../snappy/sbin/stop-all.sh" !!
  }
}