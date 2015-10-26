package io.snappydata.dunit.externalstore

import io.snappydata.dunit.cluster.ClusterManagerTestBase
import io.snappydata.dunit.cluster.ClusterManagerTestUtils

import org.apache.spark.sql.SaveMode

/**
 * Created by skumar on 20/10/15.
 */
class ColumnTableDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  def testTableCreation(): Unit = {
    // Lead is started before other servers are started.
    vm0.invoke(this.getClass, "startSnappyLead")
    vm1.invoke(this.getClass, "startSnappyServer")
    vm2.invoke(this.getClass, "startSnappyServer")
    vm3.invoke(this.getClass, "startSnappyServer")

    vm0.invoke(this.getClass, "startSparkJob")

    vm3.invoke(this.getClass, "stopSnappyServer")
    vm2.invoke(this.getClass, "stopSnappyServer")
    vm1.invoke(this.getClass, "stopSnappyServer")
    vm0.invoke(this.getClass, "stopSnappyLead")
  }

  def testCreateInsertAndDropOfTable(): Unit = {
    // Lead is started before other servers are started.
    vm0.invoke(this.getClass, "startSnappyLead")
    vm1.invoke(this.getClass, "startSnappyServer")
    vm2.invoke(this.getClass, "startSnappyServer")
    vm3.invoke(this.getClass, "startSnappyServer")

    vm0.invoke(this.getClass, "startSparkJob2")

    vm3.invoke(this.getClass, "stopSnappyServer")
    vm2.invoke(this.getClass, "stopSnappyServer")
    vm1.invoke(this.getClass, "stopSnappyServer")
    vm0.invoke(this.getClass, "stopSnappyLead")
  }
}

/**
 * Since this object derives from ClusterManagerTestUtils
 */
object ColumnTableDUnitTest extends ClusterManagerTestUtils {
  private val tableName: String = "ColumnTable"

  val props = Map(
    //"url" -> "jdbc:gemfirexd:;mcast-port=33619;user=app;password=app;persist-dd=false",
    "url" -> "jdbc:snappydata:;user=app;password=app;persist-dd=false",
    //"url" -> "jdbc:gemfirexd://localhost:1527/;user=app;password=app;persist-dd=false",
    "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
    //"driver" -> "com.pivotal.gemfirexd.jdbc.ClientDriver",
    "poolImpl" -> "tomcat",
    "user" -> "app",
    "password" -> "app"
  )

  def startSparkJob(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createExternalTable(tableName, "column", dataDF.schema, props)
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 0)

    snc.dropExternalTable(tableName, true)
    println("Successful")
  }

  def startSparkJob2(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createExternalTable(tableName, "column", dataDF.schema, props)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect
    assert(r.length == 5)

    snc.dropExternalTable(tableName, true)
    println("Successful")
  }

}

case class Data(col1: Int, col2: Int, col3: Int)