package io.snappydata.dunit.cluster

import java.sql.{DriverManager, Connection}
import com.pivotal.gemfirexd.internal.engine.Misc
import dunit.AvailablePortHelper

/**
 * Created by vbhaskar on 16/11/15.
 */
class DDLRoutingDUnitTest(val s: String) extends ClusterManagerTestBase(s) {

  private def getANetConnection(netPort: Int): Connection = {
    val driver = "com.pivotal.gemfirexd.jdbc.ClientDriver"
    Class.forName(driver).newInstance
    val url = "jdbc:snappydata://localhost:" + netPort + "/"
    DriverManager.getConnection(url)
  }

  def testDDLRouting(): Unit = {
    // Lead is started before other servers are started.
    DDLRoutingDUnitTest.startSnappyServer(locatorPort, props)
    val fullStartArgs = startArgs :+ true.asInstanceOf[AnyRef]
    vm0.invoke(this.getClass, "startSnappyLead", fullStartArgs)
    Misc.getMemStore.initExternalCatalog
    val tableName: String = "ColumnTableQR"

    try {
      val netport1 = AvailablePortHelper.getRandomAvailableTCPPort
      DDLRoutingDUnitTest.startNetServer(netport1)
      val conn = getANetConnection(netport1)
      val s = conn.createStatement()
      val options = "OPTIONS (url 'jdbc:snappydata:;user=app;password=app;persist-dd=false;route-query=false' ," +
        "driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver' ," +
        "poolImpl 'tomcat', " +
        "user 'app', " +
        "password 'app' ) "
      s.execute("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 INT) " + " USING column " + options)
      println("Successfully Created ColumnTable = " + tableName)
    }
    catch {
      case e: Exception => println("create: Caught exception " + e.getMessage +
        " for ColumnTable = " + tableName)
    }
    println("Created ColumnTable = " + tableName)
    try {
      val netport1 = AvailablePortHelper.getRandomAvailableTCPPort
      DDLRoutingDUnitTest.startNetServer(netport1)
      val conn = getANetConnection(netport1)
      val s = conn.createStatement()
      s.execute("insert into " + tableName + " values(1, 2, 3) ")
      println("Successfully inserted values in ColumnTable = " + tableName)
    }
    catch {
      case e: Exception => println("insert: Caught exception " + e.getMessage +
        " for ColumnTable = " + tableName)
    }
    println("inserted values in ColumnTable = " + tableName)
    try {
      val catalog = Misc.getMemStore.getExternalCatalog
      val tt = catalog.isColumnTable("ColumnTableQR")
      println("tt for isColumn for ColumnTable = " + tt)
    }
    catch {
      case e: Exception => println("metadata: Caught exception " + e.getMessage +
        " for isColumn for ColumnTable = " + tableName)
    }
    vm0.invoke(this.getClass, "queryData", tableName)
  }
}

/**
 * Since this object derives from ClusterManagerTestUtils
 */
object DDLRoutingDUnitTest extends ClusterManagerTestUtils {

  def queryData(tableName : String): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)
    println("Firing select on ColumnTable = " + tableName)
    val dataDF = snc.sql("Select * from " + tableName)
    dataDF.map(t => "Select Query: Col1: " + t(0) + " Col2: " + t(1) + " Col3: " + t(2)).collect().foreach(println)
  }
}

