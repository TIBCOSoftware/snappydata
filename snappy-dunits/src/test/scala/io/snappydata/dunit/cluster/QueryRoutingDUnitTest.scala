package io.snappydata.dunit.cluster

import java.sql.{DriverManager, Connection}

import dunit.AvailablePortHelper
//import io.snappydata.app.Data

import org.apache.spark.sql.{AnalysisException, SaveMode}

/**
 * Created by kneeraj on 29/10/15.
 */
class QueryRoutingDUnitTest(val s: String) extends ClusterManagerTestBase(s) {

  private def getANetConnection(netPort: Int): Connection = {
    val driver = "com.pivotal.gemfirexd.jdbc.ClientDriver"
    Class.forName(driver).newInstance
    val url = "jdbc:snappydata://localhost:" + netPort + "/"
    DriverManager.getConnection(url)
  }

  def testQueryRouting(): Unit = {
    // Lead is started before other servers are started.
    //vm0.invoke(this.getClass, "startSnappyLead", Array.fill[AnyRef](1)(Integer.valueOf(locatorPort)))
    QueryRoutingDUnitTest.startSnappyServer(locatorPort, props)
    //vm1.invoke(this.getClass, "startSnappyServer", startArgs)
    vm0.invoke(this.getClass, "startSnappyLead", startArgs)
    //vm2.invoke(this.getClass, "startSnappyServer")
    val netport1 = AvailablePortHelper.getRandomAvailableTCPPort
    //val netport2 = AvailablePortHelper.getRandomAvailableTCPPort
    //vm1.invoke(this.getClass, "startNetServer", Array(Integer.valueOf(netport1).asInstanceOf[AnyRef]))
    //vm2.invoke(this.getClass, "startNetServer", Array.fill[AnyRef](1)(Integer.valueOf(netport2)))
    QueryRoutingDUnitTest.startNetServer(netport1)

    // Execute the job
    vm0.invoke(this.getClass, "createTablesAndInsertData")
    //Thread.sleep(10000)
    val conn = getANetConnection(netport1)
    val s = conn.createStatement()
    s.execute("select Col1 from ColumnTable")
    val rs = s.getResultSet
    var cnt = 0
    while(rs.next()) {
      //println("KN: row["+cnt+"] = " + rs.getObject(1) + ", " + rs.getObject(2) + ", " + rs.getObject(3))
      println("KN: row["+cnt+"] = " + rs.getObject(1))
      cnt += 1
    }
    println("KN: total count is = " + cnt)
    assert(cnt == 5)
  }
}

case class Data(col1: Int, col2: Int, col3: Int)

/**
 * Since this object derives from ClusterManagerTestUtils
 */
object QueryRoutingDUnitTest extends ClusterManagerTestUtils {
  def createTablesAndInsertData(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)
    val tableName : String = "ColumnTable"

    val props = Map(
      "url" -> "jdbc:snappydata:;user=app;password=app;persist-dd=false",
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "poolImpl" -> "tomcat",
      "user" -> "app",
      "password" -> "app"
    )

    val options =  "OPTIONS (url 'jdbc:snappydata:;user=app;password=app;persist-dd=false' ," +
        "driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver' ," +
        "poolImpl 'tomcat', " +
        "user 'app', " +
        "password 'app' ) "

    snc.sql("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 INT) " + " USING column " +
        options )

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
  }
}

