package io.snappydata.dunit.cluster

import java.rmi.NoSuchObjectException
import java.sql.{DriverManager, Connection}
import com.pivotal.gemfirexd.internal.engine.Misc
import dunit.AvailablePortHelper

import org.apache.spark.sql.{AnalysisException, SaveMode}

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
    props.setProperty("log-level", "fine");
    DDLRoutingDUnitTest.startSnappyServer(locatorPort, props)
    val fullStartArgs = startArgs :+ true.asInstanceOf[AnyRef]
    vm0.invoke(this.getClass, "startSnappyLead", fullStartArgs)
    val netport1 = AvailablePortHelper.getRandomAvailableTCPPort
    DDLRoutingDUnitTest.startNetServer(netport1)

    //vm0.invoke(this.getClass, "createTablesAndInsertData")
//    val conn = getANetConnection(netport1)
//    val s = conn.createStatement()
//    Misc.getMemStore.initExternalCatalog
//    s.execute("select col1 from ColumnTableQR")
//    val rs = s.getResultSet
//    var cnt = 0
//    while(rs.next()) {
//      cnt += 1
//    }
//    assert(cnt == 5)
//    val catalog = Misc.getMemStore.getExternalCatalog
//    val tt = catalog.isColumnTable("ColumnTableQR")
//    println("KN: tt for isColumn for ColumnTable = " + tt)
        val conn = getANetConnection(netport1)
        val s = conn.createStatement()
        Misc.getMemStore.initExternalCatalog
        val tableName: String = "ColumnTableQR"
        val options = "OPTIONS (url 'jdbc:snappydata:;user=app;password=app;persist-dd=false;route-query=false' ," +
          "driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver' ," +
          "poolImpl 'tomcat', " +
          "user 'app', " +
          "password 'app' ) "

//        s.execute("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 INT) " + " USING column " + options )
//        println("KN: Created ColumnTable = " + tableName)
        try {
          val catalog = Misc.getMemStore.getExternalCatalog
          val tt = catalog.isColumnTable("ColumnTableQR")
          println("KN: tt for isColumn for ColumnTable = " + tt)
        }
      catch {
        case e : Exception => println("Caught exception " + e.getMessage +
          " for isColumn for ColumnTable = " + tableName)
      }
    vm0.invoke(this.getClass, "queryData")
  }
}

case class DDLRoutingData(col1: Int, col2: Int, col3: Int)

/**
 * Since this object derives from ClusterManagerTestUtils
 */
object DDLRoutingDUnitTest extends ClusterManagerTestUtils {
  def createTablesAndInsertData(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)
    val tableName : String = "ColumnTableNONE"

    val props = Map(
      "url" -> "jdbc:snappydata:;user=app;password=app;persist-dd=false;route-query=false",
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "poolImpl" -> "tomcat",
      "user" -> "app",
      "password" -> "app"
    )

    val options =  "OPTIONS (url 'jdbc:snappydata:;user=app;password=app;persist-dd=false;route-query=false' ," +
        "driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver' ," +
        "poolImpl 'tomcat', " +
        "user 'app', " +
        "password 'app' ) "

    //snc.sql("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 INT) " + " USING column " +
      //  options )

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new DDLRoutingData(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createExternalTable(tableName, "column", dataDF.schema,props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
  }

  def queryData(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)
    val tableName : String = "ColumnTableQR"

    val dataDF = snc.sql("Select * from " + tableName)
    dataDF.map(t => "Col1: " + t(0) + " Col2: " + t(1) + " Col3: " + t(2)).collect().foreach(println)
  }
}

