package io.snappydata.dunit.cluster

import java.sql.{SQLException, DriverManager, Connection}
import java.util.Properties

import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.store.GemFireStore
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedPreparedStatement
import dunit.{SerializableRunnable, AvailablePortHelper}
import io.snappydata.ServiceManager
import org.apache.spark.sql.SaveMode

/**
 * Created by vivekb on 4/11/15.
 */
class PreparedStatementDUnitTest(val s: String) extends ClusterManagerTestBase(s) {

  private def getANetConnection(netPort: Int): Connection = {
    val driver = "com.pivotal.gemfirexd.jdbc.ClientDriver"
    Class.forName(driver).newInstance //scalastyle:ignore
    val url = "jdbc:snappydata://localhost:" + netPort + "/"
    DriverManager.getConnection(url)
  }

  def testQueryRouting(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    createTablesAndInsertData()
    val conn = getANetConnection(netPort1)
    val ps = conn.prepareStatement("select col1 from ColumnTableQR where  col1 > ? and col1 < ?")
    ps.setInt(1, 1)
    ps.setInt(2, 1000)
    val rs2 = ps.executeQuery()
    var cnt2 = 0
    while(rs2.next()) {
      //println("KN: row["+cnt2+"] = " + rs2.getObject(1) + ", " + rs2.getObject(2) + ", " + rs2.getObject(3))
      println("KN: row["+cnt2+"] = " + rs2.getObject(1))
      cnt2 += 1
    }
    println("KN: total count is = " + cnt2)
    assert(cnt2 == 5)
  }

  def createTablesAndInsertData(): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)
    val tableName: String = "ColumnTableQR"

    val props = Map(
      "url" -> "jdbc:snappydata:;persist-dd=false;route-query=false",
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "poolImpl" -> "tomcat",
      "user" -> "app",
      "password" -> "app"
    )

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data1(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createExternalTable(tableName, "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
  }
}

case class Data1(col1: Int, col2: Int, col3: Int)

