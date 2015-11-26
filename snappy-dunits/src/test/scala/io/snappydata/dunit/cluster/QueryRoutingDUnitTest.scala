package io.snappydata.dunit.cluster

import java.sql.{Connection, DriverManager, SQLException}

import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import dunit.AvailablePortHelper
import io.snappydata.gemxd.SparkSQLExecuteImpl

import org.apache.spark.sql.SaveMode

/**
  * Created by kneeraj on 29/10/15.
  */
class QueryRoutingDUnitTest(val s: String) extends ClusterManagerTestBase(s) {

  private def getANetConnection(netPort: Int): Connection = {
    val driver = "com.pivotal.gemfirexd.jdbc.ClientDriver"
    Class.forName(driver).newInstance //scalastyle:ignore
    val url = "jdbc:snappydata://localhost:" + netPort + "/"
    DriverManager.getConnection(url)
  }

  def testDummy(): Unit = {

  }

  def _testQueryRouting(): Unit = {
    // Lead is started before other servers are started.
    QueryRoutingDUnitTest.startSnappyServer(locatorPort, props)
    val fullStartArgs = startArgs :+ true.asInstanceOf[AnyRef]
    vm0.invoke(this.getClass, "startSnappyLead", fullStartArgs)
    val netport1 = AvailablePortHelper.getRandomAvailableTCPPort
    QueryRoutingDUnitTest.startNetServer(netport1)

    vm0.invoke(this.getClass, "createTablesAndInsertData")
    val conn = getANetConnection(netport1)
    val s = conn.createStatement()
    s.execute("select col1 from ColumnTableQR")
    var rs = s.getResultSet
    var cnt = 0
    while(rs.next()) {
      cnt += 1
    }
    assert(cnt == 5)

    var md = rs.getMetaData
    println("KN: metadata col cnt = " + md.getColumnCount + " col name = " + md.getColumnName(1) + " col table name = " + md.getTableName(1))
    assert(md.getColumnCount == 1)
    assert(md.getColumnName(1).equals("col1"))
    assert (md.getTableName(1).equalsIgnoreCase("columnTableqr"))

    // 2nd query which compiles in gemxd too but needs to be routed
    s.execute("select * from ColumnTableQR")
    rs = s.getResultSet
    cnt = 0
    while(rs.next()) {
      cnt += 1
    }
    assert(cnt == 5)
    md = rs.getMetaData
    println("KN: 2nd metadata col cnt = " + md.getColumnCount + " col name = " + md.getColumnName(1) + " col table name = " + md.getTableName(1))
    assert(md.getColumnCount == 3)
    assert(md.getColumnName(1).equals("col1"))
    assert(md.getColumnName(2).equals("col2"))
    assert(md.getColumnName(3).equals("col3"))
    assert (md.getTableName(1).equalsIgnoreCase("columnTableqr"))
    assert (md.getTableName(2).equalsIgnoreCase("columnTableqr"))
    assert (md.getTableName(3).equalsIgnoreCase("columnTableqr"))
    val catalog = Misc.getMemStore.getExternalCatalog
    assert(catalog.isColumnTable("ColumnTableQR"))

    // Now give a syntax error which will give parse error on spark sql side as well
    try {
      s.execute("select ** from sometable")
    }
    catch {
      case sqe: SQLException => {
        println("KN: sql state = " + sqe.getSQLState)
        sqe.printStackTrace()
        val cause = sqe.getCause
        if (cause != null) {
          cause.printStackTrace()
        }
        assert("42X01".equalsIgnoreCase(sqe.getSQLState) || "38000".equalsIgnoreCase(sqe.getSQLState))
      }
      case e: Exception => throw new RuntimeException("unexpected exception " + e.getMessage, e)
    }
    s.execute("select col1, col2 from ColumnTableQR")
    rs = s.getResultSet
    cnt = 0
    while(rs.next()) {
      cnt += 1
    }
    assert(cnt == 5)
    md = rs.getMetaData
    println("KN: 3rd metadata col cnt = " + md.getColumnCount + " col name = " + md.getColumnName(1) + " col table name = " + md.getTableName(1))
    assert(md.getColumnCount == 2)

    vm0.invoke(this.getClass, "setResultStreamBatchSize")
    s.execute("select col1 from ColumnTableQR order by col1")
    rs = s.getResultSet
    cnt = 0
    while(rs.next()) {
//      println("----" + rs.getInt(1))
      cnt += 1
    }
    assert(cnt == 5)
    QueryRoutingDUnitTest.stopSpark
  }
}



case class Data(col1: Int, col2: Int, col3: Int)

/**
  * Since this object derives from ClusterManagerTestUtils
  */
object QueryRoutingDUnitTest extends ClusterManagerTestUtils {
  def createTablesAndInsertData(): Unit = {
    logger.info("KN: spark context = " + sc + " and spark conf = \n" + sc.getConf.toDebugString)
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
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createExternalTable(tableName, "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
  }

  def setResultStreamBatchSize(): Unit = {
    // reducing max no of rows in one batch
    SparkSQLExecuteImpl.NUM_ROWS_IN_BATCH = 3
    // reducing batch DML size to force flush on smaller batch size
    GemFireXDUtils.DML_MAX_CHUNK_SIZE = 50
  }
}

