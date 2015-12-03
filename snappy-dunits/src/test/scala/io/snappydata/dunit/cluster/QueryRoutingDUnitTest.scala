package io.snappydata.dunit.cluster

import java.sql.{SQLException, Connection, DriverManager}

import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import dunit.{SerializableRunnable, AvailablePortHelper}

import org.apache.spark.sql.SaveMode

/**
 * Created by kneeraj on 29/10/15.
 */
class QueryRoutingDUnitTest(val s: String) extends ClusterManagerTestBase(s) {

  private val default_chunk_size = GemFireXDUtils.DML_MAX_CHUNK_SIZE

  override def tearDown2(): Unit = {
    //reset the chunk size on lead node
    setDMLMaxChunkSize(default_chunk_size)
  }
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
    val s = conn.createStatement()
    s.execute("select col1 from ColumnTableQR")
    var rs = s.getResultSet
    var cnt = 0
    while (rs.next()) {
      cnt += 1
    }
    assert(cnt == 5)

    var md = rs.getMetaData
    println("metadata col cnt = " + md.getColumnCount + " col name = " +
        md.getColumnName(1) + " col table name = " + md.getTableName(1))
    assert(md.getColumnCount == 1)
    assert(md.getColumnName(1).equals("col1"))
    assert(md.getTableName(1).equalsIgnoreCase("columnTableqr"))

    // 2nd query which compiles in gemxd too but needs to be routed
    s.execute("select * from ColumnTableQR")
    rs = s.getResultSet
    cnt = 0
    while (rs.next()) {
      cnt += 1
    }
    assert(cnt == 5)
    md = rs.getMetaData
    println("2nd metadata col cnt = " + md.getColumnCount + " col name = " +
        md.getColumnName(1) + " col table name = " + md.getTableName(1))
    assert(md.getColumnCount == 3)
    assert(md.getColumnName(1).equals("col1"))
    assert(md.getColumnName(2).equals("col2"))
    assert(md.getColumnName(3).equals("col3"))
    assert(md.getTableName(1).equalsIgnoreCase("columnTableqr"))
    assert(md.getTableName(2).equalsIgnoreCase("columnTableqr"))
    assert(md.getTableName(3).equalsIgnoreCase("columnTableqr"))

    vm1.invoke(new SerializableRunnable() {
      override def run(): Unit = {
        val catalog = Misc.getMemStore.getExternalCatalog
        assert(catalog.isColumnTable("ColumnTableQR"))
      }
    })

    // Now give a syntax error which will give parse error on spark sql side as well
    try {
      s.execute("select ** from sometable")
    } catch {
      case sqe: SQLException =>
        if ("42X01" != sqe.getSQLState && "38000" != sqe.getSQLState) {
          throw sqe
        }
    }
    s.execute("select col1, col2 from ColumnTableQR")
    rs = s.getResultSet
    cnt = 0
    while (rs.next()) {
      cnt += 1
    }
    assert(cnt == 5)
    md = rs.getMetaData
    println("3rd metadata col cnt = " + md.getColumnCount + " col name = " +
        md.getColumnName(1) + " col table name = " + md.getTableName(1))
    assert(md.getColumnCount == 2)

    // reducing DML chunk size size to force lead node to send
    // results in multiple batches
    setDMLMaxChunkSize(50L)
    val expectedResult : Array[Int] = Array(1, 7, 9, 4, 5)
    val actualResult : Array[Int] = new Array[Int](5)
    s.execute("select col1 from ColumnTableQR order by col1")
    rs = s.getResultSet
    cnt = 0
    while(rs.next()) {
      actualResult(cnt) = rs.getInt(1)
      println("----" + rs.getInt(1))
      cnt += 1
    }
    assert(cnt == 5)
//    actualResult.foreach(println)
    assert(expectedResult.sorted.sameElements(actualResult))
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
//    val data = Seq(Seq(2, 2, 3), Seq(1, 8, 9), Seq(1, 2, 3), Seq(2, 2, 3), Seq(1, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createExternalTable(tableName, "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
  }

  def setDMLMaxChunkSize(size: Long): Unit = {
    GemFireXDUtils.DML_MAX_CHUNK_SIZE = size
  }
}

case class Data(col1: Int, col2: Int, col3: Int)
