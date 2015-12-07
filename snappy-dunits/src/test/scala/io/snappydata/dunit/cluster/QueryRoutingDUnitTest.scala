package io.snappydata.dunit.cluster

import java.sql.{SQLException, Connection, DriverManager}

import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import dunit.{SerializableRunnable, AvailablePortHelper}

import org.apache.spark.sql.{SnappyContext, SaveMode}

/**
 * Tests for query routing from JDBC client driver.
 *
 * Created by kneeraj on 29/10/15.
 */
class QueryRoutingDUnitTest(val s: String) extends ClusterManagerTestBase(s) {

  private val default_chunk_size = GemFireXDUtils.DML_MAX_CHUNK_SIZE

  override def tearDown2(): Unit = {
    //reset the chunk size on lead node
    setDMLMaxChunkSize(default_chunk_size)
    super.tearDown2()
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

    createTableAndInsertData()
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
        assert(catalog.isColumnTable("ColumnTableQR", false))
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

    s.execute("select * from ColumnTableQR where col1 > 4")
    rs = s.getResultSet
    cnt = 0
    while (rs.next()) {
      cnt += 1
    }
    assert(cnt == 3)

    s.execute("select col1 from ColumnTableQR where col1 > 0 order by col1 desc")
    rs = s.getResultSet
    cnt = 0
    // 1, 7, 9, 4, 5
    while (rs.next()) {
      cnt += 1
      cnt match {
        case 1 => assert(9 == rs.getInt(1))
        case 2 => assert(7 == rs.getInt(1))
        case 3 => assert(5 == rs.getInt(1))
        case 4 => assert(4 == rs.getInt(1))
        case 5 => assert(1 == rs.getInt(1))
      }
    }
    assert(cnt == 5)

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

    conn.close()
  }

  def testSNAP193(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    createTableAndInsertData2()
    val conn = getANetConnection(netPort1)
    val stmt = conn.createStatement()

    val numExpectedRows = 1888622
    val rs = stmt.executeQuery("select count(UniqueCarrier) from Airline")
    assert(rs.next())
    assert(rs.getInt(1) == numExpectedRows, "got rows=" + rs.getInt(1))
    assert(!rs.next())

    val md = rs.getMetaData
    println("metadata colCount=" + md.getColumnCount + " colName=" +
        md.getColumnName(1) + " tableName=" + md.getTableName(1))
    assert(md.getColumnCount == 1)
    assert(md.getColumnName(1) == "_c0", "columnName=" + md.getColumnName(1))

    // below hangs in CREATE TABLE for some reason; need to check
    /*
    stmt.execute("CREATE TABLE airline2 USING column AS " +
        "(select * from airline limit 10000)")
    rs = stmt.executeQuery("select count(*) from Airline2")
    assert(rs.next())
    assert(rs.getInt(1) == 10000, "got rows=" + rs.getInt(1))
    assert(!rs.next())

    // now check for ClassCastException with a "select *"
    rs = stmt.executeQuery("select * from Airline2")
    var cnt = 0
    while (rs.next()) {
      cnt += 1
    }
    assert(cnt == 10000, "got cnt=" + cnt)
    */

    conn.close()
  }

  def testSystablesQueries(): Unit = {
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)

    createTableAndInsertData()
    val conn = getANetConnection(netPort1)
    try {
      val s = conn.createStatement()

      // SYSTABLES queries
      s.execute("CREATE TABLE COLUMNTABLE (Col1 INT, Col2 INT, Col3 INT) USING column OPTIONS ()")
      s.execute("select * from sys.systables where tablename='COLUMNTABLE'")
      var rs = s.getResultSet
      assert(rs.next())
      var tableType = rs.getString("tabletype")
      assert("C".equals(tableType))

      s.execute("CREATE TABLE ROWTABLE (Col1 INT, Col2 INT, Col3 INT) USING row OPTIONS ()")
      s.execute("select * from sys.systables where tablename='ROWTABLE'")
      rs = s.getResultSet
      assert(rs.next())
      tableType = rs.getString("tabletype")
      assert("T".equals(tableType))

      s.execute("select * from sys.systables where tableschemaname='APP'")
      rs = s.getResultSet
      var cnt = 0
      while (rs.next()) {
        cnt += 1
      }
      assert(cnt == 3) // ColumnTableQR, COLUMNTABLE and ROWTABLE
    } finally {
      conn.close()
    }
  }

  def createTableAndInsertData(): Unit = {
    val snc = SnappyContext(sc)
    val tableName: String = "ColumnTableQR"

    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3),
      Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s =>
      new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createExternalTable(tableName, "column", dataDF.schema,
      Map.empty[String, String])
    dataDF.write.format("column").mode(SaveMode.Append)
        .saveAsTable(tableName)
  }

  def createTableAndInsertData2(): Unit = {
    val snc = SnappyContext(sc)
    val tableName: String = "Airline"

    val hfile = getClass.getResource("/2015.parquet").getPath
    val dataDF = snc.read.load(hfile)
    snc.createExternalTable(tableName, "column", dataDF.schema,
      Map.empty[String, String])
    dataDF.write.format("column").mode(SaveMode.Append)
        .saveAsTable(tableName)
  }

  def setDMLMaxChunkSize(size: Long): Unit = {
    GemFireXDUtils.DML_MAX_CHUNK_SIZE = size
  }
}

case class Data(col1: Int, col2: Int, col3: Int)
