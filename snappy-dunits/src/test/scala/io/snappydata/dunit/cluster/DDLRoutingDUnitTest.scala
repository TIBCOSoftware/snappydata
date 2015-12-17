package io.snappydata.dunit.cluster

import java.sql.{Connection, DriverManager}

import com.pivotal.gemfirexd.internal.engine.Misc
import dunit.{SerializableRunnable, AvailablePortHelper}

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

  def testColumnTableRouting(): Unit = {
    val tableName: String = "ColumnTableQR"

    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)

    // first fail a statement
    failCreateTableXD(conn, tableName, true, " column ")

    createTableXD(conn, tableName, " column ")
    tableMetadataAssertColumnTable(tableName)
    // Test create table - error for recreate
    failCreateTableXD(conn, tableName, false, " column ")

    // Drop Table and Recreate
    dropTableXD(conn, tableName)
    createTableXD(conn, tableName, " column ")

    insertDataXD(conn, tableName)
    queryData(tableName)

    createTempTableXD(conn)

    queryDataXD(conn, tableName)
    dropTableXD(conn, tableName)
  }

  def testRowTableRouting(): Unit = {
    val tableName: String = "RowTableQR"

    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)

    // first fail a statement
    failCreateTableXD(conn, tableName, true, " row ")

    createTableXD(conn, tableName, " row ")
    tableMetadataAssertRowTable(tableName)
    // Test create table - error for recreate
    failCreateTableXD(conn, tableName, false, " row ")

    // Drop Table and Recreate
    dropTableXD(conn, tableName)
    createTableXD(conn, tableName, " row ")

    insertDataXD(conn, tableName)
    queryData(tableName)

    createTempTableXD(conn)

    queryDataXD(conn, tableName)
    dropTableXD(conn, tableName)
  }

  def testRowTableByDefaultRouting(): Unit = {
    val tableName: String = "DefaultRowTableQR"

    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)

    createTableByDefaultXD(conn, tableName)
    tableMetadataAssertRowTable(tableName)

    // Drop Table and Recreate
    dropTableXD(conn, tableName)
    createTableByDefaultXD(conn, tableName)

    insertDataXD(conn, tableName)
    queryData(tableName)

    createTempTableXD(conn)

    queryDataXD(conn, tableName)
    dropTableXD(conn, tableName)
  }

  def createTableXD(conn: Connection, tableName: String, usingStr: String): Unit = {
    val s = conn.createStatement()
    val options = ""
    s.execute("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 INT) " + " USING " + usingStr +
        " " + options)
  }

  def createTableByDefaultXD(conn: Connection, tableName: String): Unit = {
    val s = conn.createStatement()
    s.execute("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 INT) ")
  }

  def failCreateTableXD(conn : Connection, tableName : String, doFail : Boolean, usingStr : String): Unit = {
    try
    {
      val s = conn.createStatement()
      val options = ""
      s.execute("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 INT) " + (if (doFail) "fail" orElse "") + " USING " + usingStr
          + " " + options)
      //println("Successfully Created ColumnTable = " + tableName)
    }
    catch {
      case e: Exception => println("create: Caught exception " + e.getMessage +
        " for ColumnTable = " + tableName)
      //println("Exception stack. create. ex=" + e.getMessage + " ,stack=" + ExceptionUtils.getFullStackTrace(e))
    }
    //println("Created ColumnTable = " + tableName)
  }

  def tableMetadataAssertColumnTable(tableName: String): Unit = {
    vm0.invoke(new SerializableRunnable() {
      override def run(): Unit = {
        val catalog = Misc.getMemStore.getExternalCatalog
        assert(catalog.isColumnTable(tableName, false))
      }
    })
  }

  def tableMetadataAssertRowTable(tableName: String): Unit = {
    vm0.invoke(new SerializableRunnable() {
      override def run(): Unit = {
        val catalog = Misc.getMemStore.getExternalCatalog
        assert(!catalog.isColumnTable(tableName, false))
      }
    })
  }

  def insertDataXD(conn: Connection, tableName: String): Unit = {
    val s = conn.createStatement()
    s.execute("insert into " + tableName + " values(10, 200, 3) ")
    s.execute("insert into " + tableName + " values(70, 800, 9),(90, 200, 3),(40, 200, 3),(50, 600, 7) ")
  }

  def dropTableXD(conn: Connection, tableName: String): Unit = {
    val s = conn.createStatement()
    s.execute("drop table " + tableName)
  }

  def createTempTableXD(conn : Connection): Unit = {
    try
    {
      val s = conn.createStatement()
      s.execute("CREATE TABLE airlineRef_temp(Code VARCHAR(25),Description VARCHAR(25)) USING parquet OPTIONS()")
      //println("Successfully Created ColumnTable = " + tableName)
    }
    catch {
      case e: java.sql.SQLException => //println("create temp: Caught exception " + e.getMessage)
      //println("Exception stack. create. ex=" + e.getMessage + " ,stack=" + ExceptionUtils.getFullStackTrace(e))
    }
    //println("Created ColumnTable = " + tableName)
  }

  def queryData(tableName : String): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)
    //println("Firing select on ColumnTable = " + tableName)
    val dataDF = snc.sql("Select * from " + tableName)
    //dataDF.map(t => "Select Query: Col1: " + t(0) + " Col2: " + t(1) + " Col3: " + t(2)).collect().foreach(println)

    assert(dataDF.map(t => t(0)).count() == 5)
    dataDF.map(t => t(0)).collect().foreach(verifyData)
  }

  def verifyData(v : Any): Unit = {
    assert(Seq(10, 70, 90, 40, 50).contains(v))
  }

  def queryDataXD(conn : Connection, tableName : String): Unit = {
    val s = conn.createStatement()
    val rs = s.executeQuery("Select col1 from " + tableName)
    var cnt = 0;
    var expected =
    while (rs.next()) {
      cnt = cnt + 1;
      assert(Seq(10, 70, 90, 40, 50).contains(rs.getInt(1)))
    }
    assert(cnt == 5, cnt)
  }
}

case class insertData(col1: Int, col2: Int, col3: Int)
