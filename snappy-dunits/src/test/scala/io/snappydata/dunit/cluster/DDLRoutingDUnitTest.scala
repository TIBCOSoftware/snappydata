package io.snappydata.dunit.cluster

import java.sql.{Connection, DriverManager}

import com.pivotal.gemfirexd.internal.engine.Misc
import dunit.{SerializableRunnable, AvailablePortHelper}

import org.apache.spark.sql.SaveMode

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
    val tableName: String = "ColumnTableQR"

    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)

    // first fail a statement
    failCreateTableXD(conn, tableName, true)

    createTableXD(conn, tableName)
    tableMetadataXD(tableName)
    // Test create table - error for recreate
    failCreateTableXD(conn, tableName, false)

    // Drop Table and Recreate
    dropTableXD(conn, tableName)
    createTableXD(conn, tableName)

    // Will be enabled after introduction of shadow table
    //insertDataXD(conn, tableName)
    insertData(tableName)
    queryData(tableName)
    createTempTableXD(conn)
  }

  def createTableXD(conn : Connection, tableName : String): Unit = {
    val s = conn.createStatement()
    val options = "OPTIONS (url 'jdbc:snappydata:;user=app;password=app;persist-dd=false;route-query=false' ," +
      "driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver' ," +
      "poolImpl 'tomcat', " +
      "user 'app', " +
      "password 'app' ) "
    s.execute("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 INT) " + " USING column " + options)
  }

  def failCreateTableXD(conn : Connection, tableName : String, doFail : Boolean): Unit = {
    try
    {
      val s = conn.createStatement()
      val options = "OPTIONS (url 'jdbc:snappydata:;user=app;password=app;persist-dd=false;route-query=false' ," +
        "driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver' ," +
        "poolImpl 'tomcat', " +
        "user 'app', " +
        "password 'app' ) "
      s.execute("CREATE TABLE " + tableName + " (Col1 INT, Col2 INT, Col3 INT) " + (if (doFail) "fail" orElse  "") + " USING column " + options)
      //println("Successfully Created ColumnTable = " + tableName)
    }
    catch {
      case e: Exception => println("create: Caught exception " + e.getMessage +
        " for ColumnTable = " + tableName)
      //println("Exception stack. create. ex=" + e.getMessage + " ,stack=" + ExceptionUtils.getFullStackTrace(e))
    }
    //println("Created ColumnTable = " + tableName)
  }

  def tableMetadataXD(tableName: String): Unit = {
    vm0.invoke(new SerializableRunnable() {
      override def run(): Unit = {
        val catalog = Misc.getMemStore.getExternalCatalog
        assert(catalog.isColumnTable("ColumnTableQR"))
      }
    })
  }

  def insertDataXD(conn: Connection, tableName: String): Unit = {
    val s = conn.createStatement()
    s.execute("insert into " + tableName + " values(1, 2, 3) ")
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

  def insertData(tableName: String): Unit = {
    val snc = org.apache.spark.sql.SnappyContext(sc)
    val data = Seq(Seq(10, 200, 3), Seq(70, 800, 9), Seq(90, 200, 3), Seq(40, 200, 3), Seq(50, 600, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new insertData(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("column").mode(SaveMode.Append).saveAsTable(tableName)
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
}

case class insertData(col1: Int, col2: Int, col3: Int)
