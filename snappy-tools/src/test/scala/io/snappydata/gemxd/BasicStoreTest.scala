package io.snappydata.gemxd

import java.sql.{ResultSet, PreparedStatement, Statement, Connection}
import com.pivotal.gemfirexd.TestUtil
import com.pivotal.gemfirexd.jdbc.JdbcTestBase

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StringType, StructType, IntegerType}

/**
 * Created by vivek on 16/10/15.
 */
class BasicStoreTest(s: String) extends JdbcTestBase(s) {

  @throws(classOf[Exception])
  def testStringAsDatatype_runInXD {
    val conn: Connection = TestUtil.getConnection
    val st: Statement = conn.createStatement
    var rs: ResultSet = null
    st.execute("create table t1 (c1 int primary key, c2 String)")
    val pstmt: PreparedStatement = conn.prepareStatement("insert into t1 values(?,?)")
    pstmt.setInt(1, 111)
    pstmt.setString(2, "aaaaa")
    pstmt.executeUpdate
    pstmt.setInt(1, 222)
    pstmt.setString(2, "")
    pstmt.executeUpdate
    st.execute("select c1 from t1 where c2 like '%a%'")
    rs = st.getResultSet
    System.out.println("rs=" + rs)
    assert(rs.next == true)
    val ret1: Int = rs.getInt(1)
    //println("vivek = " + ret1)
    assert(111 == ret1)
    assert(rs.next == false)
  }

  @throws(classOf[Exception])
  def testStringAsDatatype_runInSpark {
    // First run the above query directly in Spark
    val conf = new org.apache.spark.SparkConf().setAppName("ExternalStoreTest")
        .set("spark.logConf", "true")
    val setMaster: String = "local[6]"
    conf.setMaster(setMaster)

    val sc = new org.apache.spark.SparkContext(conf)
    val snContext = org.apache.spark.sql.SnappyContext(sc)
    snContext.sql("set spark.sql.shuffle.partitions=6")

    val props = Map(
      //"url" -> "jdbc:snappydata:;mcast-port=45672;persist-dd=false;",
      "url" -> "jdbc:gemfirexd:;mcast-port=45672;persist-dd=false;",
      "poolImpl" -> "tomcat",
      //"single-hop-enabled" -> "true",
      //"poolProps" -> "",
      //"driver" -> "com.pivotal.gemfirexd.jdbc.ClientDriver",
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "user" -> "app",
      "password" -> "app"
    )

    val data = Seq(Seq(111,"aaaaa"), Seq(222,""))
    val rdd = sc.parallelize(data, data.length).map(s => new Data1(s(0).asInstanceOf[Int], s(1).asInstanceOf[String]))
    val dataDF = snContext.createDataFrame(rdd)

    val schemaString = "col1,col2"
    val schemaArr = schemaString.split(",")
    val schemaTypes = List(IntegerType, StringType)
    val schema = StructType(schemaArr.zipWithIndex.map {
      case (fieldName, i) => StructField(
        fieldName, schemaTypes(i), i >= 4)
    })

    //dataDF.registerTempTable("t1")
    snContext.registerAndInsertIntoExternalStore(dataDF, "t1", schema, props)
    //sc.createColumnTable("t1", dataDF.schema, "jdbcColumnar", props)
    //dataDF.write.format("jdbcColumnar").mode(SaveMode.Append).options(props).saveAsTable("t1")

    val result = snContext.sql("select col1 from t1 where col2 like '%a%'")
    doPrint("")
    doPrint("=============== RESULTS START ===============")
    result.collect.foreach(verifyRows)
    doPrint("=============== RESULTS END ===============")
  }

  def verifyRows(r: Row) : Unit = {
    doPrint(r.toString())
    assert(r.toString() == "[111]", "got=" + r.toString() + " but expected 111")
  }

  // Copy from BugsTest to verify basic JUnit is running in Scala
  @throws(classOf[Exception])
  def testBug47329 {
    val conn: Connection = TestUtil.getConnection
    val st: Statement = conn.createStatement
    var rs: ResultSet = null
    st.execute("create table t1 (c1 int primary key, c2  varchar(10))")
    st.execute("create index idx on  t1(c2)")
    val pstmt: PreparedStatement = conn.prepareStatement("insert into t1 values(?,?)")
    pstmt.setInt(1, 111)
    pstmt.setString(2, "aaaaa")
    pstmt.executeUpdate
    pstmt.setInt(1, 222)
    pstmt.setString(2, "")
    pstmt.executeUpdate
    st.execute("select c1 from t1 where c2 like '%a%'")
    rs = st.getResultSet
    System.out.println("rs=" + rs)
    assert(rs.next == true)
    val ret1: Int = rs.getInt(1)
    doPrint("vivek = " + ret1)
    assert(111 == ret1)
    assert(rs.next == false)
  }

  def doPrint(s: String): Unit = {
    //println(s)
  }
}

case class Data1(col1: Int, col2: String) extends Serializable
