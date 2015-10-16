package io.snappydata.gemxd

import java.sql.{ResultSet, PreparedStatement, Statement, Connection}
import com.pivotal.gemfirexd.TestUtil
import com.pivotal.gemfirexd.jdbc.JdbcTestBase

/**
 * Created by vivek on 16/10/15.
 */
class BasicStoreTest(s: String) extends JdbcTestBase(s) {

  def testDummy(): Unit = {
  // Do nothing
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
    //println("vivek = " + ret1)
    assert(111 == ret1)
    assert(rs.next == false)
  }
}
