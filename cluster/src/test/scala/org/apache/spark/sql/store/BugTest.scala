/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.sql.store

import java.io.{BufferedReader, FileReader}
import java.sql.{DriverManager, SQLException}

import com.pivotal.gemfirexd.TestUtil
import io.snappydata.SnappyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.junit.Assert._

class BugTest extends SnappyFunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("SNAP-2342 nested query involving joins & union throws Exception") {
    snc.sql(s"create table tab1 ( " +
        "field1   string," +
        "field2   string," +
        "field3   string," +
        "field4   string," +
        "field5   string," +
        "field6   string," +
        "field7   string," +
        "field8   string," +
        "field9   string," +
        "field10   string," +
        "field11   string," +
        "field12   string," +
        "field13   string," +
        "field14   string," +
        "field15   string," +
        "field16   string," +
        "field17   string," +
        "localfield8   string," +
        "localfield9   string," +
        "localfield10   string," +
        "localfield11   string," +
        "localfield15   string," +
        "localfield16   string," +
        "field18   string," +
         "field19   string," +
        "sfield13   string," +
        "field20   string," +
        "field21   string," +
        "field22   string," +
        "field23   string )")

    snc.sql("create table tab2 (" +
        "field24   string," +
        "field9   string," +
        "field25   string," +
        "field10   string," +
        "field11   string," +
        "field12   string," +
        "field13   string," +
        "field14   string," +
        "field15   string," +
        "field16   string," +
        "field17   string," +
        "localfield9   string," +
        "localfield10   string," +
        "localfield11   string," +
        "localfield15   string," +
        "localfield16   string," +
        "field18   string," +
        "localfield23   string," +
        "field19   string," +
        "sfield13   string," +
        "field26   string," +
        "field20   string," +
         "field23   string)")

    snc.sql("create table tab3 (" +
        "field27 string, " +
        " field27description string, " +
        " field28 string )")

    snc.sql("create table tab4 (" +
        " field29  string," +
        "field29description  string," +
        " field27 string, " +
        " field27description string)")

    snc.sql("create table tab5 (" +
        "field27 string," +
        "field27description  string," +
        "field30  string," +
        "field30description  string)")

    snc.sql("create table tab6 (" +
        "field1   string," +
        "field27   string," +
        "field27description   string," +
        "field28   string," +
        "field31   string," +
        "field32   string," +
        "field32description   string)")

    snc.sql(s"create or replace view view1 as " +
        s"( select  field33,field27,first(field27Description) as field27Description, " +
        s"first(field29) as field29, " +
        s"first(field29Description) as field29Description,  first(field30) as field30, " +
        s"first(field30Description) as field30Description, " +
        s"first(field28) as field28," +
        s"format_number(sum(field34),2) as field34," +
        s" format_number(sum(field34),2) as field35,format_number(sum(total),2) as total from" +
        s" ( select a.field14 as field33,a.field17 as leLocal," +
        s" a.field19 as field36," +
        s" a.field20 as field37,a.field11 as field27," +
        s"a.localfield11 as field32," +
        s" SUM(field12) as field35,SUM(sfield13) as field34,SUM(field13) as total," +
        s" first(b.field27Description) as field27Description," +
        s" first(b.field28) as field28," +
        s" first((case when a.field20='x1' then e.field32Description " +
        s" when a.field20='b1' then b.field27Description else '' end)) " +
        s" as field32Description ," +
        s" first(c.field29Description) as field29Description," +
        s"first(d.field30Description) as field30Description, " +
        s" first(c.field29) as field29, first(d.field30) as field30 from ( select field16," +
        s"field14," +
        s" field17,field19,field9,field20,localfield11," +
        s" field11,last(localfield10),SUM(field13) as field13," +
        s" SUM(field12) as field12,SUM(sfield13) as sfield13, field11 ," +
        s" 'Y1' as field1,localfield11 as field32 from " +
        s" ( select field16,field14,field17,field19,field9,field20," +
        s" localfield11,field11,localfield10,field13,field12,sfield13" +
        s"  from tab1  where field16='0L' and field14='7600' " +
        s" AND field9='2017' and field8<=3 AND field20='b1'  union all" +
        s" select field16,field14,field17,field19,field9,field20," +
        s" localfield11,field11,localfield10,field13,field12," +
        s" sfield13  from tab2  where field16='0L' and field14='7600'" +
        s" AND field9='2017' AND field20='btb_latam' )  group by field16," +
        s" field14,field17,field19,field9,field20," +
        s" localfield11,field11 ) a" +
        s" left join tab3 b on (a.field11=b.field27) left join " +
        s" tab4 c " +
        s" on (a.field11=c.field27)  left join tab5 d on (a.field11=d.field27)" +
        s" left join tab6 e on(a.field1=e.field1 and " +
        s"  a.field11 = e.field27 and a.field32 = e.field32 ) group by a.field14," +
        s"a.field17," +
        s" a.field19,a.field20,a.field11,a.localfield11," +
        s"c.field29,d.field30) group by field33,field27)")

    snc.sql("drop view view1")
    snc.sql("drop table if exists tab1")
    snc.sql("drop table if exists tab2")
    snc.sql("drop table if exists tab3")
    snc.sql("drop table if exists tab4")
    snc.sql("drop table if exists tab5")
    snc.sql("drop table if exists tab6")

  }
/////////
  test("Bug SNAP-2332 . ParamLiteral found in First/Last Aggregate Function") {
    snc
    var serverHostPort2 = TestUtil.startNetServer()
    var conn = DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort2")
    var stmt = conn.createStatement()
    val snappy = snc.snappySession

    val insertDF = snappy.range(50).selectExpr("id", "(id * 12) as k",
      "concat('val', cast(100 as string)) as s")

    snappy.sql("drop table if exists test")
    snappy.sql("create table test (id bigint, k bigint, s varchar(10)) " +
        "using column options(buckets '8')")
    insertDF.write.insertInto("test")
    val query1 = "select sum(id) as summ, first(s, true) as firstt, last(s, true) as lastt" +
        " from test having (first(s, true) = 'val100' or last(s, true) = 'val100' )"


    var ps = conn.prepareStatement(query1)
    var resultset = ps.executeQuery()
    while (resultset.next()) {
      resultset.getDouble(1)
    }

    var rs = snappy.sql(query1)
    rs.collect()
    rs = snappy.sql(query1)
    rs.collect()

    resultset = stmt.executeQuery(query1)
    while (resultset.next()) {
      resultset.getDouble(1)
    }

    val query2 = "select sum(id) summ , first(s) firstt, last(s) lastt " +
        " from test having (first(s) = 'val100' or last(s) = 'val100' )"

    ps = conn.prepareStatement(query2)
    resultset = ps.executeQuery()
    while (resultset.next()) {
      resultset.getDouble(1)
    }

    resultset = stmt.executeQuery(query2)
    while (resultset.next()) {
      resultset.getDouble(1)
    }

    rs = snappy.sql(query2)
    rs.collect()


    stmt.execute(s"create or replace view X as ($query2)")
    val query3 = "select * from X where summ > 0"
    ps = conn.prepareStatement(query3)
    resultset = ps.executeQuery()
    while (resultset.next()) {
      resultset.getDouble(1)
    }

    rs = snappy.sql(query3)
    rs.collect()
    rs = snappy.sql(query3)
    rs.collect()
    resultset = stmt.executeQuery(query3)
    while (resultset.next()) {
      resultset.getDouble(1)
    }

    val table1 = s"create table tab1 ( " +
        "field1   string," +
        "field2   string," +
        "field3   string," +
        "field4   string," +
        "field5   string," +
        "field6   string," +
        "field7   string," +
        "field8   string," +
        "field9   string," +
        "field10   string," +
        "field11   string," +
        "field12   string," +
        "field13   string," +
        "field14   string," +
        "field15   string," +
        "field16   string," +
        "field17   string," +
        "localfield8   string," +
        "localfield9   string," +
        "localfield10   string," +
        "localfield11   string," +
        "localfield15   string," +
        "localfield16   string," +
        "field18   string," +
        "field19   string," +
        "sfield13   string," +
        "field20   string," +
        "field21   string," +
        "field22   string," +
        "field23   string )"

    val table2 = "create table tab7 (" +
        "globalfield16 string," +
        "globalfield16description string," +
        "localfield16 string, " +
        "localfield16description string," +
        "field20 string)"

    val table3 = "create table field29_hier (" +
        "field38 string," +
        "field39 string," +
        "field40 string," +
        "field41 string," +
        "field42 string," +
        "field43 string," +
        "subfield38 string," +
        "subfield39 string," +
        "field44 string," +
        "field45 string," +
        "field46 string)"

    stmt.execute(table1)
    stmt.execute(table2)
    stmt.execute(table3)

    val view = "CREATE or replace view view2 as (SELECT " +
        "A.field9,first(A.field8) as field8,A.field14, A.localfield16," +
        "A.field20,  A.field11,A.field3, A.field19, " +
        " first(A.field15) as field15," +
        " first(A.field23) as field23, first(A.field10) as field10," +
        "SUM(A.field13 ) as field13,  " +
        "first(B.globalfield16Description) as globalfield16Description," +
        " first(C.field44) as field44," +
        " first(C.field38) as field38,  " +
        "first(C.field45) as field45," +
        " first(C.subfield38) as subfield38, " +
        "first(C.field40) as field40" +
        " FROM tab1 A  LEFT JOIN tab7 B " +
        " ON A.field20 = B.field20 AND " +
        " B.globalfield16 = A.localfield16 LEFT JOIN field29_hier C ON " +
        " A.field3 = field42 WHERE A.localfield16 ='0L' " +
        " GROUP BY A.field14, A.field19, " +
        "A.field9, A.field20, A.field11, A.field3, A.localfield16 " +
        "having ( SUM(A.field13 ) > 0.001F or SUM(A.field13 ) < -0.001F) );"

    stmt.execute(view)

    val q = "SELECT field14 FROM view2 GROUP BY 1"
    ps = conn.prepareStatement(q)

    resultset = ps.executeQuery()
    while (resultset.next()) {
      resultset.getString(1)
    }
    stmt.execute("drop view view2")
    snc.sql("drop table if exists tab1")
    snc.sql("drop table if exists tab7")
    snc.sql("drop table if exists field29_hier")
    conn.close()
    TestUtil.stopNetServer()

  }

  test("big view") {
    val snc = this.snc
    val serverHostPort2 = TestUtil.startNetServer()
    val conn = DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort2")
    val session = this.snc.snappySession

    // check temporary view with USING and its meta-data
    val hfile: String = getClass.getResource("/2015.parquet").getPath
    val stagingDF = snc.read.load(hfile)
    snc.createTable("airline", "column", stagingDF.schema,
      Map.empty[String, String])


    // create a big view on it
    val viewFile = getClass.getResource("/bigviewcase.sql")
    val br = new BufferedReader(new FileReader(viewFile.getFile))
    var viewSql = ""
    var keepGoing = true
    while(keepGoing) {
      val x = br.readLine()
      if (x != null) {
        viewSql += x
      } else {
        keepGoing = false
      }
    }
    val viewname = "AIRLINEBOGUSVIEW"

    // check catalog cache is cleared for VIEWs
    val cstmt = conn.prepareCall(s"call SYS.GET_COLUMN_TABLE_SCHEMA(?, ?, ?)")
    cstmt.setString(1, "APP")
    cstmt.setString(2, viewname)
    cstmt.registerOutParameter(3, java.sql.Types.CLOB)
    try {
      cstmt.execute()
      assert(cstmt.getString(3) === "")
      fail("expected to fail")
    } catch {
      case se: SQLException if se.getSQLState == "XIE0M" =>
    }

    // create view
    session.sql(viewSql)

    // meta-data lookup should not fail now
    cstmt.execute()
    assert(cstmt.getString(3).contains(
      "CASE WHEN (YEARI > 0) THEN CAST(1 AS DECIMAL(11,1)) ELSE CAST(1.1 AS DECIMAL(11,1)) END"))

    // query on view
    session.sql(s"select count(*) from $viewname").collect()
    // check column names
    val rs = conn.getMetaData.getColumns(null, null, viewname, "%")
    var foundValidColumnName = false
    while(rs.next() && !foundValidColumnName) {
      val colName = rs.getString("COLUMN_NAME")
      if  (colName == "YEARI") {
        foundValidColumnName = true
      }
    }
    assert(foundValidColumnName)

    snc.sql(s"drop view $viewname")
    snc.sql("drop table airline")
    conn.close()
    TestUtil.stopNetServer()
  }

  test("Column table creation test - SNAP-2577") {
    snc
    var serverHostPort2 = TestUtil.startNetServer()
    var conn = DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort2")
    var stmt = conn.createStatement()
    val session = this.snc.snappySession
    stmt.execute(s"CREATE TABLE temp (username String, id Int) " +
        s" USING column ")
    val seq = Seq("USERX" -> 4, "USERX" -> 5, "USERX" -> 6, "USERY" -> 7,
      "USERY" -> 8, "USERY" -> 9)
    val rdd = sc.parallelize(seq)

    val dataDF = session.createDataFrame(rdd)

    dataDF.write.insertInto("temp")
    snc.sql("drop table temp")
    conn.close()
    TestUtil.stopNetServer()
  }

  test("Bug SNAP-2758 . view containing aggregate function & join throws error") {
    snc
    var serverHostPort2 = TestUtil.startNetServer()
    var conn = DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort2")
    var stmt = conn.createStatement()
    val snappy = snc.snappySession
    snappy.sql("drop table if exists test1")
    snappy.sql("create table test1 (col1_1 int, col1_2 int, col1_3 int, col1_4 string) " +
        "using column ")

    snappy.sql("create table test2 (col2_1 int, col2_2 int,  col2_3 int, col2_5 string) " +
        "using column ")

    snappy.sql(" CREATE OR REPLACE VIEW v1 as select col2_1, col2_2, " +
        "col2_5 as longtext from test2 where col2_3 > 10")

    val q1 = "select a.col1_1, a.col1_2, " +
        " CASE WHEN a.col1_4 = '' THEN '#' ELSE a.col1_4 END functionalAreaCode," +
        "b.longtext as name, " +
        " sum(a.col1_3)" +
        "from test1 a left outer join v1 as b on a.col1_1 = b.col2_1" +
        " group by a.col1_1, a.col1_2, " +
        " CASE WHEN a.col1_4  = '' THEN '#' ELSE a.col1_4  END," +
        " b.longtext "
    snappy.sql(q1)
    snappy.sql(s" CREATE OR REPLACE VIEW v3 as $q1")

    val q = "select a.col1_1, a.col1_2, " +
        " CASE WHEN a.col1_4 = '' THEN '#' ELSE a.col1_4 END functionalAreaCode," +
        "'#' as fsid,  " +
        "b.longtext as name, " +
        " sum(a.col1_3)" +
        "from test1 a left outer join v1 as b on a.col1_1 = b.col2_1" +
        " group by a.col1_1, a.col1_2, " +
        " CASE WHEN a.col1_4  = '' THEN '#' ELSE a.col1_4  END," +
        " '#'," +
        " b.longtext "
    snappy.sql(q)
    snappy.sql(s" CREATE OR REPLACE VIEW v2 as $q")
    snappy.sql("select count(*) from v2").collect()

    stmt.execute("drop view v3")
    stmt.execute("drop view v2")
    stmt.execute("drop view v1")
    snc.sql("drop table if exists test1")
    snc.sql("drop table if exists test2")

    conn.close()
    TestUtil.stopNetServer()

  }

  test("Bug SNAP-2791 . view projection containing null") {
    snc
    var serverHostPort2 = TestUtil.startNetServer()
    var conn = DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort2")
    var stmt = conn.createStatement()
    val snappy = snc.snappySession
    snappy.sql("drop table if exists test2")

    snappy.sql("create table test2 (col2_1 int, col2_2 int,  col2_3 int, col2_5 string) " +
        "using column ")

    snappy.sql(" CREATE OR REPLACE VIEW v1 as select col2_1, col2_2, " +
        "col2_5 as longtext, null as null_col from test2 where col2_3 > -1")

    stmt.execute("insert into test2 values (1,2,3,'test')")
    val rs = stmt.executeQuery("select * from v1")
    assertTrue(rs.next())
    assertEquals(1, rs.getInt(1))
    assertEquals(2, rs.getInt(2))
    assertEquals("test", rs.getString(3))
    rs.getObject(4)
    assertTrue(rs.wasNull())


    stmt.execute("drop view v1")
    snc.sql("drop table if exists test2")

    conn.close()
    TestUtil.stopNetServer()

  }




}
