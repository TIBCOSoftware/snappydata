/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
import java.lang
import java.sql.{Connection, DriverManager, SQLException, Statement}
import java.util.Properties

import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.internal.cache.PartitionedRegion
import com.pivotal.gemfirexd.TestUtil
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.SnappyFunSuite.resultSetToDataset
import io.snappydata.{Property, SnappyFunSuite}
import org.junit.Assert._
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.joins.HashJoinExec
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SnappyContext, SparkSession}

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
      "CASE WHEN (yeari > 0) THEN CAST(1 AS DECIMAL(11,1)) ELSE CAST(1.1 AS DECIMAL(11,1)) END"))

    // query on view
    session.sql(s"select count(*) from $viewname").collect()
    // check column names
    val rs = conn.getMetaData.getColumns(null, null, viewname, "%")
    var foundValidColumnName = false
    while(rs.next() && !foundValidColumnName) {
      val colName = rs.getString("COLUMN_NAME")
      if  (colName == "yeari") {
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

  test("Bug SNAP-2887") {
    snc
    var serverHostPort2 = TestUtil.startNetServer()
    var conn = DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort2")
    var stmt = conn.createStatement()
    val snappy = snc.snappySession
    snappy.sql("drop table if exists portfolio")
    snappy.sql(s"create table portfolio (cid int not null, sid int not null, " +
      s"qty int not null,availQty int not null, subTotal int, tid int, " +
      s"constraint portf_pk primary key (cid, sid))")

    val insertStr = s"insert into portfolio values (?, ?, ?, ?, ? , ?)"
    val ps = conn.prepareStatement(insertStr)
    for (i <- 1 until 101) {
      ps.setInt(1, i % 10)
      ps.setInt(2, i * 10)
      ps.setInt(3, i)
      ps.setInt(4, i)
      ps.setInt(5, i)
      ps.setInt(6, 10)
      ps.executeUpdate()
    }
    val query = s"select * from portfolio where cid = ? and Sid = ? and tid = ?"
    val qps = conn.prepareStatement(query)
    for (i <- 0 until 11) {
      qps.setInt(1, 8)
      qps.setInt(2, 20)
      qps.setInt(3, 10)
      val rs = qps.executeQuery()
      var count = 0
      while (rs.next()) {
        count += 1
      }
      assert(count == 0)
    }
    snappy.sql(s"create index portfolio_sid  on portfolio (sId )")

    for (i <- 0 until 11) {
      qps.setInt(1, 8)
      qps.setInt(2, 20)
      qps.setInt(3, 10)
      val rs = qps.executeQuery()
      var count = 0
      while (rs.next()) {

        count += 1
      }
      assert(count == 0)
    }
    stmt.execute("drop index  if exists portfolio_sid")
    stmt.execute("drop table if exists portfolio")
  }

  test("Bug SNAP-2890") {
    snc
    var serverHostPort2 = TestUtil.startNetServer()
    var conn = DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort2")
    var stmt = conn.createStatement()
    val snappy = snc.snappySession
    val numCols = 132
    snappy.conf.set(Property.ColumnBatchSize.name, "256")
    snappy.sql("drop table if exists test1")
    val sb = new StringBuilder
    for(i <- 1 until numCols + 1) {
      sb.append(s"col$i string,")
    }
    sb.deleteCharAt(sb.length -1)

    snappy.sql(s"create table test1 (${sb.toString()}) " +
      "using column ")
    val params = Array.fill(numCols)('?').mkString(",")
    val insertStr = s"insert into test1 values (${params})"
    val ps = conn.prepareStatement(insertStr)
    for (i <- 0 until 1000) {
      for (j <- 1 until numCols + 1) {
        ps.setString(j, j.toString)
      }
      ps.addBatch()
    }
    ps.executeBatch()
    snappy.sql(s"create table test2 using column as ( select * from test1)")
    for(i <- 1 until numCols + 1) {
      snappy.sql(s"select col${i} from test1").collect().foreach(r =>
        assert(r.getString(0).toInt == i) )
    }
    for(i <- 1 until (numCols + 1)/2) {
      snappy.sql(s"select col${i}, col${numCols - i + 1} from test1").collect().foreach(r =>
        {
          assert(r.getString(0).toInt == i)
          assert(r.getString(1).toInt == numCols -i + 1)
        } )
    }
    for(i <- 1 until numCols + 1) {
      val projSeq = for (j <- 1 until i + 1) yield {
        s"col${j}"
      }
      val projectionStr = projSeq.mkString(",")

      snappy.sql(s"select $projectionStr from test1 limit 10").collect().foreach(r =>
        for (j <- 1 until i + 1 ) {
          assert(r.getString(j -1).toInt == j)
        })
    }
    snappy.sql("select col126 from test2").collect()
    snappy.sql("select col128 from test2").collect()
    snappy.sql("select col127 from test2").collect()
    snappy.sql("select col130 from test2").collect()
    snappy.sql("select col129 from test2").collect()
    snc.sql("drop table if exists test1")
    snc.sql("drop table if exists test2")
    conn.close()
    TestUtil.stopNetServer()
  }

  test("SNAP-2718") {
    snc
    val path1 = getClass.getResource("/patients1000.csv").getPath
    val df1 = snc.read.format("csv").option("header", "true").load(path1)
    df1.registerTempTable("patients")
    val path2 = getClass.getResource("/careplans1000.csv").getPath
    val df2 = snc.read.format("csv").option("header", "true").load(path2)
    df2.registerTempTable("careplans")

    snc.sql("select p.first, p.last from (select patient from ( select *, " +
      "case when description in ('Anti-suicide psychotherapy', 'Psychiatry care plan', " +
      "'Major depressive disorder clinical management plan') then 1 else 0 end as coverage " +
      "from careplans )c group by patient having sum(coverage) = 0)q " +
      "join patients p on id = patient ").collect

    df1.createOrReplaceTempView("patients_v")
    df2.createOrReplaceTempView("careplans_v")

    snc.sql("select p.first, p.last from (select patient from ( select *, " +
      "case when description in ('Anti-suicide psychotherapy', 'Psychiatry care plan', " +
      "'Major depressive disorder clinical management plan') then 1 else 0 end as coverage " +
      "from careplans_v )c group by patient having sum(coverage) = 0)q " +
      "join patients_v p on id = patient ").collect

    snc.dropTempTable("patients")
    snc.dropTempTable("careplans")
    snc.sql("drop view patients_v")
    snc.sql("drop view careplans_v")
  }

  test("SNAP-2368") {
    snc
    try {
      var serverHostPort2 = TestUtil.startNetServer()
      var conn = DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort2")
      val schema = StructType(List(StructField("name", StringType, nullable = true)))
      val data = Seq(
        Row("abc"),
        Row("def")
      )
      val stmt = conn.createStatement()
      val sparkSession = SparkSession.builder.appName("test").
        sparkContext(snc.sparkContext).getOrCreate()
      val namesDF = sparkSession.createDataFrame(snc.sparkContext.parallelize(data), schema)
      namesDF.createOrReplaceTempView("names")
      sparkSession.table("names").
        write.mode(SaveMode.Overwrite).jdbc(
        s"jdbc:snappydata://$serverHostPort2/", "names", new Properties())
      var rs = stmt.executeQuery("select tabletype from sys.systables where tablename = 'NAMES'")
      rs.next()
      var tableType = rs.getString(1)
      assertEquals("T", tableType)
      stmt.execute("drop table names")
      rs = stmt.executeQuery("select tabletype from sys.systables where tablename = 'NAMES'")
      assertFalse(rs.next())
      val props = new Properties()
      props.put("createTableOptions", " using column options( buckets '13')")
      props.put("isolationLevel", "NONE")
      sparkSession.table("names").
        write.mode(SaveMode.Overwrite).jdbc(
        s"jdbc:snappydata://$serverHostPort2/", "names", props)

      rs = stmt.executeQuery("select tabletype from sys.systables where tablename = 'NAMES'")
      rs.next()
      tableType = rs.getString(1)
      assertEquals("C", tableType)
      stmt.execute("drop table if exists test")
    } finally {
      TestUtil.stopNetServer
    }
  }

  ignore("SNAP-2910") {
    snc
    try {
      var serverHostPort2 = TestUtil.startNetServer()
      var conn = DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort2")
      val schema = StructType(List(StructField("name", StringType, nullable = true)))
      val data = Seq(
        Row("abc"),
        Row("def")
      )

      val stmt = conn.createStatement()

      val sparkSession = SparkSession.builder.appName("test").
        sparkContext(snc.sparkContext).getOrCreate()
      val namesDF = sparkSession.createDataFrame(snc.sparkContext.parallelize(data), schema)
      namesDF.createOrReplaceTempView("names")

      val props = new Properties()
      props.put("createTableOptions", " using column options( buckets '13')")
      sparkSession.table("names").
        write.mode(SaveMode.Overwrite).jdbc(
        s"jdbc:snappydata://$serverHostPort2/", "names", props)

      val rs = stmt.executeQuery("select tabletype from sys.systables where tablename = 'NAMES'")
      rs.next()
      val tableType = rs.getString(1)
      assertEquals("C", tableType)
      stmt.execute("drop table if exists test")
    } finally {
      TestUtil.stopNetServer
    }
  }

  test("SNAP-2237") {
    snc
    snc.sql("drop table if exists test1")
    snc.sql("create table test1 (col1_1 int, col1_2 int, col1_3 int, col1_4 string) " +
      "using column ")
    val insertDF = snc.range(50).selectExpr("id", "id*2", "id * 3",
      "cast (id as string)")
    insertDF.write.insertInto("test1")
    snc.sql("select col1_2, sum(col1_1) as summ from test1 group by col1_2 " +
      "order by sum(col1_1)").collect
    snc.sql("select col1_2, sum(col1_1) as summ from test1 " +
      "group by col1_2 order by summ").collect
    snc.sql("select lower(col1_2) as x, " +
      "sum(col1_1) as summ from test1 group by lower(col1_2) ").collect
    snc.sql("select lower(col1_2) as x, sum(col1_1) as summ from test1 " +
      "group by x").collect
    snc.dropTable("test1")
  }

  test("Verify number of tasks for limit query") {
    val hfile: String = getClass.getResource("/2015.parquet").getPath
    snc.sql(s"CREATE EXTERNAL TABLE STAGING_AIRLINE USING parquet options(path '$hfile')")
    var numTasks = 0
    snc.sparkContext.addSparkListener( new SparkListener {
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = numTasks += 1
    })
    snc.sql(s"select * from STAGING_AIRLINE limit 1").collect()
    // num tasks should be 1 as only 1 partition needs to be scanned to get 1 row
    assert(numTasks == 1, s"numTasks should be 1. numTasks is $numTasks")

    snc.sql(s"CREATE TABLE T1 (COL1 INT) using column options ('buckets' '3')")
    snc.sql(s"INSERT INTO T1 VALUES (1)")
    snc.sql(s"INSERT INTO T1 VALUES (2)")
    snc.sql(s"INSERT INTO T1 VALUES (3)")
    snc.sql(s"INSERT INTO T1 VALUES (4)")
    snc.sql(s"INSERT INTO T1 VALUES (5)")
    snc.sql(s"INSERT INTO T1 VALUES (6)")
    numTasks = 0
    snc.sql(s"select * from T1 limit 1").collect()
    // num tasks should be 1 as only 1 partition needs to be scanned to get 1 row
    assert(numTasks == 1, s"numTasks should be 1. numTasks is $numTasks")
    snc.sql(s"drop table STAGING_AIRLINE")
    snc.sql(s"drop table T1")
  }

  test("multi-partition limit") {
    val snappy = snc.snappySession
    snappy.sql("create table testLimit (id long, data string, data2 string) using column " +
        "options (partition_by 'id', buckets '128') as " +
        "select id, 'someTestData_' || id, 'someOtherData_' || id from range(10000)")
    val schema = snappy.table("testLimit").schema
    val port = TestUtil.startNetserverAndReturnPort()
    val conn = DriverManager.getConnection(s"jdbc:snappydata://localhost:$port")
    val stmt = conn.createStatement()
    val rows = Utils.resultSetToSparkInternalRows(
      stmt.executeQuery("select * from testLimit limit 5000"), schema)
    assert(rows.length === 5000)
    val res = snappy.sql("select * from testLimit limit 10000")
    val expected = snappy.sql("select * from testLimit").collect()
    checkAnswer(res, expected)
    val res2 = SnappyFunSuite.resultSetToDataset(
      snappy, stmt)("select * from testLimit limit 10000")
    checkAnswer(res2, expected)

    conn.close()
    snappy.sql("drop table testLimit")
    TestUtil.stopNetServer()
  }

  test("support for 'default' schema without explicit quotes") {
    val session = snc.snappySession
    val serverHostPort = TestUtil.startNetServer()
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)

    session.sql("create table default.t1(id bigint primary key, name varchar(10))")
    var keys = session.sessionCatalog.getKeyColumns("default.t1")
    assert(keys.length === 1)
    assert(keys.head.toString === new Column("id", null, "bigint", false, false, false).toString)

    // also test from JDBC
    val stmt = conn.createStatement()
    stmt.execute("create table default.t2(id bigint not null primary key, name varchar(10))")
    keys = session.sessionCatalog.getKeyColumns("default.t2")
    assert(keys.length === 1)
    assert(keys.head.toString === new Column("id", null, "bigint", false, false, false).toString)

    session.sql("insert into default.t1 values (1, 'name1'), (2, 'name2')")
    var res = session.sql("select * from default.t1 order by id").collect()
    assert(res === Array(Row(1L, "name1"), Row(2L, "name2")))
    res = session.sql("select * from default.t1 where id = 1").collect()
    assert(res === Array(Row(1L, "name1")))
    res = session.sql("select * from `DEFAULT`.t1 where id = 2").collect()
    assert(res === Array(Row(2L, "name2")))
    session.sql("insert into `default`.`t1` values (3, 'name3'), (4, 'name4')")
    res = session.sql("select * from `default`.`t1` order by id").collect()
    assert(res === Array(Row(1L, "name1"), Row(2L, "name2"), Row(3L, "name3"), Row(4L, "name4")))
    res = session.sql("select * from default.t1 where id = 3").collect()
    assert(res === Array(Row(3L, "name3")))
    res = session.sql("select * from `DEFAULT`.t1 where id = 4").collect()
    assert(res === Array(Row(4L, "name4")))

    stmt.execute("insert into default.t2 values (1, 'name1'), (2, 'name2')")
    res = resultSetToDataset(session, stmt)("select * from default.t2 order by id").collect()
    assert(res === Array(Row(1L, "name1"), Row(2L, "name2")))
    res = resultSetToDataset(session, stmt)("select * from default.t2 where id = 1").collect()
    assert(res === Array(Row(1L, "name1")))
    res = resultSetToDataset(session, stmt)("select * from `default`.t2 where id = 2").collect()
    assert(res === Array(Row(2L, "name2")))
    stmt.execute("insert into `DEFAULT`.`T2` values (3, 'name3'), (4, 'name4')")
    res = resultSetToDataset(session, stmt)("select * from default.t2 order by id").collect()
    assert(res === Array(Row(1L, "name1"), Row(2L, "name2"), Row(3L, "name3"), Row(4L, "name4")))
    res = resultSetToDataset(session, stmt)("select * from default.t2 where id = 3").collect()
    assert(res === Array(Row(3L, "name3")))
    res = resultSetToDataset(session, stmt)("select * from `DEFAULT`.`t2` where id = 4").collect()
    assert(res === Array(Row(4L, "name4")))

    // check ALTER TABLE
    session.sql("alter table default.t1 set eviction maxsize 1000")
    session.sql("alter table `DEFAULT`.t2 set eviction maxsize 1000")
    stmt.execute("alter table default.t1 set eviction maxsize 500")
    stmt.execute("alter table \"default\".\"t2\" set eviction maxsize 500")

    stmt.close()
    conn.close()

    TestUtil.stopNetServer()
  }

  test("SNAP3007") {
    val session = snc.snappySession
    val serverHostPort = TestUtil.startNetServer()
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)

    // scalastyle:off println
    println(s"testSNAP3007: Connected to $serverHostPort")
    val stmt = conn.createStatement()
    stmt.execute("CREATE TABLE app.application(application VARCHAR(64), " +
        "content CLOB, active BOOLEAN, configuration CLOB)")
    var ps = null

    val sql = "INSERT INTO app.application VALUES (?, ?, ?, ?)"
    val pstmt1 = conn.prepareStatement(sql)
    pstmt1.setString(1, "a")
    pstmt1.setString(2, "b")
    pstmt1.setBoolean(3, true)
    pstmt1.setString(4, "c")
    pstmt1.addBatch()
    pstmt1.executeBatch
    pstmt1.close()

    val sql2 = "DELETE FROM app.application"
    val pstmt2 = conn.prepareStatement(sql2)
    pstmt2.addBatch()
    val rows = pstmt2.executeBatch
    pstmt2.close()

    val sql3 = "select count(*) from app.application"
    val rs = conn.createStatement().executeQuery(sql3)
    assert(rs.next())
    assert(rs.getInt(1) == 0, "Table should not contain any data after delete statement")
    rs.close()

  }

  test("SNAP-2730 - support NAN values") {
    val session = snc.snappySession
    val serverHostPort = TestUtil.startNetServer()
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)
    val stmt = conn.createStatement()
    val result = stmt.executeQuery("select acos(30)")
    assert(result.next(), "result set should have 1 record")
    assert(result.getDouble(1).isNaN, "result is not NaN value")
    stmt.close()
  }

  test("SNAP2765") {
    val session = snc.snappySession
    val serverHostPort = TestUtil.startNetServer()
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)

    // scalastyle:off println
    println(s"testSNAP2765: Connected to $serverHostPort")
    val stmt = conn.createStatement()
    stmt.execute("create table t1(col1 int, col2 int, col3 int, col4 int) using row")
    val ps1 = conn.prepareStatement("insert into t1(col1, col2, col3, col4) values(?, ?, ?, ?)")
    ps1.setInt(1, 1)
    ps1.setInt(2, 1)
    ps1.setInt(3, 1)
    ps1.setInt(4, 1)
    ps1.execute()

    val ps2 = conn.prepareStatement("select * from t1 where col4=?")
    ps2.setInt(1, 1)
    val rs2 = ps2.executeQuery()
    assert(rs2.next())
    assert(rs2.getInt(1) == 1)
    rs2.close()

    stmt.execute("alter table t1 drop col2 restrict")
    val se0 = intercept[SQLException] {
      conn.prepareStatement("insert into t1(col1, col2, col3, col4) values(?, ?, ?, ?)")
    }
    assert(se0.getSQLState.equals("42X14"))

    val se1 = intercept[SQLException] {
      conn.prepareStatement("insert into t1 values(?, ?, ?, ?)")
    }
    assert(se1.getSQLState.equals("42802"))

    val ps3 = conn.prepareStatement("insert into t1(col1, col3, col4) values(?, ?, ?)")
    ps3.setInt(1, 1)
    ps3.setInt(2, 1)
    ps3.setInt(3, 1)
    ps3.execute()

    val ps4 = conn.prepareStatement("select count(*) from t1 where col4=?")
    ps4.setInt(1, 1)
    val rs4 = ps4.executeQuery()
    assert(rs4.next())
    assert(rs4.getInt(1) == 2)
    rs4.close()

    val se2 = intercept[SQLException] {
      conn.prepareStatement("update t1 set col2 = ?")
    }
    assert(se2.getSQLState.equals("42X14"))

    val se3 = intercept[SQLException] {
      conn.prepareStatement("insert into t1(col1, col1, col4) values(?, ?, ?)")
    }
    assert(se3.getSQLState.equals("42X13"))

    val se4 = intercept[SQLException] {
      conn.prepareStatement("delete from t1 where col2 = ?")
    }
    assert(se4.getSQLState.equals("42X04"))
  }

  test("SNAP-3045. Incorrect Hashjoin results - 1") {
    snc
    snc.sql("create table dm_base (c1 STRING, tenant_id STRING, shop_id STRING, olet_id STRING)" +
      " using column options(BUCKETS '40', PARTITION_BY 'TENANT_ID',REDUNDANCY '1'," +
      "PERSISTENCE 'ASYNCHRONOUS')")
    snc.sql("create table hierarchy_tag_dimension (c2 clob, tenant_id clob, shop_id clob," +
      " outlet_id clob)")
    snc.sql("insert into dm_base values('abc1', '1', '1', null)")

    snc.sql("insert into hierarchy_tag_dimension values('xyz1', '1', '1', null)")
    snc.sql("insert into dm_base values('abc2', '1', '1', '1')")
    snc.sql("insert into hierarchy_tag_dimension values('xyz2', '1', '1', null)")
    snc.sql("insert into dm_base values('abc3', '1', '1', null)")
    snc.sql("insert into hierarchy_tag_dimension values('xyz3', '1', '1', '1')")
    snc.sql("insert into dm_base values('abc4', '1', '1', '1')")
    snc.sql("insert into hierarchy_tag_dimension values('xyz4', '1', '1', '1')")
    snc.sql("insert into dm_base values('abc5', '1', '1', '2')")
    snc.sql("insert into hierarchy_tag_dimension values('xyz5', '1', '1', null)")
    snc.sql("insert into dm_base values('abc6', '1', '1', null)")
    snc.sql("insert into hierarchy_tag_dimension values('xyz6', '1', '1', '2')")
    snc.sql("insert into dm_base values('abc7', '1', '1', '2')")
    snc.sql("insert into hierarchy_tag_dimension values('xyz7', '1', '1', '2')")


    val rs = snc.sql("SELECT * FROM dm_base t1 LEFT JOIN " +
      "(SELECT * FROM hierarchy_tag_dimension)" +
      " t4 ON t1.tenant_id = t4.tenant_id AND t1.shop_id = t4.shop_id " +
      "AND t1.olet_id = COALESCE (t4.outlet_id, t1.olet_id)").collect

    val rs1 = snc.sql("SELECT * FROM dm_base t1 LEFT JOIN (SELECT * " +
      "FROM hierarchy_tag_dimension ORDER BY outlet_id) t4 ON t1.tenant_id = t4.tenant_id" +
      " AND t1.shop_id = t4.shop_id AND t1.olet_id = COALESCE (t4.outlet_id, t1.olet_id);").collect

    val snc1 = snc.newSession()
    snc1.setConf("snappydata.sql.disableHashJoin", "true")
    val rs2 = snc1.sql("SELECT * FROM dm_base t1 LEFT JOIN " +
      "(SELECT * FROM hierarchy_tag_dimension)" +
      " t4 ON t1.tenant_id = t4.tenant_id AND t1.shop_id = t4.shop_id " +
      "AND t1.olet_id = COALESCE (t4.outlet_id, t1.olet_id)").collect

    checkResultsMatch(rs, rs1)
    checkResultsMatch(rs, rs2)

    def checkResultsMatch(arr1: Array[Row], arr2: Array[Row]): Unit = {
      assertEquals(arr1.length, arr2.length)
      val list = ArrayBuffer(arr2: _*)
      arr1.foreach(row => {
        val indx = list.indexOf(row)
        assertTrue(indx >= 0)
        list.remove(indx)
      })
      assertTrue(list.isEmpty)
    }
  }

  // spark.sql.autoBroadcastJoinThreshold

  test("SNAP-3192 self join query giving wrong results - with broadcasthashjoin disabled") {
    val sncToUse = snc.newSession()
    sncToUse.setConf("spark.sql.autoBroadcastJoinThreshold", "-1")
   // sncToUse.setConf("snappydata.sql.tokenize", "false")
   // sncToUse.setConf("spark.sql.exchange.reuse", "false")

    testSnap3192HashJoinBehaviour(sncToUse)
  }
  test("SNAP-3192 self join query giving wrong results - with broadcasthashjoin enabled") {
    testSnap3192HashJoinBehaviour(snc)
  }

  def testSnap3192HashJoinBehaviour(sncToUse: SnappyContext) {
    sncToUse.sql("drop table if exists SAMPLES")
    val propsSAMPLES = Map("partition_by" -> "CHANNEL", "buckets" -> "4")
    val schSAMPLES = "(TIMESTAMP bigint, SPOT_TIME timestamp, CHANNEL varchar(128), SAMPLE double)"
    sncToUse.createTable("SAMPLES", "row", schSAMPLES, propsSAMPLES, false)
    sncToUse.sql("CREATE UNIQUE INDEX SAMPLES_TIMESTAMP ON SAMPLES( TIMESTAMP, CHANNEL )")
    sncToUse.sql("CREATE INDEX SAMPLES_CHANNEL ON SAMPLES( CHANNEL )")
    sncToUse.sql("CREATE INDEX SAMPLES_SPOT_TIME ON SAMPLES( SPOT_TIME )")
    val hfile: String = getClass.getResource("/snap-3192.csv").getPath
    val stagingDF = sncToUse.read.option("header", "true").csv(hfile)
    stagingDF.write.insertInto("samples")
    var rs = sncToUse.sql("select s1.timestamp, s1.sample as vCar, s2.sample as NGears" +
      " from samples as s1, samples as s2 where s1.timestamp = s2.timestamp" +
      " and s1.channel = 'vCar' and s2.channel = 'NGears'" +
      "  ")
    rs.show()
    rs.collect().foreach(row => assertTrue(row.getDouble(1) != row.getDouble(2)))

    rs = sncToUse.sql("select s1.timestamp, s1.sample as vCar, s2.sample as NGears" +
      " from samples as s1, samples as s2 where s1.timestamp = s2.timestamp" +
      " and s1.channel = 'vCar' and s2.channel = 'NGears'" +
      "  ")
    rs.show()
    rs.collect().foreach(row => assertTrue(row.getDouble(1) != row.getDouble(2)))

    rs = sncToUse.sql("select s1.timestamp, s1.sample as vCar, s2.sample as NGears" +
      " from samples as s1, samples as s2 where s1.timestamp = s2.timestamp" +
      " and s1.channel = 'vCar' and s2.channel = 'NGears'" +
      " order by s1.timestamp asc limit 10")
    rs.show()
    rs.collect().foreach(row => assertTrue(row.getDouble(1) != row.getDouble(2)))

    rs = sncToUse.sql("select s1.timestamp, s1.sample as vCar, s2.sample as NGears" +
      " from samples as s1, samples as s2 where s1.timestamp = s2.timestamp" +
      " and s1.channel = 'vCar' and s2.channel = 'NGears'" +
      " order by s1.timestamp asc limit 10")
    rs.show()
    rs.collect().foreach(row => assertTrue(row.getDouble(1) != row.getDouble(2)))

    var serverHostPort2 = TestUtil.startNetServer()
    var conn = DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort2")
    val stmt = conn.createStatement()
    val res = stmt.executeQuery("select s1.timestamp, s1.sample as vCar, s2.sample as NGears" +
      " from samples as s1, samples as s2 where s1.timestamp = s2.timestamp" +
      " and s1.channel = 'vCar' and s2.channel = 'NGears'" +
      " order by s1.timestamp asc limit 10")

    while(res.next()) {
      assertTrue(res.getDouble(2) != res.getDouble(3))
    }

    sncToUse.dropIndex("SAMPLES_TIMESTAMP", true)
    sncToUse.dropIndex("SAMPLES_CHANNEL", true)
    sncToUse.dropIndex("SAMPLES_SPOT_TIME", true)
    sncToUse.dropTable("samples", true)
  }

  test("SNAP-3192, SNAP-3193 self join query giving wrong results - check row table") {
    testSnap3192("row")
  }
  test("SNAP-3192 self join query giving wrong results - check column table") {
    testSnap3192("column")
  }
  def testSnap3192(tableType: String) {
    snc
    snc.dropTable("test", true)
    snc.sql(s"create table test (id int, sample float, channel varchar(128)) using $tableType")
    if (tableType.equalsIgnoreCase("row")) {
      snc.sql("create index channel_index on test (channel)")
    }
    var serverHostPort2 = TestUtil.startNetServer()
    var conn = DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort2")
    val ps = conn.prepareStatement("insert into test values (?,?, ?)")
    ps.setInt(1, 1)
    ps.setFloat(2, 1.0f)
    ps.setString(3, "gear")
    ps.addBatch()
    ps.setInt(1, 1)
    ps.setFloat(2, 75342.75f)
    ps.setString(3, "car")
    ps.addBatch()

    ps.setInt(1, 2)
    ps.setFloat(2, 2.0f)
    ps.setString(3, "gear")
    ps.addBatch()
    ps.setInt(1, 2)
    ps.setFloat(2, 7552442.75f)
    ps.setString(3, "car")
    ps.addBatch()

    ps.executeBatch()
    val query = "select s1.id , s1.sample as car_sample, s2.sample as gear_sample from " +
      " test as s1, test as s2 where s1.id = s2.id and s1.channel = 'car' and s2.channel = 'gear'" +
      " order by s1.id asc limit 10"
    var rs = snc.sql(query)
    rs.collect().foreach(row => assertTrue(row.getFloat(1) != row.getFloat(2)))
    rs = snc.sql(query)
    rs.collect.foreach(row => assertTrue(row.getFloat(1) != row.getFloat(2)))

    val stmt = conn.createStatement()
    var rs1 = stmt.executeQuery(query)
    while(rs1.next()) {
      assertTrue(rs1.getFloat(2) != rs1.getFloat(3))
    }
    println("\n\n")

    rs1 = stmt.executeQuery(query)
    while(rs1.next()) {
      assertTrue(rs1.getFloat(2) != rs1.getFloat(3))
    }
    snc.dropIndex("channel_index", true)
    snc.dropTable("test", true)
    TestUtil.stopNetServer()
  }


  test("SNAP-3045. Incorrect Hashjoin result-2") {
    snc

    def checkResultsMatch(arr: Array[Row],
      expectedResults: ArrayBuffer[(Int, Int, Int, Int)]): Unit = {
      assertEquals(arr.length, expectedResults.length)

      arr.foreach(row => {
        val first = if (row.isNullAt(0)) -1 else row.getInt(0)
        val sec = if (row.isNullAt(1)) -1 else row.getInt(1)
        val th = if (row.isNullAt(2)) -1 else row.getInt(2)
        val fth = if (row.isNullAt(3)) -1 else row.getInt(3)
        val tup = (first, sec, th, fth)
        val indx = expectedResults.indexOf(tup)
        assertTrue(indx >= 0)
        expectedResults.remove(indx)
      })
      assertTrue(expectedResults.isEmpty)
    }
    snc.dropTable("t1", true)
    snc.dropTable("t2", true)
    snc.sql("create table t1 (c1 int, c2 int) using column ")
    snc.sql("create table t2 (c1 int, c2 int) using column ")

    snc.sql("insert into t1 values (1, 1), (1, 2), (2,1), (2,2), (3, 1)," +
      " (3,2), (4, 1), (4,2), (5, 1), (5,2)")
    snc.sql("insert into t2 values(1, 1), (2,2), (3,3)")

    val rs1 = snc.sql("select * from t1 left outer join t2 on t1.c1 = t2.c1").collect()
    val expectedResults1 = ArrayBuffer((1, 1, 1, 1), (1, 2, 1, 1), (2, 1, 2, 2), (2, 2, 2, 2),
      (3, 1, 3, 3), (3, 2, 3, 3), (4, 1, -1, -1), (4, 2, -1, -1), (5, 1, -1, -1), (5, 2, -1, -1))
    checkResultsMatch(rs1, expectedResults1)

    val rs2 = snc.sql("select * from t1 left outer join t2 " +
      "on t1.c1 = t2.c1 where t1.c2 <> 1").collect()
    val expectedResults2 = ArrayBuffer((1, 2, 1, 1), (2, 2, 2, 2),
      (3, 2, 3, 3), (4, 2, -1, -1), (5, 2, -1, -1))
    checkResultsMatch(rs2, expectedResults2)

    val rs3 = snc.sql("select * from t1 left outer join t2 on t1.c1 = t2.c1 " +
      "where t2.c1 is not null").collect()
    val expectedResults3 = ArrayBuffer((1, 1, 1, 1), (1, 2, 1, 1), (2, 1, 2, 2), (2, 2, 2, 2),
      (3, 1, 3, 3), (3, 2, 3, 3))
    checkResultsMatch(rs3, expectedResults3)
    snc.dropTable("t1", true)
    snc.dropTable("t2", true)
  }

  test("SNAP3082") {
    val session = snc.snappySession
    val serverHostPort = TestUtil.startNetServer()
    val conn = DriverManager.getConnection(
      "jdbc:snappydata://" + serverHostPort)


    // scalastyle:off println
    println(s"SNAP3082: Connected to $serverHostPort")
    val stmt = conn.createStatement()
    insertDataAndTestSNAP3082(conn, stmt, "DOUBLE")
    insertDataAndTestSNAP3082(conn, stmt, "STRING")
    insertDataAndTestSNAP3082(conn, stmt, "FLOAT")
    insertDataAndTestSNAP3082(conn, stmt, "DECIMAL")
    // scalastyle:on println

  }

  test("SNAP-3193: Float Column type ddl in column & row table result in different schema") {
    // create row table rowTable
    snc.sql("CREATE TABLE rowTable(floatCol FLOAT) using row")
    // create col table colTable
    snc.sql("CREATE TABLE colTable(floatCol FLOAT) using column")

    snc.table("rowTable").schema.zip(
      snc.table("colTable").schema).foreach(tup => {
      assertTrue(tup._1.dataType.equals(tup._2.dataType))
    })

    snc.sql("insert into rowTable values (1.0E8)")
    snc.sql("insert into rowTable values (-2.225E-307)")
    snc.sql("insert into colTable values (1.0E8)")
    snc.sql("insert into colTable values (-2.225E-307)")

    val rs1 = snc.sql("select floatCol from rowTable order by floatCol asc ").collect()
    val rs2 = snc.sql("select floatCol from colTable order by floatCol asc ").collect()
    rs1.zip(rs2).foreach {
      case (r1, r2) => assertEquals(r1.getFloat(0), r2.getFloat(0), 0)
    }
    snc.dropTable("rowTable", true)
    snc.dropTable("colTable", true)
  }

  private def insertDataAndTestSNAP3082(conn: Connection, stmt: Statement,
      dataTypeForSetParams: String): Unit = {
    // scalastyle:off println
    println(s"Setting prepared statement parameters as $dataTypeForSetParams")
    stmt.execute("drop table if exists column_table")
    stmt.execute("create table column_table (col1 int, col2 decimal," +
        " col3 decimal(10, 5)) using column")
    val ps1 = conn.prepareStatement("insert into column_table values (?, ?, ?)")
    val numRows = 10
    for (i <- 0 until numRows) {
      ps1.setInt(1, i)
      dataTypeForSetParams match {
        case "DOUBLE" =>
          ps1.setDouble(2, java.lang.Double.valueOf(i * 0.1))
          ps1.setDouble(3, java.lang.Double.valueOf(i * 0.1))
        case "STRING" =>
          ps1.setString(2, s"$i" + 0.1)
          ps1.setString(3, s"$i" + 0.1)
        case "FLOAT" =>
          ps1.setFloat(2, java.lang.Float.valueOf(new lang.Float(i*0.1)))
          ps1.setFloat(3, java.lang.Float.valueOf(new lang.Float(i*0.1)))
        case "DECIMAL" =>
          ps1.setBigDecimal(2, new java.math.BigDecimal(s"$i" + 0.1))
          ps1.setBigDecimal(3, new java.math.BigDecimal(s"$i" + 0.1))
      }
      ps1.executeUpdate()
    }

    println("executing prepared select statement")
    var result1: Array[(java.math.BigDecimal, java.math.BigDecimal)] = new Array(numRows)
    val ps2 = conn.prepareStatement("select * from column_table where col2 = ? order by col1")
    for (j <- 0 until numRows) {
      dataTypeForSetParams match {
        case "DOUBLE" =>
          ps2.setDouble(1, java.lang.Double.valueOf(j * 0.1))
        case "STRING" =>
          ps2.setString(1, s"$j" + 0.1)
        case "FLOAT" =>
          ps2.setFloat(1, java.lang.Float.valueOf(new lang.Float(j * 0.1)))
        case "DECIMAL" =>
          ps2.setBigDecimal(1, new java.math.BigDecimal(s"$j" + 0.1))
      }

      val rs2 = ps2.executeQuery()

      while (rs2.next()) {
        val columnValue1 = rs2.getBigDecimal(2)
        val columnValue2 = rs2.getBigDecimal(3)
        result1(j) = (columnValue1, columnValue2)
        // debug statement
//        println(s"rowNumber = $j (columnVale1, columnVale2) = ($columnValue1, $columnValue2) " +
//            s" columnVale1 precision = ${columnValue1.precision()} " +
//            s" columnVale1 scale =  ${columnValue1.scale ()} " +
//            s" columnVale2 precision = ${columnValue2.precision()} " +
//            s" columnVale2 scale =  ${columnValue2.scale ()}")
      }
    }

    println("executing unprepared select statement")
    var result2: Array[(java.math.BigDecimal, java.math.BigDecimal)] = new Array(numRows)
    for (j <- 0 until numRows) {
      var rs3: java.sql.ResultSet = null
      dataTypeForSetParams match {
        case "DOUBLE" =>
          val v = j * 0.1
          rs3 = stmt.executeQuery(s"select * from column_table" +
              s" where col2 = cast($v as double) order by col1")
        case "STRING" =>
          val v = s"$j" + 0.1
          rs3 = stmt.executeQuery(s"select * from column_table" +
              s" where col2 = cast($v as string) order by col1")
        case "FLOAT" =>
          val v = j * 0.1
          rs3 = stmt.executeQuery(s"select * from column_table" +
              s" where col2 = cast($v as float) order by col1")
        case "DECIMAL" =>
          val v = new java.math.BigDecimal(s"$j" + 0.1)
          rs3 = stmt.executeQuery(s"select * from column_table" +
              s" where col2 = cast($v as decimal) order by col1")
      }
      while (rs3.next()) {
        val columnValue1 = rs3.getBigDecimal(2)
        val columnValue2 = rs3.getBigDecimal(3)
        result2(j) = (columnValue1, columnValue2)
        // debug statement
//        println(s"rowNumber = $j (columnVale1, columnVale2) = ($columnValue1, $columnValue2) " +
//            s" columnVale1 precision = ${columnValue1.precision()} " +
//            s" columnVale1 scale =  ${columnValue1.scale ()} " +
//            s" columnVale2 precision = ${columnValue2.precision()} " +
//            s" columnVale2 scale =  ${columnValue2.scale ()}")
      }
    }

    assert(result1.sameElements(result2),
      "results of prepared and unprepared statements do not match")
    // scalastyle:on println

  }

  test("SNAP-3123: check for GUI plans and SNAP-3141: code gen failure") {
    val session = snc.snappySession.newSession()
    session.sql(s"set ${Property.UseOptimzedHashAggregate.name} = true")
    session.sql(s"set ${Property.UseOptimizedHashAggregateForSingleKey.name} = true")

    val numRows = 1000000
    val sleepTime = 7000L
    session.sql("create table test1 (id long, data string) using column " +
        s"options (buckets '8') as select id, 'data_' || id from range($numRows)")
    val ds = session.sql(
      "select avg(id) average, id % 10 from test1 group by id % 10 order by average")
    Thread.sleep(sleepTime)
    ds.collect()

    // check UI timings and plan details
    val listener = ExternalStoreUtils.getSQLListener.get
    // last one should be the query above
    val queryUIData = listener.getCompletedExecutions.last
    val duration = queryUIData.completionTime.get - queryUIData.submissionTime
    // never expect the query above to take more than 7 secs
    assert(duration > 0L)
    assert(duration < sleepTime)
    assert(queryUIData.succeededJobs.length === 2)

    val metrics = listener.getExecutionMetrics(queryUIData.executionId)
    val scanNode = queryUIData.physicalPlanGraph.allNodes.find(_.name == "ColumnTableScan").get
    val numRowsMetric = scanNode.metrics.find(_.name == "number of output rows").get
    assert(metrics(numRowsMetric.accumulatorId) ===
        SQLMetrics.stringValue(numRowsMetric.metricType, numRows :: Nil))
  }

  test("Bug SNAP-2728. SQL Built in functions throwing exception") {
    snc.dropTable("test1", true)
    snc.sql("SELECT sort_array(array('b', 'd', 'c', 'a'), true)").collect()
    snc.sql("SELECT sort_array(array('b', 'd', 'c', 'a'), true)").collect()
    val numRows = 100
    snc.sql("create table test1 (id long, data string) using column " +
      s"options (buckets '8') as select id, 'data_' || id from range($numRows)")
    snc.sql("select first_value(id, true) from test1").collect()
    snc.sql("select first_value(id, true) from test1").collect()
    snc.sql("select last_value(id, true) from test1").collect()
    snc.sql("select last_value(id, true) from test1").collect()
    snc.sql("select rand(0)").collect()
    snc.sql("select rand(0)").collect()
    snc.sql("select randn(0)").collect()
    snc.sql("select randn(0)").collect()
    snc.dropTable("test1")
  }

  test("Bug SNAP-3215 equi join yields wrong result with filter condition") {
    snc.sql("CREATE SCHEMA IF NOT EXISTS xy")
    snc.sql("DROP TABLE IF EXISTS xy.ORDERS")
    snc.sql("CREATE TABLE xy.ORDERS(O_ORDERKEY INTEGER NOT NULL," +
      " O_NAME VARCHAR(25) NOT NULL, C_CUSTKEY INTEGER NOT NULL)" +
      " USING COLUMN OPTIONS (BUCKETS '10', PARTITION_BY 'C_CUSTKEY')")

    snc.sql("INSERT INTO xy.ORDERS VALUES (6, 'order6', 2)")

    snc.sql("DROP TABLE IF EXISTS xy.CUSTOMER")
    snc.sql("CREATE TABLE xy.CUSTOMER (C_CUSTKEY     INTEGER NOT NULL," +
      " C_NAME VARCHAR(25) NOT NULL) USING COLUMN " +
      "OPTIONS (BUCKETS '10', PARTITION_BY 'C_CUSTKEY' , COLOCATE_WITH 'xy.ORDERS')")

    snc.sql("INSERT INTO xy.CUSTOMER  VALUES (2, 'user2')")
    val sorter = (row1: Row, row2: Row) => {
      if (row1.getInt(0) == row2.getInt(0)) {
        if (row1.isNullAt(2) && row2.isNullAt(2)) {
          true
        } else if (!row1.isNullAt(2) && !row2.isNullAt(2)) {
          row1.getInt(2) < row2.getInt(2)
        } else if (row1.isNullAt(2)) {
          true
        } else {
          false
        }
      } else {
        row1.getInt(0) < row2.getInt(0)
      }
    }
    val q = "SELECT * FROM xy.CUSTOMER AS c  LEFT JOIN " +
      "xy.ORDERS AS o ON c.C_CUSTKEY=o.C_CUSTKEY WHERE c.c_custkey=2"
    val snc1 = snc.newSession()
    val results1 = snc1.sql(q)
    assertTrue(results1.queryExecution.executedPlan.collectFirst {
      case x: HashJoinExec => x
    }.isDefined)
    val rs1 = results1.collect().sortWith(sorter)

       val snc2 = snc.newSession()
       snc2.setConf("snappydata.sql.disableHashJoin", "true")
       val results2 = snc2.sql(q)
    assertTrue(results2.queryExecution.executedPlan.collectFirst {
      case x: HashJoinExec => x
    }.isEmpty)
       val rs2 = results2.collect().sortWith(sorter)

       assertEquals(rs1.length, rs2.length)
       assertTrue(rs1.zip(rs2).forall(tup => tup._1.equals(tup._2)))
  }

  test("Bug SNAP-3215 Left join yields wrong result with filter condition") {
    snc.sql("CREATE SCHEMA IF NOT EXISTS xy")
    snc.sql("DROP TABLE IF EXISTS xy.ORDERS")
    snc.sql("CREATE TABLE xy.ORDERS(O_ORDERKEY INTEGER NOT NULL," +
      " O_NAME VARCHAR(25) NOT NULL, C_CUSTKEY INTEGER NOT NULL)" +
      " USING COLUMN OPTIONS (BUCKETS '10', PARTITION_BY 'C_CUSTKEY')")
    snc.sql("INSERT INTO xy.ORDERS VALUES (1, 'order1', 1)")
    snc.sql("INSERT INTO xy.ORDERS VALUES (2, 'order2', 1)")
    snc.sql("INSERT INTO xy.ORDERS VALUES (3, 'order3', 1)")
    snc.sql("INSERT INTO xy.ORDERS VALUES (4, 'order4', 1)")
    snc.sql("INSERT INTO xy.ORDERS VALUES (5, 'order5', 1)")
    snc.sql("INSERT INTO xy.ORDERS VALUES (6, 'order6', 2)")
    snc.sql("INSERT INTO xy.ORDERS VALUES (7, 'order7', 2)")
    snc.sql("INSERT INTO xy.ORDERS VALUES (8, 'order8', 2)")
    snc.sql("INSERT INTO xy.ORDERS VALUES (9, 'order9', 2)")
    snc.sql("INSERT INTO xy.ORDERS VALUES (10, 'order10', 3)")
    snc.sql("INSERT INTO xy.ORDERS VALUES (11, 'order11', 3)")
    snc.sql("INSERT INTO xy.ORDERS VALUES (12, 'order12', 3)")
    snc.sql("INSERT INTO xy.ORDERS VALUES (13, 'order13', 4)")
    snc.sql("INSERT INTO xy.ORDERS VALUES (14, 'order14', 4)")
    snc.sql("INSERT INTO xy.ORDERS VALUES (15, 'order15', 5)")
    snc.sql("DROP TABLE IF EXISTS xy.CUSTOMER")
    snc.sql("CREATE TABLE xy.CUSTOMER (C_CUSTKEY     INTEGER NOT NULL," +
      " C_NAME VARCHAR(25) NOT NULL) USING COLUMN " +
      "OPTIONS (BUCKETS '10', PARTITION_BY 'C_CUSTKEY', COLOCATE_WITH 'xy.ORDERS')")
    snc.sql("INSERT INTO xy.CUSTOMER  VALUES (1, 'user1')")
    snc.sql("INSERT INTO xy.CUSTOMER  VALUES (2, 'user2')")
    snc.sql("INSERT INTO xy.CUSTOMER  VALUES (3, 'user3')")
    snc.sql("INSERT INTO xy.CUSTOMER  VALUES (4, 'user4')")
    snc.sql("INSERT INTO xy.CUSTOMER  VALUES (5, 'user5')")

    val sorter = (row1: Row, row2: Row) => {
      if (row1.getInt(0) == row2.getInt(0)) {
        if (row1.isNullAt(2) && row2.isNullAt(2)) {
          true
        } else if (!row1.isNullAt(2) && !row2.isNullAt(2)) {
          row1.getInt(2) < row2.getInt(2)
        } else if (row1.isNullAt(2)) {
          true
        } else {
          false
        }
      } else {
        row1.getInt(0) < row2.getInt(0)
      }
    }


    val q = "SELECT * FROM xy.CUSTOMER AS c  LEFT JOIN " +
      "xy.ORDERS AS o ON c.C_CUSTKEY=o.C_CUSTKEY WHERE c.c_custkey=2"
    val snc1 = snc.newSession()
    val results1 = snc1.sql(q)
    assertTrue(results1.queryExecution.executedPlan.collectFirst {
      case x: HashJoinExec => x
    }.isDefined)
    val rs1 = results1.collect().sortWith(sorter)

    val snc2 = snc.newSession()
    snc2.setConf("snappydata.sql.disableHashJoin", "true")
    val results2 = snc2.sql(q)
    assertTrue(results2.queryExecution.executedPlan.collectFirst {
      case x: HashJoinExec => x
    }.isEmpty)
    val rs2 = results2.collect().sortWith(sorter)

    assertEquals(rs1.length, rs2.length)
    assertTrue(rs1.zip(rs2).forall(tup => tup._1.equals(tup._2)))
  }

  test("Bug SNAP-3215") {
    snc.sql("CREATE SCHEMA IF NOT EXISTS xy")
    snc.sql("DROP TABLE IF EXISTS xy.ORDERS")
    snc.sql("CREATE TABLE xy.ORDERS(O_ORDERKEY INTEGER NOT NULL," +
      " O_NAME VARCHAR(25) NOT NULL, C_CUSTKEY INTEGER NOT NULL)" +
      " USING COLUMN OPTIONS (BUCKETS '10', PARTITION_BY 'C_CUSTKEY')")

    snc.sql("INSERT INTO xy.ORDERS VALUES (6, 'order6', 2)")

    snc.sql("DROP TABLE IF EXISTS xy.CUSTOMER")
    snc.sql("CREATE TABLE xy.CUSTOMER (C_CUSTKEY     INTEGER NOT NULL," +
      " C_NAME VARCHAR(25) NOT NULL) USING COLUMN " +
      "OPTIONS (BUCKETS '10', PARTITION_BY 'C_CUSTKEY')")

    snc.sql("INSERT INTO xy.CUSTOMER  VALUES (2, 'user2')")
    val sorter = (row1: Row, row2: Row) => {
      if (row1.getInt(0) == row2.getInt(0)) {
        if (row1.isNullAt(2) && row2.isNullAt(2)) {
          true
        } else if (!row1.isNullAt(2) && !row2.isNullAt(2)) {
          row1.getInt(2) < row2.getInt(2)
        } else if (row1.isNullAt(2)) {
          true
        } else {
          false
        }
      } else {
        row1.getInt(0) < row2.getInt(0)
      }
    }
    val q = "SELECT * FROM xy.CUSTOMER AS c  LEFT JOIN " +
      "xy.ORDERS AS o ON c.C_CUSTKEY=o.C_CUSTKEY WHERE c.c_custkey=2"
    val snc1 = snc.newSession()
    val results1 = snc1.sql(q)
    assertTrue(results1.queryExecution.executedPlan.collectFirst {
      case x: HashJoinExec => x
    }.isDefined)
    val rs1 = results1.collect().sortWith(sorter)

    val snc2 = snc.newSession()
    snc2.setConf("snappydata.sql.disableHashJoin", "true")
    val results2 = snc2.sql(q)
    assertTrue(results2.queryExecution.executedPlan.collectFirst {
      case x: HashJoinExec => x
    }.isEmpty)
    val rs2 = results2.collect().sortWith(sorter)

    assertEquals(rs1.length, rs2.length)
    assertTrue(rs1.zip(rs2).forall(tup => tup._1.equals(tup._2)))
  }

}
