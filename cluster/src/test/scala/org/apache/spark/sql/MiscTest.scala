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
package org.apache.spark.sql

import java.sql.DriverManager

import scala.util.control.NonFatal

import com.pivotal.gemfirexd.TestUtil
import io.snappydata.SnappyFunSuite

import org.apache.spark.Logging
import org.apache.spark.scheduler._

/**
 * Tests that don't fall under any other category
 */
class MiscTest extends SnappyFunSuite with Logging {

  test("With Clause") {
    snc.sql("drop table if exists nulls_table")
    snc.sql(s"create table table1 (ol_1_int_id  integer," +
      s" ol_1_int2_id  integer, ol_1_str_id STRING) using column " +
      "options( partition_by 'ol_1_int2_id', buckets '2')")

    snc.sql("WITH temp_table AS ( SELECT ol_1_int2_id  as col1," +
      " sum(ol_1_int_id) AS col2 FROM table1 GROUP BY ol_1_int2_id)" +
      " SELECT ol_1_int2_id FROM temp_table ," +
      " table1 WHERE ol_1_int2_id  = col1 LIMIT 100 ").collect()
  }

  test("Pool test") {
    // create a dummy pool
    val rootPool = new Pool("lowlatency", SchedulingMode.FAIR, 0, 0)
    sc.taskScheduler.rootPool.addSchedulable(rootPool)

    try {
      snc.sql("set snappydata.scheduler.pool=xyz")
      fail("unknown spark scheduler cannot be set")
    } catch {
      case _: IllegalArgumentException => // do nothing
      case NonFatal(e) =>
        fail("setting unknown spark scheduler with a different error", e)
    }

    snc.sql("set snappydata.scheduler.pool=lowlatency")
    snc.sql("select 1").count
    assert(sc.getLocalProperty("spark.scheduler.pool") === "lowlatency")
  }

  test("SNAP-2434") {
    val sqlstrs = Seq(s"select app.test.* from app.test",
      s"select test.* from test", s"select * from test")
    sqlstrs.foreach(sqlstr =>
      try {
        snc.sql(sqlstr)
        fail(s"this should have given TableNotFoundException")
      } catch {
        case tnfe: TableNotFoundException =>
        case ae: AnalysisException => if (!ae.getMessage().contains("Table or view not found")) {
          throw ae
        }
        case t: Throwable => fail(s"unexpected exception $t")
      }
    )
  }

  test("SNAP-2438") {
    try {
      snc.sql(s"create table good(dept string, sal int) using column options()")
      snc.sql(s"create table test.good(dept string, sal int) using column options()")
      snc.sql(s"insert into test.good values('IT', 10000), ('HR', 9000), ('ADMIN', 4000)")
      var arr = snc.sql(s"select * from good").collect()
      assert(arr.size === 0)
      snc.sql(s"set schema test")
      arr = snc.sql(s"select * from good").collect()
      assert(arr.size === 3)
    } finally {
      snc.sql(s"set schema app")
    }
  }

  test("SNAP-2440") {
    snc.sql("create table test(col1 int not null, col2 int not null) using column")
    snc.sql("create table emp.test1(col1 int not null, col2 int not null) using column")
    snc.sql("insert into test values (1, 2), (4, 5), (6, 7)")
    snc.sql("insert into emp.test1 values (1, 2), (4, 5), (6, 7)")
    val sz = snc.sql(s"select * from app.test").collect().length
    val sqlstrs = Seq("select app.test.* from app.test",
      "select app.test.col1, app.test.col2 from app.test",
      "select col1, col2 from app.test",
      "select * from app.test",
      "select test.* from test",
      "select emp.test1.* from emp.test1",
      "select emp.test1.col1, emp.test1.col2 from emp.test1",
      "select col1, col2 from emp.test1",
      "select * from emp.test1",
      "select test1.* from emp.test1")
    sqlstrs.foreach(sqlstr => {
      val res = snc.sql(sqlstr).collect()
      assert(res.length === 3)
      assert(res(0).get(0) != res(0).get(1))
    })

    val badsqls = Seq("select apppp.test.* from app.test",
      "select app.test.col99, app.test.col2 from app.test",
      "select testt.* from app.test",
      "select apppp.test.* from emp.test1",
      "select emp.test1.col99, emp.test1.col2 from emp.test1",
      "select testt.* from emp.test1")
    badsqls.foreach(sqlstr =>
      try {
        snc.sql(sqlstr)
        fail(s"expected analysis exception for $sqlstr")
      } catch {
        case ae: AnalysisException => // expected ... ignore
      })
  }

  test("multiline json") {
    val jsonFile: String = getClass.getResource("/multiline.json").getPath
    val df = snc.read.json(sc.wholeTextFiles(jsonFile).values)
    df.collect()
    val df1 = snc.read.option("wholefile", "true").json(jsonFile)
    df1.collect()
    val bakeryJsonFile: String = getClass.getResource("/bakery.json").getPath
    snc.sql(s"create external table bakery_json using json options (path '$bakeryJsonFile')")
    val rs = snc.sql("select * from bakery_json")
    rs.collect()
    snc.dropTable("bakery_json", true)
    val serverHostPort2 = TestUtil.startNetServer()
    val conn = DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort2")
    val st = conn.createStatement
    st.execute(s"create external table bakery_json using json options (path '$bakeryJsonFile')")
    val results = st.executeQuery("select * from bakery_json")
    while(results.next()) {
      results.getString(3)
    }
    st.execute("drop table if exists bakery_json")
    conn.close()
    TestUtil.stopNetServer()
  }

  test("SNAP-2087 failure in JSON queries with complex types") {
    val locs = getClass.getResource("/locomotives.json").getPath
    //    val ds = snc.read.json(sc.wholeTextFiles(locs).values)
    // multi-line json file supported, hence no need of the workaround
    val ds = snc.read.format("json").option("wholeFile", "true").load(locs)

    assert(ds.count() === 89)
    assert(ds.filter("model = 'ES44AC'").count() === 12)
  }

}
