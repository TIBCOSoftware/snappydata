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

import scala.collection.mutable.ArrayBuffer

import io.snappydata.core.{Data, TestData2}
import io.snappydata.{Property, SnappyFunSuite, SnappyTableStatsProviderService}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{DynamicInSet, ParamLiteral}
import org.apache.spark.sql.internal.SQLConf

/**
  * Tests for column tables in GFXD.
  */
class TokenizationTest
    extends SnappyFunSuite
        with Logging
        with BeforeAndAfter
        with BeforeAndAfterAll {

  val table = "my_table"
  val table2 = "my_table2"
  val all_typetable = "my_table3"
  var planCaching : Boolean = false

  override def beforeAll(): Unit = {
    // System.setProperty("org.codehaus.janino.source_debugging.enable", "true")
    System.setProperty("spark.sql.codegen.comments", "true")
    System.setProperty("spark.testing", "true")
    planCaching = Property.PlanCaching.get(snc.sessionState.conf)
    Property.PlanCaching.set(snc.sessionState.conf, true)
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    // System.clearProperty("org.codehaus.janino.source_debugging.enable")
    System.clearProperty("spark.sql.codegen.comments")
    System.clearProperty("spark.testing")
    Property.PlanCaching.set(snc.sessionState.conf, planCaching)
    super.afterAll()
  }

  before {
    SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION = true
  }

  after {
    SnappyTableStatsProviderService.TEST_SUSPEND_CACHE_INVALIDATION = false
    SnappySession.clearAllCache()
    snc.dropTable(s"$table", ifExists = true)
    snc.dropTable(s"$table2", ifExists = true)
    snc.dropTable(s"$all_typetable", ifExists = true)
    snc.dropTable(s"$colTableName", ifExists = true)
  }

  test("SNAP-2712") {
    val r1 = snc.sql("select substr(soundex('laxtaxmax'), 1, 3), soundex('tax') from values 1 = 1")
    val c1s = r1.columns
    val arr1 = r1.collect()
    val r2 = snc.sql("select substr(soundex('laxtaxmax'), 2, 5), soundex('tax') from values 1 = 1")
    val arr2 = r2.collect()
    val c2s = r2.columns
    assert(!c1s.sameElements(c2s))
    assert(!arr1.sameElements(arr2))
    assert (arr1(0).getString(0) === "L23")
    assert (arr1(0).getString(1) === "T200")
    assert (arr2(0).getString(0) === "232")
    assert (arr2(0).getString(1) === "T200")

    // Only substr
    val r3 = snc.sql("select substr('suoertest', 0, 5) from values 1 = 1")
    val c3s = r3.columns
    val arr3 = r3.collect()
    val r4 = snc.sql("select substr('supertest', 0, 5) from values 1 = 1")
    val c4s = r4.columns
    val arr4 = r4.collect()
    assert(!c3s.sameElements(c4s))
    assert(!arr3.sameElements(arr4))
    assert (arr3(0).getString(0) === "suoer")
    assert (arr4(0).getString(0) === "super")

    val r5 = snc.sql("select levenshtein('supriya','swati')")
    val c5s = r5.columns
    val arr5 = r5.collect()
    val r6 = snc.sql("select levenshtein('swati','swati')")
    val c6s = r6.columns
    val arr6 = r6.collect()
    val r7 = snc.sql("select levenshtein('swati','sonal')")
    val c7s = r7.columns
    val arr7 = r7.collect()
    assert(!c5s.sameElements(c6s) && !c5s.sameElements(c7s))
    assert(!arr5.sameElements(arr6) && !arr5.sameElements(arr7))
    assert (arr5(0).getInt(0) === 5)
    assert (arr6(0).getInt(0) === 0)
    assert (arr7(0).getInt(0) === 4)
  }

  test("SNAP-2031 tpcds") {
    val sqlstr = s"WITH ss AS (SELECT s_store_sk, sum(ss_ext_sales_price) AS sales, " +
        s"sum(ss_net_profit) AS profit FROM store_sales, date_dim, store WHERE ss_sold_date_sk" +
        s" = d_date_sk AND d_date BETWEEN cast('2000-08-03' AS DATE) AND" +
        s" (cast('2000-08-03' AS DATE)" +
        s" + INTERVAL 30 days) AND ss_store_sk = s_store_sk GROUP BY s_store_sk), sr AS" +
        s" (SELECT s_store_sk, sum(sr_return_amt) AS returns, sum(sr_net_loss) AS profit_loss" +
        s" FROM store_returns, date_dim, store WHERE sr_returned_date_sk = d_date_sk AND d_date" +
        s" BETWEEN cast('2000-08-03' AS DATE) AND (cast('2000-08-03' AS DATE) + INTERVAL 30 days)" +
        s" AND sr_store_sk = s_store_sk GROUP BY s_store_sk), cs AS (SELECT cs_call_center_sk," +
        s" sum(cs_ext_sales_price) AS sales, sum(cs_net_profit) AS profit FROM catalog_sales," +
        s" date_dim WHERE cs_sold_date_sk = d_date_sk AND d_date BETWEEN " +
        s"cast('2000-08-03' AS DATE)" +
        s" AND (cast('2000-08-03' AS DATE) + INTERVAL 30 days) GROUP BY cs_call_center_sk)," +
        s" cr AS (SELECT sum(cr_return_amount) AS returns, sum(cr_net_loss) AS profit_loss FROM" +
        s" catalog_returns, date_dim WHERE cr_returned_date_sk = d_date_sk AND d_date BETWEEN" +
        s" cast('2000-08-03' AS DATE) AND (cast('2000-08-03' AS DATE) + INTERVAL 30 days))," +
        s" ws AS (SELECT wp_web_page_sk, sum(ws_ext_sales_price) AS sales, sum(ws_net_profit)" +
        s" AS profit FROM web_sales, date_dim, web_page WHERE ws_sold_date_sk = d_date_sk AND" +
        s" d_date BETWEEN cast('2000-08-03' AS DATE) AND (cast('2000-08-03' AS DATE)" +
        s" + INTERVAL 30 days) AND ws_web_page_sk = wp_web_page_sk GROUP BY wp_web_page_sk)," +
        s" wr AS (SELECT wp_web_page_sk, sum(wr_return_amt) AS returns, sum(wr_net_loss) " +
        s"AS profit_loss FROM web_returns, date_dim, web_page " +
        s"WHERE wr_returned_date_sk = d_date_sk" +
        s" AND d_date BETWEEN cast('2000-08-03' AS DATE) AND " +
        s"(cast('2000-08-03' AS DATE) + INTERVAL 30 days)" +
        s" AND wr_web_page_sk = wp_web_page_sk GROUP BY wp_web_page_sk) " +
        s"SELECT channel, id, sum(sales)" +
        s" AS sales, sum(returns) AS returns, sum(profit) AS profit FROM " +
        s"(SELECT  'store channel' AS channel," +
        s"  ss.s_store_sk AS id,  sales,  coalesce(returns, 0) AS returns, " +
        s"(profit - coalesce(profit_loss, 0))" +
        s" AS profit  FROM ss  LEFT JOIN sr  ON ss.s_store_sk = sr.s_store_sk  UNION ALL " +
        s" SELECT  'catalog channel' AS channel,  cs_call_center_sk AS id,  sales,  returns," +
        s"  (profit - profit_loss) AS profit  FROM cs, cr  UNION ALL " +
        s"SELECT  'web channel' AS channel," +
        s"  ws.wp_web_page_sk AS id,  sales,  coalesce(returns, 0) returns," +
        s" (profit - coalesce(profit_loss, 0))" +
        s" AS profit  FROM ws  LEFT JOIN wr  ON ws.wp_web_page_sk = wr.wp_web_page_sk ) x " +
        s"GROUP BY ROLLUP (channel, id) ORDER BY channel, id LIMIT 100"
    try {
      snc.sql(sqlstr)
      fail(s"this should have given TableNotFoundException")
    } catch {
      case tnfe: TableNotFoundException =>
      case t: Throwable => fail(s"unexpected exception $t")
    }
  }

  test("partition by interval - tpcds query") {
    val sqlstr = s"SELECT i_item_desc, i_category, " +
        s"i_class, i_current_price, sum(ws_ext_sales_price) " +
        s"AS itemrevenue, sum(ws_ext_sales_price) * 100 / sum(sum(ws_ext_sales_price))" +
        s" OVER (PARTITION BY i_class) AS revenueratio FROM " +
        s"web_sales, item, date_dim WHERE ws_item_sk = i_item_sk AND i_category " +
        s"IN ('Sports', 'Books', 'Home')" +
        s" AND ws_sold_date_sk = d_date_sk AND d_date BETWEEN cast('1999-02-22' AS DATE) " +
        s"AND (cast('1999-02-22' AS DATE) + INTERVAL 30 days) GROUP BY i_item_id, " +
        s"i_item_desc, i_category, i_class, i_current_price ORDER BY i_category, " +
        s"i_class, i_item_id, i_item_desc, revenueratio LIMIT 100"
    try {
      snc.sql(sqlstr)
      fail(s"this should have given TableNotFoundException")
    } catch {
      case tnfe: TableNotFoundException =>
      case t: Throwable => fail(s"unexpected exception $t")
    }
  }

  test("sql range operator") {
    var r = snc.sql(s"select id, concat('sym', cast((id) as STRING)) as" +
        s" sym from range(0, 100)").collect()
    assert(r.size === 100)
  }

  test("sql range operator2") {
    snc.sql(s"create table target (id int not null, symbol string not null) using column options()")
    var r = snc.sql(s"insert into target (select id, concat('sym', cast((id) as STRING)) as" +
        s" sym from range(0, 100))").collect()
    r = snc.sql(s"select count(*) from target").collect()
    assert(r.head.get(0) === 100)
  }

  test("like queries") {
    val numRows = 100
    createSimpleTableAndPoupulateData(numRows, s"$table")

    {
      val q = s"select * from $table where a like '10%'"
      var result = snc.sql(q).collect()

      val q2 = s"select * from $table where a like '20%'"
      var result2 = snc.sql(q2).collect()
      assert(!(result.sameElements(result2)) && result.length > 0)
    }
  }

  test("same session from different thread") {
    val numRows = 2
    createSimpleTableAndPoupulateData(numRows, s"$table")

    {
      val q = (0 until numRows) map { x =>
        s"select * from $table where a = $x"
      }
      var result = snc.sql(q(0)).collect()
      assert(result.length === 1)
      result.foreach( r => {
        assert(r.get(0) == r.get(1) && r.get(0) == 0)
      })

      val runnable = new Runnable {
        override def run() = {
          var result = snc.sql(q(1)).collect()
          assert(result.length === 1)
          result.foreach( r => {
            assert(r.get(0) == r.get(1) && r.get(0) == 1)
          })
        }
      }
      val newthread = new Thread(runnable)
      newthread.start()
      newthread.join()

      val cacheMap = SnappySession.getPlanCache.asMap()
      assert( cacheMap.size() == 1)
    }
  }

  def getAllValidKeys: Int = {
    val cacheMap = SnappySession.getPlanCache.asMap()
    cacheMap.keySet().toArray().length
  }

  test("Test no tokenize for round functions") {
    snc.sql(s"Drop Table if exists double_tab")
    snc.sql(s"Create Table double_tab (a INT, d Double) " +
        "using column options()")
    snc.sql(s"insert into double_tab values(1, 1.111111), (2, 2.222222), (3, 3.33333)")
    val cacheMap = SnappySession.getPlanCache.asMap()
    assert( cacheMap.size() == 0)
    var res = snc.sql(s"select * from double_tab where round(d, 2) < round(3.3333, 2)").collect()
    assert(res.length === 2)
    res = snc.sql(s"select * from double_tab where round(d, 3) < round(3.3333, 3)").collect()
    assert(res.length === 2)
    assert(cacheMap.size() == 2)
  }

  test("Test some more foldable expressions and limit in where clause") {
    val numRows = 10
    createSimpleTableAndPoupulateData(numRows, s"$table")
    val cacheMap = SnappySession.getPlanCache.asMap()
    assert(cacheMap.size() == 0)
    var res = snc.sql(s"select * from $table where a in " +
        s"( select b from $table where b < 5 limit 2 )").collect()
    assert(res.length == 2)
    // This should not be tokenized and so cachemap size should be 0
    assert( cacheMap.size() == 0)
    createSimpleTableWithAStringColumnAndPoupulateData(100, "tab_str")
    res = snc.sql(s"select * from tab_str where b = 'aa12bb' and b like 'aa1%'").collect()
    assert(res.length == 1)
    // this is converted to Literal by parser so plan is cached
    assert(cacheMap.size() == 1)

    // An n-tile query ... but this is not affecting Tokenization as ntile is
    // not in where clause but in from clause only.
    res = snc.sql(s"select quartile, avg(c) as avgC, max(c) as maxC" +
        s" from (select c, ntile(4) over (order by c) as quartile from $table ) x " +
        s"group by quartile order by quartile").collect()

    // Unix timestamp
    val df = snc.sql(s"select * from $table where UNIX_TIMESTAMP('2015-01-01 12:00:00') > a")
    var foundCount = 0
    df.queryExecution.logical.transformAllExpressions {
      case pl: ParamLiteral =>
        foundCount += 1
        pl
    }
    assert(foundCount == 1, "did not expect num ParamLiteral other than 1")

    val df2 = snc.sql(s"select * from $table where UNIX_TIMESTAMP('2015-01-01', 'yyyy-mm-dd') > a")
    foundCount = 0
    df2.queryExecution.logical.transformAllExpressions {
      case pl: ParamLiteral =>
        foundCount += 1
        pl
    }
    assert(foundCount == 1, "did not expect num ParamLiteral other than 1 in logical plan")

    cacheMap.clear()

    // check caching for non-code generated JsonTuple

    // RDDScanExec plans are not considered for plan caching
    checkAnswer(snc.sql(
      """
        |SELECT json_tuple(json, 'f1', 'f2')
        |FROM (SELECT '{"f1": "value1", "f2": 12}' json) test
      """.stripMargin), Row("value1", "12") :: Nil)
    checkAnswer(snc.sql(
      """
        |SELECT json_tuple(json, 'f2', 'f1')
        |FROM (SELECT '{"f1": "value1", "f2": 12}' json) test
      """.stripMargin), Row("12", "value1") :: Nil)
    checkAnswer(snc.sql(
      """
        |SELECT json_tuple(json, 'f1', 'f2')
        |FROM (SELECT '{"f1": "value2", "f2": 10}' json) test
      """.stripMargin), Row("value2", "10") :: Nil)

    assert(cacheMap.size() == 0)

    checkAnswer(snc.sql(
      """
        |SELECT json_tuple(json, 'f1')
        |FROM (SELECT '{"f1": "value2", "f2": 10}' json) test
      """.stripMargin), Row("value2") :: Nil)
    checkAnswer(snc.sql(
      """
        |SELECT json_tuple(json, 'f2')
        |FROM (SELECT '{"f1": "value2", "f2": 10}' json) test
      """.stripMargin), Row("10") :: Nil)

    assert(cacheMap.size() == 0)
  }

  test("SNAP-2566") {
    snc.sql("CREATE TABLE employees " +
        "(employee_number INT NOT NULL, last_name VARCHAR(50) NOT NULL, " +
        "first_name VARCHAR(50) NOT NULL, salary INT, dept_id INT) using column")
    snc.sql("create view v1 as SELECT dept_id, last_name, salary, " +
        "LAG(salary) OVER (PARTITION BY dept_id ORDER BY salary) " +
        "AS lower_salary FROM employees")
    snc.sql("select * from v1")
    snc.sql("drop table employees")
  }

  test("SNAP-2566-1") {
    snc.sql("CREATE TABLE employees " +
        "(employee_number INT NOT NULL, last_name VARCHAR(50) NOT NULL, " +
        "first_name VARCHAR(50) NOT NULL, salary INT, dept_id INT) using column")
    snc.sql("SELECT dept_id, last_name, salary, " +
        "LAG(salary, 1) OVER (PARTITION BY dept_id ORDER BY salary) " +
        "AS lower_salary FROM employees")
    snc.sql("SELECT dept_id, last_name, salary, " +
        "LAG(salary, 1, 1) OVER (PARTITION BY dept_id ORDER BY salary) " +
        "AS lower_salary FROM employees")
    snc.sql("SELECT dept_id, last_name, salary, " +
        "LEAD(salary, 1) OVER (PARTITION BY dept_id ORDER BY salary) " +
        "AS lower_salary FROM employees")
    snc.sql("SELECT dept_id, last_name, salary, " +
        "LEAD(salary, 1, 1) OVER (PARTITION BY dept_id ORDER BY salary) " +
        "AS lower_salary FROM employees")
    snc.sql("drop table employees")
  }

  test("Test external tables no plan caching") {
    val cacheMap = SnappySession.getPlanCache.asMap()
    val hfile: String = getClass.getResource("/2015.parquet").getPath
    val airline = snc.read.parquet(hfile)
    snc.sql(s"CREATE EXTERNAL TABLE STAGING_AIRLINE USING parquet options(path '$hfile')")
    snc.sql(s"select count(*) from STAGING_AIRLINE").collect()
    assert( cacheMap.size() == 0)
    snc.sql(s"select * from STAGING_AIRLINE where MonthI = 2").collect()
    assert( cacheMap.size() == 0)
    snc.sql(s"drop table STAGING_AIRLINE")
  }

  test("Test plan caching and tokenization disabled in session") {
    val numRows = 10
    createSimpleTableAndPoupulateData(numRows, s"$table")

    try {
      val q = (0 until numRows) map { x =>
        s"select * from $table where a = $x"
      }
      q.zipWithIndex.foreach { case (x, i) =>
        var result = snc.sql(x).collect()
        assert(result.length === 1)
        result.foreach(r => {
          assert(r.get(0) == r.get(1) && r.get(2) == i)
        })
      }

      val newSession = new SnappySession(snc.sparkSession.sparkContext)

      val cacheMap = SnappySession.getPlanCache.asMap()
      assert(cacheMap.size() == 1)
      newSession.sql(s"set snappydata.sql.planCaching=false").collect()
      assert(cacheMap.size() == 1)

      var query = s"select * from $table where a = 0"
      newSession.sql(query).collect()
      assert(cacheMap.size() == 1)

      query = s"select * from $table where a = 1"
      newSession.sql(query).collect()
      assert(cacheMap.size() == 1)

      query = s"select * from $table where b = 1"
      var res2 = newSession.sql(query).collect()
      assert(cacheMap.size() == 1)

      cacheMap.clear()

      q.zipWithIndex.foreach { case (x, i) =>
        var result = newSession.sql(x).collect()
        assert(result.length === 1)
        result.foreach(r => {
          assert(r.get(0) == r.get(1) && r.get(2) == i)
        })
      }
      assert(cacheMap.size() == 0)

      cacheMap.clear()

      val newSession2 = new SnappySession(snc.sparkSession.sparkContext)
      Property.PlanCaching.set(newSession2.sessionState.conf, true)
      assert(cacheMap.size() == 0)

      q.zipWithIndex.foreach { case (x, i) =>
        var result = newSession2.sql(x).collect()
        assert(result.length === 1)
        result.foreach(r => {
          assert(r.get(0) == r.get(1) && r.get(2) == i)
        })
      }

      assert(SnappySession.getPlanCache.asMap().size() == 1)
      newSession.clear()
      newSession2.clear()
      cacheMap.clear()

      val newSession3 = new SnappySession(snc.sparkSession.sparkContext)
      newSession3.sql(s"set snappydata.sql.tokenize=false")
      // check that SQLConf property names are case-insensitive
      newSession3.sql(s"set snappydata.sql.plancaching=true")
      assert(cacheMap.size() == 0)

      q.zipWithIndex.foreach { case (x, i) =>
        var result = newSession3.sql(x).collect()
        assert(result.length === 1)
        result.foreach(r => {
          assert(r.get(0) == r.get(1) && r.get(2) == i)
        })
      }

      assert(cacheMap.size() == 10)

      newSession3.clear()
      cacheMap.clear()

    } finally {
      snc.sql("set spark.sql.caseSensitive = false")
      snc.sql("set schema = APP")
      snc.sql(s"set snappydata.sql.tokenize=true").collect()
    }
  }

  test("Test tokenize and queryHints and noTokenize if limit or projection") {
    val numRows = 10
    createSimpleTableAndPoupulateData(numRows, s"$table")

    try {
      val q = (0 until numRows) map { x =>
        s"select * from $table where a = $x"
      }
      val start = System.currentTimeMillis()
      q.zipWithIndex.foreach  { case (x, i) =>
        var result = snc.sql(x).collect()
        assert(result.length === 1)
        result.foreach( r => {
          logInfo(s"${r.get(0)}, ${r.get(1)}, ${r.get(2)}, $i")
          assert(r.get(0) == r.get(1) && r.get(2) == i)
        })
      }
      val end = System.currentTimeMillis()

      // snc.sql(s"select * from $table where a = 1200").collect()
      logInfo("Time taken = " + (end - start))

      val cacheMap = SnappySession.getPlanCache.asMap()
      assert( cacheMap.size() == 1)
      val x = cacheMap.keySet().toArray()(0).asInstanceOf[CachedKey].sqlText
      // We expect the first query in the head which is

      // assert(x === q.head, s"x = ${x} and q.head = ${q.head}")

      // Now test query hints -- arbitrary hint given
      val hintQuery = s"select * from $table /*+ XXXX( ) */ where a = 0"
      snc.sql(hintQuery).collect()
      assert( cacheMap.size() == 2)

      // test limit
      var query = s"select * from $table where a = 0 limit 1"
      var res1 = snc.sql(query).collect()
      assert( cacheMap.size() == 3)

      query = s"select * from $table where a = 0 limit 10"
      var res2 = snc.sql(query).collect()
      assert( cacheMap.size() == 4)

      // test fetch first syntax also
      // Cache map size doesn't change because fetch first converts into limit
      // itself and the exact same thing is fired above
      query = s"select * from $table where a = 0 fetch first 1 row only"
      res1 = snc.sql(query).collect()
      assert( cacheMap.size() == 4)

      // Cache map size doesn't change because fetch first converts into limit
      // itself and the exact same thing is fired above
      query = s"select * from $table where a = 0 fetch first 10 rows only"
      res1 = snc.sql(query).collect()
      assert( cacheMap.size() == 4)

      // test constants in projection
      query = s"select a, 'x' from $table where a = 0"
      res1 = snc.sql(query).collect()
      assert( cacheMap.size() == 5)

      query = s"select a, 'y' from $table where a = 0"
      res2 = snc.sql(query).collect()
      assert( cacheMap.size() == 5)

      // check in based queries
      query = s"select * from $table where a in (0, 1)"
      res1 = snc.sql(query).collect()
      assert( cacheMap.size() == 6)

      assert( getAllValidKeys == 6)
      // new plan should not be generated so size should be same
      query = s"select * from $table where a in (5, 7)"
      res2 = snc.sql(query).collect()
      assert( cacheMap.size() == 6)
      assert(!(res1.sameElements(res2)))

      // test fetch first syntax also
      // Cache map size changes because limit '3' is different
      query = s"select * from $table where a = 0 fetch first 3 row only"
      res1 = snc.sql(query).collect()
      assert( cacheMap.size() == 7)

      // Cache map size changes because limit '4' is different
      query = s"select * from $table where a = 0 fetch first 4 rows only"
      res1 = snc.sql(query).collect()
      assert( cacheMap.size() == 8)

      // fetch first with actual result validation
      query = s"select a from $table order by a desc fetch first 1 row only"
      res1 = snc.sql(query).collect()
      assert( cacheMap.size() == 9)
      assert (res1.size == 1)
      res1 foreach { r =>
        assert (r.get(0) == 10)
      }

      // let us clear the plan cache
      snc.clear()
      assert( cacheMap.size() == 0)

      createSimpleTableAndPoupulateData(numRows, s"$table2")
      // creating table should not put anything in cache
      assert( cacheMap.size() == 0)
      snc.sql("set spark.sql.crossJoin.enabled=true")
      // fire a join query
      query = s"select * from $table t1, $table2 t2 where t1.a = 0"
      res1 = snc.sql(query).collect()
      assert(cacheMap.size() == 0) // no caching for NLJ

      query = s"select * from $table t1, $table2 t2 where t1.a = 5"
      res2 = snc.sql(query).collect()
      assert(cacheMap.size() == 0)
      assert(!(res1.sameElements(res2)))

      query = s"select * from $table t1, $table2 t2 where t2.a = 5"
      snc.sql(query).collect()
      assert(cacheMap.size() == 0)

      query = s"select * from $table t1, $table2 t2 where t1.a = t2.a"
      snc.sql(query).collect()
      // broadcast join not cached
      assert(cacheMap.size() == 0)

      query = s"select * from $table t1, $table2 t2 where t1.a = t2.b"
      snc.sql(query).collect()
      assert(cacheMap.size() == 0)

      // let us test for having
      query = s"select t1.b, SUM(t1.a) from $table t1 group by t1.b having SUM(t1.a) > 0"
      res1 = snc.sql(query).collect()
      assert( cacheMap.size() == 1)

      query = s"select t1.b, SUM(t1.a) from $table t1 group by t1.b having SUM(t1.a) > 5"
      res2 = snc.sql(query).collect()
      assert( cacheMap.size() == 1)
      assert(!res1.sameElements(res2))

      snc.sql("set spark.sql.crossJoin.enabled=false")

      snc.sql(s"drop table $table")
      snc.sql(s"drop table $table2")

    } finally {
      snc.sql("set spark.sql.caseSensitive = false")
      snc.sql("set schema = APP")
    }
    logInfo("Successful")
  }

  test("Test tokenize for sub-queries") {
    snc.sql(s"set spark.sql.autoBroadcastJoinThreshold=1")
    snc.sql(s"set spark.sql.crossJoin.enabled=true")
    val numRows = 10
    createSimpleTableAndPoupulateData(numRows, s"$table")
    createSimpleTableAndPoupulateData(numRows, s"$table2")
    var query = s"select * from $table t1, $table2 t2 where t1.a in " +
      s"( select a from $table2 where b = 5 )"
    var results1 = snc.sqlUncached(query).collect()
    var results2 = snc.sql(query).collect()
    assert(results1.toSeq === results2.toSeq)

    val cacheMap = SnappySession.getPlanCache.asMap()

    assert( cacheMap.size() == 1)

    query = s"select * from $table t1, $table2 t2 where t1.a in " +
      s"( select a from $table2 where b = 8)"
    results1 = snc.sqlUncached(query).collect()
    results2 = snc.sql(query).collect()
    assert(results1.toSeq === results2.toSeq)
    assert( cacheMap.size() == 1)

    // check for scalar subqueries (these should run without caching)
    query = s"select * from $table t1, $table2 t2 where t1.a = " +
        s"( select a from $table2 where b = 5 )"
    results1 = snc.sqlUncached(query).collect()
    results2 = snc.sql(query).collect()
    assert(results1.toSeq === results2.toSeq)
    assert( cacheMap.size() == 1)

    query = s"select * from $table t1, $table2 t2 where t1.a = " +
        s"( select a from $table2 where b = 7 )"
    results1 = snc.sqlUncached(query).collect()
    results2 = snc.sql(query).collect()
    assert(results1.toSeq === results2.toSeq)
    assert( cacheMap.size() == 1)

    logInfo("Successful")
  }

  test("Test tokenize for joins and sub-queries") {
    snc.sql(s"set spark.sql.autoBroadcastJoinThreshold=1")
    val numRows = 10
    createSimpleTableAndPoupulateData(numRows, s"$table")
    createSimpleTableAndPoupulateData(numRows, s"$table2")
    var query = s"select * from $table t1, $table2 t2 where t1.a = t2.a and t1.b = 5 limit 2"
    // snc.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    val result1 = snc.sql(query).collect()
    result1.foreach( r => {
      logInfo(r.get(0) + ", " + r.get(1) + r.get(2) + ", " + r.get(3) + r.get(4) +
          ", " + r.get(5))
    })
    val cacheMap = SnappySession.getPlanCache.asMap()

    assert( cacheMap.size() == 1)

    query = s"select * from $table t1, $table2 t2 where t1.a = t2.a and t1.b = 7 limit 2"
    val result2 = snc.sql(query).collect()
    result2.foreach( r => {
      logInfo(r.get(0) + ", " + r.get(1) + r.get(2) + ", " + r.get(3) + r.get(4) +
          ", " + r.get(5))
    })
    assert( cacheMap.size() == 1)
    assert(!result1.sameElements(result2))
    assert(result1.length > 0)
    assert(result2.length > 0)
    logInfo("Successful")
  }

  test("SNAP-1894") {

    val snap = snc
    val row = identity[(java.lang.Integer, java.lang.Double)](_)

    import snap.implicits._
    lazy val l = Seq(
      row(1, 2.0),
      row(1, 2.0),
      row(2, 1.0),
      row(2, 1.0),
      row(3, 3.0),
      row(null, null),
      row(null, 5.0),
      row(6, null)).toDF("a", "b")

    lazy val r = Seq(
      row(2, 3.0),
      row(2, 3.0),
      row(3, 2.0),
      row(4, 1.0),
      row(null, null),
      row(null, 5.0),
      row(6, null)).toDF("c", "d")


    l.write.format("row").saveAsTable("l")
    r.write.format("row").saveAsTable("r")

    assert(snc.sql("select l.a from l where (select count(*) as cnt" +
      " from r where l.a = r.c) = 0").count == 4)
    snc.sql("select case when count(*) = 1 then null else count(*) end as cnt" +
      " from r, l where l.a = r.c").collect.foreach(r => assert(r.get(0) == 6))
    assert(snc.sql("select l.a from l where (select case when count(*) = 1" +
      " then null else count(*) end as cnt from r where l.a = r.c) = 0").count == 4)
  }

  private def createSimpleTableWithAStringColumnAndPoupulateData(numRows: Int, name: String) = {
    val strs = (0 to numRows).map(i => s"aa${i}bb")
    val data = ((0 to numRows), (0 to numRows), strs).zipped.toArray
    val rdd = sc.parallelize(data, data.length)
      .map(s => TestData2(s._1, s._3, s._2))
    val dataDF = snc.createDataFrame(rdd)

    snc.sql(s"Drop Table if exists $name")
    snc.sql(s"Create Table $name (a INT, b string, c INT) " +
      "using column options()")
    dataDF.write.insertInto(s"$name")
    SnappyTableStatsProviderService.getService.getAggregatedStatsOnDemand
  }

  private def createSimpleTableAndPoupulateData(numRows: Int, name: String) = {
    val data = ((0 to numRows), (0 to numRows), (0 to numRows)).zipped.toArray
    val rdd = sc.parallelize(data, data.length)
      .map(s => Data(s._1, s._2, s._3))
    val dataDF = snc.createDataFrame(rdd)

    snc.sql(s"Drop Table if exists $name")
    snc.sql(s"Create Table $name (a INT, b INT, c INT) " +
      "using column options()")
    dataDF.write.insertInto(s"$name")
    SnappyTableStatsProviderService.getService.getAggregatedStatsOnDemand
  }

  private def createAllTypeTableAndPoupulateData(numRows: Int,
      name: String) = {
    val ints = (0 to numRows).zipWithIndex.map {case (_, i) =>
      i
    }
    val longs = (0 to numRows).zipWithIndex.map    { case (_, i) => 1L*1000*i }
    val floats = (0 to numRows).zipWithIndex.map   { case (_, i) => 0.1f*i    }
    val decimals = (0 to numRows).zipWithIndex.map { case (_, i) => 0.1d*i    }
    val strs = (0 to numRows).zipWithIndex.map     { case (_, i) => s"abc$i"  }
    val dates = (0 to numRows).zipWithIndex.map    { case (_, i) => 1         }
    val tstmps = (0 to numRows).zipWithIndex.map   { case (_, i) => s"abc$i"  }

    val x = ((((((ints, longs).zipped.toArray, floats).zipped.toArray,
      decimals).zipped.toArray, strs).zipped.toArray, dates).zipped.toArray, tstmps).zipped.toArray
    val data = x map { case ((((((i, l), f), d), s), dt), ts) =>
      (i, l, f, d, s, dt, ts)
    }

    val rdd = sc.parallelize(data, data.length)
      .map(s => (s._1, s._2, s._3, s._4, s._5, s._6, s._7))
    val dataDF = snc.createDataFrame(rdd)

    snc.sql(s"Drop Table if exists $name")
    snc.sql(s"Create Table $name (a INT, b Long, c Float, d Double, s String, dt Int, ts Long) " +
      "using column options()")

    dataDF.write.insertInto(name)
    SnappyTableStatsProviderService.getService.getAggregatedStatsOnDemand
  }

  val colTableName = "airlineColTable"

  test("Test broadcast hash joins, scalar sub-queries, updates/deletes") {
      val ddlStr = "(YearI INT," + // NOT NULL
          "MonthI INT," + // NOT NULL
          "DayOfMonth INT," + // NOT NULL
          "DayOfWeek INT," + // NOT NULL
          "DepTime INT," +
          "CRSDepTime INT," +
          "ArrTime INT," +
          "CRSArrTime INT," +
          "UniqueCarrier VARCHAR(20)," + // NOT NULL
          "FlightNum INT," +
          "TailNum VARCHAR(20)," +
          "ActualElapsedTime INT," +
          "CRSElapsedTime INT," +
          "AirTime INT," +
          "ArrDelay INT," +
          "DepDelay INT," +
          "Origin VARCHAR(20)," +
          "Dest VARCHAR(20)," +
          "Distance INT," +
          "TaxiIn INT," +
          "TaxiOut INT," +
          "Cancelled INT," +
          "CancellationCode VARCHAR(20)," +
          "Diverted INT," +
          "CarrierDelay INT," +
          "WeatherDelay INT," +
          "NASDelay INT," +
          "SecurityDelay INT," +
          "LateAircraftDelay INT," +
          "ArrDelaySlot INT)"

      val hfile: String = getClass.getResource("/2015.parquet").getPath
      val snContext = snc

      val airlineDF = snContext.read.load(hfile)
      val airlineparquetTable = "airlineparquetTable"
      airlineDF.createOrReplaceTempView(airlineparquetTable)

      // val colTableName = "airlineColTable"

      snc.sql(s"CREATE TABLE $colTableName $ddlStr USING column")

      airlineDF.write.insertInto(colTableName)

      snc.conf.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, 100000000L)

      val rs0 = snc.sql("select avg(taxiin + taxiout) avgTaxiTime, count( * ) numFlights, " +
          s"dest, avg(arrDelay) arrivalDelay from $colTableName " +
          s" where (taxiin > 20 or taxiout > 20) and dest in  (select dest from $colTableName " +
          s" group by dest having count ( * ) > 100) group by dest order " +
          s" by avgTaxiTime desc")

      val rows0 = rs0.collect()

      val rs1 = snc.sql("select avg(taxiin + taxiout) avgTaxiTime, count( * ) numFlights, " +
          s"dest, avg(arrDelay) arrivalDelay from $colTableName " +
          s" where (taxiin > 20 or taxiout > 20) and dest in  (select dest from $colTableName " +
          s" group by dest having count ( * ) > 100) group by dest order " +
          s" by avgTaxiTime desc")

      val rows1 = rs1.collect()
      assert(rows0.sameElements(rows1))

      val cacheMap = SnappySession.getPlanCache.asMap()
      assert(cacheMap.size() == 0)

      val rs11 = snc.sql("select avg(taxiin + taxiout) avgTaxiTime, count( * ) numFlights, " +
          s"dest, avg(arrDelay) arrivalDelay from $colTableName " +
          s" where (taxiin > 10 or taxiout > 10) and dest in  (select dest from $colTableName " +
          s" group by dest having count ( * ) > 10000) group by dest order " +
          s" by avgTaxiTime desc")

      val rows11 = rs11.collect()
      assert(!rows11.sameElements(rows1))
      assert(cacheMap.size() == 0)

    // Test broadcast hash joins and scalar sub-queries - 2

    var df = snc.sql("select avg(taxiin + taxiout) avgTaxiTime, count( * ) numFlights, " +
        s"dest, avg(arrDelay) arrivalDelay from $colTableName " +
        s" where (taxiin > 10 or taxiout > 10) and dest in  (select dest from $colTableName " +
        s" where distance = 100 group by dest having count ( * ) > 100) group by dest order " +
        s" by avgTaxiTime desc")
    // df.explain(true)
    var res1 = df.collect()
    val r1 = normalizeRow(res1)
    df = snc.sql("select avg(taxiin + taxiout) avgTaxiTime, count( * ) numFlights, " +
        s"dest, avg(arrDelay) arrivalDelay from $colTableName " +
        s" where (taxiin > 20 or taxiout > 20) and dest in  (select dest from $colTableName " +
        s" where distance = 658 group by dest having count ( * ) > 100) group by dest order " +
        s" by avgTaxiTime desc")
    var res2 = df.collect()
    val r2 = normalizeRow(res2)
    assert(!r1.sameElements(r2))

    // no caching of broadcast plans
    assert(cacheMap.size() == 0)

    // check for out of order collects on two queries that are cached
    var query1 = "select avg(taxiin + taxiout) avgTaxiTime, count(*) numFlights, " +
        s"avg(arrDelay) arrivalDelay from $colTableName " +
        s"where (taxiin > 20 or taxiout > 20)"
    var query2 = "select avg(taxiin + taxiout) avgTaxiTime, count(*) numFlights, " +
        s"avg(arrDelay) arrivalDelay from $colTableName " +
        s"where (taxiin > 40 or taxiout > 40)"

    val spark = new SparkSession(sc)
    val colTable = snc.table(colTableName)
    spark.internalCreateDataFrame(colTable.queryExecution.toRdd, colTable.schema)
        .createOrReplaceTempView(colTableName)

    res1 = spark.sql(query1).collect()
    res2 = spark.sql(query2).collect()
    var df1 = snc.sql(query1)
    var df2 = snc.sql(query2)

    assert(cacheMap.size() == 1)

    checkAnswer(df1, res1)
    checkAnswer(df2, res2)

    // check for IN set case (> 10 elements)
    assert(cacheMap.size() == 1)
    query1 = s"select avg(arrDelay) from $colTableName where uniqueCarrier in " +
        "('YV', 'AQ', 'VX', 'WN', 'US', 'EV', 'MQ', 'DL', 'OO', 'XE', 'NW', 'UA', 'F9', 'B6')"
    query2 = s"select avg(arrDelay) from $colTableName where uniqueCarrier in " +
        "('MQ', 'DL', 'OO', 'XE', 'NW', 'UA', 'F9', 'B6', 'AA', 'FL', 'HA', 'AS', 'NK', '9E')"

    res1 = spark.sql(query1).collect()
    res2 = spark.sql(query2).collect()
    df1 = snc.sql(query1)
    df2 = snc.sql(query2)

    def checkDynInSet(df: DataFrame, present: Boolean = true): Unit = {
      assert(df.queryExecution.executedPlan.find(
        _.expressions.exists(_.isInstanceOf[DynamicInSet])).isDefined === present)
    }

    checkAnswer(df1, res1)
    checkAnswer(df2, res2)
    checkDynInSet(df1)
    checkDynInSet(df2)

    assert(cacheMap.size() == 2)

    // check with small ranges to check stats filtering
    query1 = s"select avg(arrDelay) from $colTableName where uniqueCarrier in " +
        "('AQ', 'AS', '9E')"
    query2 = s"select avg(arrDelay) from $colTableName where uniqueCarrier in " +
        "('YV', 'XE', 'WN')"

    res1 = spark.sql(query1).collect()
    res2 = spark.sql(query2).collect()
    df1 = snc.sql(query1)
    df2 = snc.sql(query2)

    checkAnswer(df1, res1)
    checkAnswer(df2, res2)
    checkDynInSet(df1)
    checkDynInSet(df2)

    assert(cacheMap.size() == 3)

    // check with null values
    query1 = s"select avg(arrDelay) from $colTableName where uniqueCarrier in " +
        "('AQ', null, '9E')"
    query2 = s"select avg(arrDelay) from $colTableName where uniqueCarrier in " +
        "('YV', null, 'WN')"

    res1 = spark.sql(query1).collect()
    res2 = spark.sql(query2).collect()
    df1 = snc.sql(query1)
    df2 = snc.sql(query2)

    checkAnswer(df1, res1)
    checkAnswer(df2, res2)
    checkDynInSet(df1)
    checkDynInSet(df2)

    assert(cacheMap.size() == 4)

    // check with null values and constant expressions
    query1 = s"select avg(arrDelay) from $colTableName where uniqueCarrier in " +
        "('YV', 'AQ', 'VX', null, 'US', 'EV', 'MQ', concat('D', 'L'), 'OO', 'XE', 'NW', 'UA')"
    query2 = s"select avg(arrDelay) from $colTableName where uniqueCarrier in " +
        "('MQ', 'DL', 'OO', null, 'NW', 'UA', 'F9', concat('B', '6'), 'AA', 'FL', 'HA', 'AS')"

    res1 = spark.sql(query1).collect()
    res2 = spark.sql(query2).collect()
    df1 = snc.sql(query1)
    df2 = snc.sql(query2)

    checkAnswer(df1, res1)
    checkAnswer(df2, res2)
    checkDynInSet(df1)
    checkDynInSet(df2)

    assert(cacheMap.size() == 5)

    // check for non-deterministic expressions

    // RAND should not be cached since seed will be different each time
    query1 = s"select avg(arrDelay) from $colTableName where uniqueCarrier in " +
        "('YV', 'AQ', 'VX', null, 'US', 'EV', 'MQ', cast(rand() as string), 'OO', 'XE', 'NW')"
    query2 = s"select avg(arrDelay) from $colTableName where uniqueCarrier in " +
        "('MQ', 'DL', 'OO', null, 'NW', 'UA', 'F9', cast(rand() as string), 'AA', 'FL', 'HA')"

    res1 = spark.sql(query1).collect()
    res2 = spark.sql(query2).collect()
    df1 = snc.sql(query1)
    df2 = snc.sql(query2)

    checkAnswer(df1, res1)
    checkAnswer(df2, res2)
    checkDynInSet(df1, present = false)
    checkDynInSet(df2, present = false)

    assert(cacheMap.size() == 5)

    // spark_partition_id() should be cached
    query1 = s"select avg(arrDelay) from $colTableName where uniqueCarrier in " +
        "('YV', 'AQ', 'VX', null, 'US', 'EV', 'MQ', cast(spark_partition_id() as string), 'OO')"
    query2 = s"select avg(arrDelay) from $colTableName where uniqueCarrier in " +
        "('MQ', 'DL', 'OO', null, 'NW', 'UA', 'F9', cast(spark_partition_id() as string), 'AA')"

    res1 = spark.sql(query1).collect()
    res2 = spark.sql(query2).collect()
    df1 = snc.sql(query1)
    df2 = snc.sql(query2)

    checkAnswer(df1, res1)
    checkAnswer(df2, res2)
    checkDynInSet(df1, present = false)
    checkDynInSet(df2, present = false)

    assert(cacheMap.size() == 6)

    // also check partial delete followed by a full delete
    val count = snc.table(colTableName).count()

    // null, non-null combinations of updates

    // implicit int to string cast will cause it to be null (SNAP-2039)
    res2 = snc.sql(s"update $colTableName set DEST = DEST + 1000 where " +
        "depdelay = 0 and arrdelay > 0 and airtime > 350").collect()
    val numUpdated0 = res2.foldLeft(0L)(_ + _.getLong(0))
    assert(numUpdated0 > 0)
    assert(snc.sql(s"select * from $colTableName where depdelay = 0 and arrdelay > 0 " +
        "and airtime > 350 and dest is not null").collect().length === 0)

    // check null updates
    res2 = snc.sql(s"update $colTableName set DEST = null where " +
        "depdelay = 0 and arrdelay > 0 and airtime > 350").collect()
    val numUpdated1 = res2.foldLeft(0L)(_ + _.getLong(0))
    assert(numUpdated1 > 0)
    assert(snc.sql(s"select * from $colTableName where depdelay = 0 and arrdelay > 0 " +
        "and airtime > 350 and dest is not null").collect().length === 0)

    // check normal non-null updates
    res2 = snc.sql(s"update $colTableName set DEST = 'DEST__UPDATED' where " +
        "depdelay = 0 and arrdelay > 0 and airtime > 350").collect()
    val numUpdated2 = res2.foldLeft(0L)(_ + _.getLong(0))
    assert(numUpdated1 === numUpdated2)
    assert(snc.sql(s"select * from $colTableName where depdelay = 0 and arrdelay > 0 " +
        "and airtime > 350 and dest not like '%__UPDATED'").collect().length === 0)

    // new overlapping null updates
    res2 = snc.sql(s"update $colTableName set DEST = null where " +
        "depdelay = 0 and arrdelay > 0 and airtime > 250").collect()
    val numUpdated3 = res2.foldLeft(0L)(_ + _.getLong(0))
    assert(numUpdated3 > numUpdated1)
    assert(snc.sql(s"select * from $colTableName where depdelay = 0 and arrdelay > 0 " +
        "and airtime > 250 and dest is not null").collect().length === 0)

    // new overlapping non-null updates
    res2 = snc.sql(s"update $colTableName set DEST = concat(DEST, '__UPDATED') where " +
        "depdelay = 0 and arrdelay > 0 and airtime > 100").collect()
    val numUpdated4 = res2.foldLeft(0L)(_ + _.getLong(0))
    assert(numUpdated4 > numUpdated3)
    assert(snc.sql(s"select * from $colTableName where depdelay = 0 and arrdelay > 0 " +
        s"and airtime > 250 and dest is not null").collect().length === 0)
    assert(snc.sql(s"select * from $colTableName where depdelay = 0 and arrdelay > 0 " +
        "and airtime > 100 and airtime <= 250 and dest not like '%__UPDATED'")
        .collect().length === 0)

    val del1 = snc.sql(s"delete from $colTableName where depdelay = 0 and arrdelay > 0")
    val delCount1 = del1.collect().foldLeft(0L)(_ + _.getLong(0))
    assert(delCount1 > 0)
    val del2 = snc.sql(s"delete from $colTableName")
    val delCount2 = del2.collect().foldLeft(0L)(_ + _.getLong(0))
    assert(delCount2 > 0)
    assert(delCount1 + delCount2 === count)
    assert(snc.table(colTableName).count() === 0)
    assert(snc.table(colTableName).collect().length === 0)
    assert(snc.sql(s"select * from $colTableName").collect().length === 0)

    snc.conf.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD,
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.defaultValue.get)

    snc.dropTable(colTableName)
  }

  test("Test BUG SNAP-1642") {
    val maxquery = s"select * from $table where a = (select max(a) from $table)"
    val numRows = 10
    createSimpleTableAndPoupulateData(numRows, s"$table")

    val rs1 = snc.sql(maxquery)
    val rows1 = rs1.collect()

    val data = ((11 to 12), (11 to 12), (11 to 12)).zipped.toArray
    val rdd = sc.parallelize(data, data.length)
        .map(s => Data(s._1, s._2, s._3))
    val dataDF = snc.createDataFrame(rdd)
    dataDF.write.mode(SaveMode.Append).saveAsTable(table)

    val rs2 = snc.sql(maxquery)
    val rows2 = rs2.collect()

    var uncachedResult = snc.sqlUncached(maxquery).collect()
    assert(rows2.sameElements(uncachedResult))
  }

  test("Test CachedDataFrame.head ") {
    val tableName = "test.table1"
    snc.sql(s"CREATE TABLE $tableName (Col1 INT, Col2 INT, Col3 INT) USING column ")
    assert(snc.sql("SELECT * FROM " + tableName).collect().length == 0)
    snc.sql(s" insert into $tableName values ( 1, 2, 3)")
    snc.sql(s" insert into $tableName values ( 2, 2, 3)")
    snc.sql(s" insert into $tableName values ( 3, 2, 3)")
    val df = snc.sql(s"SELECT col1 FROM $tableName")
    val row = df.head()
    assert(row.getInt(0) == 1)
    val rowArray = df.head(2)
    assert(rowArray(0).getInt(0) == 1)
    assert(rowArray(1).getInt(0) == 2)
    assert(rowArray.length == 2)
    snc.sql(s"DROP TABLE $tableName")
  }

  private def normalizeRow(rows: Array[Row]): Array[String] = {
    val newBuffer: ArrayBuffer[String] = new ArrayBuffer
    val sb = new StringBuilder
    rows.foreach(r => {
      r.toSeq.foreach {
        case d: Double =>
          // round to one decimal digit
          sb.append(math.floor(d * 5.0 + 0.25) / 5.0).append(',')
        case bd: java.math.BigDecimal =>
          sb.append(bd.setScale(2, java.math.RoundingMode.HALF_UP)).append(',')
        case v => sb.append(v).append(',')
      }
      newBuffer += sb.toString()
      sb.clear()
    })
    newBuffer.sortWith(_ < _).toArray
  }
}
