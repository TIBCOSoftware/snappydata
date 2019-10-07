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

package io.snappydata

import java.io.File

import scala.collection.JavaConverters._

import com.pivotal.gemfirexd.TestUtil

import org.apache.spark.sql.execution.benchmark.ColumnCacheBenchmark
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchange}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{AnalysisException, Row, SnappyContext, SnappySession, SparkSession}

class QueryTest extends SnappyFunSuite {

  test("Test exists in select") {
    val snContext = SnappyContext(sc)

    snContext.sql("CREATE TABLE titles(title_id varchar(20), title varchar(80) " +
        "not null, type varchar(12) not null, pub_id varchar(4), price int not null, " +
        "advance int not null , royalty int , ytd_sales int,notes varchar(200))")

    snContext.sql("insert into titles values ('1', 'Secrets', " +
        "'popular_comp', '1389', 20, 8000, 10, 4095,'Note 1')")
    snContext.sql("insert into titles values ('2', 'The', " +
        "'business',     '1389', 19, 5000, 10, 4095,'Note 2')")
    snContext.sql("insert into titles values ('3', 'Emotional', " +
        "'psychology',   '0736', 7,  4000, 10, 3336,'Note 3')")
    snContext.sql("insert into titles values ('4', 'Prolonged', " +
        "'psychology',   '0736', 19, 2000, 10, 4072,'Note 4')")
    snContext.sql("insert into titles values ('5', 'With', " +
        "'business',     '1389', 11, 5000, 10, 3876,'Note 5')")
    snContext.sql("insert into titles values ('6', 'Valley', " +
        "'mod_cook',     '0877', 9,  0,    12, 2032,'Note 6')")
    snContext.sql("insert into titles values ('7', 'Any?', " +
        "'trad_cook',    '0877', 14, 8000, 10, 4095,'Note 7')")
    snContext.sql("insert into titles values ('8', 'Fifty', " +
        "'trad_cook',    '0877', 11, 4000, 14, 1509,'Note 8')")

    snContext.sql("CREATE TABLE sales(stor_id varchar(4) not null, " +
        "ord_num varchar(20) not null, qty int not null, " +
        "payterms varchar(12) not null,title_id varchar(80))")

    snContext.sql("insert into sales values('1', 'QA7442.3',  75, 'ON Billing','1')")
    snContext.sql("insert into sales values('2', 'D4482',     10, 'Net 60',    '1')")
    snContext.sql("insert into sales values('3', 'N914008',   20, 'Net 30',    '2')")
    snContext.sql("insert into sales values('4', 'N914014',   25, 'Net 30',    '3')")
    snContext.sql("insert into sales values('5', '423LL922',  15, 'ON Billing','3')")
    snContext.sql("insert into sales values('6', '423LL930',  10, 'ON Billing','2')")

    val df = snContext.sql("SELECT  title, price FROM titles WHERE EXISTS (" +
        "SELECT * FROM sales WHERE sales.title_id = titles.title_id AND qty >30)")
    df.collect()
  }

  test("SNAP-1159_1482") {
    val session = SnappyContext(sc).snappySession
    session.sql(s"set ${Property.ColumnBatchSize.name}=100")
    session.sql(s"set ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key}=1")
    val data1 = session.range(20).selectExpr("id")
    val data2 = session.range(80).selectExpr("id", "cast ((id / 4) as long) as k",
      "(case when (id % 4) < 2 then cast((id % 4) as long) else null end) as v")
    data1.write.format("column").saveAsTable("t1")
    data2.write.format("column").saveAsTable("t2")

    // SNAP-1482: check for engineering format numeric values
    var r = session.sql("select 2.1e-2").collect()
    assert(r(0).getDouble(0) == 0.021)
    r = session.sql("select 2.1e+2").collect()
    assert(r(0).getDouble(0) == 210)
    r = session.sql("select 2.1e2").collect()
    assert(r(0).getDouble(0) == 210)

    SparkSession.clearActiveSession()
    val spark = SparkSession.builder().getOrCreate()
    val sdata1 = spark.range(20).selectExpr("id")
    val sdata2 = spark.createDataFrame(data2.collect().toSeq.asJava, data2.schema)
    sdata1.createOrReplaceTempView("t1")
    sdata2.createOrReplaceTempView("t2")

    val query = "select k, v from t1 inner join t2 where t1.id = t2.k order by k, v"
    val df = session.sql(query)
    val result1 = df.collect().mkString(" ")
    val result2 = spark.sql(query).collect().mkString(" ")
    if (result1 != result2) {
      fail(s"Expected result: $result2\nGot: $result1")
    }
  }

  /**
   * Distinct query failure in code generation reported on github
   * (https://github.com/SnappyDataInc/snappydata/issues/534)
   */
  test("GITHUB-534") {
    val session = SnappyContext(sc).snappySession
    session.sql("CREATE TABLE yes_with(device_id VARCHAR(200), " +
        "sdk_version VARCHAR(200)) USING COLUMN OPTIONS(PARTITION_BY 'device_id')")
    session.insert("yes_with", Row("id1", "v1"), Row("id1", "v2"),
      Row("id2", "v1"), Row("id2", "v1"), Row("id2", "v3"))
    val r = session.sql("select sdk_version, count(distinct device_id) from (" +
        "select sdk_version,device_id from YES_WITH group by sdk_version, " +
        "device_id) a group by sdk_version")
    ColumnCacheBenchmark.collect(r,
      Seq(Row("v1", 2), Row("v2", 1), Row("v3", 1)))
  }

  test("SNAP-1714") {
    val snc = new SnappySession(this.sc)
    snc.sql("CREATE TABLE ColumnTable(\"a/b\" INT ,Col2 INT, Col3 INT) " +
        "USING column " +
        "options " +
        "(" +
        "PARTITION_BY 'col2'," +
        "BUCKETS '1')")
    snc.sql("insert into ColumnTable(\"a/b\",col2,col3) values(1,2,3)")
    snc.sql("select col2,col3 from columnTable").collect()
    snc.sql("select col2, col3, `a/b` from columnTable").collect()
    snc.sql("select col2, col3, \"a/b\" from columnTable").collect()
    snc.sql("select col2, col3, \"A/B\" from columnTable").collect()
    snc.sql("select col2, col3, `A/B` from columnTable").collect()

    snc.sql("select col2,col3 from columnTable").collect()
    snc.table("columnTable").select("col3", "col2", "a/b").collect()
    snc.table("columnTable").select("col3", "Col2", "A/b").collect()
    snc.table("columnTable").select("COL3", "Col2", "A/B").collect()
    snc.table("columnTable").select("COL3", "Col2", "`A/B`").collect()
    snc.table("columnTable").select("COL3", "Col2", "`a/b`").collect()

    snc.conf.set("spark.sql.caseSensitive", "true")
    try {
      snc.table("columnTable").select("col3", "col2", "A/b").collect()
      fail("expected to fail for case-sensitive=true")
    } catch {
      case _: AnalysisException => // expected
    }
    try {
      snc.table("columnTable").select("COL3", "COL2", "A/B").collect()
      fail("expected to fail for case-sensitive=true")
    } catch {
      case _: AnalysisException => // expected
    }
    try {
      snc.sql("select col2, col3, \"A/B\" from columnTable").collect()
      fail("expected to fail for case-sensitive=true")
    } catch {
      case _: AnalysisException => // expected
    }
    try {
      snc.sql("select COL2, COL3, `A/B` from columnTable").collect()
      fail("expected to fail for case-sensitive=true")
    } catch {
      case _: AnalysisException => // expected
    }
    // hive meta-store is case-insensitive so column table names are not
    snc.sql("select col2, col3, \"a/b\" from columnTable").collect()
    snc.sql("select col2, col3, `a/b` from ColumnTable").collect()
    snc.table("columnTable").select("col3", "col2", "a/b").collect()
    snc.table("COLUMNTABLE").select("col3", "col2", "a/b").collect()

    snc.conf.set("spark.sql.caseSensitive", "false")
  }

  private def setupTestData(session: SnappySession): Unit = {
    import session.implicits._

    val row = identity[(java.lang.Integer, java.lang.Double)] _

    val l = Seq(
      row(1, 2.0),
      row(1, 2.0),
      row(2, 1.0),
      row(2, 1.0),
      row(3, 3.0),
      row(null, null),
      row(null, 5.0),
      row(6, null)).toDF("a", "b")

    val r = Seq(
      row(2, 3.0),
      row(2, 3.0),
      row(3, 2.0),
      row(4, 1.0),
      row(null, null),
      row(null, 5.0),
      row(6, null)).toDF("c", "d")

    val t = r.filter($"c".isNotNull && $"d".isNotNull)

    l.createOrReplaceTempView("l")
    r.createOrReplaceTempView("r")
    t.createOrReplaceTempView("t")
  }

  test("SNAP-1886_1888") {
    val session = this.snc.snappySession
    import session.implicits._

    setupTestData(session)

    session.dropTable("t1", ifExists = true)
    session.dropTable("t2", ifExists = true)
    session.dropTable("onerow", ifExists = true)

    Seq(1, 2).toDF("c1").write.format("column").saveAsTable("t1")
    Seq(1).toDF("c2").write.format("column").saveAsTable("t2")
    Seq(1).toDF("c1").write.format("column").saveAsTable("onerow")

    // SNAP-1886
    checkAnswer(
      session.sql(
        """
          | select c1 from onerow t1
          | where exists (select 1
          |               from   (select 1 from onerow t2 LIMIT 1)
          |               where  t1.c1=t2.c1)""".stripMargin),
      Row(1) :: Nil)

    // SNAP-1888
    checkAnswer(
      session.sql(
        """select l.a from l
          |where (
          |    select cntPlusOne + 1 as cntPlusTwo from (
          |        select cnt + 1 as cntPlusOne from (
          |            select sum(r.c) s, count(*) cnt from r where l.a = r.c having cnt = 0
          |        )
          |    )
          |) = 2""".stripMargin),
      Row(1) :: Row(1) :: Row(null) :: Row(null) :: Nil)
  }

  test("SNAP-2088 check for null handling with dictionary optimized joins and filters") {
    val snc = this.snc
    val t1 = "snap2088"
    val t2 = "snap2088_2"

    snc.sql(s"create table $t1 (airport_id int, name string, city string, country string) " +
        s"using column options (COLUMN_BATCH_SIZE '50')")
    snc.sql(s"create table $t2 (airport_id int, name string, city string, country string) " +
        s"using column options (COLUMN_BATCH_SIZE '5000')")

    val data = snc.range(10000).selectExpr("cast ((rand() * 100000) as int) as airport_id",
      "concat('name_', cast((id % 20) as string)) as name",
      "(case when id%2=0 then null else concat('city_', cast((id%10) as string)) end) as city",
      "concat('country_', cast((id % 3) as string)) as country")
    data.cache()
    data.count()
    data.createOrReplaceTempView("data")
    data.write.insertInto(t1)
    data.write.insertInto(t2)

    // Some queries that either throw exception or give incorrect results as noted in SNAP-2088.
    val queries = Array(
      "select distinct city from $t",
      "select distinct city from $t order by city",
      "select distinct city from $t where country like 'country_1%'",
      "select * from $t where city is null",
      "select * from $t where city is null and country like 'country_1%'",
      "select count(*), city from $t group by city",
      "select count(*), city from $t where country like 'country_1%' group by city",
      "select count(*), city, collect_list(airport_id), collect_list(name), " +
          "collect_list(country) from (select * from $t order by airport_id, name, country) " +
          "as t group by city order by city",
      "select count(*), city, collect_list(airport_id), collect_list(name), " +
          "collect_list(country) from (select * from $t where country like 'country_1%' " +
          "  order by airport_id, name, country) as t group by city order by city"
    )

    // To validate the results against queries directly on data disabling snappy aggregation.
    snc.sql("set snappydata.sql.hashAggregateSize=-1")
    val expectedResults = queries.map(q => snc.sql(q.replace("$t", "data")).collect())

    snc.sql("set snappydata.sql.hashAggregateSize=0")
    var results = queries.map { q =>
      snc.sql(q.replace("$t", t1)) -> snc.sql(q.replace("$t", t2))
    }

    for (((r1, r2), e) <- results.zip(expectedResults)) {
      checkAnswer(r1, e)
      checkAnswer(r2, e)
    }

    // fire updates and check again
    snc.sql(s"update $t1 set airport_id = airport_id, name = name, city = city, " +
        s"country = country where (airport_id % 3) = 0")
    snc.sql(s"update $t2 set airport_id = airport_id, name = name, city = city, " +
        s"country = country where (airport_id % 2) = 0")

    results = queries.map { q =>
      snc.sql(q.replace("$t", t1)) -> snc.sql(q.replace("$t", t2))
    }

    for (((r1, r2), e) <- results.zip(expectedResults)) {
      checkAnswer(r1, e)
      checkAnswer(r2, e)
    }
  }

  test("SNAP-2080 alter table add column and then index on that") {
    val snc = this.snc
    snc.sql(s"CREATE TABLE APP.TEST ( COL1 VARCHAR(36) NOT NULL ) using row options()")
    snc.sql(s"ALTER TABLE APP.TEST ADD COLUMN COL2 VARCHAR(36)")
    snc.sql(s"create index APP.X_TEST_COL1 on APP.TEST (col1)")
    snc.sql(s"create index APP.X_TEST_COL2 on APP.TEST (col2)")
    snc.sql(s"ALTER TABLE APP.TEST ADD COLUMN COL3 CHAR(4)")
    snc.sql(s"create index APP.X_TEST_COL3 on APP.TEST (col3)")
    snc.sql(s"insert into TEST values ('one', 'vone', 'cone'), ('two', 'vtwo', 'ctwo')")
    val r = snc.sql(s"select count(*) from TEST").collect()
    assert(1 === r.length)
    assert(2 === r.head.get(0))
    snc.sql(s"ALTER TABLE APP.TEST ADD COLUMN COL5 blob")
  }

  /** check exchange and broadcast plan reuse for row, column and parquet */
  test("SNAP-2789: check broadcast/exchange reuse") {
    val session = this.snc.snappySession

    val query = "select count(t1.data), count(*) from test1 t1 join test2 t2 on (t1.id = t2.id) " +
        "union all " +
        "select count(*), count(t1.data) from test1 t1 join test2 t2 on (t1.id = t2.id)"
    for (tableType <- Seq("column", "row", "parquet")) {
      val (declaration, options) = if (tableType == "parquet") {
        "external " -> ((table: String) => s"options (path '${table}_pq')")
      } else "" -> ((_: String) => "options (partition_by 'data')")

      def tableDeclaration(table: String, size: Int): String = {
        s"create ${declaration}table $table using $tableType ${options(table)} as " +
            s"select id, case when id % 100 = 0 then null else 'data' || id end as data " +
            s"from range($size)"
      }

      session.sql(tableDeclaration("test1", 50000))
      session.sql(tableDeclaration("test2", 20000))

      // with exchange
      session.sessionState.conf.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD, -1L)
      var df = session.sql(query)
      var plan = df.queryExecution.executedPlan
      // exactly one exchange of test1 and test2 is expected
      val exchanges = plan.collect {
        case e: ShuffleExchange if e.outputPartitioning.numPartitions > 1 => e
      }
      assert(exchanges.length === 2)
      assert(exchanges.head.treeString.toLowerCase.contains("test1"))
      assert(exchanges(1).treeString.toLowerCase.contains("test2"))

      var result = df.collect()
      assert(result.length === 2)
      assert(result(0).getLong(0) === 19800)
      assert(result(1).getLong(0) === 20000)

      // with broadcast
      session.sessionState.conf.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD,
        SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.defaultValue.get)
      df = session.sql(query)
      plan = df.queryExecution.executedPlan
      // exactly one broadcast of test1 or test2 is expected
      val broadcasts = plan.collect {
        case e: BroadcastExchangeExec => e
      }
      assert(broadcasts.length === 1)
      // both sides are small enough to be broadcast
      val broadcastString = broadcasts.head.treeString.toLowerCase
      assert(broadcastString.contains("test2") || broadcastString.contains("test1"))
      result = df.collect()
      assert(result.length === 2)
      assert(result(0).getLong(0) === 19800)
      assert(result(1).getLong(0) === 20000)

      session.sql("drop table test1")
      session.sql("drop table test2")
    }
    // delete the directories created for parquet
    TestUtil.deleteDir(new File("test1_pq"))
    TestUtil.deleteDir(new File("test2_pq"))
  }
}
