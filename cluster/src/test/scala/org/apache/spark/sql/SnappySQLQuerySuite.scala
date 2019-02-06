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
/*
 * Test for SPARK-10316 taken from Spark's DataFrameSuite having license as below.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import io.snappydata.Property.PlanCaching
import io.snappydata.{Property, SnappyFunSuite}
import org.scalatest.Matchers._

import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, SnappyHashAggregateExec}
import org.apache.spark.sql.execution.benchmark.ColumnCacheBenchmark
import org.apache.spark.sql.execution.joins.HashJoinExec
import org.apache.spark.sql.functions.{bround, rand, round}
import org.apache.spark.sql.test.SQLTestData.TestData2

class SnappySQLQuerySuite extends SnappyFunSuite {

  private lazy val session: SnappySession = snc.snappySession
  private val idPattern = "(,[0-9]+)?#[0-9L]+".r

  // Ported test from Spark
  test("SNAP-1885 : left semi greater than predicate and equal operator") {
    val df = snc.createDataFrame(snc.sparkContext.parallelize(
          TestData2(1, 1) ::
          TestData2(1, 2) ::
          TestData2(2, 1) ::
          TestData2(2, 2) ::
          TestData2(3, 1) ::
          TestData2(3, 2) :: Nil, 2))
    df.write.format("row").saveAsTable("testData2")

    checkAnswer(
      snc.sql("SELECT * FROM testData2 x LEFT SEMI JOIN testData2 y " +
          "ON x.b = y.b and x.a >= y.a + 2"),
      Seq(Row(3, 1), Row(3, 2))
    )

    checkAnswer(
      snc.sql("SELECT * FROM testData2 x LEFT SEMI JOIN testData2 y " +
          "ON x.b = y.a and x.a >= y.b + 1"),
      Seq(Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2))
    )
  }

  test("SNAP-1884 Join with temporary table not returning rows") {
    val df = snc.createDataFrame(snc.sparkContext.parallelize(
      LowerCaseData(1, "a") ::
        LowerCaseData(2, "b") ::
        LowerCaseData(3, "c") ::
        LowerCaseData(4, "d") :: Nil))
    df.write.format("row").saveAsTable("lowerCaseData")
    snc.sql("SELECT DISTINCT n FROM lowerCaseData ORDER BY n DESC")
      .limit(2)
      .createOrReplaceTempView("subset1")
    snc.sql("SELECT DISTINCT n FROM lowerCaseData ORDER BY n ASC")
      .limit(2)
      .createOrReplaceTempView("subset2")
    checkAnswer(
      snc.sql("SELECT * FROM lowerCaseData INNER JOIN subset1 ON " +
        "subset1.n = lowerCaseData.n ORDER BY lowerCaseData.n"),
      Row(3, "c", 3) ::
        Row(4, "d", 4) :: Nil)

    checkAnswer(
      snc.sql("SELECT * FROM lowerCaseData INNER JOIN subset2 " +
        "ON subset2.n = lowerCaseData.n ORDER BY lowerCaseData.n"),
      Row(1, "a", 1) ::
        Row(2, "b", 2) :: Nil)

    snc.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    df.write.format("column").saveAsTable("collowerCaseData")

    snc.sql("SELECT DISTINCT n FROM collowerCaseData ORDER BY n DESC")
      .limit(2)
      .createOrReplaceTempView("colsubset1")
    snc.sql("SELECT DISTINCT n FROM collowerCaseData ORDER BY n ASC")
      .limit(2)
      .createOrReplaceTempView("colsubset2")
    checkAnswer(
      snc.sql("SELECT * FROM collowerCaseData INNER JOIN colsubset1 ON " +
        "colsubset1.n = collowerCaseData.n ORDER BY collowerCaseData.n"),
      Row(3, "c", 3) ::
        Row(4, "d", 4) :: Nil)

    checkAnswer(
      snc.sql("SELECT * FROM collowerCaseData INNER JOIN colsubset2 " +
        "ON colsubset2.n = collowerCaseData.n ORDER BY collowerCaseData.n"),
      Row(1, "a", 1) ::
        Row(2, "b", 2) :: Nil)
  }

  import session.implicits._

  test("SNAP-1840 -> uncorrelated scalar subquery") {

    val df = Seq((1, "one"), (2, "two"), (3, "three")).toDF("key", "value")
    df.write.format("row").saveAsTable("subqueryData")

    checkAnswer(
      session.sql("select -(select max(key) from subqueryData)"),
      Array(Row(-3))
    )

    checkAnswer(
      session.sql("select (select key from subqueryData where key > 2 order by key limit 1) + 1"),
      Array(Row(4))
    )

    checkAnswer(
      session.sql("select (select value from subqueryData limit 0)"),
      Array(Row(null))
    )

    checkAnswer(
      session.sql("select (select min(value) from subqueryData" +
          " where key = (select max(key) from subqueryData) - 1)"),
      Array(Row("two"))
    )
    session.dropTable("subqueryData", ifExists = true)
  }

  test("NOT EXISTS predicate subquery") {
    val row = identity[(java.lang.Integer, java.lang.Double)] _

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

    checkAnswer(
      session.sql("select * from l where not exists (select * from r where l.a = r.c)"),
      Row(1, 2.0) :: Row(1, 2.0) :: Row(null, null) :: Row(null, 5.0) :: Nil)
    checkAnswer(
      session.sql("select * from l where not exists " +
          "(select * from r where l.a = r.c and l.b < r.d)"),
      Row(1, 2.0) :: Row(1, 2.0) :: Row(3, 3.0) ::
          Row(null, null) :: Row(null, 5.0) :: Row(6, null) :: Nil)

    session.dropTable("l", ifExists = true)
    session.dropTable("r", ifExists = true)
  }

  // taken from same test in Spark's DataFrameSuite
  test("SPARK-10316: allow non-deterministic expressions to project in PhysicalScan") {
    session.sql("create table rowTable (id long, id2 long) using row")
    session.range(1, 11).select($"id", $"id" * 2).write.insertInto("rowTable")
    val input = session.table("rowTable")

    val df = input.select($"id", rand(0).as('r))
    val result = df.as("a").join(df.filter($"r" < 0.5).as("b"), $"a.id" === $"b.id").collect()
    result.foreach { row =>
      assert(row.getDouble(1) - row.getDouble(3) === 0.0 +- 0.001)
    }
    session.sql("drop table rowTable")
  }

  test("AQP-292 snappy plan generation failure for aggregation on group by column") {
    session.sql("create table testTable (id long, tag string) using column options (buckets '2')")
    session.range(100000).selectExpr(
      "id", "concat('tag', cast ((id >> 6) as string)) as tag").write.insertInto("testTable")
    val query = "select tag, count(tag) c from testTable group by tag order by c desc"
    val rs = session.sql(query)
    // snappy aggregation should have been used for this query
    val plan = rs.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[SnappyHashAggregateExec]).isDefined)
    assert(plan.find(_.isInstanceOf[HashAggregateExec]).isEmpty)
    // collect the result to force default hashAggregateSize property take effect
    implicit val encoder: ExpressionEncoder[Row] = RowEncoder(rs.schema)
    val result = session.createDataset(rs.collect().toSeq)
    session.sql(s"set ${Property.HashAggregateSize} = -1")
    try {
      val ds = session.sql(query)
      val plan = ds.queryExecution.executedPlan
      assert(plan.find(_.isInstanceOf[SnappyHashAggregateExec]).isEmpty)
      assert(plan.find(_.isInstanceOf[HashAggregateExec]).isDefined)
      checkAnswer(result, ds.collect())
    } finally {
      session.sql(s"set ${Property.HashAggregateSize} = 0")
    }
  }

  private def getUpdateCount(df: DataFrame, tableType: String): Long = {
    // row table execution without keys is done directly on store that returns integer counts
    if (tableType == "row") df.collect().map(_.getInt(0)).sum
    else df.collect().map(_.getLong(0)).sum
  }

  test("Double exists and update exists sub-query") {
    val snc = new SnappySession(sc)
    for (tableType <- Seq("row", "column")) {
      snc.sql("create table r1(col1 INT, col2 STRING, col3 String, col4 Int)" +
          s" using $tableType")
      snc.sql("create table r2(col1 INT, col2 STRING, col3 String, col4 Int)" +
          s" using $tableType")
      snc.sql("create table r3(col1 INT, col2 STRING, col3 String, col4 Int)" +
          s" using $tableType")

      snc.insert("r1", Row(1, "1", "1", 100))
      snc.insert("r1", Row(2, "2", "2", 2))
      snc.insert("r1", Row(4, "4", "4", 4))
      snc.insert("r1", Row(7, "7", "7", 4))

      snc.insert("r2", Row(1, "1", "1", 1))
      snc.insert("r2", Row(2, "2", "2", 2))
      snc.insert("r2", Row(3, "3", "3", 3))

      snc.insert("r3", Row(1, "1", "1", 1))
      snc.insert("r3", Row(2, "2", "2", 2))
      snc.insert("r3", Row(4, "4", "4", 4))

      val df = snc.sql("select * from r1 where " +
          "(exists (select col1 from r2 where r2.col1=r1.col1) " +
          "or exists(select col1 from r3 where r3.col1=r1.col1))")

      df.collect()
      checkAnswer(df, Seq(Row(1, "1", "1", 100),
        Row(2, "2", "2", 2), Row(4, "4", "4", 4)))

      var updateSql = "update r1 set col1 = 100 where exists " +
          s"(select 1 from r1 t where t.col1 = r1.col1 and t.col1 = 4)"
      assert(getUpdateCount(snc.sql(updateSql), tableType) == 1)
      assert(getUpdateCount(snc.sql(updateSql), tableType) == 0)

      updateSql = "update r1 set col1 = 200 where exists " +
          s"(select 1 from r2 t where t.col1 = r1.col1 and t.col1 = 2)"
      assert(getUpdateCount(snc.sql(updateSql), tableType) == 1)
      assert(getUpdateCount(snc.sql(updateSql), tableType) == 0)

      checkAnswer(snc.sql("select * from r1"), Seq(Row(1, "1", "1", 100),
        Row(200, "2", "2", 2), Row(100, "4", "4", 4), Row(7, "7", "7", 4)))

      snc.sql("drop table r1")
      snc.sql("drop table r2")
      snc.sql("drop table r3")
    }
  }

  test("SNAP-2387") {
    val numRows = 400000
    val snappy = this.snc.snappySession
    val spark = new SparkSession(snappy.sparkContext)
    var ds = snappy.range(numRows).selectExpr(
      "id as fare_amount", "(rand() * 1000.0) as tip_amount")
    ds.createOrReplaceTempView("taxi_trip_fare")
    ds.cache()
    assert(ds.count() === numRows)
    ds = spark.internalCreateDataFrame(ds.queryExecution.toRdd, ds.schema)
    ds.createOrReplaceTempView("taxi_trip_fare")
    ds.cache()
    assert(ds.count() === numRows)

    val q1 = "SELECT (ROUND( (tip_amount / fare_amount) * 100)) tip_pct " +
        "FROM taxi_trip_fare WHERE fare_amount > 0.00 and tip_amount < 50 " +
        "GROUP BY (ROUND( tip_amount / fare_amount * 100 ))"
    val q2 = "SELECT (ROUND( (tip_amount / fare_amount) * (90 + 10))) tip_pct " +
        "FROM taxi_trip_fare WHERE fare_amount > 0.00 and tip_amount < 50 " +
        "GROUP BY (ROUND( tip_amount / fare_amount * (90 + 10)))"
    val q3 = "SELECT (ROUND((tip_amount / fare_amount) * 100)) tip_pct " +
        "FROM taxi_trip_fare WHERE fare_amount > 0.00 and tip_amount < 50 " +
        "GROUP BY (ROUND(tip_amount / fare_amount * 100)) " +
        "ORDER BY (ROUND(tip_amount / fare_amount * 100)) limit 30"
    val q4 = "SELECT (ROUND((tip_amount / fare_amount) * (90 + 10))) tip_pct " +
        "FROM taxi_trip_fare WHERE fare_amount > 0.00 and tip_amount < 50 " +
        "GROUP BY (ROUND(tip_amount / fare_amount * (90 + 10))) " +
        "ORDER BY (ROUND(tip_amount / fare_amount * (90 + 10))) limit 10"
    ColumnCacheBenchmark.collect(snappy.sql(q1), spark.sql(q1).collect())
    ColumnCacheBenchmark.collect(snappy.sql(q2), spark.sql(q2).collect())
    ColumnCacheBenchmark.collect(snappy.sql(q3), spark.sql(q3).collect())
    ColumnCacheBenchmark.collect(snappy.sql(q4), spark.sql(q4).collect())

    // check with different values of constants
    val q5 = "SELECT (ROUND( (tip_amount / fare_amount) * 99)) tip_pct " +
        "FROM taxi_trip_fare WHERE fare_amount > 0.00 and tip_amount < 40 " +
        "GROUP BY (ROUND( tip_amount / fare_amount * 99 ))"
    val q6 = "SELECT (ROUND( (tip_amount / fare_amount) * (40 + 68))) tip_pct " +
        "FROM taxi_trip_fare WHERE fare_amount > 0.00 and tip_amount < 55 " +
        "GROUP BY (ROUND( tip_amount / fare_amount * (40 + 68)))"
    val q7 = "SELECT (ROUND((tip_amount / fare_amount) * 98)) tip_pct " +
        "FROM taxi_trip_fare WHERE fare_amount > 0.00 and tip_amount < 60 " +
        "GROUP BY (ROUND(tip_amount / fare_amount * 98)) " +
        "ORDER BY (ROUND(tip_amount / fare_amount * 98)) limit 30"
    val q8 = "SELECT (ROUND((tip_amount / fare_amount) * (32 + 60))) tip_pct " +
        "FROM taxi_trip_fare WHERE fare_amount > 0.00 and tip_amount < 45 " +
        "GROUP BY (ROUND(tip_amount / fare_amount * (32 + 60))) " +
        "ORDER BY (ROUND(tip_amount / fare_amount * (32 + 60))) limit 10"
    ColumnCacheBenchmark.collect(snappy.sql(q5), spark.sql(q5).collect())
    ColumnCacheBenchmark.collect(snappy.sql(q6), spark.sql(q6).collect())
    ColumnCacheBenchmark.collect(snappy.sql(q7), spark.sql(q7).collect())
    ColumnCacheBenchmark.collect(snappy.sql(q8), spark.sql(q8).collect())

    // check error cases
    val q9 = "SELECT (ROUND( (tip_amount / fare_amount) * 108)) tip_pct " +
        "FROM taxi_trip_fare WHERE fare_amount > 0.00 and tip_amount < 55 " +
        "GROUP BY (ROUND( tip_amount / fare_amount * (40 + 68)))"
    val q10 = "SELECT (ROUND((tip_amount / fare_amount) * 98)) tip_pct " +
        "FROM taxi_trip_fare WHERE fare_amount > 0.00 and tip_amount < 60 " +
        "GROUP BY (ROUND(tip_amount / fare_amount * 100)) " +
        "ORDER BY (ROUND(tip_amount / fare_amount * 100)) limit 30"
    val q11 = "SELECT (ROUND((tip_amount / fare_amount) * (40 + 68))) tip_pct " +
        "FROM taxi_trip_fare WHERE fare_amount > 0.00 and tip_amount < 60 " +
        "GROUP BY (ROUND(tip_amount / fare_amount * (40 + 68)) " +
        "ORDER BY (ROUND(tip_amount / fare_amount * 108)) limit 30"
    intercept[AnalysisException](snappy.sql(q9))
    intercept[AnalysisException](snappy.sql(q10))
    intercept[AnalysisException](snappy.sql(q11))
  }

  test("test SNAP-2508") {

    snc.sql("create table asif_test.gl_account (glAccountNumber clob)")
    snc.sql("create table asif_test.gl_account_text (glAccountNumber clob)")

    snc.sql("insert into asif_test.gl_account values('0660130001')")
    snc.sql("insert into asif_test.gl_account_text values('0660130001')")

    snc.sql(s"create table leaf1 (glaccountnumber clob, parentnodeid clob)")
    snc.sql(s"insert into leaf1 values('0660130001', '6010100'), ('ABCDEF', '50000')")

    snc.sql("create table asif_test.node_hierarchy(nodeid clob)")
    snc.sql("insert into asif_test.node_hierarchy values('6010100')")


    snc.sql("create table asif_test.node_object_mapping(nodeid clob, " +
      "glaccountnumber clob, fromvalue clob)")

    snc.sql("insert into asif_test.node_object_mapping values('6010100', '0660130001', '8')")


    snc.sql("CREATE OR REPLACE VIEW suranjan_t2 as SELECT" +
      " a.glaccountnumber" +
      " FROM asif_test.gl_account a " +
      " LEFT OUTER JOIN asif_test.gl_account_text b " +
      " ON a.glAccountNumber = b.glAccountNumber")

    snc.sql("create or replace view suranjan_t as SELECT" +
      " a.nodeid" +
      ", c.glaccountnumber" +
      " FROM asif_test.node_hierarchy a" +
      " LEFT JOIN asif_test.node_object_mapping b" +
      " ON a.nodeid = b.nodeid" +
      " LEFT JOIN suranjan_t2 c" +
      " ON c.glaccountnumber between b.fromvalue and b.fromvalue")


    snc.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    snc.sql("set snappydata.sql.disableHashJoin=false")

    val result = snc.sql("select " +
      " LeafLevel.glaccountnumber" +
      " , HLVL06.nodeid  AS HIER_LEVEL_06_NODE_ID" +
      " FROM leaf1 LeafLevel" +
      " JOIN suranjan_t HLVL06" +
      " On (HLVL06.nodeid = LeafLevel.parentnodeid)")

    // scalastyle:off
    // println(result.queryExecution.optimizedPlan)
    // println(result.queryExecution.executedPlan)
    // scalastyle:on

    // result.show()
    assert(result.count == 1)
  }

  test("IS DISTINCT, IS NOT DISTINCT and <=> expressions") {
    val snappy = this.snc.snappySession
    val df = snappy.sql("select (case when id & 1 = 0 then null else id end) as a, " +
        "(case when id & 2 = 0 then null else id end) b from range(1000)")
    val rs1 = df.selectExpr("a is not distinct from b").collect()
    val rs2 = df.selectExpr(
      "(a is not null AND b is not null AND a = b) OR (a is null AND b is null)").collect()
    assert(rs1.length === 1000)
    assert(rs2.length === 1000)
    assert(rs1 === rs2)
    assert(df.selectExpr("a is not distinct from b").collect() === df.selectExpr(
      "a <=> b").collect())

    assert(df.selectExpr("a is distinct from b").collect() === df.selectExpr(
      "(a is not null AND b is not null AND a <> b) OR (a is null AND b is not null) OR " +
          "(a is not null AND b is null)").collect())
    assert(df.selectExpr("a IS DISTINCT FROM b").collect() === df.selectExpr(
      "NOT (a <=> b)").collect())
  }

  test("Push down TPCH Q19") {
    session.sql("set spark.sql.autoBroadcastJoinThreshold=-1")
    session.sql("set snappydata.sql.planCaching=true").collect()
    val planCaching = PlanCaching.get(snc.sessionState.conf)
    PlanCaching.set(snc.sessionState.conf, true)
    try {
      // this loop exists because initial implementation had a problem
      // in RefParamLiteral.hashCode() that caused it to fail once in 2-3 runs
      for (_ <- 1 to 4) {
        testTPCHQ19()
      }
    } finally {
      session.sql(s"set spark.sql.autoBroadcastJoinThreshold=${10L * 1024 * 1024}")
      PlanCaching.set(snc.sessionState.conf, planCaching)
    }
  }

  private def testTPCHQ19(): Unit = {
    // check common sub-expression elimination in query leading to push down
    // of filters should not be inhibited due to ParamLiterals
    import session.implicits._

    session.sql("create table ct1 (id long, data string) using column")
    session.sql("create table ct2 (id long, data string) using column")
    session.sql("insert into ct1 select id, 'data' || id from range(100000)")
    session.sql("insert into ct2 select id, 'data' || id from range(100000)")

    var ds = session.sql("select ct1.id, ct2.data from ct1 join ct2 on (ct1.id = ct2.id) where " +
        "(ct1.id < 1000 and ct2.data = 'data100') or (ct1.id < 1000 and ct1.data = 'data100')")
    var analyzedFilter = "Filter (((ID#0 < cast(ParamLiteral:0#0,1000 as bigint)) && " +
        "(DATA#0 = ParamLiteral:1#0,data100)) || ((ID#0 < cast(ParamLiteral:2#0,1000 as " +
        "bigint)) && (DATA#0 = ParamLiteral:3#0,data100)))"

    def expectedTree: String =
      s"""Project [ID#0, DATA#0]
         |+- $analyzedFilter
         |   +- Join Inner, (ID#0 = ID#0)
         |      :- SubqueryAlias CT1
         |      :  +- Relation[ID#0,DATA#0] ColumnFormatRelation[APP.CT1]
         |      +- SubqueryAlias CT2
         |         +- Relation[ID#0,DATA#0] ColumnFormatRelation[APP.CT2]
         |""".stripMargin
    assert(idPattern.replaceAllIn(ds.queryExecution.analyzed.treeString, "#0") === expectedTree)
    assert(ds.collect() === Array(Row(100L, "data100")))

    // check filter push down in the plan
    var filters = ds.queryExecution.executedPlan.collect {
      case f: FilterExec => assert(f.child.nodeName === "ColumnTableScan"); f
    }
    assert(filters.length === 2)
    assert(filters.forall(_.toString.contains("<")))
    // check pushed down filters should not be in HashJoin
    var joins = ds.queryExecution.executedPlan.collect {
      case j: HashJoinExec => j
    }
    assert(joins.length === 1)
    assert(joins.head.condition.isDefined)
    var condString = joins.head.condition.get.toString()
    assert(condString.contains("DATA#"))
    assert(!condString.contains("ID#"))

    // similar query but different values in the two positions should lead to a different
    // plan with no filter push down
    ds = session.sql("select ct1.id, ct2.data from ct1 join ct2 on (ct1.id = ct2.id) where " +
        "(ct1.id < 1000 and ct2.data = 'data100') or (ct1.id < 20 and ct1.data = 'data100')")
    analyzedFilter = "Filter (((ID#0 < cast(ParamLiteral:0#0,1000 as bigint)) && " +
        "(DATA#0 = ParamLiteral:1#0,data100)) || ((ID#0 < cast(ParamLiteral:2#0,20 as " +
        "bigint)) && (DATA#0 = ParamLiteral:3#0,data100)))"
    assert(idPattern.replaceAllIn(ds.queryExecution.analyzed.treeString, "#0") === expectedTree)
    assert(ds.collect() === Array(Row(100L, "data100")))

    // check no filter push down in the plan
    filters = ds.queryExecution.executedPlan.collect {
      case f: FilterExec => assert(f.child.nodeName === "ColumnTableScan"); f
    }
    assert(filters.length === 2)
    assert(filters.forall(!_.toString.contains("<")))
    // check all filters should be in HashJoin
    joins = ds.queryExecution.executedPlan.collect {
      case j: HashJoinExec => j
    }
    assert(joins.length === 1)
    assert(joins.head.condition.isDefined)
    condString = joins.head.condition.get.toString()
    assert(condString.contains("DATA#"))
    assert(condString.contains("ID#"))

    ds = session.sql("select ct1.id, ct2.data from ct1 join ct2 on (ct1.id = ct2.id) where " +
        "(ct1.id < 10 and ct2.data = 'data100') or (ct1.id < 10 and ct1.data = 'data100')")
    assert(ds.collect().length === 0)
    // check filter push down in the plan
    filters = ds.queryExecution.executedPlan.collect {
      case f: FilterExec => assert(f.child.nodeName === "ColumnTableScan"); f
    }
    assert(filters.length === 2)
    assert(filters.forall(_.toString.contains("<")))
    ds = session.sql("select ct1.id, ct2.data from ct1 join ct2 on (ct1.id = ct2.id) where " +
        "(ct1.id < 10 and ct2.data = 'data100') or (ct1.id < 20 and ct1.data = 'data100')")
    assert(ds.collect().length === 0)
    // check no filter push down in the plan
    filters = ds.queryExecution.executedPlan.collect {
      case f: FilterExec => assert(f.child.nodeName === "ColumnTableScan"); f
    }
    assert(filters.length === 2)
    assert(filters.forall(!_.toString.contains("<")))

    session.sql("drop table ct1")
    session.sql("drop table ct2")

    // check for some combinations of repeated constants
    val df = Seq(5, 55, 555).map(Tuple1(_)).toDF("a")
    checkAnswer(
      df.select(round('a), round('a, -1), round('a, -2)),
      Seq(Row(5, 10, 0), Row(55, 60, 100), Row(555, 560, 600))
    )
    checkAnswer(
      df.select(bround('a), bround('a, -1), bround('a, -2)),
      Seq(Row(5, 0, 0), Row(55, 60, 100), Row(555, 560, 600))
    )

    val pi = BigDecimal("3.1415")
    checkAnswer(
      session.sql(s"SELECT round($pi, -3), round($pi, -2), round($pi, -1), " +
          s"round($pi, 0), round($pi, 1), round($pi, 2), round($pi, 3)"),
      Seq(Row(BigDecimal("0E3"), BigDecimal("0E2"), BigDecimal("0E1"), BigDecimal(3),
        BigDecimal("3.1"), BigDecimal("3.14"), BigDecimal("3.142")))
    )
    checkAnswer(
      session.sql(s"SELECT bround($pi, -3), bround($pi, -2), bround($pi, -1), " +
          s"bround($pi, 0), bround($pi, 1), bround($pi, 2), bround($pi, 3)"),
      Seq(Row(BigDecimal("0E3"), BigDecimal("0E2"), BigDecimal("0E1"), BigDecimal(3),
        BigDecimal("3.1"), BigDecimal("3.14"), BigDecimal("3.142")))
    )

    // more than 4 constants to replace
    val pi1 = pi + 1
    val pi2 = pi + 2
    val pi3 = pi + 3
    val pi4 = pi + 4
    val pi5 = pi + 5
    val pi6 = pi + 6
    checkAnswer(
      session.sql(s"SELECT round($pi, -3), round($pi6, -2), round($pi, -1), " +
          s"round($pi, 0), round($pi1, 1), round($pi, 2), round($pi2, 3), " +
          s"round($pi3, 1), round($pi1, 2), round($pi4, 2), round($pi, 3), " +
          s"round($pi5, 3), round($pi4, 0), round($pi, 1), round($pi6, 2)"),
      Seq(Row(BigDecimal("0E3"), BigDecimal("0E2"), BigDecimal("0E1"),
        BigDecimal(3), BigDecimal("4.1"), BigDecimal("3.14"), BigDecimal("5.142"),
        BigDecimal("6.1"), BigDecimal("4.14"), BigDecimal("7.14"), BigDecimal("3.142"),
        BigDecimal("8.142"), BigDecimal(7), BigDecimal("3.1"), BigDecimal("9.14")))
    )
    checkAnswer(
      session.sql(s"SELECT bround($pi, -3), bround($pi6, -2), bround($pi, -1), " +
          s"bround($pi, 0), bround($pi1, 1), bround($pi, 2), bround($pi2, 3), " +
          s"bround($pi3, 1), bround($pi1, 2), bround($pi4, 2), bround($pi, 3), " +
          s"bround($pi5, 3), bround($pi4, 0), bround($pi, 1), bround($pi6, 2)"),
      Seq(Row(BigDecimal("0E3"), BigDecimal("0E2"), BigDecimal("0E1"),
        BigDecimal(3), BigDecimal("4.1"), BigDecimal("3.14"), BigDecimal("5.142"),
        BigDecimal("6.1"), BigDecimal("4.14"), BigDecimal("7.14"), BigDecimal("3.142"),
        BigDecimal("8.142"), BigDecimal(7), BigDecimal("3.1"), BigDecimal("9.14")))
    )
  }
}


case class LowerCaseData(n: Int, l: String)
