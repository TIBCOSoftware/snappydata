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

import java.sql.SQLException

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem
import io.snappydata.SnappyFunSuite.checkAnswer
import io.snappydata.{Property, SnappyFunSuite}
import org.scalatest.Assertions

import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashJoinExec}
import org.apache.spark.sql.{AnalysisException, Dataset, Row, SnappySession}

/**
 * Tests for temporary, global and persistent views.
 */
class ViewTest extends SnappyFunSuite {

  override def beforeAll(): Unit = {
    super.beforeAll()
    ViewTest.createTables(this.snc.snappySession)
  }

  override def afterAll(): Unit = {
    ViewTest.dropTables(this.snc.snappySession)
    super.afterAll()
  }

  test("temporary view") {
    ViewTest.testTemporaryView(snc.snappySession.sql, () => new SnappySession(sc).sql)
  }

  test("global temporary view") {
    ViewTest.testGlobalTemporaryView(snc.snappySession.sql, () => new SnappySession(sc).sql)
  }

  test("temporary view using") {
    ViewTest.testTemporaryViewUsing(snc.snappySession.sql, () => new SnappySession(sc).sql)
  }

  test("global temporary view using") {
    ViewTest.testGlobalTemporaryViewUsing(snc.snappySession.sql, () => new SnappySession(sc).sql)
  }

  test("persistent view") {
    ViewTest.testPersistentView(snc.snappySession.sql, checkPlans = true,
      () => new SnappySession(sc).sql, restartSpark)
  }

  private def restartSpark(): Unit = {
    stopAll()
    val sys = InternalDistributedSystem.getConnectedInstance
    if (sys ne null) {
      sys.disconnect()
    }
  }
}

object ViewTest extends Assertions {

  private val columnTable = "viewColTable"
  private val rowTable = "viewRowTable"
  private val numRows = 10
  private val viewQuery = "select id, addr, rank() over (order by id) as rank"
  private val viewTempMeta = Seq(Row("ID", "int", null), Row("ADDR", "string", null),
    Row("RANK", "int", null))

  private def getExpectedResult: Seq[Row] = {
    (0 until numRows).map(i => Row(i, "address_" + (i + 1), i + 1))
  }

  private def tableExists(executeSQL: String => Dataset[Row], name: String): Boolean = {
    try {
      executeSQL(s"select 1 from $name where 1 = 0")
      true
    } catch {
      case _: Exception => false
    }
  }

  def createTables(session: SnappySession): Unit = {
    session.sql(s"create table $columnTable (id int, addr varchar(20)) using column " +
        "options (partition_by 'id')")
    session.sql(s"create table $rowTable (id int, addr varchar(20)) using row " +
        s"options (partition_by 'id', colocate_with '$columnTable')")

    val rows = (0 until numRows).map(i => Row(i, "address_" + (i + 1)))
    session.insert(columnTable, rows: _*)
    session.insert(rowTable, rows: _*)
  }

  def dropTables(session: SnappySession): Unit = {
    session.sql(s"drop table $rowTable")
    session.sql(s"drop table $columnTable")
  }

  def testTemporaryView(executeSQL: String => Dataset[Row],
      newExecution: () => String => Dataset[Row]): Unit = {
    val tableMeta = Seq(Row("ID", "int", null), Row("ADDR", "varchar(20)", null))

    checkAnswer(executeSQL(s"describe $columnTable"), tableMeta)
    checkAnswer(executeSQL(s"describe $rowTable"), tableMeta)

    val expected = getExpectedResult
    val showResult = Seq(Row("", "VIEWONTABLE", true, false))

    // check temporary view and its meta-data for column table
    executeSQL(s"create temporary view viewOnTable as $viewQuery from $columnTable")

    assert(tableExists(executeSQL, "viewOnTable") === true)
    checkAnswer(executeSQL("describe viewOnTable"), viewTempMeta)
    checkAnswer(executeSQL("select * from viewOnTable"), expected)
    checkAnswer(executeSQL("show views"), showResult)
    checkAnswer(executeSQL("show views in app"), showResult)
    checkAnswer(executeSQL("show views from app"), showResult)

    // should not be visible from another session
    val executeSQL2 = newExecution()
    assert(tableExists(executeSQL2, "viewOnTable") === false)

    // drop and check unavailability
    executeSQL("drop view viewOnTable")
    assert(tableExists(executeSQL, "viewOnTable") === false)
    assert(tableExists(executeSQL2, "viewOnTable") === false)

    // check the same for view on row table
    executeSQL(s"create temporary view viewOnTable as $viewQuery from $rowTable")

    assert(tableExists(executeSQL, "viewOnTable") === true)
    checkAnswer(executeSQL("describe viewOnTable"), viewTempMeta)
    checkAnswer(executeSQL("select * from viewOnTable"), expected)

    assert(tableExists(executeSQL2, "viewOnTable") === false)
    executeSQL("drop view viewOnTable")
    assert(tableExists(executeSQL, "viewOnTable") === false)
    assert(tableExists(executeSQL2, "viewOnTable") === false)
  }

  def testGlobalTemporaryView(executeSQL: String => Dataset[Row],
      newExecution: () => String => Dataset[Row]): Unit = {
    val expected = getExpectedResult
    val showResult = Seq(Row("GLOBAL_TEMP", "VIEWONTABLE", true, true))

    // check temporary view and its meta-data for column table
    executeSQL(s"create global temporary view viewOnTable as $viewQuery from $columnTable")

    assert(executeSQL("show views in global_temp").collect() ===
        Array(Row("GLOBAL_TEMP", "VIEWONTABLE", true, true)))
    checkAnswer(executeSQL("describe global_temp.viewOnTable"), viewTempMeta)
    checkAnswer(executeSQL("select * from viewOnTable"), expected)
    checkAnswer(executeSQL("show views"), Nil)
    checkAnswer(executeSQL("show views in global_temp"), showResult)
    checkAnswer(executeSQL("show views from global_temp"), showResult)

    // should be visible from another session
    val executeSQL2 = newExecution()
    assert(executeSQL2("show views in global_temp").collect() ===
        Array(Row("GLOBAL_TEMP", "VIEWONTABLE", true, true)))
    checkAnswer(executeSQL2("describe global_temp.viewOnTable"), viewTempMeta)
    checkAnswer(executeSQL2("select * from viewOnTable"), expected)

    // drop and check unavailability
    executeSQL("drop view viewOnTable")
    assert(executeSQL("show views in global_temp").collect().isEmpty)
    assert(executeSQL2("show views in global_temp").collect().isEmpty)

    // check the same for view on row table
    executeSQL(s"create global temporary view viewOnTable as $viewQuery from $columnTable")

    assert(executeSQL("show views in global_temp").collect() ===
        Array(Row("GLOBAL_TEMP", "VIEWONTABLE", true, true)))
    checkAnswer(executeSQL("describe global_temp.viewOnTable"), viewTempMeta)
    checkAnswer(executeSQL("select * from viewOnTable"), expected)

    assert(executeSQL2("show views in global_temp").collect() ===
        Array(Row("GLOBAL_TEMP", "VIEWONTABLE", true, true)))
    checkAnswer(executeSQL2("describe global_temp.viewOnTable"), viewTempMeta)
    checkAnswer(executeSQL2("select * from viewOnTable"), expected)

    executeSQL("drop view viewOnTable")
    assert(executeSQL("show views in global_temp").collect().isEmpty)
    assert(executeSQL2("show views in global_temp").collect().isEmpty)
  }

  def testTemporaryViewUsing(executeSQL: String => Dataset[Row],
      newExecution: () => String => Dataset[Row]): Unit = {
    // check temporary view with USING and its meta-data
    val hfile: String = getClass.getResource("/2015.parquet").getPath
    executeSQL(s"create external table airlineTemp using parquet options (path '$hfile')")
    val airline = executeSQL("select * from airlineTemp limit 1")
    executeSQL(s"create temporary view airlineView using parquet options(path '$hfile')")
    val airlineView = executeSQL("select * from airlineView limit 1")

    assert(tableExists(executeSQL, "airlineView") === true)
    assert(airlineView.schema === airline.schema)
    checkAnswer(executeSQL("select count(*) from airlineView"),
      executeSQL("select count(*) from airlineTemp").collect())

    // should not be visible from another session
    val executeSQL2 = newExecution()
    assert(tableExists(executeSQL2, "airlineView") === false)

    // drop and check unavailability
    executeSQL("drop table airlineTemp")
    executeSQL("drop table airlineView")
    assert(tableExists(executeSQL, "airlineTemp") === false)
    assert(tableExists(executeSQL2, "airlineTemp") === false)
    assert(tableExists(executeSQL, "airlineView") === false)
    assert(tableExists(executeSQL2, "airlineView") === false)
  }

  def testGlobalTemporaryViewUsing(executeSQL: String => Dataset[Row],
      newExecution: () => String => Dataset[Row]): Unit = {
    // check global temporary view with USING and its meta-data
    val hfile: String = getClass.getResource("/2015.parquet").getPath
    executeSQL(s"create external table airlineTemp using parquet options (path '$hfile')")
    val airline = executeSQL("select * from airlineTemp limit 1")
    executeSQL(s"create global temporary view airlineView using parquet options(path '$hfile')")
    val airlineView = executeSQL("select * from airlineView limit 1")

    assert(executeSQL("show views in global_temp").collect() ===
        Array(Row("GLOBAL_TEMP", "AIRLINEVIEW", true, true)))
    assert(airlineView.schema === airline.schema)
    checkAnswer(executeSQL("select count(*) from airlineView"),
      executeSQL("select count(*) from airlineTemp").collect())

    // should be visible from another session
    val executeSQL2 = newExecution()
    assert(executeSQL2("show views in global_temp").collect() ===
        Array(Row("GLOBAL_TEMP", "AIRLINEVIEW", true, true)))
    checkAnswer(executeSQL2("select count(*) from airlineView"),
      executeSQL("select count(*) from airlineTemp").collect())

    // drop and check unavailability
    executeSQL("drop table airlineTemp")
    executeSQL("drop table airlineView")
    assert(tableExists(executeSQL, "airlineTemp") === false)
    assert(tableExists(executeSQL2, "airlineTemp") === false)
    assert(executeSQL("show views in global_temp").collect().isEmpty)
    assert(executeSQL2("show views in global_temp").collect().isEmpty)
  }

  def testPersistentView(executeSQL: String => Dataset[Row], checkPlans: Boolean,
      newExecution: () => String => Dataset[Row], restartSpark: () => Unit): Unit = {
    val expected = getExpectedResult
    // check temporary view and its meta-data for column table
    checkPersistentView(columnTable, rowTable, checkPlans, executeSQL, newExecution,
      expected, restartSpark)
    // check the same for view on row table with new session since old one would not be valid
    val newExecuteSQL = newExecution()
    checkPersistentView(rowTable, columnTable, checkPlans, newExecuteSQL, newExecution,
      expected, restartSpark)
  }

  private def checkPersistentView(table: String, otherTable: String, checkPlans: Boolean,
      executeSQL: String => Dataset[Row], newExecution: () => String => Dataset[Row],
      expectedResult: Seq[Row], restartSpark: () => Unit): Unit = {
    executeSQL(s"create view viewOnTable as $viewQuery from $table")

    val viewMeta = Seq(Row("ID", "int", null), Row("ADDR", "varchar(20)", null),
      Row("RANK", "int", null))
    val showResult = Seq(Row("APP", "VIEWONTABLE", false, false))

    assert(tableExists(executeSQL, "viewOnTable") === true)
    checkAnswer(executeSQL("describe viewOnTable"), viewMeta)
    checkAnswer(executeSQL("select * from viewOnTable"), expectedResult)
    checkAnswer(executeSQL("show views"), showResult)
    checkAnswer(executeSQL("show views in app"), showResult)
    checkAnswer(executeSQL("show views from app"), showResult)

    // should be visible from another session
    var executeSQL2 = newExecution()
    assert(tableExists(executeSQL2, "viewOnTable") === true)
    checkAnswer(executeSQL2("describe viewOnTable"), viewMeta)
    checkAnswer(executeSQL2("select * from viewOnTable"), expectedResult)

    // test for SNAP-2205: see CompressionCodecId.isCompressed for a description of the problem
    executeSQL(s"set ${Property.ColumnBatchSize.name}=10k")
    // 21 columns mean 63 for ColumnStatsSchema so total of 64 fields including the COUNT
    // in the stats row which will fit in exactly one long for the nulls bitset
    val cols = (1 to 21).map(i => s"col$i string").mkString(", ")
    executeSQL(s"CREATE TABLE test2205 ($cols) using column options (buckets '4')")

    val numElements = 10000
    val projection = (1 to 21).map(i => s"null as col$i")
    executeSQL(
      s"insert into test2205 select ${projection.mkString(", ")} from range($numElements)")

    checkAnswer(executeSQL("select count(*), count(col10) from test2205"),
      Seq(Row(numElements, 0)))

    // test large view
    val longStr = (1 to 2000).mkString("test data ", "", "")
    val largeViewStr = (1 to 100).map(i =>
      s"case when $i % 3 == 0 then cast(null as string) else '$longStr[$i]' end as c$i").mkString(
      "create view largeView as select ", ", ", "")
    assert(largeViewStr.length > 200000)
    executeSQL2(largeViewStr).collect()
    var rs = executeSQL("select * from largeView").collect()
    assert(rs.length == 1)

    Thread.sleep(10000000)
    // should be available after a restart
    restartSpark()
    executeSQL2 = newExecution()
    assert(tableExists(executeSQL2, "viewOnTable") === true)
    checkAnswer(executeSQL2("describe viewOnTable"), viewMeta)
    checkAnswer(executeSQL2("select * from viewOnTable"), expectedResult)

    checkAnswer(executeSQL2("select count(*), count(col10) from test2205"),
      Seq(Row(numElements, 0)))

    try {
      executeSQL2("drop table viewOnTable")
      fail("expected drop table to fail for view")
    } catch {
      case _: AnalysisException | _: SQLException => // expected
    }
    // drop and check unavailability
    executeSQL2("drop view viewOnTable")
    assert(tableExists(executeSQL2, "viewOnTable") === false)
    executeSQL2("drop table test2205")

    // test large view after restart
    rs = executeSQL2("select * from largeView").collect()
    assert(rs.length == 1)
    executeSQL2("drop view largeView")

    // check colocated joins with VIEWs (SNAP-2204)

    val query = s"select c.id, r.addr from $columnTable c inner join $rowTable r on (c.id = r.id)"
    // first check with normal query
    var ds = executeSQL2(query)
    checkAnswer(ds, expectedResult.map(r => Row(r.get(0), r.get(1))))
    if (checkPlans) {
      val plan = ds.queryExecution.executedPlan
      assert(plan.find(_.isInstanceOf[HashJoinExec]).isDefined)
      assert(plan.find(_.isInstanceOf[BroadcastHashJoinExec]).isEmpty)
    }

    val expectedResult2 = expectedResult.map(r => Row(r.get(0), r.get(1)))
    // check for normal view join with table
    executeSQL2(s"create view viewOnTable as select id, addr, id + 1 from $table")
    ds = executeSQL2("select t.id, v.addr from viewOnTable v " +
        s"inner join $otherTable t on (v.id = t.id)")
    checkAnswer(ds, expectedResult2)
    if (checkPlans) {
      val plan = ds.queryExecution.executedPlan
      assert(plan.find(_.isInstanceOf[HashJoinExec]).isDefined)
      assert(plan.find(_.isInstanceOf[BroadcastHashJoinExec]).isEmpty)
    }

    executeSQL2("drop view viewOnTable")
    assert(tableExists(executeSQL2, "viewOnTable") === false)

    // next query on a join view
    executeSQL2(s"create view viewOnJoin as $query")
    ds = executeSQL2("select * from viewOnJoin")
    checkAnswer(ds, expectedResult2)
    if (checkPlans) {
      val plan = ds.queryExecution.executedPlan
      assert(plan.find(_.isInstanceOf[HashJoinExec]).isDefined)
      assert(plan.find(_.isInstanceOf[BroadcastHashJoinExec]).isEmpty)
    }

    executeSQL2("drop view viewOnJoin")
    assert(tableExists(executeSQL2, "viewOnJoin") === false)
  }
}
