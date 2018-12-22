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

import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem
import io.snappydata.{Property, SnappyFunSuite}

import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, HashJoinExec}
import org.apache.spark.sql.{AnalysisException, Row, SnappySession}

/**
 * Tests for temporary, global and persistent views.
 */
class ViewTest extends SnappyFunSuite {

  private val columnTable = "viewColTable"
  private val rowTable = "viewRowTable"
  private val numRows = 10
  private val viewQuery = "select id, addr, rank() over (order by id) as rank"
  private val viewTempMeta = Seq(Row("ID", "int", null), Row("ADDR", "string", null),
    Row("RANK", "int", null))

  override def beforeAll(): Unit = {
    super.beforeAll()
    val session = this.snc.snappySession
    session.sql(s"create table $columnTable (id int, addr varchar(20)) using column " +
        "options (partition_by 'id')")
    session.sql(s"create table $rowTable (id int, addr varchar(20)) using row " +
        s"options (partition_by 'id', colocate_with '$columnTable')")

    val rows = (0 until numRows).map(i => Row(i, "address_" + (i + 1)))
    snc.insert(columnTable, rows: _*)
    snc.insert(rowTable, rows: _*)
  }

  private def getExpectedResult: Seq[Row] = {
    (0 until numRows).map(i => Row(i, "address_" + (i + 1), i + 1))
  }

  private def tableExists(session: SnappySession, name: String): Boolean = {
    val identifier = session.tableIdentifier(name)
    session.sessionCatalog.isTemporaryTable(identifier) ||
        session.sessionCatalog.tableExists(identifier)
  }

  test("temporary view") {
    val session = this.snc.snappySession

    val tableMeta = Seq(Row("ID", "int", null), Row("ADDR", "varchar(20)", null))

    checkAnswer(session.sql(s"describe $columnTable"), tableMeta)
    checkAnswer(session.sql(s"describe $rowTable"), tableMeta)

    val expected = getExpectedResult
    val showResult = Seq(Row("", "VIEWONTABLE", true, false))

    // check temporary view and its meta-data for column table
    session.sql(s"create temporary view viewOnTable as $viewQuery from $columnTable")

    assert(tableExists(session, "viewOnTable") === true)
    checkAnswer(session.sql("describe viewOnTable"), viewTempMeta)
    checkAnswer(session.sql("select * from viewOnTable"), expected)
    checkAnswer(session.sql("show views"), showResult)
    checkAnswer(session.sql("show views in app"), showResult)
    checkAnswer(session.sql("show views from app"), showResult)

    // should not be visible from another session
    val session2 = session.newSession()
    assert(tableExists(session2, "viewOnTable") === false)

    // drop and check unavailability
    session.sql("drop view viewOnTable")
    assert(tableExists(session, "viewOnTable") === false)
    assert(tableExists(session2, "viewOnTable") === false)

    // check the same for view on row table
    session.sql(s"create temporary view viewOnTable as $viewQuery from $rowTable")

    assert(tableExists(session, "viewOnTable") === true)
    checkAnswer(session.sql("describe viewOnTable"), viewTempMeta)
    checkAnswer(session.sql("select * from viewOnTable"), expected)

    assert(tableExists(session2, "viewOnTable") === false)
    session.sql("drop view viewOnTable")
    assert(tableExists(session, "viewOnTable") === false)
    assert(tableExists(session2, "viewOnTable") === false)

    session2.close()
  }

  test("global temporary view") {
    val session = this.snc.snappySession

    val expected = getExpectedResult
    val showResult = Seq(Row("GLOBAL_TEMP", "VIEWONTABLE", true, true))

    // check temporary view and its meta-data for column table
    session.sql(s"create global temporary view viewOnTable as $viewQuery from $columnTable")

    assert(session.sessionCatalog.getGlobalTempView("viewOnTable").isDefined)
    checkAnswer(session.sql("describe global_temp.viewOnTable"), viewTempMeta)
    checkAnswer(session.sql("select * from viewOnTable"), expected)
    checkAnswer(session.sql("show views"), Nil)
    checkAnswer(session.sql("show views in global_temp"), showResult)
    checkAnswer(session.sql("show views from global_temp"), showResult)

    // should be visible from another session
    val session2 = session.newSession()
    assert(session2.sessionCatalog.getGlobalTempView("viewOnTable").isDefined)
    checkAnswer(session2.sql("describe global_temp.viewOnTable"), viewTempMeta)
    checkAnswer(session2.sql("select * from viewOnTable"), expected)

    // drop and check unavailability
    session.sql("drop view viewOnTable")
    assert(session.sessionCatalog.getGlobalTempView("viewOnTable").isEmpty)
    assert(session2.sessionCatalog.getGlobalTempView("viewOnTable").isEmpty)

    // check the same for view on row table
    session.sql(s"create global temporary view viewOnTable as $viewQuery from $columnTable")

    assert(session.sessionCatalog.getGlobalTempView("viewOnTable").isDefined)
    checkAnswer(session.sql("describe global_temp.viewOnTable"), viewTempMeta)
    checkAnswer(session.sql("select * from viewOnTable"), expected)

    assert(session2.sessionCatalog.getGlobalTempView("viewOnTable").isDefined)
    checkAnswer(session2.sql("describe global_temp.viewOnTable"), viewTempMeta)
    checkAnswer(session2.sql("select * from viewOnTable"), expected)

    session.sql("drop view viewOnTable")
    assert(session.sessionCatalog.getGlobalTempView("viewOnTable").isEmpty)
    assert(session2.sessionCatalog.getGlobalTempView("viewOnTable").isEmpty)

    session2.close()
  }

  test("temporary view using") {
    val session = this.snc.snappySession

    // check temporary view with USING and its meta-data
    val hfile: String = getClass.getResource("/2015.parquet").getPath
    val airline = session.read.parquet(hfile)
    session.sql(s"create temporary view airlineView using parquet options(path '$hfile')")
    val airlineView = session.table("airlineView")

    assert(tableExists(session, "airlineView") === true)
    assert(airlineView.schema === airline.schema)
    checkAnswer(session.sql("select count(*) from airlineView"), Seq(Row(airline.count())))
    assert(airlineView.count() == airline.count())

    // should not be visible from another session
    val session2 = session.newSession()
    assert(tableExists(session2, "airlineView") === false)

    // drop and check unavailability
    session.sql("drop table airlineView")
    assert(tableExists(session, "airlineView") === false)
    assert(tableExists(session2, "airlineView") === false)

    session2.close()
  }

  test("global temporary view using") {
    val session = this.snc.snappySession

    // check global temporary view with USING and its meta-data
    val hfile: String = getClass.getResource("/2015.parquet").getPath
    val airline = session.read.parquet(hfile)
    session.sql(s"create global temporary view airlineView using parquet options(path '$hfile')")
    val airlineView = session.table("airlineView")

    assert(session.sessionCatalog.getGlobalTempView("airlineView").isDefined)
    assert(airlineView.schema === airline.schema)
    checkAnswer(session.sql("select count(*) from airlineView"), Seq(Row(airline.count())))
    assert(airlineView.count() == airline.count())

    // should be visible from another session
    val session2 = session.newSession()
    assert(session2.sessionCatalog.getGlobalTempView("airlineView").isDefined)
    checkAnswer(session2.sql("select count(*) from airlineView"), Seq(Row(airline.count())))

    // drop and check unavailability
    session.sql("drop table airlineView")
    assert(session.sessionCatalog.getGlobalTempView("airlineView").isEmpty)
    assert(session2.sessionCatalog.getGlobalTempView("airlineView").isEmpty)

    session2.close()
  }

  test("persistent view") {
    val expected = getExpectedResult
    // check temporary view and its meta-data for column table
    checkPersistentView(columnTable, rowTable, snc.snappySession, expected)
    // check the same for view on row table
    checkPersistentView(rowTable, columnTable, snc.snappySession, expected)
  }

  private def checkPersistentView(table: String, otherTable: String, session: SnappySession,
      expectedResult: Seq[Row]): Unit = {
    session.sql(s"create view viewOnTable as $viewQuery from $table")

    val viewMeta = Seq(Row("ID", "int", null), Row("ADDR", "varchar(20)", null),
      Row("RANK", "int", null))
    val showResult = Seq(Row("APP", "VIEWONTABLE", false, false))

    assert(tableExists(session, "viewOnTable") === true)
    checkAnswer(session.sql("describe viewOnTable"), viewMeta)
    checkAnswer(session.sql("select * from viewOnTable"), expectedResult)
    checkAnswer(session.sql("show views"), showResult)
    checkAnswer(session.sql("show views in app"), showResult)
    checkAnswer(session.sql("show views from app"), showResult)

    // should be visible from another session
    var session2 = session.newSession()
    assert(tableExists(session2, "viewOnTable") === true)
    checkAnswer(session2.sql("describe viewOnTable"), viewMeta)
    checkAnswer(session2.sql("select * from viewOnTable"), expectedResult)

    // test for SNAP-2205: see CompressionCodecId.isCompressed for a description of the problem
    session.conf.set(Property.ColumnBatchSize.name, "10k")
    // 21 columns mean 63 for ColumnStatsSchema so total of 64 fields including the COUNT
    // in the stats row which will fit in exactly one long for the nulls bitset
    val cols = (1 to 21).map(i => s"col$i string").mkString(", ")
    session.sql(s"CREATE TABLE test2205 ($cols) using column options (buckets '4')")

    val numElements = 10000
    val projection = (1 to 21).map(i => s"null as col$i")
    session.range(numElements).selectExpr(projection: _*).write.insertInto("test2205")

    checkAnswer(session.sql("select count(*), count(col10) from test2205"),
      Seq(Row(numElements, 0)))

    // should be available after a restart
    session.close()
    session2.close()
    stopAll()
    val sys = InternalDistributedSystem.getConnectedInstance
    if (sys ne null) {
      sys.disconnect()
    }

    session2 = new SnappySession(sc)
    assert(tableExists(session2, "viewOnTable") === true)
    checkAnswer(session2.sql("describe viewOnTable"), viewMeta)
    checkAnswer(session2.sql("select * from viewOnTable"), expectedResult)

    checkAnswer(session2.sql("select count(*), count(col10) from test2205"),
      Seq(Row(numElements, 0)))

    try {
      session2.sql("drop table viewOnTable")
      fail("expected drop table to fail for view")
    } catch {
      case _: AnalysisException => // expected
    }
    // drop and check unavailability
    session2.sql("drop view viewOnTable")
    assert(tableExists(session2, "viewOnTable") === false)
    session2.sql("drop table test2205")

    // check colocated joins with VIEWs (SNAP-2204)

    val query = s"select c.id, r.addr from $columnTable c inner join $rowTable r on (c.id = r.id)"
    // first check with normal query
    var ds = session2.sql(query)
    checkAnswer(ds, expectedResult.map(r => Row(r.get(0), r.get(1))))
    var plan = ds.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[HashJoinExec]).isDefined)
    assert(plan.find(_.isInstanceOf[BroadcastHashJoinExec]).isEmpty)

    val expectedResult2 = expectedResult.map(r => Row(r.get(0), r.get(1)))
    // check for normal view join with table
    session2.sql(s"create view viewOnTable as select id, addr, id + 1 from $table")
    ds = session2.sql("select t.id, v.addr from viewOnTable v " +
        s"inner join $otherTable t on (v.id = t.id)")
    checkAnswer(ds, expectedResult2)
    plan = ds.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[HashJoinExec]).isDefined)
    assert(plan.find(_.isInstanceOf[BroadcastHashJoinExec]).isEmpty)

    session2.sql("drop view viewOnTable")
    assert(tableExists(session2, "viewOnTable") === false)

    // next query on a join view
    session2.sql(s"create view viewOnJoin as $query")
    ds = session2.sql("select * from viewOnJoin")
    checkAnswer(ds, expectedResult2)
    plan = ds.queryExecution.executedPlan
    assert(plan.find(_.isInstanceOf[HashJoinExec]).isDefined)
    assert(plan.find(_.isInstanceOf[BroadcastHashJoinExec]).isEmpty)

    session2.sql("drop view viewOnJoin")
    assert(tableExists(session2, "viewOnJoin") === false)
  }
}
