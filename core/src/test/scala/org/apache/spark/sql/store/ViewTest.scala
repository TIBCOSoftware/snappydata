/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
import io.snappydata.SnappyFunSuite

import org.apache.spark.sql.{AnalysisException, Row, SnappySession}

/**
 * Tests for temporary, global and persistent views.
 */
class ViewTest extends SnappyFunSuite {

  private val columnTable = "viewColTable"
  private val rowTable = "viewRowTable"
  private val numRows = 10
  private val viewQuery = "select id, addr, rank() over (order by id) as rank"
  private val viewTempMeta = Array(Row("ID", "int", null), Row("ADDR", "string", null),
    Row("RANK", "int", null))

  override def beforeAll(): Unit = {
    super.beforeAll()
    val session = this.snc.snappySession
    session.sql(s"create table $columnTable (id int, addr varchar(20)) using column")
    session.sql(s"create table $rowTable (id int, addr varchar(20)) using row")

    val rows = (0 until numRows).map(i => Row(i, "address_" + (i + 1)))
    snc.insert(columnTable, rows: _*)
    snc.insert(rowTable, rows: _*)
  }

  private def getExpectedResult: Array[Row] = {
    Array.tabulate(numRows)(i => Row(i, "address_" + (i + 1), i + 1))
  }

  test("temporary view") {
    val session = this.snc.snappySession

    val tableMeta = Array(Row("id", "int", null), Row("addr", "string", null))

    assert(session.sql(s"describe $columnTable").collect() === tableMeta)
    assert(session.sql(s"describe $rowTable").collect() === tableMeta)

    val expected = getExpectedResult

    // check temporary view and its meta-data for column table
    session.sql(s"create temporary view viewOnTable as $viewQuery from $columnTable")

    assert(session.sessionCatalog.tableExists("viewOnTable") === true)
    assert(session.sql(s"describe viewOnTable").collect() === viewTempMeta)
    assert(session.sql("select * from viewOnTable").collect() === expected)

    // should not be visible from another session
    val session2 = session.newSession()
    assert(session2.sessionCatalog.tableExists("viewOnTable") === false)

    // drop and check unavailability
    session.sql("drop view viewOnTable")
    assert(session.sessionCatalog.tableExists("viewOnTable") === false)
    assert(session2.sessionCatalog.tableExists("viewOnTable") === false)

    // check the same for view on row table
    session.sql(s"create temporary view viewOnTable as $viewQuery from $rowTable")

    assert(session.sessionCatalog.tableExists("viewOnTable") === true)
    assert(session.sql(s"describe viewOnTable").collect() === viewTempMeta)
    assert(session.sql("select * from viewOnTable").collect() === expected)

    assert(session2.sessionCatalog.tableExists("viewOnTable") === false)
    session.sql("drop view viewOnTable")
    assert(session.sessionCatalog.tableExists("viewOnTable") === false)
    assert(session2.sessionCatalog.tableExists("viewOnTable") === false)

    session2.close()
  }

  test("global temporary view") {
    val session = this.snc.snappySession

    val expected = getExpectedResult

    // check temporary view and its meta-data for column table
    session.sql(s"create global temporary view viewOnTable as $viewQuery from $columnTable")

    assert(session.sessionCatalog.getGlobalTempView("viewOnTable").isDefined)
    assert(session.sql(s"describe global_temp.viewOnTable").collect() === viewTempMeta)
    assert(session.sql("select * from viewOnTable").collect() === expected)

    // should be visible from another session
    val session2 = session.newSession()
    assert(session2.sessionCatalog.getGlobalTempView("viewOnTable").isDefined)
    assert(session2.sql(s"describe global_temp.viewOnTable").collect() === viewTempMeta)
    assert(session2.sql("select * from viewOnTable").collect() === expected)

    try {
      session.sql("drop table viewOnTable")
      fail("expected drop table to fail for view")
    } catch {
      case _: AnalysisException => // expected
    }
    // drop and check unavailability
    session.sql("drop view viewOnTable")
    assert(session.sessionCatalog.getGlobalTempView("viewOnTable").isEmpty)
    assert(session2.sessionCatalog.getGlobalTempView("viewOnTable").isEmpty)

    // check the same for view on row table
    session.sql(s"create global temporary view viewOnTable as $viewQuery from $columnTable")

    assert(session.sessionCatalog.getGlobalTempView("viewOnTable").isDefined)
    assert(session.sql(s"describe global_temp.viewOnTable").collect() === viewTempMeta)
    assert(session.sql("select * from viewOnTable").collect() === expected)

    assert(session2.sessionCatalog.getGlobalTempView("viewOnTable").isDefined)
    assert(session2.sql(s"describe global_temp.viewOnTable").collect() === viewTempMeta)
    assert(session2.sql("select * from viewOnTable").collect() === expected)

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

    assert(session.sessionCatalog.tableExists("airlineView") === true)
    assert(airlineView.schema === airline.schema)
    assert(session.sql("select count(*) from airlineView").collect()(0).getLong(0) ===
        airline.count())
    assert(airlineView.count() == airline.count())

    // should not be visible from another session
    val session2 = session.newSession()
    assert(session2.sessionCatalog.tableExists("airlineView") === false)

    // drop and check unavailability
    session.sql("drop table airlineView")
    assert(session.sessionCatalog.tableExists("airlineView") === false)
    assert(session2.sessionCatalog.tableExists("airlineView") === false)

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
    assert(session.sql("select count(*) from airlineView").collect()(0).getLong(0) ===
        airline.count())
    assert(airlineView.count() == airline.count())

    // should be visible from another session
    val session2 = session.newSession()
    assert(session2.sessionCatalog.getGlobalTempView("airlineView").isDefined)
    assert(session2.sql("select count(*) from airlineView").collect()(0).getLong(0) ===
        airline.count())

    try {
      session.sql("drop table airlineView")
      fail("expected drop table to fail for view")
    } catch {
      case _: AnalysisException => // expected
    }
    // drop and check unavailability
    session.sql("drop view airlineView")
    assert(session.sessionCatalog.getGlobalTempView("airlineView").isEmpty)
    assert(session2.sessionCatalog.getGlobalTempView("airlineView").isEmpty)

    session2.close()
  }

  test("persistent view") {
    val expected = getExpectedResult
    // check temporary view and its meta-data for column table
    checkPersistentView(columnTable, snc.snappySession, expected)
    // check the same for view on row table
    checkPersistentView(rowTable, snc.snappySession, expected)
  }

  private def checkPersistentView(table: String, session: SnappySession,
      expectedResult: Array[Row]): Unit = {
    session.sql(s"create view viewOnTable as $viewQuery from $table")

    val viewMeta = Array(Row("id", "int", null), Row("addr", "string", null),
      Row("rank", "int", null))

    assert(session.sessionCatalog.tableExists("viewOnTable") === true)
    assert(session.sql(s"describe viewOnTable").collect() === viewMeta)
    assert(session.sql("select * from viewOnTable").collect() === expectedResult)

    // should be visible from another session
    var session2 = session.newSession()
    assert(session2.sessionCatalog.tableExists("viewOnTable") === true)
    assert(session2.sql(s"describe viewOnTable").collect() === viewMeta)
    assert(session2.sql("select * from viewOnTable").collect() === expectedResult)

    // should be available after a restart
    session.close()
    session2.close()
    stopAll()
    val sys = InternalDistributedSystem.getConnectedInstance
    if (sys ne null) {
      sys.disconnect()
    }

    session2 = new SnappySession(sc)
    assert(session2.sessionCatalog.tableExists("viewOnTable") === true)
    assert(session2.sql(s"describe viewOnTable").collect() === viewMeta)
    assert(session2.sql("select * from viewOnTable").collect() === expectedResult)

    try {
      session.sql("drop table viewOnTable")
      fail("expected drop table to fail for view")
    } catch {
      case _: AnalysisException => // expected
    }
    // drop and check unavailability
    session2.sql("drop view viewOnTable")
    assert(session2.sessionCatalog.tableExists("viewOnTable") === false)
  }
}
