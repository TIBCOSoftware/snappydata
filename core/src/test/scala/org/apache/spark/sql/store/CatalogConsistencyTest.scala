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

import java.sql.{Connection, DriverManager, SQLException}

import com.pivotal.gemfirexd.internal.iapi.error.StandardException
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedSQLException
import io.snappydata.SnappyFunSuite
import io.snappydata.core.Data
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.Logging
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.sql.{AnalysisException, SaveMode, TableNotFoundException}

class CatalogConsistencyTest
    extends SnappyFunSuite
    with Logging
    with BeforeAndAfter
    with BeforeAndAfterAll {

  val props = Map.empty[String, String]

  after {
    snc.dropTable("column_table1", ifExists = true)
    snc.dropTable("column_table2", ifExists = true)
    snc.dropTable("row_table1", ifExists = true)
    snc.dropTable("row_table2", ifExists = true)
  }

  def assertTableDoesNotExist(routeQueryDisabledConn: Connection, table: String,
      isColumnTable: Boolean = false): Unit = {
    // after the catalog is repaired the table should not exist
    // either in Hive metastore or snappy-store
    intercept[TableNotFoundException] {
      snc.snappySession.sessionCatalog.lookupRelation(
        snc.snappySession.tableIdentifier(table))
    }
    val se = intercept[SQLException] {
      routeQueryDisabledConn.createStatement().executeQuery(
        "select * from " + table)
    }
    assert(se.getSQLState.equals("42X05"))

    if (isColumnTable) {
      val se = intercept[SQLException] {
        routeQueryDisabledConn.createStatement().executeQuery(
          "select * from " + ColumnFormatRelation.columnBatchTableName("app." + table))
      }
      assert(se.getSQLState.equals("42X05"))
    }
  }

  def getConnection(routeQuery: Boolean = true): Connection = {
    val driver = "io.snappydata.jdbc.EmbeddedDriver"
    Class.forName(driver).newInstance //scalastyle:ignore
    val url = {
      if (!routeQuery) {
        "jdbc:snappydata:;route-query=false;internal-connection=true"
      } else {
        "jdbc:snappydata:"
      }
    }
    DriverManager.getConnection(url)
  }


  test("Column table: hive catalog entry missing") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable("column_table1", "column", dataDF.schema, props)

    // remove the table entry from Hive store but not from store DD
    snc.snappySession.sessionCatalog.externalCatalog.dropTable("app", "column_table1",
      ignoreIfNotExists = false, purge = false)

    // should throw an exception since the table has been removed from Hive store
    intercept[AnalysisException] {
      val result = snc.sql("SELECT * FROM column_table1")
    }

    val routeQueryDisabledConn = getConnection(routeQuery = false)

    // should not throw an exception on a JDBC connection route-query=false
    // as entry exists in the store
    val rs = routeQueryDisabledConn.createStatement().executeQuery("select * from column_table1")
    rs.close()

    // repair the catalog
    val connection = getConnection()
    connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('true', 'true')")

    assertTableDoesNotExist(routeQueryDisabledConn, "column_table1", isColumnTable = true)

    snc.createTable("column_table1", "column", dataDF.schema, props)
    snc.createTable("column_table2", "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("column_table2")

    // remove the table entry from Hive store but not from store DD
    snc.snappySession.sessionCatalog.externalCatalog.dropTable("app", "column_table1",
      ignoreIfNotExists = false, purge = false)

    // repair the catalog
    connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('true', 'true')")

    intercept[TableNotFoundException] {
      snc.snappySession.sessionCatalog.lookupRelation(
        snc.snappySession.tableIdentifier("column_table1"))
    }
    // should throw an exception since the catalog is repaired and table entry
    // should have been removed
    val se2 = intercept[SQLException] {
      routeQueryDisabledConn.createStatement().executeQuery("select * from column_table1")
    }
    assert(se2.getSQLState.equals("42X05"))

    val result = snc.sql("SELECT * FROM column_table2")
    assert(result.collect.length == 5)
  }

  test("Column table: column buffer and hive catalog entry missing") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable("column_table1", "column", dataDF.schema, props)
    snc.createTable("column_table2", "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("column_table2")

    val routeQueryDisabledConn = getConnection(routeQuery = false)

    val rs = routeQueryDisabledConn.createStatement().executeQuery("select * from column_table1")
    rs.close()

    // remove the column buffer DD entry by dropping table just from the store
    // (don't drop the row buffer)
    routeQueryDisabledConn.createStatement().execute("drop table " +
        ColumnFormatRelation.columnBatchTableName("app.column_table1"))
    // remove the table entry from Hive store
    snc.snappySession.sessionCatalog.externalCatalog.dropTable("app", "column_table1",
      ignoreIfNotExists = false, purge = false)

    // make sure that the table does not exist in Hive metastore
    intercept[TableNotFoundException] {
      snc.snappySession.sessionCatalog.lookupRelation(
        snc.snappySession.tableIdentifier("column_table1"))
    }

    // should not throw an exception as row buffer exists for column_table1
    val rs2 = routeQueryDisabledConn.createStatement().executeQuery("select * from column_table1")
    rs2.close()

    // repair the catalog
    val connection = getConnection()
    connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('true', 'true')")

    assertTableDoesNotExist(routeQueryDisabledConn, "column_table1", isColumnTable = true)

    snc.createTable("column_table1", "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("column_table1")
    val result = snc.sql("SELECT * FROM column_table1")
    assert(result.collect.length == 5)
  }

  test("Column table: column buffer missing") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable("column_table1", "column", dataDF.schema, props)
    snc.createTable("column_table2", "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("column_table2")

    val routeQueryDisabledConn = getConnection(routeQuery = false)

    // remove the DD entry by dropping table just from the store
    val rs = routeQueryDisabledConn.createStatement().executeQuery("select * from column_table1")
    rs.close()
    routeQueryDisabledConn.createStatement().execute("drop table " +
        ColumnFormatRelation.columnBatchTableName("app.column_table1"))

    // make sure that the table exists in Hive metastore
    // should not throw an exception
    snc.snappySession.sessionCatalog.lookupRelation(
      snc.snappySession.tableIdentifier("column_table1"))

    val connection = getConnection()
    connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('true', 'true')")

    assertTableDoesNotExist(routeQueryDisabledConn, "column_table1", true)

    snc.createTable("column_table1", "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("column_table1")
    val result = snc.sql("SELECT * FROM column_table1")
    assert(result.collect.length == 5)
  }

  // can this ever happen?
  test("Column table: no entry in DD") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable("column_table1", "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("column_table1")
    snc.createTable("column_table2", "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("column_table2")

    val routeQueryDisabledConn = getConnection(routeQuery = false)

    // remove the DD entry by dropping table just from the store
    val rs = routeQueryDisabledConn.createStatement().executeQuery("select * from column_table1")
    rs.close()
    routeQueryDisabledConn.createStatement().execute("drop table " +
        ColumnFormatRelation.columnBatchTableName("app.column_table1"))
    routeQueryDisabledConn.createStatement().execute("drop table column_table1")

    // make sure that the table exists in Hive metastore
    // should not throw an exception
    snc.snappySession.sessionCatalog.lookupRelation(
      snc.snappySession.tableIdentifier("column_table1"))

    val connection = getConnection()
    connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('true', 'true')")

    assertTableDoesNotExist(routeQueryDisabledConn, "column_table1", isColumnTable = true)

    snc.createTable("column_table1", "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable("column_table1")
    val result = snc.sql("SELECT * FROM column_table1")
    assert(result.collect.length == 5)
  }

  test("Row table: hive catalog entry missing") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable("row_table1", "row", dataDF.schema, props)

    // remove the table entry from Hive store but not from store DD
    snc.snappySession.sessionCatalog.externalCatalog.dropTable("app", "row_table1",
      ignoreIfNotExists = false, purge = false)

    // should throw an exception since the table has been removed from Hive store
    intercept[AnalysisException] {
      val result = snc.sql("SELECT * FROM row_table1")
    }

    // repair the catalog
    val connection = getConnection()
    connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('true', 'true')")
    // make sure that the table does not exist
    val routeQueryDisabledConn = getConnection(routeQuery = false)
    assertTableDoesNotExist(routeQueryDisabledConn, "row_table1", isColumnTable = false)

    snc.createTable("row_table1", "row", dataDF.schema, props)

    snc.createTable("row_table2", "row", dataDF.schema, props)
    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("row_table2")

    // remove the table entry from Hive store but not from store DD
    snc.snappySession.sessionCatalog.externalCatalog.dropTable("app", "row_table1",
      ignoreIfNotExists = false, purge = false)

    // repair the catalog
    connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('true', 'true')")
    // make sure that the table does not exist
    assertTableDoesNotExist(routeQueryDisabledConn, "row_table1", isColumnTable = false)

    val result = snc.sql("SELECT * FROM row_table2")
    assert(result.collect.length == 5)
  }

  // can this ever happen?
  test("Row table: no entry in DD") {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable("row_table1", "row", dataDF.schema, props)
    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("row_table1")
    snc.createTable("row_table2", "row", dataDF.schema, props)
    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("row_table2")

    val routeQueryDisabledConn = getConnection(routeQuery = false)

    // remove the DD entry by dropping table just from the store
    snc.sql("select * from row_table1").collect()
    val rs = routeQueryDisabledConn.createStatement().executeQuery("select * from row_table1")
    rs.close()
    routeQueryDisabledConn.createStatement().execute("drop table row_table1")

    // make sure that the table exists in Hive metastore
    // should not throw an exception
    snc.snappySession.sessionCatalog.lookupRelation(
      snc.snappySession.tableIdentifier("row_table1"))

    val connection = getConnection()
    // repair the catalog
    connection.createStatement().execute("CALL SYS.REPAIR_CATALOG('true', 'true')")
    // make sure that the table does not exist
    assertTableDoesNotExist(routeQueryDisabledConn, "row_table1", isColumnTable = false)

    snc.createTable("row_table1", "row", dataDF.schema, props)
    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("row_table1")
    val result = snc.sql("SELECT * FROM row_table1")
    val r = result.collect
    assert(r.length == 5)

  }

  test("REMOVE_METASTORE_ENTRY procedure should drop table from catalog") {
    snc.sql("create table app.rowtable1 (c1 integer, c2 string, c3 float)")
    snc.sql("insert into app.rowtable1 values (11, '11', 1.1)")

    try {
      snc.sql("call sys.REMOVE_METASTORE_ENTRY('app.rowtable1', 'false');")
    } catch {
      case e: EmbedSQLException =>
        assert(e.getMessage.contains("Table retrieved successfully"))
    }

    snc.sql("call sys.REMOVE_METASTORE_ENTRY('app.rowtable1', 'true');")

    intercept[TableNotFoundException] {
      snc.snappySession.sessionCatalog.lookupRelation(
        snc.snappySession.tableIdentifier("rowtable1"))
    }
    val result = snc.sql("show tables")
    assert(result.collect.length == 0)
    val routeQueryDisabledConn = getConnection(routeQuery = false)
    routeQueryDisabledConn.createStatement().execute("drop table if exists rowtable1")
  }
}
