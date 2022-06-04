/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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

import java.sql.{Connection, DriverManager, Types}
import com.pivotal.gemfirexd.TestUtil
import com.pivotal.gemfirexd.internal.shared.common.reference.Limits
import io.snappydata.{Constant, SnappyFunSuite}
import org.junit.Assert._


class StringTypeToVarcharTest extends SnappyFunSuite {
  var serverHostPort2: String = _

  override def beforeAll(): Unit = {
    snc.sparkContext.stop()
    super.beforeAll()
    serverHostPort2 = TestUtil.startNetServer()
  }


  override def afterAll(): Unit = {
    TestUtil.stopNetServer()
    super.afterAll()
  }

  test("string_type_as in column table creation with schema using snappysession ,to be ignored") {
    this.stringTypeOptionUsingSnappySessionIgnored(true)
  }

  test("string_type_as in row table creation with schema using snappysession ,to be ignored") {
    this.stringTypeOptionUsingSnappySessionIgnored(false)
  }

  private def stringTypeOptionUsingSnappySessionIgnored(isColumnTable: Boolean): Unit = {
    val tableType = if (isColumnTable) "column" else "row"
    snc.sql("drop table if exists test1")
    snc.sql("create table test1 (col1 char(12), col2 string, col3 string, col4 clob) " +
      s"using $tableType options(${Constant.STRING_TYPE_AS} 'varchar')")
    snc.sql("insert into test1 values ('str1_1', 'str1_2', 'str1_3', 'str1_4' )," +
      "('str2_1', 'str2_2', 'str2_3', 'str2_4')")
    val conn = getSqlConnection
    val st = conn.createStatement()
    val rs = st.executeQuery("select * from test1")
    val rsmd = rs.getMetaData
    assertEquals(Types.CHAR, rsmd.getColumnType(1))
    assertEquals(12, rsmd.getPrecision(1))
    assertEquals(Types.CLOB, rsmd.getColumnType(rsmd.getColumnCount))
    snc.dropTable("test1")
  }


  test("string_type_as in column table creation with schema using jdbcconn ,to be ignored") {
    this.stringTypeOptionUsingJdbcConnIgnored(true)
  }

  test("string_type_as in row table creation with schema using jdbcconn ,to be ignored") {
    this.stringTypeOptionUsingJdbcConnIgnored(false)
  }

  private def stringTypeOptionUsingJdbcConnIgnored(isColumnTable: Boolean): Unit = {
    val tableType = if (isColumnTable) "column" else "row"
    val conn = getSqlConnection
    val st = conn.createStatement()
    st.execute("drop table if exists test1")
    st.execute("create table test1 (col1 char(12), col2 string, " +
      "col3 string, col4 clob) " +
      s"using $tableType options(${Constant.STRING_TYPE_AS} 'varchar')")
    st.executeUpdate("insert into test1 values ('str1_1', 'str1_2', 'str1_3', 'str1_4' )," +
      "('str2_1', 'str2_2', 'str2_3', 'str2_4')")

    val rs = st.executeQuery("select * from test1")
    val rsmd = rs.getMetaData
    assertEquals(Types.CHAR, rsmd.getColumnType(1))
    assertEquals(12, rsmd.getPrecision(1))
    assertEquals(Types.CLOB, rsmd.getColumnType(rsmd.getColumnCount))

    st.execute("drop table if exists test1")
  }

  test("string_type_as option in column table using CTAS") {
    testOptionOnTableCreationUsingCTAS(true)
  }

  test("string_type_as option in row table using CTAS") {
    testOptionOnTableCreationUsingCTAS(false)
  }

  def testOptionOnTableCreationUsingCTAS(isColumn: Boolean): Unit = {
    val tableType = if (isColumn) "column" else "row"
    snc.sql("drop table if exists test1")
    val eventsPath: String = getClass.getResource("/events_clob.parquet").getPath

    snc.sql(s"create external table events_external using " +
      s"parquet options (path '$eventsPath')")
    val extSchema = snc.table("events_external").schema
    val trueClobCols = extSchema.filter(sf => sf.metadata == null ||
      (sf.metadata.contains(Constant.CHAR_TYPE_BASE_PROP) &&
        sf.metadata.getString(Constant.CHAR_TYPE_BASE_PROP).equalsIgnoreCase("clob")))
    val conn = getSqlConnection
    val st = conn.createStatement()
    val rs1 = st.executeQuery("select * from events_external limit 10")
    val rsmd1 = rs1.getMetaData
    // find out false clob types
    val falseClobs = for (i <- 1 to rsmd1.getColumnCount) yield {
      val toYield = if (!trueClobCols.exists(_.name.equalsIgnoreCase(rsmd1.getColumnName(i)))) {
        if (rsmd1.getColumnType(i) == Types.CLOB) {
          rsmd1.getColumnName(i)
        } else {
          ""
        }
      } else {
        ""
      }
      toYield
    }.filterNot(_.isWhitespace)
    assertFalse(falseClobs.isEmpty)
    snc.sql("create table test1  " +
      s"using $tableType options(${Constant.STRING_TYPE_AS} 'varchar') as " +
      s"(select * from events_external limit 100)")
    val rs = st.executeQuery("select * from test1 limit 10")
    val rsmd = rs.getMetaData
    for (i <- 1 to rsmd.getColumnCount) {
      if (falseClobs.exists(_.equalsIgnoreCase(rsmd.getColumnName(i)))) {
        assertEquals(Types.VARCHAR, rsmd.getColumnType(i))
        assertEquals(Limits.DB2_VARCHAR_MAXWIDTH, rsmd.getPrecision(i))
      } else if (trueClobCols.exists(_.name.equalsIgnoreCase(rsmd.getColumnName(i)))) {
        assertTrue(rsmd.getColumnType(i) == Types.CLOB)
      }
    }

    snc.dropTable("events_external")
    snc.dropTable("test1")
  }

  test("string_type_as option in column table using CTAS using jdbcconn") {
    testOptionOnTableCreationUsingCTASWithJdbcConn(true)
  }

  test("string_type_as option in row table using CTAS using jdbcconn") {
    testOptionOnTableCreationUsingCTASWithJdbcConn(false)
  }


  def testOptionOnTableCreationUsingCTASWithJdbcConn(isColumn: Boolean): Unit = {
    val tableType = if (isColumn) "column" else "row"
    val conn = getSqlConnection
    val st = conn.createStatement()
    st.execute("drop table if exists test1")
    val eventsPath: String = getClass.getResource("/events_clob.parquet").getPath

    st.execute(s"create external table events_external using " +
      s"parquet options (path '$eventsPath')")

    val extSchema = snc.table("events_external").schema
    val trueClobCols = extSchema.filter(sf => sf.metadata == null ||
      (sf.metadata.contains(Constant.CHAR_TYPE_BASE_PROP) &&
        sf.metadata.getString(Constant.CHAR_TYPE_BASE_PROP).equalsIgnoreCase("clob")))

    val rs1 = st.executeQuery("select * from events_external limit 10")
    val rsmd1 = rs1.getMetaData
    // find out false clob types
    val falseClobs = for (i <- 1 to rsmd1.getColumnCount) yield {
      val toYield = if (!trueClobCols.exists(_.name.equalsIgnoreCase(rsmd1.getColumnName(i)))) {
        if (rsmd1.getColumnType(i) == Types.CLOB) {
          rsmd1.getColumnName(i)
        } else {
          ""
        }
      } else {
        ""
      }
      toYield
    }.filterNot(_.isWhitespace)
    assertFalse(falseClobs.isEmpty)

    st.execute("create table test1  " +
      s"using $tableType options(${Constant.STRING_TYPE_AS} 'varchar') as " +
      s"(select * from events_external limit 100)")
    val rs = st.executeQuery("select * from test1 limit 10")
    val rsmd = rs.getMetaData
    for (i <- 1 to rsmd.getColumnCount) {
      if (falseClobs.exists(_.equalsIgnoreCase(rsmd.getColumnName(i)))) {
        assertEquals(Types.VARCHAR, rsmd.getColumnType(i))
        assertEquals(Limits.DB2_VARCHAR_MAXWIDTH, rsmd.getPrecision(i))
      } else if (trueClobCols.exists(_.name.equalsIgnoreCase(rsmd.getColumnName(i)))) {
        assertTrue(rsmd.getColumnType(i) == Types.CLOB)
      }
    }
    st.execute("drop table if exists events_external")
    st.execute("drop table if exists test1")
  }

  test("row table created using CTAS from column table should respect stringtype option") {
    val conn = getSqlConnection
    val st = conn.createStatement()
    st.execute("drop table if exists test1")
    val eventsPath: String = getClass.getResource("/events_clob.parquet").getPath

    st.execute(s"create external table events_external using " +
      s"parquet options (path '$eventsPath')")

    val extSchema = snc.table("events_external").schema
    val trueClobCols = extSchema.filter(sf => sf.metadata == null ||
      (sf.metadata.contains(Constant.CHAR_TYPE_BASE_PROP) &&
        sf.metadata.getString(Constant.CHAR_TYPE_BASE_PROP).equalsIgnoreCase("clob")))

    val rs1 = st.executeQuery("select * from events_external limit 10")
    val rsmd1 = rs1.getMetaData
    // find out false clob types
    val falseClobs = for (i <- 1 to rsmd1.getColumnCount) yield {
      val toYield = if (!trueClobCols.exists(_.name.equalsIgnoreCase(rsmd1.getColumnName(i)))) {
        if (rsmd1.getColumnType(i) == Types.CLOB) {
          rsmd1.getColumnName(i)
        } else {
          ""
        }
      } else {
        ""
      }
      toYield
    }.filterNot(_.isWhitespace)
    assertFalse(falseClobs.isEmpty)

    st.execute("create table test1  " +
      s"using column  as " +
      s"(select * from events_external limit 100)")

    // now create row table
    st.execute("create table test2  " +
      s"using row options(${Constant.STRING_TYPE_AS} 'varchar') as " +
      s"(select * from test1 limit 100)")
    val rs2 = st.executeQuery("select * from test2 limit 10")
    val rsmd2 = rs2.getMetaData
    for (i <- 1 to rsmd2.getColumnCount) {
      if (falseClobs.exists(_.equalsIgnoreCase(rsmd2.getColumnName(i)))) {
        assertEquals(Types.VARCHAR, rsmd2.getColumnType(i))
        assertEquals(Limits.DB2_VARCHAR_MAXWIDTH, rsmd2.getPrecision(i))
      } else if (trueClobCols.exists(_.name.equalsIgnoreCase(rsmd2.getColumnName(i)))) {
        assertTrue(rsmd2.getColumnType(i) == Types.CLOB)
      }
    }
    st.execute("drop table if exists events_external")
    st.execute("drop table if exists test1")
    st.execute("drop table if exists test2")
  }

  test("column table created using CTAS from row table should NOT respect stringtype option") {
    val conn = getSqlConnection
    val st = conn.createStatement()
    st.execute("drop table if exists test1")
    val eventsPath: String = getClass.getResource("/events_clob.parquet").getPath

    st.execute(s"create external table events_external using " +
      s"parquet options (path '$eventsPath')")

    val extSchema = snc.table("events_external").schema
    val trueClobCols = extSchema.filter(sf => sf.metadata == null ||
      (sf.metadata.contains(Constant.CHAR_TYPE_BASE_PROP) &&
        sf.metadata.getString(Constant.CHAR_TYPE_BASE_PROP).equalsIgnoreCase("clob")))

    val rs1 = st.executeQuery("select * from events_external limit 10")
    val rsmd1 = rs1.getMetaData
    // find out false clob types
    val falseClobs = for (i <- 1 to rsmd1.getColumnCount) yield {
      val toYield = if (!trueClobCols.exists(_.name.equalsIgnoreCase(rsmd1.getColumnName(i)))) {
        if (rsmd1.getColumnType(i) == Types.CLOB) {
          rsmd1.getColumnName(i)
        } else {
          ""
        }
      } else {
        ""
      }
      toYield
    }.filterNot(_.isWhitespace)
    assertFalse(falseClobs.isEmpty)

    st.execute("create table test1  " +
      s"using row options(${Constant.STRING_TYPE_AS} 'varchar') as " +
      s"(select * from events_external limit 100)")

    val rs = st.executeQuery("select * from test1 limit 10")
    val rsmd = rs.getMetaData
    for (i <- 1 to rsmd.getColumnCount) {
      if (falseClobs.exists(_.equalsIgnoreCase(rsmd.getColumnName(i)))) {
        assertEquals(Types.VARCHAR, rsmd.getColumnType(i))
        assertEquals(Limits.DB2_VARCHAR_MAXWIDTH, rsmd.getPrecision(i))
      } else if (trueClobCols.exists(_.name.equalsIgnoreCase(rsmd.getColumnName(i)))) {
        assertTrue(rsmd.getColumnType(i) == Types.CLOB)
      }
    }

    // now create column table
    st.execute("create table test2  " +
      s"using column options(${Constant.STRING_TYPE_AS} 'varchar') as " +
      s"(select * from test1 limit 100)")
    val rs2 = st.executeQuery("select * from test2 limit 10")
    val rsmd2 = rs2.getMetaData
    for (i <- 1 to rsmd2.getColumnCount) {
      if (falseClobs.exists(_.equalsIgnoreCase(rsmd2.getColumnName(i)))) {
        assertEquals(Types.VARCHAR, rsmd2.getColumnType(i))
        assertEquals(Limits.DB2_VARCHAR_MAXWIDTH, rsmd2.getPrecision(i))
      } else if (trueClobCols.exists(_.name.equalsIgnoreCase(rsmd2.getColumnName(i)))) {
        assertTrue(rsmd2.getColumnType(i) == Types.CLOB)
      }
    }
    st.execute("drop table if exists events_external")
    st.execute("drop table if exists test1")
    st.execute("drop table if exists test2")
  }

  test("datatype as varchar only") {
    val conn = getSqlConnection
    val st = conn.createStatement()
    st.execute("drop table if exists test1")
    st.execute("create table test1(id int,name varchar) using column")
    st.execute("drop table if exists test1")
    st.execute("create table test1(id int,name varchar) using row")
    st.execute("drop table if exists test1")
    snc.sql("create table test1(id int,name varchar) using column")
    snc.sql("drop table if exists test1")
    st.execute("drop table if exists test1")
    snc.sql("create table test1(id int,name varchar) using row")
    snc.sql("drop table if exists test1")
  }

  test("row table created using CTAS from row table should NOT respect stringtype option") {
    val conn = getSqlConnection
    val st = conn.createStatement()
    st.execute("drop table if exists test1")
    val eventsPath: String = getClass.getResource("/events_clob.parquet").getPath

    st.execute(s"create external table events_external using " +
      s"parquet options (path '$eventsPath')")

    val extSchema = snc.table("events_external").schema
    val trueClobCols = extSchema.filter(sf => sf.metadata == null ||
      (sf.metadata.contains(Constant.CHAR_TYPE_BASE_PROP) &&
        sf.metadata.getString(Constant.CHAR_TYPE_BASE_PROP).equalsIgnoreCase("clob")))

    val rs1 = st.executeQuery("select * from events_external limit 10")
    val rsmd1 = rs1.getMetaData
    // find out false clob types
    val falseClobs = for (i <- 1 to rsmd1.getColumnCount) yield {
      val toYield = if (!trueClobCols.exists(_.name.equalsIgnoreCase(rsmd1.getColumnName(i)))) {
        if (rsmd1.getColumnType(i) == Types.CLOB) {
          rsmd1.getColumnName(i)
        } else {
          ""
        }
      } else {
        ""
      }
      toYield
    }.filterNot(_.isWhitespace)
    assertFalse(falseClobs.isEmpty)

    st.execute("create table test1  " +
      s"using row options(${Constant.STRING_TYPE_AS} 'varchar') as " +
      s"(select * from events_external limit 100)")

    val rs = st.executeQuery("select * from test1 limit 10")
    val rsmd = rs.getMetaData
    for (i <- 1 to rsmd.getColumnCount) {
      if (falseClobs.exists(_.equalsIgnoreCase(rsmd.getColumnName(i)))) {
        assertEquals(Types.VARCHAR, rsmd.getColumnType(i))
        assertEquals(Limits.DB2_VARCHAR_MAXWIDTH, rsmd.getPrecision(i))
      } else if (trueClobCols.exists(_.name.equalsIgnoreCase(rsmd.getColumnName(i)))) {
        assertTrue(rsmd.getColumnType(i) == Types.CLOB)
      }
    }

    // now create column table
    st.execute("create table test2  " +
      s"using row options(${Constant.STRING_TYPE_AS} 'varchar') as " +
      s"(select * from test1 limit 100)")
    val rs2 = st.executeQuery("select * from test2 limit 10")
    val rsmd2 = rs2.getMetaData
    for (i <- 1 to rsmd2.getColumnCount) {
      if (falseClobs.exists(_.equalsIgnoreCase(rsmd2.getColumnName(i)))) {
        assertEquals(Types.VARCHAR, rsmd2.getColumnType(i))
        assertEquals(Limits.DB2_VARCHAR_MAXWIDTH, rsmd2.getPrecision(i))
      } else if (trueClobCols.exists(_.name.equalsIgnoreCase(rsmd2.getColumnName(i)))) {
        assertTrue(rsmd2.getColumnType(i) == Types.CLOB)
      }
    }
    st.execute("drop table if exists test2")
    snc.sql("create table test2  " +
      s"using row options(${Constant.STRING_TYPE_AS} 'varchar') as " +
      s"(select * from test1 limit 100)")
    val rs3 = st.executeQuery("select * from test2 limit 10")
    val rsmd3 = rs3.getMetaData
    for (i <- 1 to rsmd3.getColumnCount) {
      if (falseClobs.exists(_.equalsIgnoreCase(rsmd3.getColumnName(i)))) {
        assertEquals(Types.VARCHAR, rsmd3.getColumnType(i))
        assertEquals(Limits.DB2_VARCHAR_MAXWIDTH, rsmd3.getPrecision(i))
      } else if (trueClobCols.exists(_.name.equalsIgnoreCase(rsmd3.getColumnName(i)))) {
        assertTrue(rsmd3.getColumnType(i) == Types.CLOB)
      }
    }
    st.execute("drop table if exists test2")
    st.execute("drop table if exists events_external")
    st.execute("drop table if exists test1")
  }

  def getSqlConnection: Connection =
    DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort2")

}
