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

import java.io.File
import java.sql.{Connection, DriverManager, Types}

import com.pivotal.gemfirexd.TestUtil
import com.pivotal.gemfirexd.internal.shared.common.reference.Limits
import io.snappydata.{Constant, SnappyFunSuite}
import org.junit.Assert._

import org.apache.spark.sql.types.{MetadataBuilder, StructType}


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

  test("stringtypeas option in column table with schema should not respect stringTypeAs") {
    snc.sql("drop table if exists test1")
    snc.sql("create table test1 (col1 char(12), col2 string, col3 string, col4 clob) " +
      "using column options(stringTypeAs 'varchar')")
    snc.sql("insert into test1 values ('str1_1', 'str1_2', 'str1_3', 'str1_4' )," +
      "('str2_1', 'str2_2', 'str2_3', 'str2_4')")
    val conn = getSqlConnection
    val st = conn.createStatement()
    val rs = st.executeQuery("select * from test1")
    val rsmd = rs.getMetaData
    for(i <- 2 to rsmd.getColumnCount -1) {
      assertEquals(Types.VARCHAR, rsmd.getColumnType(i))
      assertEquals(Limits.DB2_VARCHAR_MAXWIDTH, rsmd.getPrecision(i))
    }
    assertEquals(Types.CLOB, rsmd.getColumnType(rsmd.getColumnCount))
    assertEquals(Types.CHAR, rsmd.getColumnType(1))
    assertEquals(12, rsmd.getPrecision(1))

    snc.dropTable("test1")
  }

  test("stringtypeas option in column table using CTAS") {
    testOptionOnTableCreation(true)
  }

  test("stringtypeas option in row table using CTAS") {
    testOptionOnTableCreation(false)
  }


  def testOptionOnTableCreation(isColumn: Boolean): Unit = {
    val tableType = if (isColumn) "column" else "row"
    snc.sql("drop table if exists test1")
    val eventsPath: String = getClass.getResource("/events_clob.parquet").getPath

    snc.sql(s"create external table events_external using " +
      s"parquet options (path '$eventsPath')")
    val conn = getSqlConnection
    val st = conn.createStatement()
    val rs1 = st.executeQuery("select * from events_external limit 10")
    val rsmd1 = rs1.getMetaData
    // check if data type is clob
    for(i <- 1 to 2) {
      assertEquals(Types.CLOB, rsmd1.getColumnType(i))
    }
    snc.sql("create table test1  " +
      s"using $tableType options(stringTypeAs 'varchar') as " +
      s"(select * from events_external limit 100)")
    val rs = st.executeQuery("select * from test1 limit 10")
    val rsmd = rs.getMetaData
    for(i <- 1 to 2) {
      assertEquals(Types.VARCHAR, rsmd.getColumnType(i))
      assertEquals(Limits.DB2_VARCHAR_MAXWIDTH, rsmd.getPrecision(i))
    }
    for(i <- 3 to rsmd.getColumnCount) {
      assertFalse(rsmd.getColumnType(i) == Types.CLOB)
    }
    snc.dropTable("events_external")
    snc.dropTable("test1")
  }

  def getSqlConnection: Connection =
    DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort2")

}
