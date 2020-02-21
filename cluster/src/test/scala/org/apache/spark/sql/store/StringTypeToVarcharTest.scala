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

import java.sql.{Connection, DriverManager, Types}

import com.pivotal.gemfirexd.TestUtil
import io.snappydata.SnappyFunSuite
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

  test("stringtypeas option in column table with schema") {
    snc.sql("drop table if exists test1")
    snc.sql("create table test1 (col1 string, col2 string, col3 string, col4 String) " +
      "using column options(stringTypeAs 'varchar(2048)')")
    snc.sql("insert into test1 values ('str1_1', 'str1_2', 'str1_3', 'str1_4' )," +
      "('str2_1', 'str2_2', 'str2_3', 'str2_4')")
    val conn = getSqlConnection
    val st = conn.createStatement()
    val rs = st.executeQuery("select * from test1")
    val rsmd = rs.getMetaData
    for(i <- 1 to rsmd.getColumnCount) {
      assertEquals(Types.VARCHAR, rsmd.getColumnType(i))
      assertEquals(2048, rsmd.getPrecision(i))
    }
    snc.dropTable("test1")
  }

  test("stringtypeas option in column table using CTAS") {
    snc.sql("drop table if exists test1")
    val eventsPath: String = getClass.getResource("/events.parquet").getPath

    snc.sql(s"create external table events_external using " +
      s"parquet options (path '$eventsPath')")
    snc.table("events_external").printSchema()
    val conn = getSqlConnection
    val st = conn.createStatement()
    val rs1 = st.executeQuery("select * from events_external limit 10")
    val rsmd1 = rs1.getMetaData
    for(i <- 1 to rsmd1.getColumnCount) {
      println(s"data type = ${rsmd1.getColumnTypeName(i)} and size = ${rsmd1.getPrecision(i)}")
    }
    snc.sql("create table test1  " +
      "using column options(stringTypeAs 'varchar(2048)') as (select * from events_external limit 100)")
    val rs = st.executeQuery("select * from test1 limit 10")
    val rsmd = rs.getMetaData
    for(i <- 1 to rsmd.getColumnCount) {
      //assertEquals(Types.VARCHAR, rsmd.getColumnType(i))
      //assertEquals(2048, rsmd.getPrecision(i))
      println(s"data type = ${rsmd.getColumnTypeName(i)} and size = ${rsmd.getPrecision(i)}")

    }
    snc.dropTable("test1")
  }

  def getSqlConnection: Connection =
    DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort2")

}
