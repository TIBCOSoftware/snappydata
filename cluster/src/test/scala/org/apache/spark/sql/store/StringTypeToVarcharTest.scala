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

import java.sql.{Connection, DriverManager}

import com.pivotal.gemfirexd.TestUtil
import io.snappydata.{Property, SnappyFunSuite}

import org.apache.spark.SparkConf

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

  test("single key string group by column with aggregagte function using grouping key") {

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
      println("data type = "+ rsmd.getColumnTypeName(i))
      println("size = "+ rsmd.getPrecision(i))
    }

  }

  def getSqlConnection: Connection =
    DriverManager.getConnection(s"jdbc:snappydata://$serverHostPort2")

}
