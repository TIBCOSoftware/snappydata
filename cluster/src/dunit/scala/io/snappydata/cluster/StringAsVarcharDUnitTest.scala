package io.snappydata.cluster

/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

import java.sql.{Connection, DriverManager}

import io.snappydata.test.dunit.AvailablePortHelper

import org.apache.spark.Logging
import org.apache.spark.sql.{SaveMode, SnappyContext}

/**
 * Tests for verifying rendering of STRING happens as VARCHAR or CLOB,
 * depending upon the conf property or query hint.
 */
class StringAsVarcharDUnitTest(val s: String)
    extends ClusterManagerTestBase(s) with Logging {

  val tableName1 = "colTab1"

  val tableName2 = "colTab2"

  override def tearDown2(): Unit = {
    super.tearDown2()
  }

  private def getANetConnection(netPort: Int,
      useGemXDURL: Boolean = false): Connection = {
    val driver = "com.pivotal.gemfirexd.jdbc.ClientDriver"
    Class.forName(driver).newInstance //scalastyle:ignore
    var url: String = null
    if (useGemXDURL) {
      url = "jdbc:gemfirexd://localhost:" + netPort + "/"
    } else {
      url = "jdbc:snappydata://localhost:" + netPort + "/"
    }

    DriverManager.getConnection(url)
  }

  def testSelectQuery(): Unit = {
    executeQuery("none")
  }

  def testSelectQueryWithQueryHint(): Unit = {
    executeQuery("hint")
  }

  def testSelectQueryWithSQLConf(): Unit = {
    executeQuery("sqlConf")
  }

  def testSelectJoinQuery(): Unit = {
    executeQuery("none", true)
  }

  def testSelectJoinQueryWithQueryHint(): Unit = {
    executeQuery("hint", true)
  }

  def testSelectJoinQueryWithSQLConf(): Unit = {
    executeQuery("sqlConf", true)
  }

  def executeQuery(conf: String, join: Boolean = false): Unit = {
    var stringType = "VARCHAR"
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)

    createTableAndInsertData(conn)

    val s = conn.createStatement()
    var (query, count, cols, tName) = join match {
      case true => (s"select t1.col_int, t1.col_string, t2.col_varchar, t2.col_clob, t2.col_char " +
          s"from $tableName1 t1, $tableName2 t2 where t1.col_int = t2.col_int", 2, 5, "T1")
      case false => (s"select * from $tableName2", 2, 5, tableName2)
    }
    conf match {
      case "none" =>
      case "hint" => query += " --+ stringAsClob(1)"
        stringType = "CLOB"
      case "sqlConf" => s.execute("set spark.sql.stringAsClob=true")
        stringType = "CLOB"
      case _ => throw new IllegalArgumentException(s"Invalid argument $conf")
    }

    s.executeQuery(query)
    val rs = s.getResultSet
    var rows = 0
    while (rs.next()) {
      rows += 1
      if (join) {
        if (rs.getInt(1) == 1) {
          assert(rs.getString(2).equals("t1.1.string"), s"actual string ${rs.getString(2)}")
          assert(rs.getString(4).equals("t2.1.clob"), s"actual clob ${rs.getString(4)}")
        } else {
          assert(rs.getInt(1) == 4, s"actual int ${rs.getInt(1)}")
          assert(rs.getString(2).equals("t1.4.string"), s"actual string ${rs.getString(2)}")
          assert(rs.getString(3).equals("t2.4.varchar"), s"actual varchar ${rs.getString(3)}")
        }
      } else {
        if (rs.getInt(1) == 1) {
          assert(rs.getString(2).equals("t2.1.string"), s"actual string ${rs.getString(2)}")
          assert(rs.getString(3).equals("t2.1.varchar"), s"actual varchar ${rs.getString(3)}")
        } else {
          assert(rs.getInt(1) == 4, s"actual int ${rs.getInt(1)}")
          assert(rs.getString(2).equals("t2.4.string"), s"actual string ${rs.getString(2)}")
          assert(rs.getString(4).equals("t2.4.clob"), s"actual clob ${rs.getString(4)}")
        }
      }
    }
    assert(rows == count)

    val md = rs.getMetaData
    assert(md.getColumnCount == cols)
    logInfo("metadata col cnt = " + md.getColumnCount)
    var i = 0
    while (i < cols) {
      i += 1
      logInfo("col name = " + md.getColumnName(i) +
          ", col type " + md.getColumnTypeName(i) + ", col table name = " + md.getTableName(i))
    }
    assert(md.getColumnName(1).equals("COL_INT"))
    assert(md.getColumnTypeName(1).equals("INTEGER"))

    assert(md.getColumnName(2).equals("COL_STRING"))
    assert(md.getColumnTypeName(2).equals(stringType))

    assert(md.getColumnName(3).equals("COL_VARCHAR"))
    assert(md.getColumnTypeName(3).equals("VARCHAR"))

    assert(md.getColumnName(4).equals("COL_CLOB"))
    assert(md.getColumnTypeName(4).equals("CLOB"))

    assert(md.getColumnName(5).equals("COL_CHAR"))
    assert(md.getColumnTypeName(5).equals("VARCHAR"))

    assert(md.getTableName(1).equalsIgnoreCase(tName))

    conn.close()
  }

  def createTableAndInsertData(conn: Connection): Unit = {
    val snc = SnappyContext(sc)

    snc.sql(s"create table $tableName1 (col_int int, col_string string, " +
        "col_varchar varchar(100)) using column options(buckets '7')")

    snc.sql(s"create table $tableName2 (col_int int, col_string string, " +
        "col_varchar varchar(100), col_clob clob, col_char char(10)) using column options(buckets '7')")

    val data = Seq(Seq(1, "t1.1.string", "t1.1.varchar"), Seq(7, "t1.7.string", "t1.7.varchar"), Seq(9, "t1.9.string", "t1.9.varchar"),
      Seq(4, "t1.4.string", "t1.4.varchar"), Seq(5, "t1.5.string", "t1.5.varchar"))
    val rdd = sc.parallelize(data, data.length).map(s =>
      Data2(s.head.asInstanceOf[Int], s(1).toString, s(2).toString))
    val dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("column").mode(SaveMode.Append)
        .saveAsTable(tableName1)

    snc.sql(s"insert into $tableName2 values (1, 't2.1.string', 't2.1.varchar', 't2.1.clob', 't2.1.char')")
    snc.sql(s"insert into $tableName2 values (4, 't2.4.string', 't2.4.varchar', 't2.4.clob', 't2.4.char')")
  }

}

case class Data2(col1: Int, col2: String, col3: String)

