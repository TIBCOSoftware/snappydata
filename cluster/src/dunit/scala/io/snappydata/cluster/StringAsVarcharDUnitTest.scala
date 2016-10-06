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

import java.sql.Connection

import io.snappydata.Constant
import io.snappydata.test.dunit.AvailablePortHelper

import org.apache.spark.Logging
import org.apache.spark.sql.{SaveMode, SnappyContext}

/**
 * Tests for verifying rendering of STRING happens as VARCHAR or CLOB,
 * depending upon the query hint.
 */
class StringAsVarcharDUnitTest(val s: String)
    extends ClusterManagerTestBase(s) with Logging {

  val colTab1 = "colTab1"
  val rowTab1 = "rowTab1"

  val varcharSize = 20;
  val charSize = 10;

  /**
   * Test 'select *' and 'select cast(* as)' queries on a column table, without query hint.
   */
  def testQueries(): Unit = {
    executeQuery()
  }

  /**
   * Test 'select *' and 'select cast(* as)' queries on a column table, with query hint with
   * specific column names.
   */
  def testQueriesWithHint(): Unit = {
    executeQuery("col_string,col_varchar")
  }

  /**
   * Test 'select *' and 'select cast(* as)' queries on a column table, with query hint '*'.
   */
  def testQueriesWithHintAll(): Unit = {
    executeQuery("*")
  }

  /**
   * Test 'select *' and 'select cast(* as)' queries on a column table, with invalid query hint.
   */
  def testQueriesWithInvalidHint(): Unit = {
    executeQuery("inv,lid")
  }

  /**
   * Test select query on join of row table with column table, and 'select cast(* as)' query on a
   * column table, without query hint.
   */
  def testJoinQuery(): Unit = {
    executeQuery("FALSE", true)
  }

  /**
   * Test select query on join of row table with column table, and 'select cast(* as)' query on a
   * column table, with query hint '*'.
   */
  def testJoinQueryWithHint(): Unit = {
    executeQuery("*", true)
  }

  def executeQuery(hint: String = "FALSE", join: Boolean = false): Unit = {
    var stringType = "VARCHAR"
    var affix = ""
    val netPort1 = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort1)
    val conn = getANetConnection(netPort1)

    createTablesAndInsertData(conn)

    val s = conn.createStatement()

    def getType(value: String): String = value match {
      case "*" => "CLOB"
      case s: String => s.contains("col_string") match {
        case true => "CLOB"
        case false => "VARCHAR"
      }
    }

    hint match {
      case "FALSE" =>
      case _ =>
        affix = s" --+ columnsAsClob($hint)"
        logInfo(s"affix: '$affix'")
        stringType = getType(hint)
    }

    if (!join) {
      s.executeQuery(s"select * from $colTab1 $affix")
      val rs = s.getResultSet
      verify(rs, 5, stringType, colTab1)
    } else {
      s.execute(s"select t1.col_int, t1.col_string, t1.col_varchar, t2.col_clob, t2.col_string " +
          s"from $rowTab1 t1, $colTab1 t2 where t1.col_int = t2.col_int $affix")
      val rs = s.getResultSet
      verify(rs, 5, stringType, "T1", true)

      // Verify result
      while (rs.next()) {
        if (rs.getInt(1) == 1) {
          assert(rs.getString(2).equals("t1.1.string"), s"actual string ${rs.getString(2)}")
          assert(rs.getString(3).equals("t1.1.varchar"), s"actual varchar ${rs.getString(3)}")
        } else if (rs.getInt(1) == 4) {
          assert(rs.getInt(1) == 4, s"actual int ${rs.getInt(1)}")
          assert(rs.getString(2).equals("t1.4.string"), s"actual string ${rs.getString(2)}")
          assert(rs.getString(4).equals("t2.4.clob"), s"actual clob ${rs.getString(4)}")
        }
      }
    }
    s.executeQuery(s"select cast(col_int as string), cast(col_string as clob), " +
        s"cast(col_char as varchar(100)) from $colTab1 $affix")
    val rSet = s.getResultSet
    var count = 0
    while (rSet.next()) {
      count += 1
    }
    assert(count == 2)

    conn.close()
  }

  /**
   * Verify the metadata of the result set.
   *
   * @param rs
   * @param cols
   * @param stringType
   * @param tName
   * @param join
   */
  private def verify(rs: java.sql.ResultSet, cols: Int,
      stringType: String, tName: String, join: Boolean = false): Unit = {
    val md = rs.getMetaData
    assert(md.getColumnCount == cols)
    logInfo("metadata col cnt = " + md.getColumnCount)
    for (i <- 1 to cols) {
      logInfo("col name = " + md.getColumnName(i) + ", col type " +
          md.getColumnTypeName(i) + ", col table name = " + md.getTableName(i))
    }
    assertMetaData(md, stringType, tName, join)
  }

  private def assertMetaData(md: java.sql.ResultSetMetaData,
      stringType: String, tName: String, join: Boolean = false): Unit = {
    assert(md.getColumnName(1).equals("COL_INT"))
    assert(md.getColumnTypeName(1).equals("INTEGER"))

    assert(md.getColumnName(2).equals("COL_STRING"))
    if (join) {
      assert(md.getColumnTypeName(2).equals("CLOB")) // row table
    } else {
      assert(md.getColumnTypeName(2).equals(stringType))
      if (stringType.equals("VARCHAR")) {
        assert(md.getPrecision(2) == Constant.MAX_VARCHAR_SIZE)
      }
    }

    assert(md.getColumnName(3).equals("COL_VARCHAR"))
    assert(md.getColumnTypeName(3).equals("VARCHAR"))
    assert(md.getPrecision(3) == varcharSize)

    assert(md.getColumnName(4).equals("COL_CLOB"))
    assert(md.getColumnTypeName(4).equals("CLOB"))

    if (join) {
      assert(md.getColumnName(5).equals("COL_STRING"))
      assert(md.getColumnTypeName(5).equals(stringType))
      if (stringType.equals("VARCHAR")) {
        assert(md.getPrecision(5) == Constant.MAX_VARCHAR_SIZE)
      }
    } else {
      assert(md.getColumnName(5).equals("COL_CHAR"))
      assert(md.getColumnTypeName(5).equals("CHAR"))
      assert(md.getPrecision(5) == charSize)
    }

    assert(md.getTableName(1).equalsIgnoreCase(tName))
  }

  /**
   * Create a row table and a column table with five columns each. Row table has five entries while
   * the column table has just two entries.
   * 
   * @param conn
   */
  def createTablesAndInsertData(conn: Connection): Unit = {
    val snc = SnappyContext(sc)

    snc.sql(s"create table $rowTab1 (col_int int, col_string string, " +
        s"col_varchar varchar($varcharSize), col_clob clob, col_char char($charSize)) using row")

    snc.sql(s"create table $colTab1 (col_int int, col_string string, " +
        s"col_varchar varchar($varcharSize), col_clob clob, col_char char($charSize)) " +
        "using column options(buckets '7')")

    val data = Seq(Seq(1, "t1.1.string", "t1.1.varchar", "t1.1.clob", "t1.1.char"),
      Seq(7, "t1.7.string", "t1.7.varchar", "t1.7.clob", "t1.7.char"),
      Seq(9, "t1.9.string", "t1.9.varchar", "t1.9.clob", "t1.9.char"),
      Seq(4, "t1.4.string", "t1.4.varchar", "t1.4.clob", "t1.4.char"),
      Seq(5, "t1.5.string", "t1.5.varchar", "t1.5.clob", "t1.5.char"))

    val rdd = sc.parallelize(data, data.length).map(s =>
        Data9(s(0).asInstanceOf[Int], s(1).toString, s(2).toString, s(3).toString, s(4).toString))
    val dataDF = snc.createDataFrame(rdd)
    dataDF.write.format("row").mode(SaveMode.Append)
        .saveAsTable(rowTab1)

    snc.sql(s"insert into $colTab1 values (1, 't2.1.string', " +
        s"'t2.1.varchar', 't2.1.clob', 't2.1.char')")
    snc.sql(s"insert into $colTab1 values (4, 't2.4.string', " +
        s"'t2.4.varchar', 't2.4.clob', 't2.4.char')")
  }

}

case class Data9(col1: Int, col2: String, col3: String, col4: String, col5: String)

