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

package io.snappydata.cluster

import java.sql.{Connection, Statement}

import io.snappydata.Constant
import io.snappydata.test.dunit.AvailablePortHelper

import org.apache.spark.Logging
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SnappyContext}

/**
 * Tests for verifying rendering of STRING happens as VARCHAR or CLOB,
 * depending upon the query hint.
 */
class StringAsVarcharDUnitTest(val s: String)
    extends ClusterManagerTestBase(s) with Logging {

  val colTab1 = "colTab1"
  val rowTab1 = "rowTab1"
  val rowTab2 = "rowTab2"
  val extTab1 = "extTab1"
  val extTab2 = "extTab2"

  val varcharSize = 20
  val charSize = 10

  /**
   * Test 'select *' on column, row and external tables and 'select cast(* as)' on a column/row
   * tables, with different possible query hints. The tables are created via DDLs.
   */
  def testQueries(): Unit = {
    executeAndVerify()
  }

  /**
   * Test 'select *' on column, row and external tables and 'select cast(* as)' on a column/row
   * tables, with different possible query hints. The tables are created via APIs.
   */
  def testQueriesOnTablesCreatedViaAPI(): Unit = {
    executeAndVerify(false)
    validateUtilsFunctions()
  }

  def executeAndVerify(useDDL: Boolean = true, join: Boolean = false): Unit = {
    val netPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", netPort)
    val conn = getANetConnection(netPort)

    if (useDDL) {
      createTablesViaDDLAndInsertData(conn)
    } else {
      createTablesViaAPIAndInsertData(conn)
    }

    val s = conn.createStatement()

    Seq( // all possible hint values
      "FALSE", "*", "col_string,col_varchar", "inv,lid"
    ).foreach(str => runQueriesAndVerify(s, useDDL, str))

    conn.close()
  }

  def runQueriesAndVerify(s: Statement, useDDL: Boolean, hint: String): Unit = {
    var stringType = "VARCHAR"
    var affix = ""
    hint match {
      case "FALSE" =>
      case _ =>
        affix = s" --+ columnsAsClob($hint)"
        logInfo(s"affix: '$affix'")
        stringType = hint match {
          case "*" => "CLOB"
          case s: String => if (s.contains("col_string")) "CLOB" else "VARCHAR"
        }
    }

    def eNv(stmt: Statement, t: String, s: String, checkCount: Boolean = false, expectedCount:
    Int = 0): Unit = {
      stmt.executeQuery(s"select * from $t $affix")
      val rs = stmt.getResultSet
      verify(rs, 5, s, t, hint)
      if (checkCount) {
        var count = 0
        while (rs.next()) {
          count += 1
        }
        assert(count == expectedCount,
          s"Expected count = $expectedCount but got $count")
      }
    }

    eNv(s, colTab1, stringType)
    // row table metadata is from store so rs metadata shows CLOB
    eNv(s, rowTab1, "CLOB")
    eNv(s, extTab1, stringType, true, 0)
    if (!useDDL) {
      eNv(s, rowTab2, "CLOB")
      eNv(s, extTab2, stringType, true, 5)
    }

    def testCastOperator(s: Statement, t: String, expectedCount: Int): Unit = {
      s.executeQuery(s"select cast(col_int as string), cast(col_string as clob), " +
          s"cast(col_char as varchar(100)) from $t $affix")
      val rSet = s.getResultSet
      var count = 0
      while (rSet.next()) {
        count += 1
      }
      assert(count == expectedCount)
    }

    testCastOperator(s, colTab1, 2)
    testCastOperator(s, rowTab1, 5)
  }

  /**
   * Verify the metadata of the result set.
   */
  private def verify(rs: java.sql.ResultSet, cols: Int,
      stringType: String, tName: String, hint: String = "FALSE"): Unit = {
    val md = rs.getMetaData
    assert(md.getColumnCount == cols)
    logInfo(s"$tName metadata column count = ${md.getColumnCount}, " +
        s"hint = $hint expectedStringType = $stringType")
    for (i <- 1 to cols) {
      logInfo(s"col name = ${md.getColumnName(i)}, col type ${md.getColumnTypeName(i)}, table " +
          s"name = ${md.getTableName(i)}")
    }
    assertMetaData(md, stringType, tName)
  }

  private def assertMetaData(md: java.sql.ResultSetMetaData,
      stringType: String, tName: String): Unit = {
    assert(md.getColumnName(1).equalsIgnoreCase("COL_INT"))
    assert(md.getColumnTypeName(1).equals("INTEGER"))

    assert(md.getColumnName(2).equalsIgnoreCase("COL_STRING"))
    assert(md.getColumnTypeName(2).equals(stringType),
      s"Expected type to be $stringType but got ${md.getColumnTypeName(2)}")
    if (stringType.equals("VARCHAR")) {
      assert(md.getPrecision(2) == Constant.MAX_VARCHAR_SIZE)
    }

    assert(md.getColumnName(3).equalsIgnoreCase("COL_VARCHAR"))
    assert(md.getColumnTypeName(3).equals("VARCHAR"),
      s"Expected type to be VARCHAR but got ${md.getColumnTypeName(3)}")
    assert(md.getPrecision(3) == varcharSize)

    assert(md.getColumnName(4).equalsIgnoreCase("COL_CLOB"))
    assert(md.getColumnTypeName(4).equals("CLOB"),
      s"Expected type to be CLOB but got ${md.getColumnTypeName(4)}")

    assert(md.getColumnName(5).equalsIgnoreCase("COL_CHAR"))
    assert(md.getColumnTypeName(5).equals("CHAR"),
      s"Expected type to be CHAR but got ${md.getColumnTypeName(5)}")
    assert(md.getPrecision(5) == charSize)

    assert(md.getTableName(1).equalsIgnoreCase(tName),
      s"Expected $tName but got ${md.getTableName(1)}")
  }

  /**
   * Create a row table and a column table with five columns each. Row table has five entries while
   * the column table has just two entries.
   */
  def createTablesViaDDLAndInsertData(conn: Connection): Unit = {
    val snc = SnappyContext(sc)

    snc.sql(s"create table $rowTab1 (col_int int, col_string string, " +
        s"col_varchar varchar($varcharSize), col_clob clob, col_char char($charSize)) using row")

    snc.sql(s"create table $colTab1 (col_int int, col_string string, " +
        s"col_varchar varchar($varcharSize), col_clob clob, col_char char($charSize)) " +
        "using column options(buckets '8')")

    snc.sql(s"create external table $extTab1 (col_int int, col_string string, " +
        s"col_varchar varchar($varcharSize), col_clob clob, col_char char($charSize)) " +
        s"USING csv OPTIONS(path '${getClass.getResource("/empty.csv").getPath}')")

    insertData(snc)
  }

  /**
   * Create a row, column and external tables with five columns each via APIs. Column table
   * has two records while others have five records.
   *
   * @param conn
   */
  def createTablesViaAPIAndInsertData(conn: Connection): Unit = {
    val snc = SnappyContext(sc)

    val schema = StructType(Array(
      StructField("col_int", IntegerType, false),
      StructField("col_string", StringType, false),
      StructField("col_varchar", StringType, false, Utils.varcharMetadata(varcharSize)),
      StructField("col_clob", StringType, false, Utils.stringMetadata()),
      StructField("col_char", StringType, false, Utils.charMetadata(charSize))
    ))

    snc.createTable(rowTab1, "row", schema, Map.empty[String, String])

    snc.createTable(rowTab2, "row", s"(col_int int, col_string string, col_varchar varchar" +
        s"($varcharSize), col_clob clob, col_char char($charSize))",
      Map.empty[String, String], false)

    snc.createTable(colTab1, "column", schema, Map("buckets" -> "8"))

    snc.createExternalTable(extTab1, "csv", schema,
      Map("path" -> getClass.getResource("/empty.csv").getPath))

    val df = snc.read
        .format("com.databricks.spark.csv")
        .option("header", "false")
        .option("maxCharsPerColumn", "4096")
        .schema(schema)
        .load(getClass.getResource("/allstringtypes.csv").getPath)

    df.write.format("column").saveAsTable(extTab2)

    insertData(snc)
  }

  def validateUtilsFunctions(): Unit = {
    try {
      Utils.varcharMetadata(Constant.MAX_VARCHAR_SIZE + 1)
      assert(false, "Validation for Utils.varcharMetadata() failed")
    } catch {
      case iae: IllegalArgumentException => // ignore
      case t: Throwable => throw t
    }
    var md = Utils.varcharMetadata()
    assert(md.getString(Constant.CHAR_TYPE_BASE_PROP).equals("VARCHAR"))
    assert(md.getLong(Constant.CHAR_TYPE_SIZE_PROP) == Constant.MAX_VARCHAR_SIZE)

    try {
      Utils.charMetadata(Constant.MAX_CHAR_SIZE + 1)
      assert(false, "Validation for Utils.charMetadata() failed")
    } catch {
      case iae: IllegalArgumentException => // ignore
      case t: Throwable => throw t
    }
    md = Utils.charMetadata()
    assert(md.getString(Constant.CHAR_TYPE_BASE_PROP).equals("CHAR"))
    assert(md.getLong(Constant.CHAR_TYPE_SIZE_PROP) == Constant.MAX_CHAR_SIZE)

    md = Utils.stringMetadata()
    assert(md.getString(Constant.CHAR_TYPE_BASE_PROP).equals("CLOB"),
      "Validation for Utils.stringMetadata() failed")
  }

  def insertData(snc: SnappyContext): Unit = {
    // Insert into row table
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

    // Insert into column table
    snc.sql(s"insert into $colTab1 values (1, 't2.1.string', " +
        s"'t2.1.varchar', 't2.1.clob', 't2.1.char')")
    snc.sql(s"insert into $colTab1 values (4, 't2.4.string', " +
        s"'t2.4.varchar', 't2.4.clob', 't2.4.char')")
  }
}

case class Data9(col1: Int, col2: String, col3: String, col4: String, col5: String)
