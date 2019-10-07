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
package io.snappydata.gemxd

import java.util.Properties

import scala.util.control.NonFatal

import com.pivotal.gemfirexd.TestUtil
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.core.LocalSparkConf

import org.apache.spark.sql.{Row, SaveMode}

class BasicStoreSuite(s: String) extends TestUtil(s) {

  override protected def tearDown(): Unit = {
    if (Misc.getMemStoreBootingNoThrow == null) return
    val conn = TestUtil.getConnection("jdbc:snappydata:;", new Properties())
    try {
      conn.createStatement().execute("drop table if exists t1")
    } catch {
      case NonFatal(e) => // ignore
      case other: Throwable => throw other
    }
    conn.close()
  }

//  @throws[Exception]
//  def testStringAsDatatype_runInXD {
//    val conn: Connection = TestUtil.getConnection("jdbc:snappydata:;", new Properties())
//    val st: Statement = conn.createStatement
//    var rs: ResultSet = null
//    st.execute("create table t1 (c1 int primary key, c2 String)")
//    val pstmt: PreparedStatement = conn.prepareStatement("insert into t1 values(?,?)")
//    pstmt.setInt(1, 111)
//    pstmt.setString(2, "aaaaa")
//    pstmt.executeUpdate
//    pstmt.setInt(1, 222)
//    pstmt.setString(2, "")
//    pstmt.executeUpdate
//    st.execute("select c1 from t1 where c2 like '%a%'")
//    rs = st.getResultSet
//    System.out.println("rs=" + rs)
//    assert(rs.next == true)
//    val ret1: Int = rs.getInt(1)
//    //println("vivek = " + ret1)
//    assert(111 == ret1)
//    assert(rs.next == false)
//    conn.close()
//  }

  @throws[Exception]
  def testStringAsDatatype_runInSpark() {

    val sc = new org.apache.spark.SparkContext(LocalSparkConf.newConf())
    val snContext = org.apache.spark.sql.SnappyContext(sc)
    snContext.sql("set spark.sql.shuffle.partitions=6")

    val data = Seq(Seq(111, "aaaaa"), Seq(222, ""))
    val rdd = sc.parallelize(data, data.length).map(s =>
      new Data1(s(0).asInstanceOf[Int], s(1).asInstanceOf[String]))
    val dataDF = snContext.createDataFrame(rdd)

    /*
    val schemaString = "col1,col2"
    val schemaArr = schemaString.split(",")
    val schemaTypes = List(IntegerType, StringType)
    val schema = StructType(schemaArr.zipWithIndex.map {
      case (fieldName, i) => StructField(
        fieldName, schemaTypes(i), i >= 4)
    })
    */

    //dataDF.registerTempTable("t1")
    dataDF.write.format("column").mode(SaveMode.Ignore).
        options(Map.empty[String, String]).saveAsTable("t1")
    //snContext.registerAndInsertIntoExternalStore(dataDF, "t1", schema, props)
    //sc.createColumnTable("t1", dataDF.schema, "jdbcColumnar", props)
    //dataDF.write.format("jdbcColumnar").mode(SaveMode.Append).options(props).saveAsTable("t1")

    val result = snContext.sql("select col1 from t1 where col2 like '%a%'")
    doPrint("")
    doPrint("=============== RESULTS START ===============")
    result.collect().foreach(verifyRows)
    doPrint("=============== RESULTS END ===============")
    sc.stop()
  }

  def verifyRows(r: Row): Unit = {
    doPrint(r.toString())
    assert(r.toString() == "[111]", "got=" + r.toString() + " but expected 111")
  }

  // Copy from BugsTest to verify basic JUnit is running in Scala
//  @throws[Exception]
//  def testBug47329 {
//    val conn: Connection = TestUtil.getConnection("jdbc:snappydata:;", new Properties())
//    val st: Statement = conn.createStatement
//    var rs: ResultSet = null
//    st.execute("create table t1 (c1 int primary key, c2  varchar(10))")
//    st.execute("create index idx on  t1(c2)")
//    val pstmt: PreparedStatement = conn.prepareStatement("insert into t1 values(?,?)")
//    pstmt.setInt(1, 111)
//    pstmt.setString(2, "aaaaa")
//    pstmt.executeUpdate
//    pstmt.setInt(1, 222)
//    pstmt.setString(2, "")
//    pstmt.executeUpdate
//    st.execute("select c1 from t1 where c2 like '%a%'")
//    rs = st.getResultSet
//    System.out.println("rs=" + rs)
//    assert(rs.next == true)
//    val ret1: Int = rs.getInt(1)
//    doPrint("vivek = " + ret1)
//    assert(111 == ret1)
//    assert(rs.next == false)
//  }

  def doPrint(s: String): Unit = {
    //println(s)
  }
}

case class Data1(col1: Int, col2: String) extends Serializable
