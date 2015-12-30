/*
 * Copyright (c) 2010-2016 SnappyData, Inc. All rights reserved.
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
package io.snappydata.app

import io.snappydata.SnappyFunSuite

import org.apache.spark.sql.{Row, SaveMode}

/**
 * Created by vivekb on 27/10/15.
 */
class CreateIndexTest extends SnappyFunSuite {

  test("Test create Index on Column Table using Snappy API") {
    val tableName : String = "tcol1"
    val snContext = org.apache.spark.sql.SnappyContext(sc)

    val props = Map(
      "url" -> "jdbc:snappydata:;mcast-port=33619;user=app;password=app;persist-dd=false",
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "user" -> "app",
      "password" -> "app"
    )

    snContext.sql("drop table if exists " + tableName)

    val data = Seq(Seq(111,"aaaaa"), Seq(222,""))
    val rdd = sc.parallelize(data, data.length).map(s => new Data1(s(0).asInstanceOf[Int], s(1).asInstanceOf[String]))
    val dataDF = snContext.createDataFrame(rdd)
    snContext.createExternalTable(tableName, "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    val result = snContext.sql("select col1 from " +
        tableName +
        " where col2 like '%a%'")
    doPrint("")
    doPrint("=============== RESULTS START ===============")
    result.collect.foreach(verifyRows)
    doPrint("=============== RESULTS END ===============")

    try {
      snContext.sql("create index test1 on " + tableName + " (COL1)")
      fail("Should not create index on column table")
    } catch {
      case ae: org.apache.spark.sql.AnalysisException => // ignore
      case e: Exception => throw e
    }

    try {
      snContext.sql("drop index test1 ")
      fail("Should not drop index on column table")
    } catch {
      case ae: com.pivotal.gemfirexd.internal.impl.jdbc.EmbedSQLException => // ignore
      case ax:  com.pivotal.gemfirexd.internal.impl.jdbc.SQLExceptionFactory40.EmbedSQLSyntaxErrorException => // ignore
      case e: Exception => throw e
    }
  }

  test("Test create Index on Row Table using Snappy API") {
    val snContext = org.apache.spark.sql.SnappyContext(sc)
    val tableName : String = "trow1"
    val props = Map(
      "url" -> "jdbc:snappydata:;mcast-port=33619;user=app;password=app;persist-dd=false",
      "driver" -> "com.pivotal.gemfirexd.jdbc.EmbeddedDriver",
      "user" -> "app",
      "password" -> "app"
    )

    snContext.sql("drop table if exists " + tableName)

    val data = Seq(Seq(111,"aaaaa"), Seq(222,""))
    val rdd = sc.parallelize(data, data.length).map(s => new Data1(s(0).asInstanceOf[Int], s(1).asInstanceOf[String]))
    val dataDF = snContext.createDataFrame(rdd)
    snContext.createExternalTable(tableName, "jdbc", dataDF.schema, props)
    dataDF.write.format("jdbc").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    doPrint("Create Index - Start")
    snContext.sql("create index test1 on " + tableName + " (COL1)")
    doPrint("Create Index - Done")

    // TODO fails if column name not in caps
    val result = snContext.sql("select COL1 from " +
        tableName +
        " where COL2 like '%a%'")
    doPrint("")
    doPrint("=============== RESULTS START ===============")
    result.collect.foreach(doPrint)
    result.collect.foreach(verifyRows)
    doPrint("=============== RESULTS END ===============")

    doPrint("Drop Index - Start")
    snContext.sql("drop index test1")
    doPrint("Drop Index - Done")
  }

  def verifyRows(r: Row) : Unit = {
    doPrint(r.toString())
    assert(r.toString() == "[111]", "got=" + r.toString() + " but expected 111")
  }

  def doPrint(s: Any): Unit = {
    //println(s)
  }
}
