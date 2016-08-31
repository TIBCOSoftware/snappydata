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
package io.snappydata.app

import io.snappydata.SnappyFunSuite

import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.{Row, SaveMode}

class CreateIndexTest extends SnappyFunSuite {

  test("Test create Index on Column Table using Snappy API") {
    val tableName: String = "tcol1"
    val snContext = org.apache.spark.sql.SnappyContext(sc)
    snContext.setConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")

    val props = Map(
      "PARTITION_BY" -> "col1")
    snContext.sql("drop table if exists " + tableName)

    val data = Seq(Seq(111, "aaa", "hello"),
      Seq(222, "bbb", "halo"),
      Seq(333, "aaa", "hello"),
      Seq(444, "bbb", "halo"),
      Seq(555, "ccc", "halo"),
      Seq(666, "ccc", "halo")
    )

    val table2 = "table2"
    val table3 = "table3"
    val rdd = sc.parallelize(data, data.length).map(s =>
      new Data2(s(0).asInstanceOf[Int], s(1).asInstanceOf[String], s(2).asInstanceOf[String]))
    val dataDF = snContext.createDataFrame(rdd)
    val props2 = props + ("PARTITION_BY" -> "col2,col3")
    snContext.createTable(s"$table2", "column", dataDF.schema, props2)

    snContext.createTable(s"$table3", "column", dataDF.schema, props)

    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)
    dataDF.write.insertInto(table2)
    dataDF.write.insertInto(table2)
    dataDF.write.insertInto(table3)

    doPrint("Verify index create and drop for various index types")
    snContext.sql("create index test1 on " + tableName + " (COL1)")
    snContext.sql("create index test2 on " + tableName +
        s" (COL1) Options (colocate_with  '$table3')")

    snContext.sql("create index test3 on " + tableName + " (COL2, COL3)")
    snContext.sql("create index test4 on " + tableName +
        s" (COL2, COL3) Options (colocate_with  '$table2')")

    def executeQ(sqlText: String): Unit =
    {
      var selectRes = snContext.sql(sqlText)
      selectRes.explain(true)
      selectRes.show
    }

    executeQ("select * from tcol1 where col1 = 111")

    executeQ("select * from tcol1 where col2 = 'aaa' ")

    executeQ("select * from tcol1 where col2 = 'bbb' and col3 = 'halo' ")

    executeQ(s"select * from $tableName tab1 " +
        s"join $table3 tab2 on tab1.col1 = tab2.col1")

    executeQ(s"select * from $tableName tab1 " +
        s"join $table3 tab2 on tab1.col1 = tab2.col1 where tab1.col1 = 111 ")

    try {
      snContext.sql(s"drop table $tableName")
      fail("This should fail as there are indexes associated with this table")
    } catch {
      case e: Throwable =>
    }
    snContext.sql("drop index test1")
    snContext.sql("drop index test2")
    snContext.sql("drop index test3")
    snContext.sql("drop index test4")
    try {
      snContext.sql("create index a1.test1 on " + tableName + " (COL1)")
      fail("This should fail as the index should have same database as the table")
    } catch {
      case e: Throwable =>
    }

    snContext.sql("create index test1 on " + tableName + " (COL1 asc)")
    snContext.sql("drop index test1")

    snContext.createIndex("test1", tableName,
      Map(("col1" -> None)), Map("colocate_with" -> tableName))
    snContext.dropIndex("test1", false)
    snContext.createIndex("test1", tableName, Map(("col1" -> None)),
      Map.empty[String, String])
    snContext.dropIndex("test1", false)

    snContext.createIndex("test1", tableName, Map(("col1" -> Some(Ascending))),
      Map.empty[String, String])
    snContext.dropIndex("test1", false)

    // drop non-existent indexes with if exist clause
    snContext.dropIndex("test1", true)
    snContext.sql("drop index if exists test1")

    snContext.sql(s"drop table $tableName")
    snContext.sql(s"drop table $table2")
  }

  test("Test create Index on Row Table using Snappy API") {
    val snContext = org.apache.spark.sql.SnappyContext(sc)
    val tableName: String = "trow1"
    val props = Map("PARTITION_BY" -> "col2")

    val data = Seq(Seq(111, "aaaaa"), Seq(222, ""))
    val rdd = sc.parallelize(data, data.length).
        map(s => new Data1(s(0).asInstanceOf[Int], s(1).asInstanceOf[String]))
    val dataDF = snContext.createDataFrame(rdd)
    snContext.createTable(tableName, "row", dataDF.schema, props)
    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    doPrint("Verify index create and drop for various index types")
    snContext.sql("create index test1 on " + tableName + " (COL1)")
    snContext.sql("drop index test1")
    snContext.sql("create unique index test1 on " + tableName + " (COL1)")
    snContext.sql("drop index test1")
    snContext.sql("create global hash index test1 on " + tableName + " (COL1)")
    snContext.sql("drop index test1")
    snContext.sql("create index test1 on " + tableName + " (COL1 asc)")
    snContext.sql("drop index test1")


    snContext.createIndex("test1", tableName, Map(("col1" -> None)), Map("index_type" -> "unique"))
    snContext.dropIndex("test1", false)
    snContext.createIndex("test1", tableName,
      Map(("col1" -> None)), Map("index_type" -> "global hash"))
    snContext.dropIndex("test1", false)
    snContext.createIndex("test1", tableName, Map(("col1" -> None)),
      Map.empty[String, String])
    snContext.dropIndex("test1", false)

    snContext.createIndex("test1", tableName,
      Map(("col1" -> Some(Descending))), Map("index_type" -> "unique"))
    snContext.dropIndex("test1", false)

    // drop non-existent indexes with if exist clause
    snContext.dropIndex("test1", true)
    snContext.sql("drop index if exists test1")


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

  def verifyRows(r: Row): Unit = {
    doPrint(r.toString())
    assert(r.toString() == "[111]", "got=" + r.toString() + " but expected 111")
  }

  def doPrint(s: Any): Unit = {
    //println(s)
  }
}
