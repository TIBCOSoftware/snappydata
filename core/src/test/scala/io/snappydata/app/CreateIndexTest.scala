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
import org.apache.spark.sql.catalyst.expressions.{Descending, Ascending}

import org.apache.spark.sql.{Row, SaveMode}

class CreateIndexTest extends SnappyFunSuite {

  test("Test create Index on Column Table using Snappy API") {
    val tableName : String = "tcol1"
    val snContext = org.apache.spark.sql.SnappyContext(sc)

    val props = Map (
      "PARTITION_BY" -> "col1")
    snContext.sql("drop table if exists " + tableName)

    val data = Seq(Seq(111, "aaaaa"), Seq(222, "bbb"))
    val rdd = sc.parallelize(data, data.length).map(s =>
      new Data1(s(0).asInstanceOf[Int], s(1).asInstanceOf[String]))
    val dataDF = snContext.createDataFrame(rdd)
    val x = dataDF.schema
    snContext.createTable("table2", "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).options(props).saveAsTable(tableName)

    doPrint("Verify index create and drop for various index types")
    snContext.sql("create index test1 on " + tableName + " (COL1)")
    snContext.sql("create index test2 on " + tableName +
      s" (COL1) Options (colocate_with  '$tableName')")
    try {
      snContext.sql(s"drop table $tableName")
      fail("This should fail as there are indexes associated with this table")
    } catch {
      case e: Throwable =>
    }
    snContext.sql("drop index test1")
    snContext.sql("drop index test2")
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

  }

  test("Test create Index on Row Table using Snappy API") {
    val snContext = org.apache.spark.sql.SnappyContext(sc)
    val tableName : String = "trow1"
    val props = Map ("PARTITION_BY" -> "col2")

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

  def verifyRows(r: Row) : Unit = {
    doPrint(r.toString())
    assert(r.toString() == "[111]", "got=" + r.toString() + " but expected 111")
  }

  def doPrint(s: Any): Unit = {
    //println(s)
  }
}
