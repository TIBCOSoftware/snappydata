/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import scala.collection.mutable

import io.snappydata.Property

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.memory.SnappyUnifiedMemoryManager
import org.apache.spark.sql.execution.columnar.ColumnTableScan
import org.apache.spark.sql.SnappySession

/**
 * Tests for column table having sorted columns.
 */
class SortedColumnTests extends ColumnTablesTestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    stopAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    stopAll()
  }

  override protected def newSparkConf(addOn: (SparkConf) => SparkConf): SparkConf = {
    val conf = new SparkConf()
    conf.setIfMissing("spark.master", "local[*]")
        .setAppName(getClass.getName)
    conf.set("snappydata.store.critical-heap-percentage", "95")
    if (SnappySession.isEnterpriseEdition) {
      conf.set("snappydata.store.memory-size", "1200m")
    }
    conf.set("spark.memory.manager", classOf[SnappyUnifiedMemoryManager].getName)
    conf.set("spark.serializer", "org.apache.spark.serializer.PooledKryoSerializer")
    conf.set("spark.closure.serializer", "org.apache.spark.serializer.PooledKryoSerializer")
    conf
  }

  test("basic insert") {
    SortedColumnTests.testBasicInsert(this.snc.snappySession)
  }
}

object SortedColumnTests extends Logging {

  def testBasicInsert(session: SnappySession): Unit = {
    session.conf.set(Property.ColumnMaxDeltaRows.name, "100")

    // To force SMJ
    session.conf.set(Property.HashJoinSize.name, "-1")
    session.conf.set(Property.PutIntoInnerJoinCacheSize.name, "-1")

    // Only use while debugging
    session.conf.set("spark.sql.autoBroadcastJoinThreshold", (-1).toString)

    val numElements = 551

    session.sql("drop table if exists colDeltaTable")

    session.sql("create table colDeltaTable (id int, addr string, status boolean) " +
        "using column options(buckets '2', partition_by 'id', key_columns 'id')")

    session.sql("create table row_table(id int, addr string, status boolean)")

    session.range(numElements).filter(_ % 10 < 6).selectExpr("id",
      "concat('addr', cast(id as string))",
      "case when (id % 2) = 0 then true else false end").write.insertInto("colDeltaTable")

    session.range(numElements).filter(_ % 10 > 5).selectExpr("id",
      "concat('addr', cast(id as string))",
      "case when (id % 2) = 0 then true else false end").write.insertInto("row_table")

    def verifyTotalRows(assertCount: Int, callCount: Int): Unit = {
      val rs1 = session.sql("select * from colDeltaTable").collect()
      // scalastyle:off println
      println("")
      println(s"verifyTotalRows $callCount = " + rs1.length)
      println("")
      // scalastyle:on println
      var i = 0
      val allRows = mutable.SortedSet[Int]()
      if (callCount == 2) {
        List.range(0, numElements).foreach(allRows += _)
      }
      rs1.foreach(r => {
        val firstRow = r.getInt(0)
        // scalastyle:off println
        println(s"verifyTotalRows : " + i + " = " + firstRow)
        // scalastyle:on println
        i = i + 1
        if (callCount == 2) {
          if (allRows.contains(firstRow)) {
            allRows.remove(firstRow)
          }
        }
      })
      if (callCount == 2) {
        // scalastyle:off println
        println(s"verifyTotalRows Remaining: " + allRows)
        // scalastyle:on println
      }
      // assert(rs1.length == assertCount, rs1.length)
    }

    try {
      val num2ndPhase = 220
      verifyTotalRows(numElements - num2ndPhase, 1)
      try {
        ColumnTableScan.setCaseOfSortedInsertValue(true)
        ColumnTableScan.setDebugMode(true)
        session.sql("put into table colDeltaTable select * from row_table")
      } finally {
        ColumnTableScan.setCaseOfSortedInsertValue(false)
        ColumnTableScan.setDebugMode(false)
      }
      verifyTotalRows(numElements, 2)
    } catch {
      case t: Throwable =>
        logError(t.getMessage, t)
        throw t
    }

    // Disable verifying rows in sorted order
    // def sorted(l: List[Row]) = l.isEmpty ||
    //    l.view.zip(l.tail).forall(x => x._1.getInt(0) <= x._2.getInt(0))
    // assert(sorted(rs2.toList))

    session.sql("drop table colDeltaTable")
    session.conf.unset(Property.ColumnBatchSize.name)
    session.conf.unset(Property.ColumnMaxDeltaRows.name)
  }
}
