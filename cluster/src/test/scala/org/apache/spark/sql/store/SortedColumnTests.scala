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

import java.io.File

import scala.collection.mutable

import io.snappydata.Property

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.memory.SnappyUnifiedMemoryManager
import org.apache.spark.sql.execution.columnar.ColumnTableScan
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.snappy._

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
    val snc = this.snc.snappySession
    val generateData = false

    if (generateData) {
      val numElements = 999999551

      val dataDirInsert = new File(SortedColumnTests.filePathInsert(numElements))
      dataDirInsert.mkdir()
      snc.sql(s"create EXTERNAL TABLE insert_table(id int, addr string, status boolean)" +
          s" USING parquet OPTIONS(path '${SortedColumnTests.filePathInsert(numElements)}')")
      snc.range(numElements).filter(_ % 10 < 6).selectExpr("id", "concat('addr'," +
          "cast(id as string))",
        "case when (id % 2) = 0 then true else false end").write.insertInto("insert_table")

      val dataDirUpdate = new File(SortedColumnTests.filePathUpdate(numElements))
      dataDirUpdate.mkdir()
      snc.sql(s"create EXTERNAL TABLE update_table(id int, addr string, status boolean)" +
          s" USING parquet OPTIONS(path '${SortedColumnTests.filePathUpdate(numElements)}')")
      snc.range(numElements).filter(_ % 10 > 5).selectExpr("id", "concat('addr'," +
          "cast(id as string))",
        "case when (id % 2) = 0 then true else false end").write.insertInto("update_table")
    } else {
      SortedColumnTests.testBasicInsert(this.snc.snappySession)
    }
  }
}

object SortedColumnTests extends Logging {

  val baseDataPath = s"/home/vivek/work/testData/local_index"
  def filePathInsert(n: Long) : String = s"$baseDataPath/insert$n"
  def filePathUpdate(n: Long) : String = s"$baseDataPath/update$n"

  def testBasicInsert(session: SnappySession): Unit = {
    session.conf.set(Property.ColumnMaxDeltaRows.name, "100")

    // To force SMJ
    session.conf.set(Property.HashJoinSize.name, "-1")
    session.conf.set(Property.PutIntoInnerJoinCacheSize.name, "-1")

    // Only use while debugging
    session.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
    session.conf.set(SQLConf.WHOLESTAGE_FALLBACK.key, "false")

    val colTableName = "colDeltaTable"
    val numElements = 99999551
    val numBuckets = 2
    val doDebug = false

    def numFirstInserts(totalNum: Long) : Long = {
      val a = totalNum/10 * 6
      val c = (totalNum % 10).toInt
      val b = (1 to c).count(_ % 10 < 6)
      a + b
    }

    session.sql(s"drop table if exists $colTableName")
    session.sql(s"create table $colTableName (id int, addr string, status boolean) " +
        s"using column options(buckets '$numBuckets', partition_by 'id', key_columns 'id')")


    val insertDF = session.read.load(filePathInsert(numElements))
    insertDF.write.insertInto(colTableName)
    val updateDF = session.read.load(filePathUpdate(numElements))

    def verifyTotalRows(assertCount: Long, callCount: Int): Unit = {
      val rs1 = session.sql("select * from colDeltaTable").collect()
      // scalastyle:off println
      println("")
      println(s"verifyTotalRows $callCount expected=$assertCount actual=${rs1.length} ")
      // scalastyle:on println
      var i = 0
      val allRows = mutable.SortedSet[Int]()
      if (callCount == 2) {
        List.range(0, numElements).foreach(allRows += _)
      }
      var lastRow = Int.MaxValue
      rs1.foreach(r => {
        val firstRow = r.getInt(0)
        if (lastRow > firstRow) {
          if (i > 0) {
            // scalastyle:off println
            println(s"verifyTotalRows : " + (i - 1) + " = " + lastRow)
            // scalastyle:on println
          }
          // scalastyle:off println
          println(s"verifyTotalRows : " + i + " = " + firstRow)
          // scalastyle:on println
        } else if (i == assertCount - 1) {
          // scalastyle:off println
          println(s"verifyTotalRows : " + i + " = " + firstRow)
          // scalastyle:on println
        }
        lastRow = firstRow
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
      assert(rs1.length == assertCount, rs1.length)
    }

    try {
      verifyTotalRows(numFirstInserts(numElements), 1)
      try {
        ColumnTableScan.setCaseOfSortedInsertValue(true)
        ColumnTableScan.setDebugMode(doDebug)
        updateDF.write.putInto(colTableName)
      } finally {
        ColumnTableScan.setDebugMode(false)
        ColumnTableScan.setCaseOfSortedInsertValue(false)
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

    session.sql(s"drop table $colTableName")
    session.conf.unset(Property.ColumnBatchSize.name)
    session.conf.unset(Property.ColumnMaxDeltaRows.name)
  }
}
