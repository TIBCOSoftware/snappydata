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

import io.snappydata.Property

import org.apache.spark.{Logging, SparkConf}
import org.apache.spark.memory.SnappyUnifiedMemoryManager
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.execution.columnar.ColumnTableScan
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
    val colTableName = "colDeltaTable"
    val numElements = 551
    val numBuckets = 2

    SortedColumnTests.verfiyInsertDataExists(numElements, snc)
    SortedColumnTests.verfiyUpdateDataExists(numElements, snc)
    SortedColumnTests.testBasicInsert(snc, colTableName, numBuckets, numElements)
  }
}

object SortedColumnTests extends Logging {
  private val baseDataPath = s"/home/vivek/work/testData/local_index"

  def filePathInsert(n: Long) : String = s"$baseDataPath/insert$n"
  def verfiyInsertDataExists(n: Long, snc: SnappySession) : Unit = {
    val dataDirInsert = new File(SortedColumnTests.filePathInsert(n))
    if (!dataDirInsert.exists()) {
      dataDirInsert.mkdir()
      snc.sql(s"create EXTERNAL TABLE insert_table(id int, addr string, status boolean)" +
          s" USING parquet OPTIONS(path '${SortedColumnTests.filePathInsert(n)}')")
      snc.range(n).filter(_ % 10 < 6).selectExpr("id", "concat('addr'," +
          "cast(id as string))",
        "case when (id % 2) = 0 then true else false end").write.insertInto("insert_table")
    }
  }

  def filePathUpdate(n: Long) : String = s"$baseDataPath/update$n"
  def verfiyUpdateDataExists(n: Long, snc: SnappySession) : Unit = {
    val dataDirUpdate = new File(SortedColumnTests.filePathUpdate(n))
    if (!dataDirUpdate.exists()) {
      dataDirUpdate.mkdir()
      snc.sql(s"create EXTERNAL TABLE update_table(id int, addr string, status boolean)" +
          s" USING parquet OPTIONS(path '${SortedColumnTests.filePathUpdate(n)}')")
      snc.range(n).filter(_ % 10 > 5).selectExpr("id", "concat('addr'," +
          "cast(id as string))",
        "case when (id % 2) = 0 then true else false end").write.insertInto("update_table")
    }
  }

  def verifyTotalRows(session: SnappySession, columnTable: String, numElements: Long,
      finalCall: Boolean): Unit = {
    val colDf = session.sql(s"select * from $columnTable")
    // scalastyle:off
    // println(s"verifyTotalRows = ${colDf.collect().length}")
    // scalastyle:on
    val insDF = session.read.parquet(filePathInsert(numElements))
    val verifyDF = if (finalCall) {
      insDF.union(session.read.parquet(filePathUpdate(numElements)))
    } else insDF
    val resCount = colDf.except(verifyDF).count()
    assert(resCount == 0, resCount)
  }

  def createColumnTable(session: SnappySession, colTableName: String, numBuckets: Int,
      numElements: Long): Unit = {
    session.sql(s"drop table if exists $colTableName")
    session.sql(s"create table $colTableName (id int, addr string, status boolean) " +
        s"using column options(buckets '$numBuckets', partition_by 'id', key_columns 'id')")
  }

  def testBasicInsert(session: SnappySession, colTableName: String, numBuckets: Int,
      numElements: Long): Unit = {
    session.conf.set(Property.ColumnMaxDeltaRows.name, "100")
    session.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    session.conf.set(SQLConf.WHOLESTAGE_FALLBACK.key, "false")

    createColumnTable(session, colTableName, numBuckets, numElements)
    val insertDF = session.read.load(filePathInsert(numElements))
    insertDF.write.insertInto(colTableName)
    val updateDF = session.read.load(filePathUpdate(numElements))

    // To force SMJ
    session.conf.set(Property.HashJoinSize.name, "-1")
    session.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")

    // Only use while debugging
    session.conf.set(Property.PutIntoInnerJoinCacheSize.name, "-1")

    try {
      verifyTotalRows(session: SnappySession, colTableName, numElements, finalCall = false)
      try {
        ColumnTableScan.setCaseOfSortedInsertValue(true)
        ColumnTableScan.setDebugMode(false)
        updateDF.write.putInto(colTableName)
      } finally {
        ColumnTableScan.setDebugMode(false)
        ColumnTableScan.setCaseOfSortedInsertValue(false)
      }
      verifyTotalRows(session: SnappySession, colTableName, numElements, finalCall = true)
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
    session.conf.unset(Property.HashJoinSize.name)
    session.conf.unset(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key)
    session.conf.unset(Property.PutIntoInnerJoinCacheSize.name)
  }
}
