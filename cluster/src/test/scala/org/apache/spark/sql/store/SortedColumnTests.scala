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
import org.apache.spark.sql.{DataFrame, DataFrameReader, SnappySession}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.snappy._
import org.apache.spark.util.Benchmark

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

    SortedColumnTests.verfiyInsertDataExists(snc, numElements)
    SortedColumnTests.verfiyUpdateDataExists(snc, numElements)
    SortedColumnTests.testBasicInsert(snc, colTableName, numBuckets, numElements)
  }

  test("multiple insert") {
    val snc = this.snc.snappySession
    val colTableName = "colDeltaTable"
    val numElements = 300
    SortedColumnTests.testMultipleInsert(snc, colTableName, numBuckets = 1, numElements)
    SortedColumnTests.testMultipleInsert(snc, colTableName, numBuckets = 2, numElements)
  }

  test("update and insert") {
    val snc = this.snc.snappySession
    val colTableName = "colDeltaTable"
    val numElements = 300
    SortedColumnTests.testUpdateAndInsert(snc, colTableName, numBuckets = 1, numElements)
    SortedColumnTests.testUpdateAndInsert(snc, colTableName, numBuckets = 2, numElements)
  }

  test("join query") {
    val session = this.snc.snappySession
    val colTableName = "colDeltaTable"
    val joinTableName = "joinDeltaTable"
    val numBuckets = 4

    SortedColumnTests.testColocatedJoin(session, colTableName, joinTableName, numBuckets,
      numElements = 10000000, expectedResCount = 1000000000,
      numTimesInsert = 10, numTimesUpdate = 10)
    SortedColumnTests.testColocatedJoin(session, colTableName, joinTableName, numBuckets,
      numElements = 100000000, expectedResCount = 100000000)
    // Thread.sleep(50000000)
  }
}

object SortedColumnTests extends Logging {
  private val baseDataPath = s"/home/vivek/work/testData/local_index"

  def filePathInsert(size: Long, multiple: Int) : String = s"$baseDataPath/insert${size}_$multiple"
  def verfiyInsertDataExists(snc: SnappySession, size: Long, multiple: Int = 1) : Unit = {
    val dataDirInsert = new File(SortedColumnTests.filePathInsert(size, multiple))
    if (!dataDirInsert.exists()) {
      dataDirInsert.mkdir()
      snc.sql(s"create EXTERNAL TABLE insert_table_${size}_$multiple(id int, addr string," +
          s" status boolean)" +
          s" USING parquet OPTIONS(path '${SortedColumnTests.filePathInsert(size, multiple)}')")
      var j = 0
      while (j < multiple) {
        snc.range(size).filter(_ % 10 < 6).selectExpr("id", "concat('addr'," +
            "cast(id as string))",
          "case when (id % 2) = 0 then true else false end").write.
            insertInto(s"insert_table_${size}_$multiple")
        j += 1
      }
    }
  }

  def filePathUpdate(size: Long, multiple: Int) : String = s"$baseDataPath/update${size}_$multiple"
  def verfiyUpdateDataExists(snc: SnappySession, size: Long, multiple: Int = 1) : Unit = {
    val dataDirUpdate = new File(SortedColumnTests.filePathUpdate(size, multiple))
    if (!dataDirUpdate.exists()) {
      dataDirUpdate.mkdir()
      snc.sql(s"create EXTERNAL TABLE update_table_${size}_$multiple(id int, addr string," +
          s" status boolean)" +
          s" USING parquet OPTIONS(path '${SortedColumnTests.filePathUpdate(size, multiple)}')")
      var j = 0
      while (j < multiple) {
        snc.range(size).filter(_ % 10 > 5).selectExpr("id", "concat('addr'," +
            "cast(id as string))",
          "case when (id % 2) = 0 then true else false end").write.
            insertInto(s"update_table_${size}_$multiple")
        j += 1
      }
    }
  }

  def verifyTotalRows(session: SnappySession, columnTable: String, numElements: Long,
      finalCall: Boolean, numTimesInsert: Int, numTimesUpdate: Int): Unit = {
    val colDf = session.sql(s"select * from $columnTable")
    // scalastyle:off
    // println(s"verifyTotalRows = ${colDf.collect().length}")
    // scalastyle:on
    val dataFrameReader: DataFrameReader = session.read
    val insDF = dataFrameReader.parquet(filePathInsert(numElements, numTimesInsert))
    val verifyDF = if (finalCall) {
      insDF.union(dataFrameReader.parquet(filePathUpdate(numElements, numTimesUpdate)))
    } else insDF
    val resCount = colDf.except(verifyDF).count()
    assert(resCount == 0, resCount)
  }

  def createColumnTable(session: SnappySession, colTableName: String, numBuckets: Int,
      numElements: Long, colocateTableName: Option[String] = None): Unit = {
    dropColumnTable(session, colTableName)
    val additionalString = if (colocateTableName.isDefined) {
      s", COLOCATE_WITH '${colocateTableName.get}'"
    } else ""
    session.sql(s"create table $colTableName (id int, addr string, status boolean) " +
        s"using column options(buckets '$numBuckets', partition_by 'id', key_columns 'id' " +
        additionalString + s")")
  }

  def dropColumnTable(session: SnappySession, colTableName: String): Unit = {
    session.sql(s"drop table if exists $colTableName")
  }

  def testBasicInsert(session: SnappySession, colTableName: String, numBuckets: Int,
      numElements: Long): Unit = {
    session.conf.set(Property.ColumnMaxDeltaRows.name, "100")
    session.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    session.conf.set(SQLConf.WHOLESTAGE_FALLBACK.key, "false")

    createColumnTable(session, colTableName, numBuckets, numElements)
    val dataFrameReader : DataFrameReader = session.read
    val insertDF : DataFrame = dataFrameReader.load(filePathInsert(numElements, multiple = 1))
    insertDF.write.insertInto(colTableName)
    val updateDF : DataFrame = dataFrameReader.load(filePathUpdate(numElements, multiple = 1))

    try {
      verifyTotalRows(session: SnappySession, colTableName, numElements, finalCall = false,
        numTimesInsert = 1, numTimesUpdate = 1)
      try {
        ColumnTableScan.setCaseOfSortedInsertValue(true)
        ColumnTableScan.setDebugMode(false)
        updateDF.write.putInto(colTableName)
      } finally {
        ColumnTableScan.setDebugMode(false)
        ColumnTableScan.setCaseOfSortedInsertValue(false)
      }
      verifyTotalRows(session: SnappySession, colTableName, numElements, finalCall = true,
        numTimesInsert = 1, numTimesUpdate = 1)
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
    session.conf.unset(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key)
    session.conf.unset(SQLConf.WHOLESTAGE_FALLBACK.key)
  }

  def fixedFilePath(fileName: String): String = s"$baseDataPath/$fileName"

  def createFixedData(snc: SnappySession, size: Long, fileName: String)
      (f: (Long) => Boolean): Unit = {
    val dataDir = new File(fixedFilePath(fileName))
    if (dataDir.exists()) {
      def deleteRecursively(file: File): Unit = {
        if (file.isDirectory) {
          file.listFiles.foreach(deleteRecursively)
        }
        if (file.exists && !file.delete) {
          throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
        }
      }
      deleteRecursively(dataDir)
    }
    dataDir.mkdir()
    snc.sql(s"drop TABLE if exists insert_table_$fileName")
    snc.sql(s"create EXTERNAL TABLE insert_table_$fileName(id int, addr string," +
        s" status boolean)" +
        s" USING parquet OPTIONS(path '${fixedFilePath(fileName)}')")
    snc.range(size).filter(f(_)).selectExpr("id", "concat('addr'," +
        "cast(id as string))",
      "case when (id % 2) = 0 then true else false end").write.
        insertInto(s"insert_table_$fileName")
  }

  def testMultipleInsert(session: SnappySession, colTableName: String, numBuckets: Int,
      numElements: Long): Unit = {
    val testName = "testMultipleInsert"
    val dataFile_1 = s"${testName}_1"
    SortedColumnTests.createFixedData(session, numElements, dataFile_1)(i => {
      i == 0 || i == 99 || i == 200 || i == 299
    })
    val dataFile_2 = s"${testName}_2"
    SortedColumnTests.createFixedData(session, numElements, dataFile_2)(i => {
      i == 100 || i == 199
    })
    val dataFile_3 = s"${testName}_3"
    SortedColumnTests.createFixedData(session, numElements, dataFile_3)(i => {
      i == 50 || i == 250
    })
    val dataFile_4 = s"${testName}_4"
    SortedColumnTests.createFixedData(session, numElements, dataFile_4)(i => {
      i == 25 || i == 175
    })
    val dataFile_5 = s"${testName}_5"
    SortedColumnTests.createFixedData(session, numElements, dataFile_5)(i => {
      i == 125 || i == 275
    })
    val dataFile_6 = s"${testName}_6"
    SortedColumnTests.createFixedData(session, numElements, dataFile_6)(i => {
      i == 150 || i == 225
    })

    session.conf.set(Property.ColumnMaxDeltaRows.name, "100")
    session.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    session.conf.set(SQLConf.WHOLESTAGE_FALLBACK.key, "false")

    try {
      createColumnTable(session, colTableName, numBuckets, numElements)
      val dataFrameReader : DataFrameReader = session.read
      dataFrameReader.load(fixedFilePath(dataFile_1)).write.insertInto(colTableName)
      // scalastyle:off
      println(s"$testName loaded $dataFile_1")
      // scalastyle:on

      try {
        ColumnTableScan.setCaseOfSortedInsertValue(true)
        ColumnTableScan.setDebugMode(false)
        dataFrameReader.load(fixedFilePath(dataFile_2)).write.putInto(colTableName)
        // scalastyle:off
        println(s"$testName loaded $dataFile_2")
        // scalastyle:on
        dataFrameReader.load(fixedFilePath(dataFile_3)).write.putInto(colTableName)
        // scalastyle:off
        println(s"$testName loaded $dataFile_3")
        // scalastyle:on
        dataFrameReader.load(fixedFilePath(dataFile_4)).write.putInto(colTableName)
        // scalastyle:off
        println(s"$testName loaded $dataFile_4")
        // scalastyle:on
        dataFrameReader.load(fixedFilePath(dataFile_5)).write.putInto(colTableName)
        // scalastyle:off
        println(s"$testName loaded $dataFile_5")
        // scalastyle:on
        dataFrameReader.load(fixedFilePath(dataFile_6)).write.putInto(colTableName)
        // scalastyle:off
        println(s"$testName loaded $dataFile_6")
        // scalastyle:on
      } finally {
        ColumnTableScan.setDebugMode(false)
        ColumnTableScan.setCaseOfSortedInsertValue(false)
      }

      ColumnTableScan.setDebugMode(true)
      val colDf = session.sql(s"select * from $colTableName")
      val res = colDf.collect()
      val expected = Array(0, 25, 50, 99, 100, 125, 150, 175, 199, 200, 225, 250, 275, 299)
      assert(res.length == expected.length)
      // scalastyle:off
      // println(s"verifyTotalRows = ${colDf.collect().length}")
      // scalastyle:on
      if (numBuckets == 1) {
        var i = 0
        res.foreach(r => {
          val col1 = r.getInt(0)
          assert(col1 == expected(i), s"$i : $col1")
          i += 1
        })
      }
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
    session.conf.unset(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key)
    session.conf.unset(SQLConf.WHOLESTAGE_FALLBACK.key)
  }

  def testUpdateAndInsert(session: SnappySession, colTableName: String, numBuckets: Int,
      numElements: Long): Unit = {
    val testName = "testUpdateAndInsert"
    val dataFile_1 = s"${testName}_1"
    SortedColumnTests.createFixedData(session, numElements, dataFile_1)(i => {
      i == 0 || i == 99 || i == 200 || i == 299
    })
    val dataFile_2 = s"${testName}_2"
    SortedColumnTests.createFixedData(session, numElements, dataFile_2)(i => {
      i == 100 || i == 199
    })
    val dataFile_3 = s"${testName}_3"
    SortedColumnTests.createFixedData(session, numElements, dataFile_3)(i => {
      i == 50 || i == 250
    })
    val dataFile_4 = s"${testName}_4"
    SortedColumnTests.createFixedData(session, numElements, dataFile_4)(i => {
      i == 25 || i == 175
    })
    val dataFile_5 = s"${testName}_5"
    SortedColumnTests.createFixedData(session, numElements, dataFile_5)(i => {
      i == 125 || i == 275
    })
    val dataFile_6 = s"${testName}_6"
    SortedColumnTests.createFixedData(session, numElements, dataFile_6)(i => {
      i == 150 || i == 225
    })

    session.conf.set(Property.ColumnMaxDeltaRows.name, "100")
    session.conf.set(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key, "true")
    session.conf.set(SQLConf.WHOLESTAGE_FALLBACK.key, "false")

    def doUpdate(queryStr: String, whereClause: String = ""): String = {
      val update_query = s"update $colTableName set addr = '$queryStr' $whereClause"
      // scalastyle:off
      println(s"$testName started UPDATE $update_query")
      // scalastyle:on
      ColumnTableScan.setDebugMode(true)
      val upd = session.sql(update_query)
      // scalastyle:off
      println(s"$testName done UPDATE $update_query")
      // scalastyle:on
      queryStr
    }

    def doPutInto(fileName: String, dataFrameReader: DataFrameReader): Unit = {
      try {
        ColumnTableScan.setCaseOfSortedInsertValue(true)
        // scalastyle:off
        println(s"$testName start loading $fileName")
        // scalastyle:on
        dataFrameReader.load(fixedFilePath(fileName)).write.putInto(colTableName)
        // scalastyle:off
        println(s"$testName loaded $fileName")
        // scalastyle:on
      } finally {
        ColumnTableScan.setCaseOfSortedInsertValue(false)
      }
    }

    def verifyUpdate(expected: String): Unit = {
      val select_query = s"select * from $colTableName"
      val colDf = session.sql(select_query)
      val res = colDf.collect()
      res.foreach(r => {
        val col1 = r.getString(1)
        assert(col1.equalsIgnoreCase(expected), s"$col1 : $expected")
      })
    }

    try {
      createColumnTable(session, colTableName, numBuckets, numElements)

      // scalastyle:off
      println(s"$testName start loading $dataFile_1")
      // scalastyle:on
      val dataFrameReader: DataFrameReader = session.read
      dataFrameReader.load(fixedFilePath(dataFile_1)).write.insertInto(colTableName)
      // scalastyle:off
      println(s"$testName loaded $dataFile_1")
      // scalastyle:on
      verifyUpdate(doUpdate("updated1"))

      doPutInto(dataFile_2, dataFrameReader)
      verifyUpdate(doUpdate("updated2"))

      doPutInto(dataFile_3, dataFrameReader)
      verifyUpdate(doUpdate("updated3"))

      doPutInto(dataFile_4, dataFrameReader)
      verifyUpdate(doUpdate("updated4"))

      doPutInto(dataFile_5, dataFrameReader)
      verifyUpdate(doUpdate("updated5"))

      doPutInto(dataFile_6, dataFrameReader)
      verifyUpdate(doUpdate("updated6"))

      val select_query = s"select * from $colTableName"
      // scalastyle:off
      println(s"$testName started SELECT $select_query")
      // scalastyle:on
      ColumnTableScan.setDebugMode(true)
      val colDf = session.sql(select_query)
      val res = colDf.collect()
      val expected = Array(0, 25, 50, 99, 100, 125, 150, 175, 199, 200, 225, 250, 275, 299)
      assert(res.length == expected.length)
      // scalastyle:off
      println(s"$testName SELECT = ${res.length}")
      // scalastyle:on
      if (numBuckets == 1) {
        var i = 0
        res.foreach(r => {
          val col1 = r.getInt(0)
          assert(col1 == expected(i), s"$i : $col1")
          i += 1
        })
      }
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
    session.conf.unset(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key)
    session.conf.unset(SQLConf.WHOLESTAGE_FALLBACK.key)
  }

  def testColocatedJoin(session: SnappySession, colTableName: String, joinTableName: String,
      numBuckets: Int, numElements: Long, expectedResCount: Int, numTimesInsert: Int = 1,
      numTimesUpdate: Int = 1): Unit = {
    val totalElements = (numElements * 0.6 * numTimesUpdate +
        numElements * 0.4 * numTimesUpdate).toLong
    SortedColumnTests.verfiyInsertDataExists(session, numElements, numTimesInsert)
    SortedColumnTests.verfiyUpdateDataExists(session, numElements, numTimesUpdate)
    val dataFrameReader : DataFrameReader = session.read
    val insertDF: DataFrame = dataFrameReader.load(SortedColumnTests.filePathInsert(numElements,
      numTimesInsert))
    val updateDF: DataFrame = dataFrameReader.load(SortedColumnTests.filePathUpdate(numElements,
      numTimesUpdate))

    SortedColumnTests.createColumnTable(session, colTableName, numBuckets, numElements)
    SortedColumnTests.createColumnTable(session, joinTableName, numBuckets, numElements,
      Some(colTableName))
    try {
      session.conf.set(Property.ColumnBatchSize.name, "24M") // default
      session.conf.set(Property.ColumnMaxDeltaRows.name, "100")
      insertDF.write.insertInto(colTableName)
      insertDF.write.insertInto(joinTableName)

      ColumnTableScan.setCaseOfSortedInsertValue(true)
      updateDF.write.putInto(colTableName)
      updateDF.write.putInto(joinTableName)
    } finally {
      ColumnTableScan.setCaseOfSortedInsertValue(false)
      session.conf.unset(Property.ColumnBatchSize.name)
      session.conf.unset(Property.ColumnMaxDeltaRows.name)
    }

    try {
      // Force SMJ
      session.conf.set(Property.HashJoinSize.name, "-1")
      session.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      val query = s"select AVG(A.id), COUNT(B.id) " +
          s" from $colTableName A inner join $joinTableName B where A.id = B.id"
      val result = session.sql(query).collect()
      // scalastyle:off
      println(s"Query = $query result=${result.length}")
      result.foreach(r => {
        val avg = r.getDouble(0)
        val count = r.getLong(1)
        println(s"[$avg, $count], ")
        assert(count == expectedResCount)
      })
      // scalastyle:on
    } finally {
      session.sql(s"drop TABLE if exists $joinTableName")
      session.sql(s"drop TABLE if exists $colTableName")
      session.conf.unset(Property.HashJoinSize.name)
      session.conf.unset(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key)
    }
  }
}
