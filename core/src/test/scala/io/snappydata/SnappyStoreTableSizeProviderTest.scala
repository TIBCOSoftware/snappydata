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

package io.snappydata

import java.sql.DriverManager

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation

class SnappyStoreTableSizeProviderTest
    extends SnappyFunSuite
        with BeforeAndAfter
        with BeforeAndAfterAll {

  val columnTableName = "COLUMNTABLE"
  val rowTableName = "ROWTABLE"
  val serviceInterval = "3"

  after {
    snc.dropTable(columnTableName, ifExists = true)
    snc.dropTable(rowTableName, ifExists = true)
  }

  override protected def newSparkConf(addOn: SparkConf => SparkConf = null): SparkConf = {
    val sparkConf = super.newSparkConf(addOn)
    sparkConf.set("spark.snappy.calcTableSizeInterval", serviceInterval)
    sparkConf.set("spark.sql.inMemoryColumnarStorage.batchSize", "3")
  }

  test("Test Row Table Size for all the values") {
    val dataDF = getDF

    snc.sql("CREATE TABLE " + rowTableName + " (a INT, b INT, c INT) " +
        "USING row  OPTIONS (PARTITION_BY 'a')")
    dataDF.write.insertInto(s"$rowTableName")

    val result = snc.sql(s"SELECT * FROM $rowTableName")
    assert(result.collect().length == 5)

    val fullTableName = s"APP.$rowTableName"

    waitForCriterion(StoreTableValueSizeProviderService.getTableSize(fullTableName).
        getOrElse(0) == 140,
      s"Comparing the value Size of $rowTableName with ApproxTableSizeCalculator",
      serviceInterval.toInt * 3, serviceInterval.toInt, throwOnTimeout = true)

    snc.sql(s"drop table $rowTableName")
  }

  test("Test Column Table Size for all the values") {
    val dataDF = getDF

    snc.sql(s"Create Table $columnTableName (a INT, b INT, c INT) " +
        s"using column options(buckets '1')")
    dataDF.write.format("column").mode(SaveMode.Append).saveAsTable(columnTableName)

    val result = snc.sql(s"SELECT * FROM $columnTableName")
    assert(result.collect().length == 5)

    val fullTableName = Constant.DEFAULT_SCHEMA + "." + columnTableName
    val colBufferName = ColumnFormatRelation.cachedBatchTableName(fullTableName)

    waitForCriterion({
      val rowBufferSize: Long = StoreTableValueSizeProviderService.
          getTableSize(fullTableName).getOrElse(0)
      val colBufferSize: Long = StoreTableValueSizeProviderService.
          getTableSize(colBufferName).getOrElse(0)
      val totalSize: Long = StoreTableValueSizeProviderService.
          getTableSize(fullTableName, true).getOrElse(0)
      rowBufferSize + colBufferSize == totalSize
    },
      s"Comparing the value Size of $columnTableName with ApproxTableSizeCalculator",
      serviceInterval.toInt * 3, serviceInterval.toInt, throwOnTimeout = true)
    snc.sql(s"drop table $columnTableName")
  }


  test("Test exact row table size") {

    val dataDF = getDF

    snc.sql(s"Create Table $rowTableName (a INT, b INT, c INT) ")
    dataDF.write.insertInto(s"$rowTableName")

    val result = snc.sql(s"SELECT * FROM $rowTableName")
    assert(result.collect().length == 5)

    val fullTableName = s"APP.$rowTableName"
    val analytics = queryMemoryAnalytics(fullTableName)

    def check(expectedTotalSize: Long): Boolean = {
      val row = OnDemandTableSizeProvider.getTableSizes.
          filter(uiAnalytics => uiAnalytics.tableName == fullTableName).head
      expectedTotalSize == row.rowBufferSize
    }

    waitForCriterion(check(analytics._2),
      "Comparing the results of MemoryAnalytics with Snappy Service for Row table",
      serviceInterval.toInt * 2, serviceInterval.toInt, throwOnTimeout = true)

    snc.sql(s"drop table $rowTableName")
  }


  test("Test exact column table size") {
    val dataDF = getDF
    snc.createTable(columnTableName, "column", dataDF.schema, Map.empty[String, String])
    dataDF.write.insertInto(s"$columnTableName")

    val result = snc.sql("SELECT * FROM " + columnTableName)
    val r = result.collect()
    assert(r.length == 5)

    val fullTableName = s"APP.$columnTableName"
    val analyticsRowBuffer = queryMemoryAnalytics(fullTableName)
    val analyticsColumnBuffer =
      queryMemoryAnalytics(ColumnFormatRelation.cachedBatchTableName(fullTableName))

    def check(expectedRowSize: Long,
        expectedColumnSize: Long): Boolean = {
      val sizeList = OnDemandTableSizeProvider.getTableSizes
      val currentTable = sizeList.filter(uiDetails => uiDetails.tableName == fullTableName)

      !currentTable.isEmpty &&
          currentTable.head.rowBufferSize == expectedRowSize &&
          currentTable.head.columnBufferSize == expectedColumnSize
    }

    waitForCriterion(check(analyticsRowBuffer._2, analyticsColumnBuffer._2),
      "Comparing the Column table results of MemoryAnalytics with Snappy Service",
      serviceInterval.toInt * 2, serviceInterval.toInt, throwOnTimeout = true)
  }


  private def getDF = {
    val data = Seq(Seq(1, 2, 3), Seq(4, 5, 6), Seq(7, 8, 9), Seq(10, 11, 12), Seq(13, 14, 15))
    val rdd = sc.parallelize(data, data.length).
        map(s => new io.snappydata.core.Data(s.head, s(1), s(2)))
    snc.createDataFrame(rdd)
  }


  private def queryMemoryAnalytics(tableName: String): (String, Long) = {
    val query = "SELECT  SUM(TOTAL_SIZE) FROM SYS.MEMORYANALYTICS" +
        s" WHERE TABLE_NAME = '$tableName'"
    var valueSize: Long = 0
    var totalSize: Long = 0
    val conn = DriverManager.getConnection(Constant.DEFAULT_EMBEDDED_URL)
    val rs = conn.createStatement().executeQuery(query)
    if (rs.next()) {
      totalSize = (rs.getString(1).toDouble * 1024).toLong
    }

    (tableName, totalSize)
  }


}
