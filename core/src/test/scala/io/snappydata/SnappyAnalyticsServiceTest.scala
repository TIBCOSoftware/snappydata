/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Changes for SnappyData data platform.
 *
 * Portions Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

class SnappyAnalyticsServiceTest extends SnappyFunSuite
    with BeforeAndAfter
    with BeforeAndAfterAll {

  val columnTableName = "COLUMNTABLE"
  val rowTableName = "ROWTABLE"
  val serviceInterval = "1000"

  after {
    snc.dropTable(columnTableName, ifExists = true)
    snc.dropTable(rowTableName, ifExists = true)
  }

  override protected def newSparkConf(addOn: SparkConf => SparkConf = null): SparkConf = {
    val sparkConf = super.newSparkConf(addOn)
    sparkConf.set("spark.snappy.analyticsService.interval", serviceInterval)
  }

  test("Test Stats for Row Table") {
    val dataDF = getDF

    snc.sql(s"Create Table $rowTableName (a INT, b INT, c INT)")
    dataDF.write.format("row").mode(SaveMode.Append).saveAsTable(s"$rowTableName")

    val result = snc.sql(s"SELECT * FROM $rowTableName")
    assert(result.collect.length == 5)

    val analytics = queryMemoryAnalytics(s"APP.$rowTableName")

    def check(expectedValueSize: Long, expectedTotalSize: Long): Boolean = {
      val mValueSize = SnappyAnalyticsService.getTableSize(s"APP.$rowTableName")
      val mtotalSize = SnappyAnalyticsService.getUIInfo
          .filter(_.tableName.equals(s"APP.$rowTableName")).head.rowBufferSize
      expectedValueSize == mValueSize && expectedTotalSize == mtotalSize
    }
    waitForCriterion(check(analytics.valueSize, analytics.totalSize),
      "Comparing the results of MemoryAnalytics with Snappy Service",
      serviceInterval.toInt * 2, serviceInterval.toInt, true)

    snc.sql(s"drop table $rowTableName")
  }

  test("Test Stats for Column Table") {
    val dataDF = getDF
    snc.createTable(columnTableName, "column", dataDF.schema, Map.empty[String, String])
    dataDF.write.format("column").mode(SaveMode.Append).saveAsTable(s"$columnTableName")

    val result = snc.sql("SELECT * FROM " + columnTableName)
    val r = result.collect
    assert(r.length == 5)

    val analyticsRowBuffer = queryMemoryAnalytics(s"APP.$columnTableName")
    val analyticsColumnBuffer = queryMemoryAnalytics(
      ColumnFormatRelation.cachedBatchTableName(columnTableName))

    def check(expectedValueSize: Long, expectedRowSize: Long, expectedColumnSize: Long): Boolean = {
      val mValueSize = SnappyAnalyticsService.getTableSize(s"APP.$columnTableName")
      val uiDetails = SnappyAnalyticsService.getUIInfo
          .filter(_.tableName.equals(s"APP.$columnTableName")).head
      expectedValueSize == mValueSize &&
          expectedRowSize == uiDetails.rowBufferSize &&
          expectedColumnSize == uiDetails.columnBufferSize
    }


    waitForCriterion(check(analyticsRowBuffer.valueSize + analyticsColumnBuffer.valueSize,
      analyticsRowBuffer.totalSize,
      analyticsColumnBuffer.totalSize),
      "Comparing the results of MemoryAnalytics with Snappy Service",
      serviceInterval.toInt * 2, serviceInterval.toInt, true)

  }

  private def getDF = {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).
        map(s => new io.snappydata.core.Data(s(0), s(1), s(2)))
    snc.createDataFrame(rdd)
  }

  private def queryMemoryAnalytics(tableName: String): MemoryAnalytics = {
    val query = "SELECT  SUM(VALUE_SIZE) , SUM(TOTAL_SIZE) FROM SYS.MEMORYANALYTICS" +
        s" WHERE TABLE_NAME = '$tableName'"
    var valueSize: Long = 0
    var totalSize: Long = 0
    val conn = DriverManager.getConnection(Constant.DEFAULT_EMBEDDED_URL)
    val rs = conn.createStatement().executeQuery(query)
    if (rs.next()) {
      valueSize = (rs.getString(1).toDouble * 1024).toLong
      totalSize = (rs.getString(2).toDouble * 1024).toLong
    }

    return new MemoryAnalytics(0, 0, valueSize, 0, totalSize)
  }

}
