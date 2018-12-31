/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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

import io.snappydata.SnappyFunSuite
import io.snappydata.core.Data
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode}

class CubeRollupGroupingSetsTest extends SnappyFunSuite with BeforeAndAfterAll {
  private var testData: DataFrame = _

  override def beforeAll() {
    val data = Seq(Seq(1, 2005, 12000), Seq(1, 2006, 18000), Seq(1, 2007, 25000),
      Seq(2, 2005, 15000), Seq(2, 2006, 6000), Seq(2, 2007, 25000))
    val testRDD = sc.parallelize(data, data.length).map(s => Data(s.head, s(1), s(2)))
    testData = snc.createDataFrame(testRDD)
    snc.sql("Create table mytable (col1 INT, col2 INT, col3 INT)")
    testData.write.format("row").mode(SaveMode.Append).saveAsTable("mytable")
  }

  override def afterAll(): Unit = {
    snc.sql("drop table mytable")
  }

  test("snappy cube_rollup query") {
    // group by
    val dfGroupByResult = testData.groupBy("col1", "col2")
        .agg(sum("col2")).orderBy("col1", "col2").collect()
    val snappyGroupByResult = snc.sql("select col1, col2, sum(col2) from mytable " +
        "group by col1, col2 order by col1, col2").collect()

    logInfo("DataFrame group by result")
    logInfo(dfGroupByResult.mkString("\n"))
    logInfo("SnappySQL group by result")
    logInfo(snappyGroupByResult.mkString("\n"))
    assert(dfGroupByResult.sameElements(snappyGroupByResult))

    // roll up
    val dfRollupResult = testData.rollup("col1", "col2")
        .agg(sum("col3")).orderBy("col1", "col2").collect()
    val snappyRollupResult = snc.sql(
      "select col1, col2, sum(col3) from mytable group by col1, col2 " +
          "with rollup order by col1, col2").collect()

    logInfo("DataFrame rollup result")
    logInfo(dfRollupResult.mkString("\n"))
    logInfo("SnappySQL rollup result")
    logInfo(snappyRollupResult.mkString("\n"))
    assert(dfRollupResult.sameElements(snappyRollupResult))

    // cube
    val dfCubeResult = testData.cube("col1", "col2")
        .agg(sum("col3")).orderBy("col1", "col2").collect()
    val snappyCubeResult = snc.sql(
      "select col1, col2, sum(col3) from mytable group by col1, col2 " +
          "with cube order by col1, col2").collect()

    logInfo("DataFrame cube result")
    logInfo(dfCubeResult.mkString("\n"))
    logInfo("SnappySQL cube result")
    logInfo(snappyCubeResult.mkString("\n"))
    assert(dfCubeResult.sameElements(snappyCubeResult))

    // grouping sets query equivalent to above cube query
    val snappyGoupingSetResult = snc.sql("select col1, col2, sum(col3) from mytable group by col1, col2 " +
        "grouping sets ((col1, col2), (col1), (col2), ()) order by col1, col2").collect()
    logInfo("DataFrame cube result")
    logInfo(dfCubeResult.mkString("\n"))
    logInfo("SnappySQL gouping sets result")
    logInfo(snappyGoupingSetResult.mkString("\n"))
    assert(dfCubeResult.sameElements(snappyGoupingSetResult))
  }
}
