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
package org.apache.spark.sql.store

import java.sql.DriverManager
import java.util

import scala.util.{Failure, Success, Try}
import com.gemstone.gemfire.cache.{EvictionAction, EvictionAlgorithm}
import com.gemstone.gemfire.internal.cache.{GemFireCacheImpl, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection
import com.pivotal.gemfirexd.internal.impl.sql.compile.ParserImpl
import io.snappydata.{SnappyFunSuite, SnappyTableStatsProviderService}
import io.snappydata.core.{Data, TestData, TestData2}
import org.apache.hadoop.hive.ql.parse.ParseDriver
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.apache.spark.Logging
import org.apache.spark.sql.SnappySession.CachedKey
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.columnar.JDBCAppendableRelation
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql._

/**
  * Tests for column tables in GFXD.
  */
class TokenizationTest
    extends SnappyFunSuite
        with Logging
        with BeforeAndAfter
        with BeforeAndAfterAll {

  val schema = "test"
  val table = "my_table"

  after {
    snc.dropTable(s"$schema.$table", ifExists = true)
  }

  test("Test tokenize") {
    val numRows = 10
    val data = ((0 to numRows), (0 to numRows), (0 to numRows)).zipped.toArray

    val rdd = sc.parallelize(data, data.length)
      .map(s => Data(s._1, s._2, s._3))
    val dataDF = snc.createDataFrame(rdd)

    snc.sql(s"Drop Table if exists $schema.$table")
    snc.sql(s"Create Table $schema.$table (a INT, b INT, c INT) " +
      "using column options()")

    try {
      dataDF.write.insertInto(s"$schema.$table")
      // This sleep was necessary as it has some dependency on the region size
      // collector thread frequency. Can't remember right now.
      Thread.sleep(1000)

      val q = (0 until numRows) map { x =>
        s"SELECT * FROM $schema.$table where a = $x"
      }
      val start = System.currentTimeMillis()
      q.zipWithIndex.map  { case (x, i) =>
        var result = snc.sql(x).collect()
        assert(result.length === 1)
        result.foreach( r => {
          assert(r.get(0) == r.get(1) && r.get(0) == i)
        })
      }
      val end = System.currentTimeMillis()

      // snc.sql(s"SELECT * FROM $schema.$table where a = 1200").collect()
      println("Time taken = " + (end - start))

      val cacheMap = SnappySession.getPlanCache.asMap()
      assert( cacheMap.size() == 1)
      val x = cacheMap.keySet().toArray()(0).asInstanceOf[CachedKey].sqlText
      assert(x.equals(q(0)))
      snc.sql(s"drop table $schema.$table")
    } finally {
      snc.sql("set spark.sql.caseSensitive = false")
      snc.sql("set schema = APP")
    }

    logInfo("Successful")
  }
}
