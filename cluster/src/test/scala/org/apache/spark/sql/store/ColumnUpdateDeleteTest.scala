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

import com.pivotal.gemfirexd.TestUtil
import io.snappydata.ColumnUpdateDeleteTests
import io.snappydata.cluster.PreparedQueryRoutingSingleNodeSuite

import org.apache.spark.SparkConf
import org.apache.spark.memory.SnappyUnifiedMemoryManager
import org.apache.spark.sql.SparkSupport

/**
 * Tests for updates/deletes on column table.
 */
class ColumnUpdateDeleteTest extends ColumnTablesTestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    stopAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    stopAll()
  }

  override protected def newSparkConf(addOn: SparkConf => SparkConf): SparkConf = {
    val conf = new SparkConf()
    conf.setIfMissing("spark.master", "local[*]")
        .setAppName(getClass.getName)
    conf.set("snappydata.store.critical-heap-percentage", "95")
    if (SparkSupport.isEnterpriseEdition) {
      conf.set("snappydata.store.memory-size", "1200m")
    }
    conf.set("spark.memory.manager", classOf[SnappyUnifiedMemoryManager].getName)
    conf.set("spark.serializer", "org.apache.spark.serializer.PooledKryoSerializer")
    conf.set("spark.closure.serializer", "org.apache.spark.serializer.PooledKryoSerializer")
    conf
  }

  test("basic update") {
    ColumnUpdateDeleteTests.testBasicUpdate(this.snc.snappySession)
  }

  test("stats check after updates") {
    ColumnUpdateDeleteTests.testDeltaStats(this.snc.snappySession)
  }

  test("basic delete") {
    ColumnUpdateDeleteTests.testBasicDelete(this.snc.snappySession)
  }

  test("SNAP-1925") {
    ColumnUpdateDeleteTests.testSNAP1925(this.snc.snappySession)
  }

  test("SNAP-1926") {
    ColumnUpdateDeleteTests.testSNAP1926(this.snc.snappySession)
  }

  test("concurrent ops") {
    ColumnUpdateDeleteTests.testConcurrentOps(this.snc.snappySession)
  }

  test("SNAP-2124 update missed") {
    ColumnUpdateDeleteTests.testSNAP2124(this.snc.snappySession)
  }

  test("SNAP-1985: update delete on string type") {
    val tableName1 = "order_line_1_col_str"
    val tableName2 = "order_line_2_ud_str"

    snc.sql(s"create table $tableName1 (ol_1_int_id  integer," +
        s" ol_1_int2_id  integer, ol_1_str_id STRING) using column " +
        "options( partition_by 'ol_1_int2_id', buckets '2'," +
        " COLUMN_BATCH_SIZE '100')")
    snc.sql(s"create table $tableName2 (ol_1_int_id  integer," +
        s" ol_1_int2_id  integer, ol_1_str_id STRING) using row " +
        "options( partition_by 'ol_1_int2_id', buckets '2')")

    // println("network server started")
    val serverHostPort = TestUtil.startNetServer()
    PreparedQueryRoutingSingleNodeSuite.insertRows(tableName1, 1000, serverHostPort)
    PreparedQueryRoutingSingleNodeSuite.insertRows(tableName2, 1000, serverHostPort)

    snc.sql(s"update $tableName2 set ol_1_str_id = '7777_a_1' where ol_1_int2_id = 500 ")
    snc.sql(s"update $tableName2 set ol_1_str_id = '7777_b_2' where ol_1_int2_id = 500 ")

    snc.sql(s"update $tableName1 set ol_1_str_id = '7777_a_1' where ol_1_int2_id = 500 ")
    snc.sql(s"update $tableName1 set ol_1_str_id = '7777_b_2' where ol_1_int2_id = 500 ")
  }
}
