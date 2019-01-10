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

import com.gemstone.gemfire.internal.cache.{DistributedRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.SnappyFunSuite
import io.snappydata.core.{TestData, TestData2}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql._

/**
 * Tests for GFXD ROW table properties.
 */
class RowRelationAPISuite extends SnappyFunSuite with BeforeAndAfterAll {

  val props = Map.empty[String, String]

  test("Create replicated row table with DataFrames") {
    val rdd = sc.parallelize((1 to 1000).map(i => TestData(i, s"$i")))
    val dataDF = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS row_table1")
    snc.createTable("row_table1", "row", dataDF.schema, props)
    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("row_table1")
    val count = snc.sql("select * from row_table1").count()
    assert(count === 1000)
  }

  test("Test Partitioned row tables") {
    val rdd = sc.parallelize(
      (1 to 1000).map(i => TestData(i, i.toString)))

    val dataDF = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS row_table22")

    snc.sql("CREATE TABLE row_table22(OrderId INT NOT NULL,ItemId INT)" +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId')")

    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("row_table22")

    val count = snc.sql("select * from row_table22").count()
    assert(count === 1000)
  }

  test("Test PreserverPartition on  row tables") {
    val k = 113
    val rdd = sc.parallelize(1 to 1000, k).map(i => TestData(i, i.toString))

    val dataDF = snc.createDataFrame(rdd)
    snc.sql("DROP TABLE IF EXISTS row_table3")

    snc.sql("CREATE TABLE row_table3(OrderId INT NOT NULL,ItemId INT) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId')")

    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("row_table3")
    val count = snc.sql("select * from row_table3").count()
    assert(count === 1000)
  }

  test("Test PR with Primary Key") {
    snc.sql("DROP TABLE IF EXISTS ROW_TEST_TABLE1")
    snc.sql("CREATE TABLE ROW_TEST_TABLE1(OrderId INT NOT NULL PRIMARY KEY,ItemId INT) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'PRIMARY KEY'," +
        "REDUNDANCY '2')")

    val region = Misc.getRegionForTable("APP.ROW_TEST_TABLE1", true).asInstanceOf[PartitionedRegion]

    val rCopy = region.getPartitionAttributes.getRedundantCopies
    assert(rCopy === 2)
  }

  test("Test PR with buckets") {
    snc.sql("DROP TABLE IF EXISTS ROW_TEST_TABLE2")
    snc.sql("CREATE TABLE ROW_TEST_TABLE2(OrderId INT NOT NULL,ItemId INT) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "BUCKETS '213')")

    val region = Misc.getRegionForTable("APP.ROW_TEST_TABLE2", true).asInstanceOf[PartitionedRegion]

    val numPartitions = region.getTotalNumberOfBuckets
    assert(numPartitions === 213)
  }

  test("Test PR with REDUNDANCY") {
    snc.sql("DROP TABLE IF EXISTS ROW_TEST_TABLE3")
    snc.sql("CREATE TABLE ROW_TEST_TABLE3(OrderId INT NOT NULL,ItemId INT) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "BUCKETS '213'," +
        "REDUNDANCY '2')")

    val region = Misc.getRegionForTable("APP.ROW_TEST_TABLE3", true).asInstanceOf[PartitionedRegion]
    snc.sql("insert into ROW_TEST_TABLE3 VALUES(1,11)")

    val rCopy = region.getPartitionAttributes.getRedundantCopies
    assert(rCopy === 2)
  }

  test("Test PR with RECOVERDELAY") {
    snc.sql("DROP TABLE IF EXISTS ROW_TEST_TABLE4")
    snc.sql("CREATE TABLE ROW_TEST_TABLE4(OrderId INT NOT NULL PRIMARY KEY,ItemId INT) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "BUCKETS '213'," +
        "RECOVERYDELAY '2')")

    val region = Misc.getRegionForTable("APP.ROW_TEST_TABLE4", true).asInstanceOf[PartitionedRegion]

    val rDelay = region.getPartitionAttributes.getRecoveryDelay
    assert(rDelay === 2)
  }

  test("Test PR with MAXPART") {
    snc.sql("DROP TABLE IF EXISTS ROW_TEST_TABLE5")
    snc.sql("CREATE TABLE ROW_TEST_TABLE5(OrderId INT NOT NULL PRIMARY KEY,ItemId INT) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "MAXPARTSIZE '200')")

    val region = Misc.getRegionForTable("APP.ROW_TEST_TABLE5", true).asInstanceOf[PartitionedRegion]

    val rMaxMem = region.getPartitionAttributes.getLocalMaxMemory
    assert(rMaxMem === 200)
  }

  test("Test PR with EVICTION BY") {
    val snc = org.apache.spark.sql.SnappyContext(sc)
    snc.sql("DROP TABLE IF EXISTS ROW_TEST_TABLE6")
    snc.sql("CREATE TABLE ROW_TEST_TABLE6(OrderId INT NOT NULL PRIMARY KEY,ItemId INT) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "EVICTION_BY 'LRUMEMSIZE 200')")

    Misc.getRegionForTable("APP.ROW_TEST_TABLE6", true).asInstanceOf[PartitionedRegion]
  }

  test("Test PR with PERSISTENT") {
    snc.sql("DROP TABLE IF EXISTS ROW_TEST_TABLE7")
    snc.sql("CREATE TABLE ROW_TEST_TABLE7(OrderId INT NOT NULL PRIMARY KEY,ItemId INT) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'PRIMARY KEY'," +
        "PERSISTENT 'ASYNCHRONOUS')")

    val region = Misc.getRegionForTable("APP.ROW_TEST_TABLE7", true).asInstanceOf[PartitionedRegion]
    assert(region.getDiskStore != null)
    assert(!region.getAttributes.isDiskSynchronous)
  }

  test("Test RR with PERSISTENT") {
    val snc = org.apache.spark.sql.SnappyContext(sc)
    snc.sql("DROP TABLE IF EXISTS ROW_TEST_TABLE8")
    snc.sql("CREATE TABLE ROW_TEST_TABLE8(OrderId INT NOT NULL PRIMARY KEY,ItemId INT) " +
        "USING row " +
        "options " +
        "(" +
        "PERSISTENT 'ASYNCHRONOUS')")

    val region = Misc.getRegionForTable("APP.ROW_TEST_TABLE8", true).asInstanceOf[DistributedRegion]
    assert(region.getDiskStore != null)
    assert(!region.getAttributes.isDiskSynchronous)
  }

  test("Test PR with multiple columns") {
    snc.sql("DROP TABLE IF EXISTS ROW_TEST_TABLE9")
    snc.sql("CREATE TABLE ROW_TEST_TABLE9(OrderId INT NOT NULL PRIMARY KEY," +
        "  ItemId INT, ItemRef INT) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderID, ItemRef'," +
        "PERSISTENT 'ASYNCHRONOUS')")

    val rdd = sc.parallelize(
      (1 to 1000).map(i => TestData2(i, i.toString, i)))

    val dataDF = snc.createDataFrame(rdd)

    dataDF.write.format("row").mode(SaveMode.Append).options(props).saveAsTable("ROW_TEST_TABLE9")
    val count = snc.sql("select * from ROW_TEST_TABLE9").count()
    assert(count === 1000)
  }

  test("Test PR with EXPIRY") {
    val snc = org.apache.spark.sql.SnappyContext(sc)
    snc.sql("DROP TABLE IF EXISTS ROW_TEST_TABLE10")
    snc.sql("CREATE TABLE ROW_TEST_TABLE10(OrderId INT NOT NULL PRIMARY KEY,ItemId INT) " +
        "USING row " +
        "options " +
        "(" +
        "PARTITION_BY 'OrderId'," +
        "EXPIRE '200')")

    val region = Misc.getRegionForTable("APP.ROW_TEST_TABLE10", true)
        .asInstanceOf[PartitionedRegion]
    assert(region.getAttributes.getEntryTimeToLive.getTimeout == 200)
  }
}
