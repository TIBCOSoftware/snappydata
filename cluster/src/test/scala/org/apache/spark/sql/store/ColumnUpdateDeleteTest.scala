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

import io.snappydata.gemxd.SnappyDataVersion
import io.snappydata.{ColumnUpdateDeleteTests, Property}

import org.apache.spark.SparkConf
import org.apache.spark.memory.SnappyUnifiedMemoryManager

/**
 * Tests for updates/deletes on column table.
 */
class ColumnUpdateDeleteTest extends ColumnTablesTestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    stopAll()
  }

  override protected def newSparkConf(addOn: (SparkConf) => SparkConf): SparkConf = {
    val conf = new SparkConf()
    conf.setIfMissing("spark.master", "local[*]")
        .setAppName(getClass.getName)
    conf.set("snappydata.store.critical-heap-percentage", "95")
    if (SnappyDataVersion.isEnterpriseEdition) {
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

  test("basic delete") {
    ColumnUpdateDeleteTests.testBasicDelete(this.snc.snappySession)
  }

  test("SNAP-1925") {
    ColumnUpdateDeleteTests.testSNAP1925(this.snc.snappySession)
  }

  test("SNAP-1926") {
    ColumnUpdateDeleteTests.testSNAP1926(this.snc.snappySession)
  }

  ignore("test update for all types") {
    val session = this.snc.snappySession
    // reduced size to ensure both column table and row buffer have data
    session.conf.set(Property.ColumnBatchSize.name, "100k")
    runAllTypesTest(session)
    session.conf.unset(Property.ColumnBatchSize.name)
  }
}
