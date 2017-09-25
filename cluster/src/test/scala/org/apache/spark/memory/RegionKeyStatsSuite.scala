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
package org.apache.spark.memory

import com.gemstone.gemfire.internal.cache.{BucketRegion, GemFireCacheImpl, LocalRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.SnappyFunSuite
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import org.apache.spark.sql.{Row, SnappyContext, SnappySession}


class RegionKeyStatsSuite extends SnappyFunSuite with BeforeAndAfter with BeforeAndAfterAll {

  after {
    if (SnappyContext.globalSparkContext != null) {
      val snappySession = new SnappySession(SnappyContext.globalSparkContext)
      snappySession.dropTable("t1", true)
    }
  }

  test("PR-non-generated key case") {
    val snSession = new SnappySession(sc)
    snSession.sql("create table t1(col1 Int, col2 Int, col3 Int, PRIMARY KEY (col1)) " +
        " using row options (PARTITION_BY 'col1', buckets '1'," +
        " EVICTION_BY 'LRUCOUNT 1', OVERFLOW 'true')")

    var row = Row(1, 1, 1)
    snSession.insert("t1", row)
    val br = getBucketRegion("app.t1")
    val size1 = br.getKeySizeInMemory
    assert(size1 == 0)
    row = Row(2, 1, 1)
    snSession.insert("t1", row)
    val size2 = br.getKeySizeInMemory
    // One Row evicted hence key size should have increased
    assert(br.getNumEntriesInVM == 1)
    assert(size2 > size1)
    row = Row(3, 1, 1)
    snSession.insert("t1", row)
    val size3 = br.getKeySizeInMemory
    assert(size3 == size2 * 2)
    // Delete an evicted entry
    snSession.delete("t1", "col1 = 1")
    val size4 = br.getKeySizeInMemory
    assert(size4 == size2)

    snSession.update("t1", "COL1 = 2", Row(99), "COL3")
    assert(br.getNumEntriesInVM == 1)
    val size5 = br.getKeySizeInMemory
    assert(size5 == size2)
  }

  test("PR-generated key case") {
    val snSession = new SnappySession(sc)
    snSession.sql("create table t1(col1 Int, col2 Int, col3 Int) " +
        " using row options (PARTITION_BY 'col1', buckets '1'," +
        " EVICTION_BY 'LRUCOUNT 1', OVERFLOW 'true')")

    var row = Row(1, 1, 1)
    snSession.insert("t1", row)
    val br = getBucketRegion("app.t1")
    val size1 = br.getKeySizeInMemory
    assert(size1 > 0)
    row = Row(2, 1, 1)
    snSession.insert("t1", row)
    val size2 = br.getKeySizeInMemory
    // One Row evicted hence key size should have increased
    assert(br.getNumEntriesInVM == 1)
    assert(size2 == 2 * size1)
    row = Row(3, 1, 1)
    snSession.insert("t1", row)
    val size3 = br.getKeySizeInMemory
    assert(size3 == size1 * 3)
    // Delete an evicted entry
    snSession.delete("t1", "col1 = 1")
    val size4 = br.getKeySizeInMemory
    assert(size4 == size2)

    snSession.update("t1", "COL1 = 2", Row(99), "COL3")
    assert(br.getNumEntriesInVM == 1)
    val size5 = br.getKeySizeInMemory
    assert(size5 == size2)
  }

  test("RR-generated key case") {
    val snSession = new SnappySession(sc)
    snSession.sql("create table t1(col1 Int, col2 Int, col3 Int) " +
        " using row options (EVICTION_BY 'LRUCOUNT 1', OVERFLOW 'true')")

    var row = Row(1, 1, 1)
    snSession.insert("t1", row)
    val br = getLocalRegion("app.t1")
    val size1 = br.getKeySizeInMemory
    assert(size1 > 0)
    row = Row(2, 1, 1)
    snSession.insert("t1", row)
    val size2 = br.getKeySizeInMemory
    // One Row evicted hence key size should have increased
    assert(size2 == 2 * size1)
    row = Row(3, 1, 1)
    snSession.insert("t1", row)
    val size3 = br.getKeySizeInMemory
    assert(size3 == size1 * 3)
    // Delete an evicted entry
    snSession.delete("t1", "col1 = 1")
    val size4 = br.getKeySizeInMemory
    assert(size4 == size2)

    snSession.update("t1", "COL1 = 2", Row(99), "COL3")
    val size5 = br.getKeySizeInMemory
    assert(size5 == size2)
  }

  test("RR for non-generated key case") {
    val snSession = new SnappySession(sc)
    snSession.sql("create table t1(col1 Int, col2 Int, col3 Int, PRIMARY KEY (col1)) " +
        " using row options (EVICTION_BY 'LRUCOUNT 1', OVERFLOW 'true')")

    var row = Row(1, 1, 1)
    snSession.insert("t1", row)
    val br = getLocalRegion("app.t1")
    val size1 = br.getKeySizeInMemory
    assert(size1 == 0)
    row = Row(2, 1, 1)
    snSession.insert("t1", row)
    val size2 = br.getKeySizeInMemory
    // One Row evicted hence key size should have increased
    assert(size2 > size1)
    row = Row(3, 1, 1)
    snSession.insert("t1", row)
    val size3 = br.getKeySizeInMemory
    assert(size3 == size2 * 2)
    // Delete an evicted entry
    snSession.delete("t1", "col1 = 1")
    val size4 = br.getKeySizeInMemory
    assert(size4 == size2)

    snSession.update("t1", "COL1 = 2", Row(99), "COL3")
    val size5 = br.getKeySizeInMemory
    assert(size5 == size2)
  }


  test("PR-Key-Non-Generated-insert-delete") {
    val snSession = new SnappySession(sc)
    snSession.sql("create table t1(col1 Int, col2 Int, col3 Int, PRIMARY KEY (col1)) " +
        " using row options (PARTITION_BY 'col1', buckets '1'," +
        " EVICTION_BY 'LRUCOUNT 1', OVERFLOW 'true')")

    var row = Row(1, 1, 1)
    snSession.insert("t1", row)
    val br = getBucketRegion("app.t1")
    val size1 = br.getKeySizeInMemory
    assert(size1 == 0)
    // Delete will put a tombstone, hence key is copied to map
    snSession.delete("t1", "col1 = 1")
    val size2 = br.getKeySizeInMemory
    assert(size2 > size1)
  }

  private def getBucketRegion(tableName: String): BucketRegion = {
    val cache = GemFireCacheImpl.getInstance
    val regionName = Misc.getRegionPath(tableName).toUpperCase
    val pr = cache.getRegion(regionName).asInstanceOf[PartitionedRegion]
    pr.getRegionAdvisor.getBucket(0).asInstanceOf[BucketRegion]
  }

  private def getLocalRegion(tableName: String): LocalRegion = {
    val cache = GemFireCacheImpl.getInstance
    val regionName = Misc.getRegionPath(tableName).toUpperCase
    cache.getRegion(regionName).asInstanceOf[LocalRegion]
  }
}
