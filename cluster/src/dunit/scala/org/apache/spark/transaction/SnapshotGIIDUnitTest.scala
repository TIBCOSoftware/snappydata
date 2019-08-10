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
package org.apache.spark.transaction

import java.util.Properties

import com.gemstone.gemfire.internal.cache.{BucketRegion, GemFireCacheImpl, LocalRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.SerializableRunnable

import org.apache.spark.sql.{SaveMode, SnappyContext}

case class Data(col1: Int, col2: Int, col3: Int)

class SnapshotGIIDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  def testColumnTableGII(): Unit = {

    val snc = SnappyContext(sc)
    val tableName = "app.test_table"
    createTable(snc, tableName, Map("BUCKETS" -> "1", "REDUNDANCY" -> "2"))

    var props = bootProps.clone().asInstanceOf[java.util.Properties]
    val port = ClusterManagerTestBase.locPort

    def restartServer(props: Properties): SerializableRunnable = new SerializableRunnable() {
      override def run(): Unit = ClusterManagerTestBase.startSnappyServer(port, props)
    }

    vm1.invoke(classOf[ClusterManagerTestBase], "stopAny")

    val data = for (i <- 1 to 50) yield (Seq(i, (i + 1), (i + 2)))
    val rdd = snc.sparkContext.parallelize(data, 2).map(s =>
      Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    dataDF.write.insertInto(tableName)
    vm1.invoke(restartServer(props))
    vm1.invoke(waitForRegionInit(tableName))
    dataDF.write.insertInto(tableName)
    val numRows = snc.sql(s"select * from $tableName").collect().length
    assert(numRows == 150)
  }

  def createTable(snc: SnappyContext,
      tableName: String,
      props: Map[String, String]): Unit = {
    val data = for (i <- 1 to 50) yield (Seq(i, (i + 1), (i + 2)))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(tableName, "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).saveAsTable(tableName)
  }

  @throws[Exception]
  protected def waitForRegionInit(tableName: String): SerializableRunnable = {
    new SerializableRunnable() {
      def run() {
        val regionName = Misc.getRegionPath(tableName).toUpperCase
        while (!Misc.initialDDLReplayDone()) Thread.sleep(100)
        val cache = GemFireCacheImpl.getInstance
        val pr = cache.getRegion(regionName).asInstanceOf[PartitionedRegion]
        while (!pr.getRegionAdvisor.areBucketsInitialized) Thread.sleep(100)
        while (!pr.getRegionAdvisor.getBucket(0).isInstanceOf[BucketRegion]) Thread.sleep(100)
        val lr = pr.getRegionAdvisor.getBucket(0).asInstanceOf[LocalRegion]
        lr.waitOnInitialization()
      }
    }
  }

}
