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


import java.util.Properties

import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.SerializableRunnable

import org.apache.spark.SparkEnv
import org.apache.spark.sql.{SaveMode, SnappyContext}
import SnappyUnifiedMemoryManagerDUnitTest._

case class DummyData(col1: Int, col2: Int, col3: Int)
class SnappyUnifiedMemoryManagerDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  val col_table = "app.col_table"
  val rr_table = "app.rr_table"
  val memoryMode = MemoryMode.ON_HEAP

  bootProps.setProperty(io.snappydata.Property.CachedBatchSize.name, "500")

  def newContext(): SnappyContext = {
    val snc = SnappyContext(sc).newSession()
    snc
  }

  def assertApproximate(value1: Long, value2: Long): Unit = {
    if (value1 == value2) return
    if(Math.abs(value1 - value2) > value2 / 10){
      throw new java.lang.AssertionError(s"assertion " +
          s"failed $value1 & $value2 are not within permissable limit")
    }
  }

  def testMemoryUsedInReplication(): Unit = {
    val snc = newContext()
    vm1.invoke(getClass, "resetStorageMemory")
    vm2.invoke(getClass, "resetStorageMemory")
    val data = for (i <- 1 to 700) yield (Seq(i, (i + 1), (i + 2)))
    val rdd = snc.sparkContext.parallelize(data.toSeq, data.length).map(s =>
      DummyData(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(rr_table, "row", dataDF.schema, Map.empty[String, String])
    dataDF.write.insertInto(rr_table)
    val vm1_memoryUsed = vm1.invoke(getClass, "getStorageMemory").asInstanceOf[Long]
    val vm2_memoryUsed = vm2.invoke(getClass, "getStorageMemory").asInstanceOf[Long]
    println(s"vm1_memoryUsed $vm1_memoryUsed vm2_memoryUsed $vm2_memoryUsed")
    assertApproximate(vm1_memoryUsed , vm2_memoryUsed)
  }

  def testMemoryUsedInBucketRegions(): Unit = {
    val snc = newContext()
    vm1.invoke(getClass, "resetStorageMemory")
    vm2.invoke(getClass, "resetStorageMemory")
    val data = for (i <- 1 to 700) yield (Seq(i, (i + 1), (i + 2)))
    val rdd = snc.sparkContext.parallelize(data.toSeq, data.length).map(s =>
      DummyData(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    val options = "OPTIONS (PARTITION_BY 'Col1', REDUNDANCY '1')"
    snc.sql("CREATE TABLE " + rr_table + " (Col1 INT, Col2 INT, Col3 INT) " + " USING row " +
        options
    )
    dataDF.write.insertInto(rr_table)
    val vm1_memoryUsed = vm1.invoke(getClass, "getStorageMemory").asInstanceOf[Long]
    val vm2_memoryUsed = vm2.invoke(getClass, "getStorageMemory").asInstanceOf[Long]
    println(s"vm1_memoryUsed $vm1_memoryUsed vm2_memoryUsed $vm2_memoryUsed")
    assertApproximate(vm1_memoryUsed , vm2_memoryUsed)
  }

  def _testMemoryAfterRecovery(): Unit = {
    var props = bootProps.clone().asInstanceOf[java.util.Properties]
    val port = ClusterManagerTestBase.locPort

    props.setProperty("eviction-heap-percentage", "80")

    def restartServer(props: Properties): SerializableRunnable = new SerializableRunnable() {
      override def run(): Unit = ClusterManagerTestBase.startSnappyServer(port, props)
    }

    val snc = newContext()
    vm1.invoke(getClass, "resetStorageMemory")
    vm2.invoke(getClass, "resetStorageMemory")

    val data = for (i <- 1 to 700) yield (Seq(i, (i + 1), (i + 2)))
    val rdd = snc.sparkContext.parallelize(data.toSeq, data.length).map(s =>
      DummyData(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    val options = "OPTIONS (PARTITION_BY 'Col1', PERSISTENT 'SYNCHRONOUS')"
    snc.sql("CREATE TABLE " + rr_table + " (Col1 INT, Col2 INT, Col3 INT) " + " USING row " +
        options
    )
    dataDF.write.insertInto(rr_table)
    var vm1_memoryUsed = vm1.invoke(getClass, "getStorageMemory").asInstanceOf[Long]
    val vm2_memoryUsed = vm2.invoke(getClass, "getStorageMemory").asInstanceOf[Long]
    println(s"vm1_memoryUsed $vm1_memoryUsed vm2_memoryUsed $vm2_memoryUsed")
    assertApproximate(vm1_memoryUsed , vm2_memoryUsed)
    vm1.invoke(classOf[ClusterManagerTestBase], "stopAny")
    props = bootProps

    vm1.invoke(restartServer(props))
    val post_restart_vm1_memoryUsed = vm1.invoke(getClass, "getStorageMemory").asInstanceOf[Long]
    println(s"vm1_memoryUsed $vm1_memoryUsed post_restart_vm1_memoryUsed $post_restart_vm1_memoryUsed")
    assertApproximate(vm1_memoryUsed , post_restart_vm1_memoryUsed)

  }
}

object SnappyUnifiedMemoryManagerDUnitTest {
  private def sc = SnappyContext.globalSparkContext
  val memoryMode = MemoryMode.ON_HEAP

  def resetStorageMemory() =
    SparkEnv.get.memoryManager.asInstanceOf[SnappyUnifiedMemoryManager].dropAllObjects(memoryMode)

  def getStorageMemory(): Long = SparkEnv.get.memoryManager.storageMemoryUsed

  def failTheExecutors: Unit = {
    sc.parallelize(1 until 100, 5).map { i =>
      throw new InternalError()
    }.collect()
  }

  def failAllExecutors : Unit = {
    try {
      failTheExecutors
    } catch {
      case _: Throwable =>
    }
    Thread.sleep(1000)
  }

}
