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


import java.lang.management.ManagementFactory
import java.util.Properties

import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.{SerializableRunnable, VM}

import org.apache.spark.SparkEnv
import org.apache.spark.sql.{SaveMode, SnappyContext}
import SnappyUnifiedMemoryManagerDUnitTest._

case class DummyData(col1: Int, col2: Int, col3: Int)

class WaitAssert(val error: Int, clazz: Class[_]) {

  var value1 = 0L
  var value2 = 0L
  var excString = ""

  def assertStorageUsed(vm1: VM, vm2: VM, ignoreByteCount: Int = 0): Boolean = {
    value1 = vm1.invoke(clazz, "getStorageMemory").asInstanceOf[Long]
    value2 = vm2.invoke(clazz, "getStorageMemory").asInstanceOf[Long]
    println(s"vm1_memoryUsed $value1 vm2_memoryUsed $value2")
    excString = s"failed $value1 & $value2 are not within permissable limit \n"

    if (value1 == value2) return true
    if (value1 < value2) {
      value1 += ignoreByteCount
    } else {
      value2 -= ignoreByteCount
    }
    if (Math.abs(value1 - value2) < ((value2 * error) / 100)) return true else false

  }

  def exceptionString(): String = excString
}

class SnappyUnifiedMemoryManagerDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  val col_table = "app.col_table"
  val rr_table = "app.rr_table"
  val memoryMode = MemoryMode.ON_HEAP

  bootProps.setProperty(io.snappydata.Property.CachedBatchSize.name, "500")
  bootProps.setProperty("critical-heap-percentage", "90")

  def newContext(): SnappyContext = {
    val snc = SnappyContext(sc).newSession()
    println("PID = " + ManagementFactory.getRuntimeMXBean().getName())
    snc

  }

  def resetMemoryManagers(): Unit = {
    vm0.invoke(getClass, "resetStorageMemory")
    vm1.invoke(getClass, "resetStorageMemory")
    vm2.invoke(getClass, "resetStorageMemory")
  }

  override def tearDown2(): Unit = {
    resetMemoryManagers
    super.tearDown2()
  }

  //Approximate because we include hash map size also, which can vary across VMs
  def assertApproximate(value1: Long, value2: Long, error: Int = 5): Unit = {
    if (value1 == value2) return
    if (Math.abs(value1 - value2) > ((value2 * error) / 100)) {
      // Error target should be 1
      throw new java.lang.AssertionError(s"assertion " +
          s"failed $value1 & $value2 are not within permissable limit")
    }
  }

  def assertForWait(value1: Long, value2: Long, error: Int = 5): Boolean = {
    if (value1 == value2) return true
    if (Math.abs(value1 - value2) < ((value2 * error) / 100)) return true else false
  }

  def testMemoryUsedInReplication(): Unit = {
    val snc = newContext()
    val data = for (i <- 1 to 500) yield (Seq(i, (i + 1), (i + 2)))
    val rdd = snc.sparkContext.parallelize(data.toSeq, data.length).map(s =>
      DummyData(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(rr_table, "row", dataDF.schema, Map.empty[String, String])
    dataDF.write.insertInto(rr_table)
    val vm1_memoryUsed = vm1.invoke(getClass, "getStorageMemory").asInstanceOf[Long]
    val vm2_memoryUsed = vm2.invoke(getClass, "getStorageMemory").asInstanceOf[Long]
    println(s"vm1_memoryUsed $vm1_memoryUsed vm2_memoryUsed $vm2_memoryUsed")
    assertApproximate(vm1_memoryUsed, vm2_memoryUsed)
  }

  def testMemoryUsedInBucketRegions_RowTables(): Unit = {
    val snc = newContext()
    val data = for (i <- 1 to 500) yield (Seq(i, (i + 1), (i + 2)))
    val rdd = snc.sparkContext.parallelize(data.toSeq, data.length).map(s =>
      DummyData(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    val options = "OPTIONS (BUCKETS '113', PARTITION_BY 'Col1', REDUNDANCY '2')"
    snc.sql("CREATE TABLE " + rr_table + " (Col1 INT, Col2 INT, Col3 INT) " + " USING row " +
        options
    )
    dataDF.write.insertInto(rr_table)

    val vm1_memoryUsed = vm1.invoke(getClass, "getStorageMemory").asInstanceOf[Long]
    val vm2_memoryUsed = vm2.invoke(getClass, "getStorageMemory").asInstanceOf[Long]
    println(s"vm1_memoryUsed $vm1_memoryUsed vm2_memoryUsed $vm2_memoryUsed")
    assertApproximate(vm1_memoryUsed, vm2_memoryUsed)
  }

  def testMemoryUsedInBucketRegions_ColumntTables(): Unit = {
    val snc = newContext()
    val data = for (i <- 1 to 500) yield (Seq(i, (i + 1), (i + 2)))
    val rdd = snc.sparkContext.parallelize(data.toSeq, data.length).map(s =>
      DummyData(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    val options = "OPTIONS (BUCKETS '113', PARTITION_BY 'Col1', REDUNDANCY '2')"
    snc.sql("CREATE TABLE " + col_table + " (Col1 INT, Col2 INT, Col3 INT) " + " USING column " +
        options
    )
    dataDF.write.insertInto(col_table)

    val vm1_memoryUsed = vm1.invoke(getClass, "getStorageMemory").asInstanceOf[Long]
    val vm2_memoryUsed = vm2.invoke(getClass, "getStorageMemory").asInstanceOf[Long]
    println(s"vm1_memoryUsed $vm1_memoryUsed vm2_memoryUsed $vm2_memoryUsed")
    assertApproximate(vm1_memoryUsed, vm2_memoryUsed)
  }

  /**
    * This test checks column table memory usage when GII is done in a node.
    * It checks memory usage with reference to the node which was alive at the time
    * of GII.
    */
  def testMemoryUsedInColumnTableWithGII(): Unit = {
    var props = bootProps.clone().asInstanceOf[java.util.Properties]
    val port = ClusterManagerTestBase.locPort
    props.setProperty("eviction-heap-percentage", "80")

    def restartServer(props: Properties): SerializableRunnable = new SerializableRunnable() {
      override def run(): Unit = ClusterManagerTestBase.startSnappyServer(port, props)
    }

    vm1.invoke(classOf[ClusterManagerTestBase], "stopAny")
    val snc = newContext()
    val data = for (i <- 1 to 500) yield (Seq(i, (i + 1), (i + 2)))
    val rdd = snc.sparkContext.parallelize(data.toSeq, data.length).map(s =>
      DummyData(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    val options = "OPTIONS (BUCKETS '5', PARTITION_BY 'Col1', REDUNDANCY '2')"
    snc.sql("CREATE TABLE " + col_table + " (Col1 INT, Col2 INT, Col3 INT) " + " USING column " +
        options
    )
    dataDF.write.insertInto(col_table)
    vm1.invoke(restartServer(props))

    val waitAssert = new WaitAssert(2, getClass)
    //Setting ignore bytecount as VM doing GII does have a valid value, hence key is kept as null
    // This decreases the size of entry overhead. @TODO find out why only column table needs this ?
    ClusterManagerTestBase.waitForCriterion(waitAssert.assertStorageUsed(vm1, vm2, 16000),
      waitAssert.exceptionString(),
      20000, 5000, true)
  }

  /**
    * This test checks replicated table memory usage when GII is done in a node.
    * It checks memory usage with reference to the node which was alive at the time
    * of GII.
    */
  def testMemoryUsedInReplicatedTableWithGII(): Unit = {
    var props = bootProps.clone().asInstanceOf[java.util.Properties]
    val port = ClusterManagerTestBase.locPort
    props.setProperty("eviction-heap-percentage", "80")

    def restartServer(props: Properties): SerializableRunnable = new SerializableRunnable() {
      override def run(): Unit = ClusterManagerTestBase.startSnappyServer(port, props)
    }

    vm1.invoke(classOf[ClusterManagerTestBase], "stopAny")
    val snc = newContext()
    val data = for (i <- 1 to 500) yield (Seq(i, (i + 1), (i + 2)))
    val rdd = snc.sparkContext.parallelize(data.toSeq, data.length).map(s =>
      DummyData(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(rr_table, "row", dataDF.schema, Map.empty[String, String])
    dataDF.write.insertInto(rr_table)
    vm1.invoke(restartServer(props))

    val waitAssert = new WaitAssert(2, getClass)
    ClusterManagerTestBase.waitForCriterion(waitAssert.assertStorageUsed(vm1, vm2),
      waitAssert.exceptionString(),
      20000, 5000, true)
  }

  /**
    * This test checks row partitioned table memory usage when GII is done in a node.
    * It checks memory usage with reference to the node which was alive at the time
    * of GII.
    */
  def testMemoryUsedInRowPartitionedTableWithGII(): Unit = {
    val props = bootProps.clone().asInstanceOf[java.util.Properties]
    val port = ClusterManagerTestBase.locPort

    def restartServer(props: Properties): SerializableRunnable = new SerializableRunnable() {
      override def run(): Unit = ClusterManagerTestBase.startSnappyServer(port, props)
    }

    vm1.invoke(classOf[ClusterManagerTestBase], "stopAny")
    val snc = newContext()
    val data = for (i <- 1 to 500) yield (Seq(i, (i + 1), (i + 2)))
    val rdd = snc.sparkContext.parallelize(data.toSeq, data.length).map(s =>
      DummyData(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    val options = "OPTIONS (BUCKETS '5', PARTITION_BY 'Col1', REDUNDANCY '2')"
    snc.sql("CREATE TABLE " + rr_table + " (Col1 INT, Col2 INT, Col3 INT) " + " USING row " +
        options
    )
    dataDF.write.insertInto(rr_table)
    vm1.invoke(restartServer(props))

    val waitAssert = new WaitAssert(2, getClass)
    ClusterManagerTestBase.waitForCriterion(waitAssert.assertStorageUsed(vm1, vm2),
      waitAssert.exceptionString(),
      20000, 5000, true)
  }

  /**
    * This test checks row partitioned table memory usage when GII is done in a node.
    * It checks memory usage with reference to the node which was alive at the time
    * of GII. At the same time we fire deletes on the region.
    */
  def testMemoryUsedInReplicationParTableGIIWithDeletes(): Unit = {
    val props = bootProps.clone().asInstanceOf[java.util.Properties]
    val port = ClusterManagerTestBase.locPort

    def restartServer(props: Properties): SerializableRunnable = new SerializableRunnable() {
      override def run(): Unit = ClusterManagerTestBase.startSnappyServer(port, props)
    }

    vm1.invoke(classOf[ClusterManagerTestBase], "stopAny")
    val snc = newContext()
    val data = for (i <- 1 to 500) yield (Seq(i, (i + 1), (i + 2)))
    val rdd = snc.sparkContext.parallelize(data.toSeq, data.length).map(s =>
      DummyData(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    val options = "OPTIONS (BUCKETS '5', PARTITION_BY 'Col1', REDUNDANCY '2')"
    snc.sql("CREATE TABLE " + rr_table + " (Col1 INT, Col2 INT, Col3 INT) " + " USING row " +
        options
    )
    dataDF.write.insertInto(rr_table)

    val otherExecutorThread = new Thread(new Runnable {

      def run() {
        (1 to 100).map(i => snc.delete(rr_table, s"col1=$i"))
      }
    })
    otherExecutorThread.start()

    vm1.invoke(restartServer(props))

    val waitAssert = new WaitAssert(2, getClass)
    //The delete operation takes time to propagate
    ClusterManagerTestBase.waitForCriterion(waitAssert.assertStorageUsed(vm1, vm2),
      waitAssert.exceptionString(),
      30000, 5000, true)
  }


  def testMemoryAfterRecovery_ColumnTable(): Unit = {
    val props = bootProps.clone().asInstanceOf[java.util.Properties]
    val port = ClusterManagerTestBase.locPort

    def restartServer(props: Properties): SerializableRunnable = new SerializableRunnable() {
      override def run(): Unit = ClusterManagerTestBase.startSnappyServer(port, props)
    }

    val snc = newContext()

    val data = for (i <- 1 to 500) yield (Seq(i, (i + 1), (i + 2)))
    val rdd = snc.sparkContext.parallelize(data.toSeq, data.length).map(s =>
      DummyData(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    val options = "OPTIONS (BUCKETS '113', PARTITION_BY 'Col1', PERSISTENT 'SYNCHRONOUS', REDUNDANCY '2')"
    snc.sql("CREATE TABLE " + col_table + " (Col1 INT, Col2 INT, Col3 INT) " + " USING column " +
        options
    )
    dataDF.write.insertInto(col_table)
    var vm1_memoryUsed = vm1.invoke(getClass, "getStorageMemory").asInstanceOf[Long]
    val vm2_memoryUsed = vm2.invoke(getClass, "getStorageMemory").asInstanceOf[Long]
    println(s"vm1_memoryUsed $vm1_memoryUsed vm2_memoryUsed $vm2_memoryUsed")
    assertApproximate(vm1_memoryUsed, vm2_memoryUsed)
    vm1.invoke(classOf[ClusterManagerTestBase], "stopAny")

    vm1.invoke(restartServer(props))

    val waitAssert = new WaitAssert(2, getClass)
    //The delete operation takes time to propagate
    ClusterManagerTestBase.waitForCriterion(waitAssert.assertStorageUsed(vm1, vm2),
      waitAssert.exceptionString(),
      30000, 5000, true)

  }


  def testMemoryAfterRecovery_RowTable(): Unit = {
    val props = bootProps.clone().asInstanceOf[java.util.Properties]
    val port = ClusterManagerTestBase.locPort

    def restartServer(props: Properties): SerializableRunnable = new SerializableRunnable() {
      override def run(): Unit = ClusterManagerTestBase.startSnappyServer(port, props)
    }

    val snc = newContext()

    val data = for (i <- 1 to 500) yield (Seq(i, (i + 1), (i + 2)))
    val rdd = snc.sparkContext.parallelize(data.toSeq, data.length).map(s =>
      DummyData(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    val options = "OPTIONS (BUCKETS '113', PARTITION_BY 'Col1', PERSISTENT 'SYNCHRONOUS', REDUNDANCY '2')"
    snc.sql("CREATE TABLE " + rr_table + " (Col1 INT, Col2 INT, Col3 INT) " + " USING row " +
        options
    )
    dataDF.write.insertInto(rr_table)
    var vm1_memoryUsed = vm1.invoke(getClass, "getStorageMemory").asInstanceOf[Long]
    val vm2_memoryUsed = vm2.invoke(getClass, "getStorageMemory").asInstanceOf[Long]
    println(s"vm1_memoryUsed $vm1_memoryUsed vm2_memoryUsed $vm2_memoryUsed")
    assertApproximate(vm1_memoryUsed, vm2_memoryUsed)
    vm1.invoke(classOf[ClusterManagerTestBase], "stopAny")

    vm1.invoke(restartServer(props))
    
    val waitAssert = new WaitAssert(2, getClass)
    //The delete operation takes time to propagate
    ClusterManagerTestBase.waitForCriterion(waitAssert.assertStorageUsed(vm1, vm2),
      waitAssert.exceptionString(),
      30000, 5000, true)
  }
}

object SnappyUnifiedMemoryManagerDUnitTest {
  private def sc = SnappyContext.globalSparkContext

  val memoryMode = MemoryMode.ON_HEAP

  def resetStorageMemory() =
    MemoryManagerCallback.resetMemoryManager()

  def getStorageMemory(): Long = {
    SparkEnv.get.memoryManager.storageMemoryUsed
  }

  def failTheExecutors: Unit = {
    sc.parallelize(1 until 100, 5).map { i =>
      throw new InternalError()
    }.collect()
  }

  def failAllExecutors: Unit = {
    try {
      failTheExecutors
    } catch {
      case _: Throwable =>
    }
    Thread.sleep(1000)
  }

}
