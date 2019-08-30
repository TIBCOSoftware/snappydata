/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

import io.snappydata.cluster.{ClusterManagerTestBase, ExecutorInitiator}
import io.snappydata.test.dunit.{DistributedTestBase, SerializableRunnable}
import org.eclipse.collections.api.block.procedure.primitive.ObjectLongProcedure

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.collection.Utils


class MemoryManagerRestartDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  self =>

  import MemoryManagerRestartDUnitTest._

  def testExecutorRestart(): Unit = {
    vm1.invoke(getClass, "waitForExecutor")

    val oldID = vm1.invoke(getClass, "getMemoryManagerIdentity").asInstanceOf[Int]

    assert(vm1.invoke(getClass, "allocateStorage",
      Array("testExecutorRestart", false, 1000L).asInstanceOf[Array[Object]]).asInstanceOf[Boolean])

    val t1 = new Thread(new Runnable {
      override def run() = try {
        failTheExecutors()
      } catch {
        case _: Throwable =>
      }
    })
    t1.start()
    t1.join()

    DistributedTestBase.waitForCriterion(new DistributedTestBase.WaitCriterion {
      override def done(): Boolean = {
        vm1.invoke(self.getClass, "waitForExecutor")
        try {
          vm1.invoke(self.getClass, "getMemoryManagerIdentity").asInstanceOf[Int] != oldID
        } catch {
          case _: AssertionError => false // ignore and retry till timeout
        }
      }

      override def description(): String =
        "waiting for executor to restart with changed memory manager"
    }, 30000, 500, true)

    val value1 = vm1.invoke(getClass, "getMemoryForTable", "testExecutorRestart").asInstanceOf[Long]
    assert(value1 == 1000L, s"The storage for object should be 1000 rather than $value1")
  }

  def testCacheCloseRestart(): Unit = {
    vm1.invoke(getClass, "waitForExecutor")

    val props = bootProps.clone().asInstanceOf[java.util.Properties]
    val port = ClusterManagerTestBase.locPort

    val oldID = vm1.invoke(getClass, "getMemoryManagerIdentity").asInstanceOf[Int]

    assert(vm1.invoke(getClass, "allocateStorage",
      Array("testCacheCloseRestart", false, 1000L).
          asInstanceOf[Array[Object]]).asInstanceOf[Boolean])

    vm1.invoke(classOf[ClusterManagerTestBase], "stopAny")
    vm1.invoke(restartServerRunnable(props, port))

    val newID = vm1.invoke(getClass, "getMemoryManagerIdentity").asInstanceOf[Int]
    assert(newID != oldID, "The MemoryManager instance has not changed as expected")

    val value1 = vm1.invoke(getClass, "getMemoryForTable", "testExecutorRestart").asInstanceOf[Long]
    assert(value1 == 0L, s"The storage for object should be 0L rather than $value1")
  }

  private def restartServerRunnable(props: Properties, port: Int): SerializableRunnable = {
    new SerializableRunnable() {
      override def run(): Unit = {
        ClusterManagerTestBase.startSnappyServer(port, props)
        ClusterManagerTestBase.waitForCriterion(SparkEnv.get != null,
          "Executor Service did not start in specified time ", 20000, 5000, true)
      }
    }
  }

  def testCacheClose(): Unit = {
    vm1.invoke(getClass, "waitForExecutor")
    val props = bootProps.clone().asInstanceOf[java.util.Properties]
    val port = ClusterManagerTestBase.locPort

    assert(vm1.invoke(getClass, "allocateStorage",
      Array("testCacheCloseRestart", false, 1000L).
          asInstanceOf[Array[Object]]).asInstanceOf[Boolean])

    vm1.invoke(classOf[ClusterManagerTestBase], "stopAny")

    try {
      val bootMemorySize = vm1.invoke(getClass, "getBootMemoryManagerSize").asInstanceOf[Long]
      assert(bootMemorySize == 0L, "After cache close bootMemory map size is greater than 0L")
    } finally {
      vm1.invoke(restartServerRunnable(props, port))
    }

  }

  def testDriverRestart(): Unit = {
    var stopped = false
    var oldID = -1
    try {
      vm1.invoke(getClass, "waitForExecutor")
      oldID = vm1.invoke(getClass, "getMemoryManagerIdentity").asInstanceOf[Int]
      assert(vm1.invoke(getClass, "allocateStorage",
        Array("testDriverRestart", false, 1000L).asInstanceOf[Array[Object]]).asInstanceOf[Boolean])

      ClusterManagerTestBase.stopSpark()
      stopped = true
    } finally {
      val t1 = new Thread(new Runnable {
        override def run() = if (stopped) {
          ClusterManagerTestBase.startSnappyLead(ClusterManagerTestBase.locatorPort, bootProps)
        }
      })

      t1.start()
      vm1.invoke(getClass, "waitForExecutor")
      t1.join(30000)
      val newID = vm1.invoke(getClass, "getMemoryManagerIdentity").asInstanceOf[Int]
      assert(newID != oldID, "The MemoryManager instance has not changed as expected")
    }
    val value1 = vm1.invoke(getClass, "getMemoryForTable", "testDriverRestart").asInstanceOf[Long]
    assert(value1 == 1000L, s"The storage for object should be 1000 rather than $value1")
  }
}

object MemoryManagerRestartDUnitTest {

  def getBootMemoryManagerSize(): Long = {
    MemoryManagerCallback.bootMemoryManager.
        asInstanceOf[SnappyUnifiedMemoryManager].memoryForObject.size()
  }

  def waitForExecutor(): Unit = {
    var l = 0L
    while (SparkEnv.get eq null) {
      if (l > 30000) throw new Exception(s"Executors did not start in 30 seconds")
      Thread.sleep(500)
      l += 500
    }
    ExecutorInitiator.testWaitForExecutor()
  }

  def failTheExecutors(): Unit = {
    Utils.mapExecutors[Unit](sc, () => {
      throw new OutOfMemoryError("Some Random message") // See SystemFailure.isJVMFailureError
    })
  }

  private def sc = SnappyContext.globalSparkContext

  def getMemoryManagerIdentity(): Int = {
    assert(SparkEnv.get != null, "Executor is still not initialized")
    assert(SparkEnv.get.memoryManager.isInstanceOf[SnappyUnifiedMemoryManager])
    val memoryManager = SparkEnv.get.memoryManager.asInstanceOf[SnappyUnifiedMemoryManager]
    System.identityHashCode(memoryManager)
  }

  def getMemoryForTable(tableName: String): Long = {
    assert(SparkEnv.get != null, "Executor is still not initialized")
    assert(SparkEnv.get.memoryManager.isInstanceOf[SnappyUnifiedMemoryManager])
    val memoryManager = SparkEnv.get.memoryManager.asInstanceOf[SnappyUnifiedMemoryManager]
    val mMap = memoryManager.memoryForObject
    memoryManager.logStats()
    var sum = 0L
    mMap.forEachKeyValue(new ObjectLongProcedure[MemoryOwner] {
      override def value(key: MemoryOwner, value: Long): Unit = {
        if (key.owner.toLowerCase().contains(tableName.toLowerCase())) {
          sum += value
        }
      }
    })
    sum
  }

  def allocateStorage(tableName: String, offHeap: Boolean, numBytes: Long): Boolean = {
    assert(SparkEnv.get != null, "Executor is still not initialized")
    assert(SparkEnv.get.memoryManager.isInstanceOf[SnappyUnifiedMemoryManager])
    val success = SparkEnv.get.memoryManager
        .asInstanceOf[SnappyUnifiedMemoryManager]
        .acquireStorageMemoryForObject(objectName = tableName,
          blockId = MemoryManagerCallback.storageBlockId,
          numBytes = numBytes,
          memoryMode = if (offHeap) MemoryMode.OFF_HEAP else MemoryMode.ON_HEAP,
          buffer = null,
          shouldEvict = false)

    success
  }
}
