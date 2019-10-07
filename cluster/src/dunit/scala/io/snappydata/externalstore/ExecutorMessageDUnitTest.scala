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

package io.snappydata.externalstore

import scala.util.Random

import io.snappydata.cluster.{ClusterManagerTestBase, ExecutorInitiator}
import io.snappydata.test.dunit.DistributedTestBase
import io.snappydata.test.dunit.DistributedTestBase.WaitCriterion

import org.apache.spark.Logging
import org.apache.spark.sql.{SaveMode, SnappyContext}

class ExecutorMessageDUnitTest(s: String) extends ClusterManagerTestBase(s) with Logging {

  val tableName = "ExecutorMessageDUnitTest_table"

  def test01StoreBlockMapUpdatesWithExecutorDown(): Unit = {
    val snc = SnappyContext(sc)
    var props = Map.empty[String, String]
    props += ("BUCKETS" -> "7")
    executeSomething(snc, props)
    verifyMap(snc, "stopExecutor")
    restartSpark()
    getLogWriter.info("test01StoreBlockMapUpdatesWithExecutorDown() Successful")
  }

  def test02StoreBlockMapUpdatesWithNodeDown(): Unit = {
    val snc = SnappyContext(sc)
    var props = Map.empty[String, String]
    props += ("BUCKETS" -> "7")
    props += ("REDUNDANCY" -> "2")
    executeSomething(snc, props)
    verifyMap(snc, "stopProcess")
    getLogWriter.info("test02StoreBlockMapUpdatesWithNodeDown() Successful")
  }

  def executeSomething(snc: SnappyContext,
      props: Map[String, String] = Map.empty[String, String]): Unit = {
    createAndPopulateTable(snc, props)

    val wc: WaitCriterion = new WaitCriterion {
      override def done(): Boolean = {
        SnappyContext.getAllBlockIds.size == 4 // 3 servers + 1 lead/driver
      }
      override def description(): String = {
        s"Expected SnappyContext.storeToBlockMap.size: 4, actual: " +
            s"${SnappyContext.getAllBlockIds.size}"
      }
    }
    DistributedTestBase.waitForCriterion(wc, 10000, 500, true)
    for ((dm, blockId) <- SnappyContext.getAllBlockIds) {
      assert(blockId != null)
    }
  }

  def createAndPopulateTable(snc: SnappyContext, props: Map[String, String]): Unit = {
    var data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    1 to 1000 foreach { _ =>
      data = data :+ Seq.fill(3)(Random.nextInt)
    }

    val rdd = sc.parallelize(data, data.length).map(s => Data(s.head, s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)

    snc.createTable(tableName, "column", dataDF.schema, props)

    dataDF.write.format("column").mode(SaveMode.Append)
        .options(props).saveAsTable(tableName)
  }

  def verifyMap(snc: SnappyContext, m: String): Unit = {
    vm0.invoke(getClass, m)
    assert(SnappyContext.getAllBlockIds.size == 3)
    for ((dm, blockId) <- SnappyContext.getAllBlockIds) {
      assert(blockId != null)
    }
    verifyTable(snc)

    vm1.invoke(getClass, m)
    assert(SnappyContext.getAllBlockIds.size == 2)
    for ((dm, blockId) <- SnappyContext.getAllBlockIds) {
      assert(blockId != null)
    }
    verifyTable(snc)

//    vm2.invoke(getClass, m) // Don't shutdown the last executor, else cleanup will fail.
//    assert(SnappyContext.storeToBlockMap.size == 1)
  }

  def restartSpark(): Unit = {
    ClusterManagerTestBase.stopSpark()
    ClusterManagerTestBase.startSnappyLead(ClusterManagerTestBase.locatorPort, bootProps)
  }

  def verifyTable (snc: SnappyContext): Unit = {
    val count = snc.sql("SELECT * FROM " + tableName).collect().length
    assert(count == 1005, s"unexpected count $count")
  }
}

object ExecutorMessageDUnitTest {

  def stopExecutor(): Unit = {
    ExecutorInitiator.stop()
    Thread.sleep(1000)
  }

  def stopProcess(): Unit = {
    ClusterManagerTestBase.stopAny()
    // Thread.sleep(2000)
  }
}
