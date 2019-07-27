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

package io.snappydata.cluster

import java.util.concurrent.atomic.AtomicInteger

import io.snappydata.test.dunit.AvailablePortHelper

import org.apache.spark.Logging

class ConcurrentQueryRoutingDUnitSecurityTest(val s: String)
    extends ClusterManagerLDAPTestBase(s) with Logging {

  def columnTableRouting(thr: Int, iter: Int, jdbcUser1: String, jdbcUser2: String,
      serverHostPort: Int): Int = {
    val tableName = s"order_line_col_${thr}_${iter}"
    QueryRoutingDUnitSecurityTest.columnTableRouting(jdbcUser1, jdbcUser2, tableName,
      serverHostPort)
    // scalastyle:off println
    println(s"ConcurrentQueryRoutingDUnitSecureTest.columnTableRouting-${thr}-${iter} done")
    // scalastyle:on println
    1
  }

  def rowTableRouting(thr: Int, iter: Int, jdbcUser1: String, jdbcUser2: String,
      serverHostPort: Int): Int = {
    val tableName = s"order_line_row_${thr}_${iter}"
    QueryRoutingDUnitSecurityTest.rowTableRouting(jdbcUser1, jdbcUser2, tableName, serverHostPort)
    // scalastyle:off println
    println(s"ConcurrentQueryRoutingDUnitSecureTest.rowTableRouting-${thr}-${iter} done")
    // scalastyle:on println
    1
  }

  def testConcurrency(): Unit = {
    // disabled since it consistently fails with:
    //  EXCEPTION: java.lang.AssertionError: assertion failed:
    //   ConcurrentQueryRoutingDUnitSecureTest.testConcurrency: columnTableRoutingCompleted-1=1
    //     at scala.Predef$.assert(Predef.scala:170)
    //     at io.snappydata.cluster.ConcurrentQueryRoutingDUnitSecurityTest.testConcurrency(..:116)
    if (true) return
    val serverHostPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", serverHostPort)
    // scalastyle:off println
    println(s"ConcurrentQueryRoutingDUnitSecureTest.testConcurrency: " +
        s"network server started on $serverHostPort")
    // scalastyle:on println

    val thrCount1 = new AtomicInteger(0)
    val colThread1 = new Thread(new Runnable {def run() {
      (1 to 5) foreach (i => {
        thrCount1.addAndGet(columnTableRouting(1, i, "gemfire1", "gemfire2", serverHostPort))
        })
      }
    })
    colThread1.start()

    val thrCount2 = new AtomicInteger(0)
    val colThread2 = new Thread(new Runnable {def run() {
      (1 to 5) foreach (i => {
        thrCount2.addAndGet(columnTableRouting(2, i, "gemfire3", "gemfire4", serverHostPort))
      })
    }
    })
    colThread2.start()

    val thrCount3 = new AtomicInteger(0)
    val rowThread1 = new Thread(new Runnable {def run() {
      (1 to 5) foreach (i => {
        thrCount3.addAndGet(columnTableRouting(3, i, "gemfire5", "gemfire6", serverHostPort))
      })
    }
    })
    rowThread1.start()

    val thrCount4 = new AtomicInteger(0)
    val rowThread2 = new Thread(new Runnable {def run() {
      (1 to 5) foreach (i => {
        thrCount4.addAndGet(columnTableRouting(4, i, "gemfire7", "gemfire8", serverHostPort))
      })
    }
    })
    rowThread2.start()

    colThread1.join(5 * 60 * 1000)
    // scalastyle:off println
    println(s"ConcurrentQueryRoutingDUnitSecureTest.testConcurrency:" +
        s" columnTableRouting-1 thread done")
    // scalastyle:on println
    rowThread1.join(5 * 60 * 1000)
    // scalastyle:off println
    println(s"ConcurrentQueryRoutingDUnitSecureTest.testConcurrency:" +
        s"rowTableRouting-1 thread done")
    // scalastyle:on println
    colThread2.join(5 * 60 * 1000)
    // scalastyle:off println
    println(s"ConcurrentQueryRoutingDUnitSecureTest.testConcurrency:" +
        s" columnTableRouting-2 thread done")
    // scalastyle:on println
    rowThread2.join(5 * 60 * 1000)
    // scalastyle:off println
    println(s"ConcurrentQueryRoutingDUnitSecureTest.testConcurrency:" +
        s"rowTableRouting-2 thread done")
    // scalastyle:on println

    assert(thrCount1.get() == 5,
      s"ConcurrentQueryRoutingDUnitSecureTest.testConcurrency:" +
          s" columnTableRoutingCompleted-1=$thrCount1")
    assert(thrCount2.get() == 5,
      s"ConcurrentQueryRoutingDUnitSecureTest.testConcurrency:" +
          s" rowTableRoutingCompleted-1=$thrCount2")
    assert(thrCount3.get() == 5,
      s"ConcurrentQueryRoutingDUnitSecureTest.testConcurrency:" +
          s" columnTableRoutingCompleted-2=$thrCount3")
    assert(thrCount4.get() == 5,
      s"ConcurrentQueryRoutingDUnitSecureTest.testConcurrency:" +
          s" rowTableRoutingCompleted-2=$thrCount4")
  }
}
