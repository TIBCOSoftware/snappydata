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

package io.snappydata.cluster

import java.util.concurrent.atomic.AtomicInteger

import io.snappydata.test.dunit.AvailablePortHelper

import org.apache.spark.Logging

class ConcurrentQueryRoutingDUnitSecureTest(val s: String)
    extends ClusterManagerLDAPTestBase(s) with Logging {

  var columnTableRoutingCompleted: AtomicInteger = new AtomicInteger(0)
  def columnTableRouting(): Int = {
    val jdbcUser1 = "gemfire1"
    val jdbcUser2 = "gemfire5"
    val tableName = "order_line_col"
    val serverHostPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", serverHostPort)
    // scalastyle:off println
    println(s"ConcurrentQueryRoutingDUnitSecureTest.columnTableRouting:" +
        s"network server started at $serverHostPort")
    // scalastyle:on println

    QueryRoutingDUnitSecureTest.columnTableRouting(jdbcUser1, jdbcUser2, tableName, serverHostPort)
    columnTableRoutingCompleted.incrementAndGet()
  }

  var rowTableRoutingCompleted: AtomicInteger = new AtomicInteger(0)
  def rowTableRouting(): Int = {
    val jdbcUser1 = "gemfire2"
    val jdbcUser2 = "gemfire6"
    val tableName = "order_line_row"
    val serverHostPort = AvailablePortHelper.getRandomAvailableTCPPort
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer", serverHostPort)
    // scalastyle:off println
    println(s"ConcurrentQueryRoutingDUnitSecureTest.rowTableRouting:" +
        s"network server started at $serverHostPort")
    // scalastyle:on println

    QueryRoutingDUnitSecureTest.rowTableRouting(jdbcUser1, jdbcUser2, tableName, serverHostPort)
    rowTableRoutingCompleted.incrementAndGet()
  }

  def testConcurrency(): Unit = {
    val colThread = new Thread(new Runnable {
      def run() {
        (1 to 5) foreach (i => {
          // scalastyle:off println
          println(s"ConcurrentQueryRoutingDUnitSecureTest.testConcurrency:" +
              s" columnTableRouting started $i")
          // scalastyle:on println
          val completed = columnTableRouting()
          // scalastyle:off println
          println(s"ConcurrentQueryRoutingDUnitSecureTest.testConcurrency:" +
              s" columnTableRouting finished $i $completed")
          // scalastyle:on println
        })
      }
    })
    val rowThread = new Thread(new Runnable {
      def run() {
        (1 to 5) foreach (i => {
          // scalastyle:off println
          println(s"ConcurrentQueryRoutingDUnitSecureTest.testConcurrency:" +
              s" rowTableRouting started $i")
          // scalastyle:on println
          val completed = rowTableRouting()
          // scalastyle:off println
          println(s"ConcurrentQueryRoutingDUnitSecureTest.testConcurrency:" +
              s" rowTableRouting finished $i $completed")
          // scalastyle:on println
        })
      }
    })
    colThread.start()
    rowThread.start()
    colThread.join(5 * 60 * 1000)
    // scalastyle:off println
    println(s"ConcurrentQueryRoutingDUnitSecureTest.testConcurrency:" +
        s" columnTableRouting done")
    // scalastyle:on println
    rowThread.join(5 * 60 * 1000)
    // scalastyle:off println
    println(s"ConcurrentQueryRoutingDUnitSecureTest.testConcurrency:" +
        s"rowTableRouting done")
    // scalastyle:on println
    assert(columnTableRoutingCompleted.get() == 5,
      s"columnTableRoutingCompleted=${columnTableRoutingCompleted.get()}")
    assert(rowTableRoutingCompleted.get() == 5,
      s"rowTableRoutingCompleted=${rowTableRoutingCompleted.get()}")
  }
}
