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
package org.apache.spark.sql

import io.snappydata.cluster.ClusterManagerTestBase
import org.apache.spark.Logging

class PartitionPruningDUnitTest(s: String) extends ClusterManagerTestBase(s)
  with Logging {

  override val locatorNetPort: Int = TPCHUtils.locatorNetPort

  override val stopNetServersInTearDown = false

  protected val productDir =
    SmartConnectorFunctions.getEnvironmentVariable("SNAPPY_HOME")

  override def beforeClass(): Unit = {
    vm3.invoke(classOf[ClusterManagerTestBase], "startSparkCluster", productDir)
    super.beforeClass()
    startNetworkServersOnAllVMs()
  }

  override def afterClass(): Unit = {
    try {
      vm3.invoke(classOf[ClusterManagerTestBase], "stopSparkCluster", productDir)
      Array(vm2, vm1, vm0).foreach(_.invoke(getClass, "stopNetworkServers"))
      ClusterManagerTestBase.stopNetworkServers()
    } finally {
      super.afterClass()
    }
  }

  def testRowTablePruning(): Unit = {

    logInfo("Started the Row Table Partition Pruning In SmartConnector")

    vm3.invoke(classOf[SmartConnectorFunctions],
      "verifyRowTablePartitionPruning", locatorNetPort)

    logInfo("Finished the Row Table Partition Pruning In SmartConnector")
  }

  def testColumnTablePartitionPruning(): Unit = {
    logInfo("Started the Column Table Partition Pruning In SmartConnector")

    vm3.invoke(classOf[SmartConnectorFunctions],
      "verifyColumnTablePartitionPruning", locatorNetPort)

    logInfo("Finished the Column Table Partition Pruning In SmartConnector")
  }

}
