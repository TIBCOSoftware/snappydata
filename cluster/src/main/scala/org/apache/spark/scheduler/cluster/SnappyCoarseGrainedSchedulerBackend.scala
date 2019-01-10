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
package org.apache.spark.scheduler.cluster

import com.pivotal.gemfirexd.internal.engine.Misc

import org.apache.spark.SparkContext
import org.apache.spark.rpc.{RpcEndpointAddress, RpcEnv}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd, SparkListenerBlockManagerAdded, SparkListenerBlockManagerRemoved, SparkListenerExecutorAdded, SparkListenerExecutorRemoved, TaskSchedulerImpl}
import org.apache.spark.sql.collection.{ToolsCallbackInit, Utils}
import org.apache.spark.sql.{BlockAndExecutorId, SnappyContext, SnappySession}
import org.apache.spark.storage.BlockManagerId

class SnappyCoarseGrainedSchedulerBackend(scheduler: TaskSchedulerImpl,
    override val rpcEnv: RpcEnv)
    extends CoarseGrainedSchedulerBackend(scheduler, rpcEnv) {

  private val snappyAppId = "snappy-app-" + System.currentTimeMillis

  /**
   * Overriding the spark app id function to provide a snappy specific app id.
   *
   * @return An application ID
   */
  override def applicationId(): String = snappyAppId

  @volatile private var _driverUrl: String = ""

  def driverUrl: String = _driverUrl

  override def start() {

    super.start()
    _driverUrl = RpcEndpointAddress(
      scheduler.sc.conf.get("spark.driver.host"),
      scheduler.sc.conf.get("spark.driver.port").toInt,
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString
    logInfo(s"SchedulerBackend started with driverUrl $driverUrl")
  }

  override def stop() {
    super.stop()
    _driverUrl = ""
    SnappyClusterManager.cm.foreach(_.stopLead())
    logInfo(s"SchedulerBackend stopped successfully")
  }

  override protected def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    // keep the app id as part of driver property so that it can be retrieved
    // by the executor when driver properties are fetched using
    // [org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RetrieveSparkProps]
    super.createDriverEndpoint(properties ++
        Seq[(String, String)](("spark.app.id", applicationId())))
  }
}

class BlockManagerIdListener(sc: SparkContext)
    extends SparkListener {

  override def onExecutorAdded(
      msg: SparkListenerExecutorAdded): Unit = synchronized {
    val executorCores = msg.executorInfo.totalCores
    val profile = Misc.getMemStore.getDistributionAdvisor
        .getProfile(msg.executorId)
    val numProcessors = if (profile != null) profile.getNumProcessors
    else executorCores
    SnappyContext.getBlockId(msg.executorId) match {
      case None => SnappyContext.addBlockId(msg.executorId,
        new BlockAndExecutorId(null, executorCores, numProcessors))
      case Some(b) => b._executorCores = executorCores
        b._numProcessors = numProcessors
    }
    SnappyContext.getBlockId(msg.executorId) match {
      case Some(b) => if (b._blockId != null) handleNewExecutorJoin(b._blockId)
      case None =>
    }
  }

  override def onBlockManagerAdded(
      msg: SparkListenerBlockManagerAdded): Unit = synchronized {
    val executorId = msg.blockManagerId.executorId
    SnappyContext.getBlockIdIfNull(executorId) match {
      case None =>
        val numCores = sc.schedulerBackend.defaultParallelism()
        SnappyContext.addBlockId(executorId, new BlockAndExecutorId(
          msg.blockManagerId, numCores, numCores))
      case Some(b) => b._blockId = msg.blockManagerId
    }
  }

  override def onBlockManagerRemoved(
      msg: SparkListenerBlockManagerRemoved): Unit = {
    SnappyContext.removeBlockId(msg.blockManagerId.executorId)
  }

  override def onExecutorRemoved(msg: SparkListenerExecutorRemoved): Unit =
    SnappyContext.removeBlockId(msg.executorId)

  override def onApplicationEnd(msg: SparkListenerApplicationEnd): Unit =
    SnappyContext.clearBlockIds()

  private def handleNewExecutorJoin(bid: BlockManagerId): Unit = {
    val uris = SnappySession.getJarURIs
    Utils.mapExecutors[Unit](sc, () => {
      ToolsCallbackInit.toolsCallback.addURIsToExecutorClassLoader(uris)
      Iterator.empty
    }, 30, Seq(bid))
  }
}
