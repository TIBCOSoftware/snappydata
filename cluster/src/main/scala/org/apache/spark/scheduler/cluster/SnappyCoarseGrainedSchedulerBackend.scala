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
package org.apache.spark.scheduler.cluster

import java.util

import com.gemstone.gemfire.distributed.internal.MembershipListener
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils

import org.apache.spark.rpc.{RpcAddress, RpcEnv}
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.{Logging, SparkEnv}

class SnappyCoarseGrainedSchedulerBackend(scheduler: TaskSchedulerImpl, override val rpcEnv: RpcEnv)
    extends CoarseGrainedSchedulerBackend(scheduler, rpcEnv) with Logging {

  private val snappyAppId = "snappy-app-" + System.currentTimeMillis

  val membershipListener = new MembershipListener {
    override def quorumLost(failures: util.Set[InternalDistributedMember],
        remaining: util.List[InternalDistributedMember]): Unit = {}

    override def memberJoined(id: InternalDistributedMember): Unit = {}

    override def memberSuspect(id: InternalDistributedMember,
        whoSuspected: InternalDistributedMember): Unit = {}

    override def memberDeparted(id: InternalDistributedMember, crashed: Boolean): Unit = {
      StoreUtils.storeToBlockMap -= id
    }
  }

  /**
   * Overriding the spark app id function to provide a snappy specific app id.
   * @return An application ID
   */
  override def applicationId(): String = snappyAppId

  @volatile private var _driverUrl: String = ""

  def driverUrl: String = _driverUrl

  override def start() {

    super.start()
    _driverUrl = rpcEnv.uriOf(SparkEnv.driverActorSystemName,
      RpcAddress(driverEndpoint.address.host, driverEndpoint.address.port),
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME)
    GemFireXDUtils.getGfxdAdvisor.getDistributionManager
        .addMembershipListener(membershipListener)
    logInfo(s"started with driverUrl $driverUrl")
  }

  override def stop() {
    super.stop()
    _driverUrl = ""
    SnappyClusterManager.cm.map(_.stopLead()).isDefined
    GemFireXDUtils.getGfxdAdvisor.getDistributionManager
        .removeMembershipListener(membershipListener)
    logInfo(s"stopped successfully")
  }

  override protected def createDriverEndpoint(properties: Seq[(String, String)]): DriverEndpoint = {
    // keep the app id as part of driver property so that it can be retrieved
    // by the executor when driver properties are fetched using
    // [org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RetrieveSparkProps]
    super.createDriverEndpoint(properties ++
        Seq[(String, String)](("spark.app.id", applicationId())))
  }
}

