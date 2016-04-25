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
package org.apache.spark.executor

import java.net.URL

import io.snappydata.cluster.ExecutorInitiator
import org.apache.spark.SparkEnv
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rpc.RpcEnv

class SnappyCoarseGrainedExecutorBackend(
                                          override val rpcEnv: RpcEnv,
                                          driverUrl: String,
                                          executorId: String,
                                          hostPort: String,
                                          cores: Int,
                                          userClassPath: Seq[URL],
                                          env: SparkEnv)
  extends CoarseGrainedExecutorBackend(rpcEnv, driverUrl,
    executorId, hostPort, cores, userClassPath, env) {

  override def onStop() {
    exitWithoutRestart()
  }

  override def onStart(): Unit = {
    super.onStart()
  }

  /**
   * Snappy addition (Replace System.exit with exitExecutor). We could have
   * added functions calling System.exit to SnappyCoarseGrainedExecutorBackend
   * but those functions will have to be brought in sync with CoarseGrainedExecutorBackend
   * after every merge.
   */
  override def exitExecutor(): Unit = {
    exitWithoutRestart()
    // Executor may fail to connect to the driver because of
    // https://issues.apache.org/jira/browse/SPARK-9820 and
    // https://issues.apache.org/jira/browse/SPARK-8592. To overcome such
    // issues, try restarting the executor
    logWarning("Executor has failed to start: Restarting.")
    ExecutorInitiator.restartExecutor()
  }

  def exitWithoutRestart() : Unit = {
    if (executor != null) {
      // kill all the running tasks
      // InterruptThread is set as true.
      executor.killAllTasks(true)
      executor.stop()
    }
    // stop the actor system
    stop()
    if (rpcEnv != null) {
      rpcEnv.shutdown()
    }

    SparkHadoopUtil.get.stopExecutorDelegationTokenRenewer()
  }
}
