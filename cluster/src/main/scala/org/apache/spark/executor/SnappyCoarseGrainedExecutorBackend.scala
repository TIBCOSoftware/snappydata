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
package org.apache.spark.executor

import java.net.URL
import java.nio.ByteBuffer

import com.gemstone.gemfire.CancelException
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.cluster.ExecutorInitiator

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.sql.SnappyContext
import org.apache.spark.{SparkEnv, TaskState}

class SnappyCoarseGrainedExecutorBackend(
    override val rpcEnv: RpcEnv,
    driverUrl: String,
    executorId: String,
    hostName: String,
    cores: Int,
    userClassPath: Seq[URL],
    env: SparkEnv)
    extends CoarseGrainedExecutorBackend(rpcEnv, driverUrl,
      executorId, hostName, cores, userClassPath, env) {

  override def onStop() {
    SnappyContext.clearStaticArtifacts()
    exitWithoutRestart()
  }

  override def onStart(): Unit = {
    super.onStart()
  }

  override protected def registerExecutor: Executor =
    new SnappyExecutor(executorId, hostName, env,
      userClassPath, new SnappyUncaughtExceptionHandler(this),
      isLocal = false)

  /**
   * Avoid sending any message for TaskState.RUNNING which serves no purpose.
   */
  override def statusUpdate(taskId: Long, state: TaskState.TaskState,
      data: ByteBuffer): Unit = {
    if ((state ne TaskState.RUNNING) || data.hasRemaining) {
      super.statusUpdate(taskId, state, data)
    }
  }

  /**
   * Snappy addition (Replace System.exit with exitExecutor). We could have
   * added functions calling System.exit to SnappyCoarseGrainedExecutorBackend
   * but those functions will have to be brought in sync with CoarseGrainedExecutorBackend
   * after every merge.
   */
  override def exitExecutor(code: Int,
      reason: String, throwable: Throwable,
      notifyDriver: Boolean = true): Unit = {
    exitWithoutRestart()
    // See if the VM is going down
    try {
      Misc.checkIfCacheClosing(null)
    } catch {
      case _: CancelException => return
    }
    // Executor may fail to connect to the driver because of
    // https://issues.apache.org/jira/browse/SPARK-9820 and
    // https://issues.apache.org/jira/browse/SPARK-8592. To overcome such
    // issues, try restarting the executor
    val reasonStr = s"Restarting Executor that failed to start. Reason: $reason."
    if (throwable != null) {
      logError(reasonStr, throwable)
    } else {
      logError(reasonStr, throwable)
    }
    ExecutorInitiator.restartExecutor()

  }

  def exitWithoutRestart(): Unit = {
    if (executor != null) {
      // kill all the running tasks
      // When tasks are killed, the task threads cannot be interrupted
      // as snappy may be writing to an oplog and it generates a
      // DiskAccessException. This DAE ends up closing the underlying regions.
      executor.killAllTasks(interruptThread = false)
      executor.stop()
    }
    // stop the actor system
    stop()
    if (rpcEnv != null) {
      rpcEnv.shutdown()
    }

    SparkHadoopUtil.get.stopCredentialUpdater()
  }
}
