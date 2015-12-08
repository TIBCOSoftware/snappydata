package org.apache.spark.executor

import java.net.URL

import io.snappydata.cluster.ExecutorInitiator
import org.apache.spark.SparkEnv
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rpc.RpcEnv

/**
 * Created by hemantb on 10/5/15.
 */
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

  var stopped = false

  override def onStop() {
    exitExecutor()
  }

  override def onStart(): Unit = {
    super.onStart()
    // Executor may fail to connect to the driver because of
    // https://issues.apache.org/jira/browse/SPARK-9820 and
    // https://issues.apache.org/jira/browse/SPARK-8592. To overcome such
    // issues, try restarting the executor
    if (stopped) {
      logWarning("Executor has failed to start: Restarting.")
      ExecutorInitiator.restartExecutor()
    }
  }
  /**
   * Snappy addition (Replace System.exit with exitExecutor). We could have
   * added functions calling System.exit to SnappyCoarseGrainedExecutorBackend
   * but those functions will have to be brought in sync with CoarseGrainedExecutorBackend
   * after every merge.
   */
  override def exitExecutor(): Unit = {
    stopped = true

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
