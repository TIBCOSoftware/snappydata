package org.apache.spark.executor

import java.net.URL

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


  override def onStop() {
    exitExecutor()
  }
  /**
   * Snappy addition (Replace System.exit with exitExecutor). We could have
   * added functions calling System.exit to SnappyCoarseGrainedExecutorBackend
   * but those functions will have to be brought in sync with CoarseGrainedExecutorBackend
   * after every merge.
   */
  override def exitExecutor(): Unit = {
    if (executor != null) {
      // kill all the running tasks
      // InterruptThread is set as true.
      executor.killAllTasks(true)
      executor.stop()
    }
    // stop the actor system
    stop()
    if (rpcEnv != null)
      rpcEnv.shutdown()
    SparkHadoopUtil.get.stopExecutorDelegationTokenRenewer()
  }
}
