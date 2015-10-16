package org.apache.spark

import org.apache.spark.rpc.RpcEnv

/**
 * Calls that are needed to be sent to Snappy-Core classes because the variables are private[spark]
 */
object SparkCallbacks {

  def createExecutorEnv(
                         conf: SparkConf,
                         executorId: String,
                         hostname: String,
                         port: Int,
                         numCores: Int,
                         isLocal: Boolean): SparkEnv = {
    SparkEnv.createExecutorEnv(conf, executorId, hostname,
      port, numCores, isLocal)
  }

  def getRpcEnv(sparkEnv: SparkEnv): RpcEnv = {
    sparkEnv.rpcEnv
  }

  def stopExecutor(env: SparkEnv): Unit = {
    if (env != null)
      env.stop()
  }

}