package org.apache.spark

import org.apache.spark
import org.apache.spark.deploy.SparkHadoopUtil

import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RetrieveSparkProps

/**
  * Calls that are needed to be sent to snappy-tools classes because the variables are private[spark]
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
    if (env != null) {
      SparkHadoopUtil.get.runAsSparkUser { () =>
        env.stop()
        SparkEnv.set(null)
        SparkHadoopUtil.get.stopExecutorDelegationTokenRenewer()
      }
    }
  }

  def fetchDriverProperty(host: String, executorConf: SparkConf,
      port: Int, url: String): Seq[(String, String)] = {
    val fetcher = RpcEnv.create(
      "driverPropsFetcher",
      host,
      port,
      executorConf,
      new spark.SecurityManager(executorConf), clientMode = true)
    val driver = fetcher.setupEndpointRefByURI(url)
    val props = driver.askWithRetry[Seq[(String, String)]](RetrieveSparkProps)
    fetcher.shutdown()
    props
  }

  def isExecutorStartupConf(key: String): Boolean = {
    SparkConf.isExecutorStartupConf(key)
  }

  def isDriver() : Boolean = {
    SparkEnv.get != null &&
        SparkEnv.get.executorId == SparkContext.DRIVER_IDENTIFIER
  }

}