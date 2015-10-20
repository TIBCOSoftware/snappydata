package org.apache.spark

import org.apache.spark

import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RetrieveSparkProps

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

  def fetchDriverProperty(host: String, executorConf: SparkConf,
      port: Int, url : String): Seq[(String,String)]= {
    val fetcher = RpcEnv.create(
      "driverPropsFetcher",
      host,
      port,
      executorConf,
      new spark.SecurityManager(executorConf))
    val driver = fetcher.setupEndpointRefByURI(url)
    val props = driver.askWithRetry[Seq[(String, String)]](RetrieveSparkProps) ++
        Seq[(String, String)](("spark.app.id", "myappid"))
    //TODO: Hemant : change my appid
    fetcher.shutdown()
    props
  }

  def isExecutorStartupConf(key: String) : Boolean = {
    SparkConf.isExecutorStartupConf(key)
  }

}