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
package org.apache.spark

import org.apache.spark
import org.apache.spark.deploy.SparkHadoopUtil

import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RetrieveSparkProps

/**
 * Calls that are needed to be sent to snappy-cluster classes
 * because the variables are private[spark].
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

  def isDriver: Boolean = {
    val env = SparkEnv.get
    env != null && env.executorId == SparkContext.DRIVER_IDENTIFIER
  }
}
