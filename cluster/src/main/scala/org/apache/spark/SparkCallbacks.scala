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
package org.apache.spark

import org.apache.spark
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.memory.StoreUnifiedManager
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RetrieveSparkAppConfig, SparkAppConfig}
import org.apache.spark.ui.{JettyUtils, SnappyBasicAuthenticator}
import org.eclipse.jetty.security.authentication.BasicAuthenticator

/**
 * Calls that are needed to be sent to snappy-cluster classes because
 * the variables are private[spark]
 */
object SparkCallbacks {

  def createExecutorEnv(
      driverConf: SparkConf,
      executorId: String,
      hostname: String,
      port: Int,
      numCores: Int,
      ioEncryptionKey: Option[Array[Byte]],
      isLocal: Boolean): SparkEnv = {

    val env = SparkEnv.createExecutorEnv(driverConf, executorId, hostname,
      port, numCores, ioEncryptionKey, isLocal)
    env.memoryManager.asInstanceOf[StoreUnifiedManager].init()
    env
  }

  def getRpcEnv(sparkEnv: SparkEnv): RpcEnv = {
    sparkEnv.rpcEnv
  }

  def stopExecutor(env: SparkEnv): Unit = {
    if (env != null) {
      SparkHadoopUtil.get.runAsSparkUser { () =>
        // Copy the memory state to boot memory manager
        SparkEnv.get.memoryManager.asInstanceOf[StoreUnifiedManager].close
        env.stop()
        SparkEnv.set(null)
        SparkHadoopUtil.get.stopCredentialUpdater()
      }
    }
  }

  def fetchDriverProperty(appId: String, host: String, executorConf: SparkConf,
      port: Int, url: String): (Option[Array[Byte]], Seq[(String, String)]) = {
    val fetcher = RpcEnv.create(
      "driverPropsFetcher",
      host,
      port,
      executorConf,
      new spark.SecurityManager(executorConf), clientMode = true)
    val driver = fetcher.setupEndpointRefByURI(url)
    val cfg = driver.askWithRetry[SparkAppConfig](RetrieveSparkAppConfig)
    val ioEncryptionKey: Option[Array[Byte]] = cfg.ioEncryptionKey
    val props = cfg.sparkProperties ++
        Seq[(String, String)](("spark.app.id", appId))
    fetcher.shutdown()
    (ioEncryptionKey, props)
  }

  def isExecutorStartupConf(key: String): Boolean = {
    SparkConf.isExecutorStartupConf(key)
  }

  def isDriver: Boolean = {
    SparkEnv.get != null &&
        SparkEnv.get.executorId == SparkContext.DRIVER_IDENTIFIER
  }

  def setAuthenticatorForJettyServer(): Unit = {
    if (JettyUtils.customAuthenticator.isEmpty) {
      // create and set SnappyBasicAuthenticator
      JettyUtils.customAuthenticator = Some(new SnappyBasicAuthenticator)
    }
  }

  def getAuthenticatorForJettyServer(): Option[BasicAuthenticator] = {
    JettyUtils.customAuthenticator
  }

  def setSparkConf(sc: SparkContext, key: String, value: String): Unit = {
    if (value ne null) sc.conf.set(key, value) else sc.conf.remove(key)
  }
}
