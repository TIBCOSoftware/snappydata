/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

package org.apache.spark.sql.hive.thriftserver

import java.net.InetAddress

import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hive.service.cli.thrift.ThriftCLIService
import org.apache.log4j.{Level, LogManager}

import org.apache.spark.Logging
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.HiveThriftServer2Listener
import org.apache.spark.sql.hive.thriftserver.ui.ThriftServerTab
import org.apache.spark.sql.hive.{HiveUtils, SnappyHiveExternalCatalog}
import org.apache.spark.sql.{SnappyContext, SnappySession, SparkSession}

/**
 * Launch an embedded hive thrift server supporting SnappySession instead of HiveSessionState.
 */
object SnappyHiveThriftServer2 extends Logging {

  def start(useHiveSession: Boolean): HiveThriftServer2 = {
    logInfo(s"Starting HiveServer2 using ${if (useHiveSession) "hive" else "snappy"} session")

    val sc = SnappyContext.globalSparkContext match {
      case null => throw new IllegalStateException("No SparkContext available")
      case context => context
    }
    val conf = sc.conf
    val sparkSession = if (useHiveSession) {
      SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    } else new SnappySession(sc)
    SparkSQLEnv.sqlContext = sparkSession.sqlContext
    SparkSQLEnv.sparkContext = sc
    sparkSession.conf.set("spark.sql.hive.version", HiveUtils.hiveExecutionVersion)

    // New executionHive is used to get the HiveServer2 configuration. When SnappySession
    // is being used then only the hive server2 settings are copied from it while the
    // full conf used is from the internal hive client from SnappySharedState.

    // avoid meta-store init warnings
    val rootLogger = LogManager.getRootLogger
    val metaLogger = LogManager.getLogger("org.apache.hadoop.hive.metastore.MetaStoreDirectSql")
    val currentRootLevel = rootLogger.getLevel
    val currentMetaLevel = metaLogger.getLevel
    rootLogger.setLevel(Level.ERROR)
    metaLogger.setLevel(Level.ERROR)
    val externalCatalog = SnappyHiveExternalCatalog.getExistingInstance
    val hiveConf = try {
      val executionHive = HiveUtils.newClientForExecution(conf,
        sparkSession.sessionState.newHadoopConf())
      val serverConf = executionHive.conf
      // close the temporary hive client if present
      val hiveClient = executionHive.clientLoader.cachedHive
      if (hiveClient != null) {
        Hive.set(hiveClient.asInstanceOf[Hive])
        Hive.closeCurrent()
        executionHive.clientLoader.cachedHive = null
      }
      if (useHiveSession) serverConf
      else {
        // use internal hive conf adding hive.server2 configurations
        val conf = externalCatalog.client().asInstanceOf[HiveClientImpl].conf
        val itr = serverConf.iterator()
        while (itr.hasNext) {
          val entry = itr.next()
          if (entry.getKey.startsWith("hive.server2")) {
            conf.set(entry.getKey, entry.getValue)
          }
        }
        conf
      }
    } finally {
      rootLogger.setLevel(currentRootLevel)
      metaLogger.setLevel(currentMetaLevel)
    }

    val server = new HiveThriftServer2(SparkSQLEnv.sqlContext)
    externalCatalog.withHiveExceptionHandling({
      server.init(hiveConf)
      server.start()
      getHostPort(server) match {
        case None => logInfo("Started HiveServer2")
        case Some((host, port)) => logInfo(s"Started HiveServer2 on $host[$port]")
      }
      HiveThriftServer2.listener = new HiveThriftServer2Listener(
        server, SparkSQLEnv.sqlContext.conf)
      sc.addSparkListener(HiveThriftServer2.listener)
    }, handleDisconnects = false)
    server
  }

  def getHostPort(server: HiveThriftServer2): Option[(InetAddress, Int)] = {
    val itr = server.getServices.iterator()
    while (itr.hasNext) {
      itr.next() match {
        case service: ThriftCLIService =>
          val address = service.getServerIPAddress
          val port = service.getPortNumber
          return Some(address -> port)
        case _ =>
      }
    }
    None
  }

  def attachUI(): Unit = {
    if (SparkSQLEnv.sqlContext != null) {
      HiveThriftServer2.uiTab = Some(new ThriftServerTab(SparkSQLEnv.sparkContext))
    }
  }

  def close(): Unit = {
    SparkSQLEnv.sqlContext = null
    SparkSQLEnv.sparkContext = null
    HiveThriftServer2.uiTab match {
      case Some(ui) => ui.detach()
      case _ =>
    }
    HiveThriftServer2.uiTab = null
    HiveThriftServer2.listener = null
  }
}
