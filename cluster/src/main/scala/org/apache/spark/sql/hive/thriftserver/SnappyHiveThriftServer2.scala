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

package org.apache.spark.sql.hive.thriftserver

import org.apache.spark.Logging
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2.HiveThriftServer2Listener
import org.apache.spark.sql.hive.thriftserver.ui.ThriftServerTab
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

    // executionHive is used only to get the Hive configuration. If it is to be used
    // for actual querying in future, then SnappySharedState.metadataHive() should be used.
    // Latter is not used here because it ignores any hive settings required for HiveServer2.
    val executionHive = HiveUtils.newClientForExecution(conf,
      sparkSession.sessionState.newHadoopConf())

    val server = new HiveThriftServer2(SparkSQLEnv.sqlContext)
    server.init(executionHive.conf)
    server.start()
    logInfo("Started HiveServer2")
    HiveThriftServer2.listener = new HiveThriftServer2Listener(
      server, SparkSQLEnv.sqlContext.conf)
    sc.addSparkListener(HiveThriftServer2.listener)
    server
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
