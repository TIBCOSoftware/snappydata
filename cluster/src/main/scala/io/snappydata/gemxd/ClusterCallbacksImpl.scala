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
package io.snappydata.gemxd

import java.util.{Iterator => JIterator}

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.ByteArrayDataInput
import com.gemstone.gemfire.internal.shared.Version
import com.pivotal.gemfirexd.internal.iapi.types.DataValueDescriptor
import com.pivotal.gemfirexd.internal.impl.sql.execute.ValueRow
import com.pivotal.gemfirexd.internal.snappy.{CallbackFactoryProvider, ClusterCallbacks, LeadNodeExecutionContext, SparkSQLExecute}
import io.snappydata.SnappyTableStatsProviderService
import io.snappydata.cluster.ExecutorInitiator
import io.snappydata.impl.LeadImpl

import org.apache.spark.Logging
import org.apache.spark.scheduler.cluster.SnappyClusterManager
import org.apache.spark.serializer.{KryoSerializerPool, StructTypeSerializer}

/**
 * Callbacks that are sent by GemXD to Snappy for cluster management
 */
object ClusterCallbacksImpl extends ClusterCallbacks with Logging {

  CallbackFactoryProvider.setClusterCallbacks(this)

  private[snappydata] def initialize(): Unit = {
    // nothing to be done; singleton constructor does all
  }

  override def getLeaderGroup: java.util.HashSet[String] = {
    val leaderServerGroup = new java.util.HashSet[String]
    leaderServerGroup.add(LeadImpl.LEADER_SERVERGROUP)
    leaderServerGroup
  }

  override def launchExecutor(driverUrl: String,
      driverDM: InternalDistributedMember): Unit = {
    val url = if (driverUrl == null || driverUrl == "") {
      logInfo(s"call to launchExecutor but driverUrl is invalid. $driverUrl")
      None
    }
    else {
      Some(driverUrl)
    }
    logInfo(s"invoking startOrTransmute with. $url")
    ExecutorInitiator.startOrTransmuteExecutor(url, driverDM)
  }

  override def getDriverURL: String = {
    SnappyClusterManager.cm.map(_.schedulerBackend) match {
      case Some(x) =>
        logInfo(s"returning driverUrl=${x.driverUrl}")
        x.driverUrl
      case None =>
        null
    }
  }

  override def stopExecutor(): Unit = {
    ExecutorInitiator.stop()
  }

  override def getSQLExecute(sql: String, schema: String, ctx: LeadNodeExecutionContext,
      v: Version): SparkSQLExecute = new SparkSQLExecuteImpl(sql, schema, ctx, v)

  override def readDataType(in: ByteArrayDataInput): AnyRef = {
    // read the DataType
    val pooled = KryoSerializerPool.borrow()
    val input = pooled.input
    try {
      input.setBuffer(in.array(), in.position(), in.available())
      StructTypeSerializer.readType(pooled.kryo, input)
    } finally {
      KryoSerializerPool.release(pooled, clearInputBuffer = true)
    }
  }

  override def getRowIterator(dvds: Array[DataValueDescriptor],
      types: Array[Int], precisions: Array[Int], scales: Array[Int],
      dataTypes: Array[AnyRef], in: ByteArrayDataInput): JIterator[ValueRow] = {
    SparkSQLExecuteImpl.getRowIterator(dvds, types, precisions, scales,
      dataTypes, in)
  }

  override def clearSnappySessionForConnection(
      connectionId: java.lang.Long): Unit = {
    SnappySessionPerConnection.removeSnappySession(connectionId)
  }

  override def publishColumnTableStats(): Unit = {
    SnappyTableStatsProviderService.publishColumnTableRowCountStats()
  }
}
