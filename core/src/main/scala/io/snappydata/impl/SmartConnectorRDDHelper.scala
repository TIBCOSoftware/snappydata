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
package io.snappydata.impl

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.gemstone.gemfire.internal.SocketCreator
import com.pivotal.gemfirexd.internal.iapi.types.HarmonySerialBlob
import com.pivotal.gemfirexd.jdbc.ClientAttribute
import io.snappydata.sql.catalog.SmartConnectorHelper

import org.apache.spark.sql.SnappyStoreClientDialect
import org.apache.spark.sql.collection.SmartExecutorBucketPartition
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.sources.ConnectionProperties

final class SmartConnectorRDDHelper {

  def prepareScan(conn: Connection, columnTable: String, projection: Array[Int],
      serializedFilters: Array[Byte], partition: SmartExecutorBucketPartition): (PreparedStatement,
      ResultSet) = SmartConnectorHelper.withPartitionAttrs(conn, columnTable, partition.bucketId) {
    val pstmt = conn.prepareStatement("call sys.COLUMN_TABLE_SCAN(?, ?, ?)")
    pstmt.setString(1, columnTable)
    pstmt.setString(2, projection.mkString(","))
    // serialize the filters
    if ((serializedFilters ne null) && serializedFilters.length > 0) {
      pstmt.setBlob(3, new HarmonySerialBlob(serializedFilters))
    } else {
      pstmt.setNull(3, java.sql.Types.BLOB)
    }
    val rs = pstmt.executeQuery()
    (pstmt, rs)
  }

  @tailrec
  def createConnection(connProperties: ConnectionProperties, hostList: Seq[(String, String)],
      preferHost: Boolean, forSnapshot: Boolean): Connection = {
    val useLocatorURL = hostList.isEmpty
    var index = -1

    val jdbcUrl = if (useLocatorURL) {
      connProperties.url
    } else if (hostList.length == 1) {
      hostList.head._2
    } else {
      val localhost = SocketCreator.getLocalHost
      val hostOrAddress = if (preferHost) localhost.getHostName else localhost.getHostAddress
      if (index < 0) index = hostList.indexWhere(_._1.contains(hostOrAddress))
      if (index < 0) index = Random.nextInt(hostList.size)
      hostList(index)._2
    }

    // enable direct ByteBuffers for best performance
    val executorProps = connProperties.executorConnProps
    executorProps.setProperty(ClientAttribute.THRIFT_LOB_DIRECT_BUFFERS, "true")
    // setup pool properties
    val props = ExternalStoreUtils.getAllPoolProperties(jdbcUrl, null,
      connProperties.poolProps, connProperties.hikariCP, isEmbedded = false)
    try {
      // use jdbcUrl as the key since a unique pool is required for each server but separate pools
      // for snapshot enabled vs disabled ones so that there is no need to switch isolation-levels
      val poolId =
        if (forSnapshot) jdbcUrl + ConnectionPool.SNAPSHOT_POOL_SUFFIX else jdbcUrl
      ConnectionPool.getPoolConnection(poolId, SnappyStoreClientDialect, props, executorProps,
        connProperties.hikariCP, resetIsolationLevel = false)
    } catch {
      case sqle: SQLException => if (hostList.size == 1 || useLocatorURL) {
        throw sqle
      } else {
        val newHostList = new ArrayBuffer[(String, String)](hostList.length)
        newHostList ++= hostList
        newHostList.remove(index)
        createConnection(connProperties, newHostList, preferHost, forSnapshot)
      }
    }
  }
}
