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
package io.snappydata.impl

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import java.util.Collections

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import com.gemstone.gemfire.internal.SocketCreator
import com.pivotal.gemfirexd.internal.iapi.types.HarmonySerialBlob
import com.pivotal.gemfirexd.jdbc.ClientAttribute
import io.snappydata.sql.catalog.SmartConnectorHelper
import io.snappydata.thrift.internal.ClientPreparedStatement
import org.apache.spark.sql.SnappyStoreClientDialect
import org.apache.spark.sql.collection.SmartExecutorBucketPartition
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.sources.ConnectionProperties

final class SmartConnectorRDDHelper {

  def prepareScan(conn: Connection, txId: String, columnTable: String, projection: Array[Int],
      serializedFilters: Array[Byte], partition: SmartExecutorBucketPartition,
      catalogVersion: Long): (PreparedStatement, ResultSet) = {
    val pstmt = conn.prepareStatement("call sys.COLUMN_TABLE_SCAN(?, ?, ?, 1)")
    pstmt.setString(1, columnTable)
    pstmt.setString(2, projection.mkString(","))
    // serialize the filters
    if ((serializedFilters ne null) && serializedFilters.length > 0) {
      pstmt.setBlob(3, new HarmonySerialBlob(serializedFilters))
    } else {
      pstmt.setNull(3, java.sql.Types.BLOB)
    }
    pstmt match {
      case clientStmt: ClientPreparedStatement =>
        val bucketSet = Collections.singleton(Int.box(partition.bucketId))
        clientStmt.setLocalExecutionBucketIds(bucketSet, columnTable, true)
        clientStmt.setCatalogVersion(catalogVersion)
        clientStmt.setSnapshotTransactionId(txId)
      case _ =>
        pstmt.execute("call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION(" +
            s"'$columnTable', '${partition.bucketId}', $catalogVersion)")
        if (txId ne null) {
          pstmt.execute(s"call sys.USE_SNAPSHOT_TXID('$txId')")
        }
    }

    val rs = pstmt.executeQuery()
    (pstmt, rs)
  }

  def getConnectionAndTXId(connProperties: ConnectionProperties,
      part: SmartExecutorBucketPartition, preferHost: Boolean): (Connection, String) = {
    SmartConnectorHelper.snapshotTxIdForRead.get() match {
      case null | ("", _) =>
        createConnection(connProperties, part.hostList, preferHost) -> null
      case (tx, hostAndUrl) =>
        // create connection to the same host where TX was started
        createConnection(connProperties, new ArrayBuffer[(String, String)](1) += hostAndUrl,
          preferHost) -> tx
    }
  }

  private def createConnection(connProperties: ConnectionProperties,
      hostList: Seq[(String, String)], preferHost: Boolean): Connection = {
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
      // use jdbcUrl as the key since a unique pool is required for each server
      ConnectionPool.getPoolConnection(jdbcUrl, SnappyStoreClientDialect, props,
        executorProps, connProperties.hikariCP)
    } catch {
      case sqle: SQLException => if (hostList.size == 1 || useLocatorURL) {
        throw sqle
      } else {
        val newHostList = new ArrayBuffer[(String, String)](hostList.length)
        newHostList ++= hostList
        newHostList.remove(index)
        createConnection(connProperties, hostList, preferHost)
      }
    }
  }
}
