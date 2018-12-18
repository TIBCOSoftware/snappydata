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
package org.apache.spark.sql.execution.columnar

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException}
import java.util.Collections

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.gemstone.gemfire.internal.SocketCreator
import com.pivotal.gemfirexd.internal.iapi.types.HarmonySerialBlob
import com.pivotal.gemfirexd.jdbc.ClientAttribute
import io.snappydata.Constant
import io.snappydata.collection.ObjectObjectHashMap
import io.snappydata.thrift.internal.ClientPreparedStatement

import org.apache.spark.Partition
import org.apache.spark.sql.collection.{SharedUtils, SmartExecutorBucketPartition}
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.row.SnappyStoreClientDialect
import org.apache.spark.sql.sources.ConnectionProperties

final class SmartConnectorRDDHelper {

  private var useLocatorURL: Boolean = _

  def prepareScan(conn: Connection, txId: String, columnTable: String, projection: Array[Int],
      serializedFilters: Array[Byte], bucketId: Int,
      relDestroyVersion: Int, useKryoSerializer: Boolean): (PreparedStatement, ResultSet) = {
    val pstmt = if (useKryoSerializer) {
      conn.prepareStatement("call sys.COLUMN_TABLE_SCAN(?, ?, ?, 1)")
    } else {
      conn.prepareStatement("call sys.COLUMN_TABLE_SCAN(?, ?, ?, 0)")
    }
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
        val bucketSet = Collections.singleton(Int.box(bucketId))
        clientStmt.setLocalExecutionBucketIds(bucketSet, columnTable, true)
        clientStmt.setMetadataVersion(relDestroyVersion)
        clientStmt.setSnapshotTransactionId(txId)
      case _ =>
        pstmt.execute("call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION(" +
            s"'$columnTable', '${bucketId}', $relDestroyVersion)")
        if (txId ne null) {
          pstmt.execute(s"call sys.USE_SNAPSHOT_TXID('$txId')")
        }
    }

    val rs = pstmt.executeQuery()
    (pstmt, rs)
  }

  def getConnection(connectionProperties: ConnectionProperties,
      part: SmartExecutorBucketPartition): Connection = {
    val urlsOfNetServerHost = part.hostList
    useLocatorURL = SmartConnectorRDDHelper.useLocatorUrl(urlsOfNetServerHost)
    createConnection(connectionProperties, urlsOfNetServerHost)
  }

  def createConnection(connProperties: ConnectionProperties,
      hostList: ArrayBuffer[(String, String)]): Connection = {
    val localhost = SocketCreator.getLocalHost
    var index = -1

    val jdbcUrl = if (useLocatorURL) {
      connProperties.url
    } else {
      if (index < 0) index = hostList.indexWhere(_._1.contains(localhost.getHostAddress))
      if (index < 0) index = Random.nextInt(hostList.size)
      hostList(index)._2
    }

    // enable direct ByteBuffers for best performance
    val executorProps = connProperties.executorConnProps
    executorProps.setProperty(ClientAttribute.THRIFT_LOB_DIRECT_BUFFERS, "true")
    // setup pool properties
    val props = SharedExternalStoreUtils.getAllPoolProperties(jdbcUrl, null,
      connProperties.poolProps, connProperties.hikariCP, isEmbedded = false)
    try {
      // use jdbcUrl as the key since a unique pool is required for each server
      ConnectionPool.getPoolConnection(jdbcUrl, SnappyStoreClientDialect, props,
        executorProps, connProperties.hikariCP)
    } catch {
      case sqle: SQLException => if (hostList.size == 1 || useLocatorURL) {
        throw sqle
      } else {
        hostList.remove(index)
        createConnection(connProperties, hostList)
      }
    }
  }
}

object SmartConnectorRDDHelper {

  var snapshotTxIdForRead: ThreadLocal[String] = new ThreadLocal[String]
  var snapshotTxIdForWrite: ThreadLocal[String] = new ThreadLocal[String]

  DriverRegistry.register(Constant.JDBC_CLIENT_DRIVER)

  def getPartitions(bucketToServerList: Array[ArrayBuffer[(String, String)]]): Array[Partition] = {
    val numPartitions = bucketToServerList.length
    val partitions = new Array[Partition](numPartitions)
    for (p <- 0 until numPartitions) {
      if (SharedUtils.TEST_RANDOM_BUCKETID_ASSIGNMENT) {
        partitions(p) = new SmartExecutorBucketPartition(p, p,
          bucketToServerList(scala.util.Random.nextInt(numPartitions)))
      } else {
        partitions(p) = new SmartExecutorBucketPartition(p, p, bucketToServerList(p))
      }
    }
    partitions
  }

  private def useLocatorUrl(hostList: ArrayBuffer[(String, String)]): Boolean =
    hostList.isEmpty


  def setBucketToServerMappingInfo(bucketToServerMappingStr: String,
      preferHost: Boolean): Array[ArrayBuffer[(String, String)]] = {
    val urlPrefix = Constant.DEFAULT_THIN_CLIENT_URL
    // no query routing or load-balancing
    val urlSuffix = "/" + ClientAttribute.ROUTE_QUERY + "=false;" +
        ClientAttribute.LOAD_BALANCE + "=false"
    if (bucketToServerMappingStr != null) {
      val arr: Array[String] = bucketToServerMappingStr.split(":")
      var orphanBuckets: ArrayBuffer[Int] = null
      val noOfBuckets = arr(0).toInt
      // val redundancy = arr(1).toInt
      val allNetUrls = new Array[ArrayBuffer[(String, String)]](noOfBuckets)
      val bucketsServers: String = arr(2)
      val newarr: Array[String] = bucketsServers.split("\\|")
      val availableNetUrls = ObjectObjectHashMap.withExpectedSize[String, String](4)
      for (x <- newarr) {
        val aBucketInfo: Array[String] = x.split(";")
        val bid: Int = aBucketInfo(0).toInt
        if (!(aBucketInfo(1) == "null")) {
          // get (host,addr,port)
          val hostAddressPort = returnHostPortFromServerString(aBucketInfo(1))
          val hostName = hostAddressPort._1
          val host = if (preferHost) hostName else hostAddressPort._2
          val netUrl = urlPrefix + hostName + "[" + hostAddressPort._3 + "]" + urlSuffix
          val netUrls = new ArrayBuffer[(String, String)](1)
          netUrls += host -> netUrl
          allNetUrls(bid) = netUrls
          if (!availableNetUrls.containsKey(host)) {
            availableNetUrls.put(host, netUrl)
          }
        } else {
          // Save the bucket which does not have a neturl,
          // and later assign available ones to it.
          if (orphanBuckets eq null) {
            orphanBuckets = new ArrayBuffer[Int](2)
          }
          orphanBuckets += bid
        }
      }
      if (orphanBuckets ne null) {
        val netUrls = new ArrayBuffer[(String, String)](availableNetUrls.size())
        val netUrlsIter = availableNetUrls.entrySet().iterator()
        while (netUrlsIter.hasNext) {
          val entry = netUrlsIter.next()
          netUrls += entry.getKey -> entry.getValue
        }
        for (bucket <- orphanBuckets) {
          allNetUrls(bucket) = netUrls
        }
      }
      return allNetUrls
    }
    Array.empty
  }

  def setReplicasToServerMappingInfo(replicaNodesStr: String,
      preferHost: Boolean): Array[ArrayBuffer[(String, String)]] = {
    val urlPrefix = Constant.DEFAULT_THIN_CLIENT_URL
    // no query routing or load-balancing
    val urlSuffix = "/" + ClientAttribute.ROUTE_QUERY + "=false;" +
        ClientAttribute.LOAD_BALANCE + "=false"
    val hostInfo = replicaNodesStr.split(";")
    val netUrls = ArrayBuffer.empty[(String, String)]
    for (host <- hostInfo) {
      val hostAddressPort = returnHostPortFromServerString(host)
      val hostName = hostAddressPort._1
      val h = if (preferHost) hostName else hostAddressPort._2
      netUrls += h ->
          (urlPrefix + hostName + "[" + hostAddressPort._3 + "]" + urlSuffix)
    }
    Array(netUrls)
  }

  /*
  * The pattern to extract addresses from the result of
  * GET_ALLSERVERS_AND_PREFSERVER2 procedure; format is:
  *
  * host1/addr1[port1]{kind1},host2/addr2[port2]{kind2},...
  */
  private lazy val addrPattern =
    java.util.regex.Pattern.compile("([^,/]*)(/[^,\\[]+)?\\[([\\d]+)\\](\\{[^}]+\\})?")

  private def returnHostPortFromServerString(serverStr: String): (String, String, String) = {
    if (serverStr == null || serverStr.length == 0) {
      return null
    }
    val matcher: java.util.regex.Matcher = addrPattern.matcher(serverStr)
    val matchFound: Boolean = matcher.find
    if (!matchFound) {
      (null, null, null)
    } else {
      val host: String = matcher.group(1)
      var address = matcher.group(2)
      if ((address ne null) && address.length > 0) {
        address = address.substring(1)
      } else {
        address = host
      }
      val portStr: String = matcher.group(3)
      (host, address, portStr)
    }
  }
}
