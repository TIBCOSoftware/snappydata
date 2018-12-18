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

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.gemstone.gemfire.internal.SocketCreator
import com.pivotal.gemfirexd.internal.iapi.types.HarmonySerialBlob
import com.pivotal.gemfirexd.jdbc.ClientAttribute
import io.snappydata.Constant
import io.snappydata.thrift.BucketOwners
import io.snappydata.thrift.internal.ClientPreparedStatement
import org.eclipse.collections.impl.map.mutable.UnifiedMap

import org.apache.spark.Partition
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.collection.{SmartExecutorBucketPartition, Utils}
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.row.SnappyStoreClientDialect
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.sql.store.StoreUtils

final class SmartConnectorRDDHelper {

  def prepareScan(conn: Connection, txId: String, columnTable: String, projection: Array[Int],
      serializedFilters: Array[Byte], partition: SmartExecutorBucketPartition,
      catalogVersion: Long): (PreparedStatement, ResultSet) = {
    val pstmt = conn.prepareStatement("call sys.COLUMN_TABLE_SCAN(?, ?, ?)")
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
    SmartConnectorRDDHelper.snapshotTxIdForRead.get() match {
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

object SmartConnectorRDDHelper {

  DriverRegistry.register(Constant.JDBC_CLIENT_DRIVER)

  val snapshotTxIdForRead: ThreadLocal[(String, (String, String))] =
    new ThreadLocal[(String, (String, String))]
  val snapshotTxIdForWrite: ThreadLocal[String] = new ThreadLocal[String]

  private[this] val urlPrefix: String = Constant.DEFAULT_THIN_CLIENT_URL
  // no query routing or load-balancing
  private[this] val urlSuffix: String = "/" + ClientAttribute.ROUTE_QUERY + "=false;" +
      ClientAttribute.LOAD_BALANCE + "=false"

  /**
   * Get pair of TXId and (host, network server URL) pair.
   */
  def getTxIdAndHostUrl(txIdAndHost: String, preferHost: Boolean): (String, (String, String)) = {
    val index = txIdAndHost.indexOf('@')
    if (index < 0) {
      throw new AssertionError(s"Unexpected TXId@HostURL string = $txIdAndHost")
    }
    val server = txIdAndHost.substring(index + 1)
    val hostUrl = getNetUrl(server, preferHost, urlPrefix, urlSuffix, availableNetUrls = null)
    txIdAndHost.substring(0, index) -> hostUrl
  }

  def getPartitions(bucketToServerList: Array[ArrayBuffer[(String, String)]]): Array[Partition] = {
    val numPartitions = bucketToServerList.length
    val partitions = new Array[Partition](numPartitions)
    // choose only one server from the list so that partition routing and connection
    // creation are attempted on the same server if possible (i.e. should not happen
    // that routing selected some server but connection create used some other from the list)
    val numServers = bucketToServerList(0).length
    val chosenServerIndex = if (numServers > 1) scala.util.Random.nextInt(numServers) else 0
    for (p <- 0 until numPartitions) {
      if (StoreUtils.TEST_RANDOM_BUCKETID_ASSIGNMENT) {
        partitions(p) = new SmartExecutorBucketPartition(p, p,
          bucketToServerList(scala.util.Random.nextInt(numPartitions)))
      } else {
        val servers = bucketToServerList(p)
        partitions(p) = new SmartExecutorBucketPartition(p, p,
          if (servers.isEmpty) Nil
          else if (servers.length >= numServers) servers(chosenServerIndex) :: Nil
          else servers(0) :: Nil)
      }
    }
    partitions
  }

  def preferHostName(session: SnappySession): Boolean = {
    // check if Spark executors are using IP addresses or host names
    Utils.executorsListener(session.sparkContext) match {
      case Some(l) =>
        val preferHost = l.activeStorageStatusList.collectFirst {
          case status if status.blockManagerId.executorId != "driver" =>
            val host = status.blockManagerId.host
            host.indexOf('.') == -1 && host.indexOf("::") == -1
        }
        preferHost.isDefined && preferHost.get
      case _ => false
    }
  }

  private def getNetUrl(server: String, preferHost: Boolean, urlPrefix: String,
      urlSuffix: String, availableNetUrls: UnifiedMap[String, String]): (String, String) = {
    val hostAddressPort = returnHostPortFromServerString(server)
    val hostName = hostAddressPort._1
    val host = if (preferHost) hostName else hostAddressPort._2
    val netUrl = urlPrefix + hostName + "[" + hostAddressPort._3 + "]" + urlSuffix
    if ((availableNetUrls ne null) && !availableNetUrls.containsKey(host)) {
      availableNetUrls.put(host, netUrl)
    }
    host -> netUrl
  }

  def setBucketToServerMappingInfo(numBuckets: Int, buckets: java.util.List[BucketOwners],
      session: SnappySession): Array[ArrayBuffer[(String, String)]] = {
    if (!buckets.isEmpty) {
      // check if Spark executors are using IP addresses or host names
      val preferHost = preferHostName(session)
      val preferPrimaries = session.preferPrimaries
      var orphanBuckets: ArrayBuffer[Int] = null
      val allNetUrls = new Array[ArrayBuffer[(String, String)]](numBuckets)
      val availableNetUrls = new UnifiedMap[String, String](4)
      for (bucket <- buckets.asScala) {
        val bucketId = bucket.getBucketId
        // use primary so that DMLs are routed optimally
        val primary = bucket.getPrimary
        if (primary ne null) {
          // get (host,addr,port)
          val secondaries = if (preferPrimaries) Collections.emptyList()
          else bucket.getSecondaries
          val netUrls = new ArrayBuffer[(String, String)](secondaries.size() + 1)
          netUrls += getNetUrl(primary, preferHost, urlPrefix, urlSuffix, availableNetUrls)
          if (secondaries.size() > 0) {
            for (secondary <- secondaries.asScala) {
              netUrls += getNetUrl(secondary, preferHost, urlPrefix, urlSuffix, availableNetUrls)
            }
          }
          allNetUrls(bucketId) = netUrls
        } else {
          // Save the bucket which does not have a neturl,
          // and later assign available ones to it.
          if (orphanBuckets eq null) {
            orphanBuckets = new ArrayBuffer[Int](2)
          }
          orphanBuckets += bucketId
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

  def setReplicasToServerMappingInfo(replicaNodes: java.util.List[String],
      session: SnappySession): Array[ArrayBuffer[(String, String)]] = {
    // check if Spark executors are using IP addresses or host names
    val preferHost = preferHostName(session)
    val urlPrefix = Constant.DEFAULT_THIN_CLIENT_URL
    // no query routing or load-balancing
    val urlSuffix = "/" + ClientAttribute.ROUTE_QUERY + "=false;" +
        ClientAttribute.LOAD_BALANCE + "=false"
    val netUrls = ArrayBuffer.empty[(String, String)]
    for (host <- replicaNodes.asScala) {
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
