/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import java.sql.{Connection, ResultSet, SQLException, Statement}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.gemstone.gemfire.cache.Region
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.SocketCreator
import com.gemstone.gemfire.internal.cache.{DistributedRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.jdbc.ClientAttribute
import io.snappydata.Constant

import org.apache.spark.Partition
import org.apache.spark.sql.collection.ExecutorMultiBucketLocalShellPartition
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.columnar.impl.{ColumnDelta, ColumnFormatEntry}
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.row.GemFireXDClientDialect
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.sql.types.StructType

final class SparkConnectorRDDHelper {

  var useLocatorURL: Boolean = false

  def getSQLStatement(resolvedTableName: String,
      partitionId: Int, requiredColumns: Array[String],
      schema: StructType): (String, String) = {

    val schemaWithIndex = schema.zipWithIndex
    // to fetch columns create a union all string
    // (select data, columnIndex from table where
    //  partitionId = 1 and uuid = ? and columnIndex = 1)
    // union all
    // (select data, columnIndex from table where
    //  partitionId = 1 and uuid = ? and columnIndex = 7)
    // An OR query like the following results in a bulk table scan
    // select data, columnIndex from table where partitionId = 1 and
    //  (columnIndex = 1 or columnIndex = 7)
    val fetchCols = requiredColumns.toSeq.map(col => {
      schemaWithIndex.filter(_._1.name.equalsIgnoreCase(col)).last._2 + 1
    })
    val fetchColString = (fetchCols.flatMap { col =>
      val deltaCol = ColumnDelta.deltaColumnIndex(col - 1 /* zero based */, 0)
      (col +: (deltaCol until (deltaCol - ColumnDelta.MAX_DEPTH, -1))).map(
        i => s"(select data, columnIndex from $resolvedTableName where " +
            s"partitionId = $partitionId and uuid = ? and columnIndex = $i)")
    } :+ s"(select data, columnIndex from $resolvedTableName where " +
        s"partitionId = $partitionId and uuid = ? and columnIndex = " +
        s"${ColumnFormatEntry.DELETE_MASK_COL_INDEX})").mkString(" union all ")
    // fetch stats query and fetch columns query
    (s"select data, uuid from $resolvedTableName where columnIndex = " +
        s"${ColumnFormatEntry.STATROW_COL_INDEX}", fetchColString)
  }

  def executeQuery(conn: Connection, tableName: String,
      split: Partition, query: String, relDestroyVersion: Int): (Statement, ResultSet, String) = {
    DriverRegistry.register(Constant.JDBC_CLIENT_DRIVER)
    val partition = split.asInstanceOf[ExecutorMultiBucketLocalShellPartition]
    val statement = conn.createStatement()
    if (!useLocatorURL) {
      val buckets = partition.buckets.mkString(",")
      statement.execute(
        s"call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION('$tableName', '$buckets', $relDestroyVersion)")
      // TODO: currently clientStmt.setLocalExecutionBucketIds is not taking effect probably
      // due to a bug. Need to be looked into before enabling the code below.

//      statement match {
//        case clientStmt: ClientStatement =>
//          val numBuckets = partition.buckets.size
//          val bucketSet = if (numBuckets == 1) {
//            Collections.singleton[Integer](partition.buckets.head)
//          } else {
//            val buckets = new java.util.HashSet[Integer](numBuckets)
//            partition.buckets.foreach(buckets.add(_))
//            buckets
//          }
//          clientStmt.setLocalExecutionBucketIds(bucketSet, resolvedName)
//        case _ =>
//          val buckets = partition.buckets.mkString(",")
//          statement.execute(
//            s"call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION('$resolvedName', '$buckets')")
//      }
    }

    val txId = SparkConnectorRDDHelper.snapshotTxIdForRead.get()
    if (!txId.equals("null")) {
      statement.execute(
        s"call sys.USE_SNAPSHOT_TXID('$txId')")
    }

    val rs = statement.executeQuery(query)
    (statement, rs, txId)
  }

  def getConnection(connectionProperties: ConnectionProperties,
      split: Partition): Connection = {
    val urlsOfNetServerHost = split.asInstanceOf[
        ExecutorMultiBucketLocalShellPartition].hostList
    useLocatorURL = SparkConnectorRDDHelper.useLocatorUrl(urlsOfNetServerHost)
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
    val props = ExternalStoreUtils.getAllPoolProperties(jdbcUrl, null,
      connProperties.poolProps, connProperties.hikariCP, isEmbedded = false)
    try {
      // use jdbcUrl as the key since a unique pool is required for each server
      ConnectionPool.getPoolConnection(jdbcUrl, GemFireXDClientDialect, props,
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

object SparkConnectorRDDHelper {

  var snapshotTxIdForRead: ThreadLocal[String] = new ThreadLocal[String]
  var snapshotTxIdForWrite: ThreadLocal[String] = new ThreadLocal[String]

  def getPartitions(tableName: String): Array[Partition] = {
    val bucketToServerList = getBucketToServerMapping(tableName)
    getPartitions(bucketToServerList)
  }

  def getPartitions(bucketToServerList: Array[ArrayBuffer[(String, String)]]): Array[Partition] = {
    val numPartitions = bucketToServerList.length
    val partitions = new Array[Partition](numPartitions)
    for (p <- 0 until numPartitions) {
      var buckets = new mutable.HashSet[Int]()
      buckets += p
      partitions(p) = new ExecutorMultiBucketLocalShellPartition(p,
        buckets, bucketToServerList(p))
    }
    partitions
  }

  private def useLocatorUrl(hostList: ArrayBuffer[(String, String)]): Boolean =
    hostList.isEmpty

  private def fillNetUrlsForServer(node: InternalDistributedMember,
      membersToNetServers: java.util.Map[InternalDistributedMember, String],
      urlPrefix: String, urlSuffix: String,
      netUrls: ArrayBuffer[(String, String)]): Unit = {
    val netServers: String = membersToNetServers.get(node)
    if (netServers != null && !netServers.isEmpty) {
      // check the rare case of multiple network servers
      if (netServers.indexOf(',') > 0) {
        for (netServer <- netServers.split(",")) {
          netUrls += node.getIpAddress.getHostAddress ->
              (urlPrefix + org.apache.spark.sql.collection.Utils
                  .getClientHostPort(netServer) + urlSuffix)
        }
      } else {
        netUrls += node.getIpAddress.getHostAddress ->
            (urlPrefix + org.apache.spark.sql.collection.Utils
                .getClientHostPort(netServers) + urlSuffix)
      }
    }
  }

  /**
   * Called when using smart connector mode that uses accessor
   * to get SnappyData cluster info.
   */
  private def getBucketToServerMapping(
      resolvedName: String): Array[ArrayBuffer[(String, String)]] = {
    val urlPrefix = "jdbc:" + Constant.JDBC_URL_PREFIX
    // no query routing or load-balancing
    val urlSuffix = "/" + ClientAttribute.ROUTE_QUERY + "=false;" +
        ClientAttribute.LOAD_BALANCE + "=false"
    val membersToNetServers = GemFireXDUtils.getGfxdAdvisor.
        getAllNetServersWithMembers
    val availableNetUrls = ArrayBuffer.empty[(String, String)]
    val orphanBuckets = ArrayBuffer.empty[Int]
    Misc.getRegionForTable(resolvedName, true).asInstanceOf[Region[_, _]] match {
      case pr: PartitionedRegion =>
        val bidToAdvisorMap = pr.getRegionAdvisor.
            getAllBucketAdvisorsHostedAndProxies
        val numBuckets = bidToAdvisorMap.size()
        val allNetUrls = new Array[ArrayBuffer[(String, String)]](numBuckets)
        for (bid <- 0 until numBuckets) {
          val pbr = bidToAdvisorMap.get(bid).getProxyBucketRegion
          // throws PartitionOfflineException if appropriate
          pbr.checkBucketRedundancyBeforeGrab(null, false)
          val bOwners = pbr.getBucketOwners
          val netUrls = ArrayBuffer.empty[(String, String)]
          bOwners.asScala.foreach(fillNetUrlsForServer(_,
            membersToNetServers, urlPrefix, urlSuffix, netUrls))

          if (netUrls.isEmpty) {
            // Save the bucket which does not have a neturl,
            // and later assign available ones to it.
            orphanBuckets += bid
          } else {
            for (e <- netUrls) {
              if (!availableNetUrls.contains(e)) {
                availableNetUrls += e
              }
            }
            allNetUrls(bid) = netUrls
          }
        }
        for (bucket <- orphanBuckets) {
          allNetUrls(bucket) = availableNetUrls
        }
        allNetUrls

      case dr: DistributedRegion =>
        val owners = dr.getDistributionAdvisor.adviseInitializedReplicates()
        val netUrls = ArrayBuffer.empty[(String, String)]
        owners.asScala.foreach(fillNetUrlsForServer(_, membersToNetServers,
          urlPrefix, urlSuffix, netUrls))
        Array(netUrls)

      case r => sys.error("unexpected region with dataPolicy=" +
          s"${r.getAttributes.getDataPolicy} attributes: ${r.getAttributes}")
    }
  }

  def setBucketToServerMappingInfo(
      bucketToServerMappingStr: String): Array[ArrayBuffer[(String, String)]] = {
    val urlPrefix = "jdbc:" + Constant.JDBC_URL_PREFIX
    // no query routing or load-balancing
    val urlSuffix = "/" + ClientAttribute.ROUTE_QUERY + "=false;" +
        ClientAttribute.LOAD_BALANCE + "=false"
    if (bucketToServerMappingStr != null) {
      val arr: Array[String] = bucketToServerMappingStr.split(":")
      val orphanBuckets = ArrayBuffer.empty[Int]
      val noOfBuckets = arr(0).toInt
      // val redundancy = arr(1).toInt
      val allNetUrls = new Array[ArrayBuffer[(String, String)]](noOfBuckets)
      val bucketsServers: String = arr(2)
      val newarr: Array[String] = bucketsServers.split("\\|")
      val availableNetUrls = ArrayBuffer.empty[(String, String)]
      for (x <- newarr) {
        val aBucketInfo: Array[String] = x.split(";")
        val bid: Int = aBucketInfo(0).toInt
        if (!(aBucketInfo(1) == "null")) {
          // get (addr,host,port)
          val hostAddressPort = returnHostPortFromServerString(aBucketInfo(1))
          val netUrls = ArrayBuffer.empty[(String, String)]
          netUrls += hostAddressPort._1 ->
              (urlPrefix + hostAddressPort._2 + "[" + hostAddressPort._3 + "]" + urlSuffix)
          allNetUrls(bid) = netUrls
          for (e <- netUrls) {
            if (!availableNetUrls.contains(e)) {
              availableNetUrls += e
            }
          }
        } else {
          // Save the bucket which does not have a neturl,
          // and later assign available ones to it.
          orphanBuckets += bid
        }
      }
      for (bucket <- orphanBuckets) {
        allNetUrls(bucket) = availableNetUrls
      }
      return allNetUrls
    }
    Array.empty
  }

  def setReplicasToServerMappingInfo(
      replicaNodesStr: String): Array[ArrayBuffer[(String, String)]] = {
    val urlPrefix = "jdbc:" + Constant.JDBC_URL_PREFIX
    // no query routing or load-balancing
    val urlSuffix = "/" + ClientAttribute.ROUTE_QUERY + "=false;" +
        ClientAttribute.LOAD_BALANCE + "=false"
    val hostInfo = replicaNodesStr.split(";")
    val netUrls = ArrayBuffer.empty[(String, String)]
    for (host <- hostInfo) {
      val hostAddressPort = returnHostPortFromServerString(host)
      netUrls += hostAddressPort._1 ->
          (urlPrefix + hostAddressPort._2 + "[" + hostAddressPort._3 + "]" + urlSuffix)
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
      // val address: String = matcher.group(2)
      val portStr: String = matcher.group(3)
      // (address, host, portStr)
      (host, host, portStr)
    }
  }
}
