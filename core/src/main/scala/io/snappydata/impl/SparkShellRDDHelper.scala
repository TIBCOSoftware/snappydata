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

import org.apache.spark.sql.{ThinClientConnectorMode, SnappyContext}
import org.apache.spark.{SparkContext, Logging, Partition}
import org.apache.spark.sql.collection.ExecutorMultiBucketLocalShellPartition
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.row.GemFireXDClientDialect
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.sql.store.StoreUtils

final class SparkShellRDDHelper {

  var useLocatorURL: Boolean = false

  def getSQLStatement(resolvedTableName: String,
      requiredColumns: Array[String], partitionId: Int): String = {
    val whereClause = if (useLocatorURL) s" where bucketId = $partitionId" else ""
    "select " + requiredColumns.mkString(", ") +
        ", numRows, stats from " + resolvedTableName + whereClause
  }

  def executeQuery(conn: Connection, tableName: String,
      split: Partition, query: String): (Statement, ResultSet) = {
    DriverRegistry.register(Constant.JDBC_CLIENT_DRIVER)
    val resolvedName = StoreUtils.lookupName(tableName, conn.getSchema)

    val partition = split.asInstanceOf[ExecutorMultiBucketLocalShellPartition]
    val buckets = partition.buckets.mkString(",")
    val statement = conn.createStatement()
    if (!useLocatorURL) {
      statement.execute(
        s"call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION('$resolvedName', '$buckets')")
    }

    val rs = statement.executeQuery(query)
    (statement, rs)
  }

  def getConnection(connectionProperties: ConnectionProperties,
      split: Partition): Connection = {
    val urlsOfNetServerHost = split.asInstanceOf[
        ExecutorMultiBucketLocalShellPartition].hostList
    useLocatorURL = SparkShellRDDHelper.useLocatorUrl(urlsOfNetServerHost)
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

    // setup pool properties
    val props = ExternalStoreUtils.getAllPoolProperties(jdbcUrl, null,
      connProperties.poolProps, connProperties.hikariCP, isEmbedded = false)
    try {
      // use jdbcUrl as the key since a unique pool is required for each server
      ConnectionPool.getPoolConnection(jdbcUrl, GemFireXDClientDialect,
        props, connProperties.executorConnProps, connProperties.hikariCP)
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

object SparkShellRDDHelper extends Logging {

  def getPartitions(tableName: String, conn: Connection, sc: SparkContext,
      isPartitioned: Boolean): Array[Partition] = {
    val resolvedName = ExternalStoreUtils.lookupName(tableName, conn.getSchema)
    val bucketToServerList = SnappyContext.getClusterMode(sc) match {
      case ThinClientConnectorMode(_, _) => getBucketToServerMapping(conn, resolvedName,
        isPartitioned)
      case _ => getBucketToServerMapping(resolvedName)
    }
//    logInfo("getPartitions bucketToServerList =  " + bucketToServerList.deep.mkString("\n"))
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

  /*
  * Called when using smart connector mode that uses accessor
  * to get SnappyData cluster info
  **/
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

  /*
   * Called when using connector mode that uses thin client connection
   * to get SnappyData cluster info. This uses system procs to get
   * information.
  **/
  private def getBucketToServerMapping(connection: Connection, tableName: String,
      isPartitioned: Boolean): Array[ArrayBuffer[(String, String)]] = {
    if (isPartitioned) {
      val getBktLocationProc = connection.prepareCall(s"call SYS.GET_BUCKET_TO_SERVER_MAPPING2(?, ?)")
      getBktLocationProc.setString(1, tableName)
      getBktLocationProc.registerOutParameter(2, java.sql.Types.CLOB)
      getBktLocationProc.execute
      val bucketToServerMappingStr: String = getBktLocationProc.getString(2)
      val allNetUrls = setBucketToServerMappingInfo(bucketToServerMappingStr)
      allNetUrls
    } else {
      val getReplicaNodes = connection.prepareCall(s"call SYS.GET_INITIALIZED_REPLICAS(?, ?)")
      getReplicaNodes.setString(1, tableName)
      getReplicaNodes.registerOutParameter(2, java.sql.Types.CLOB)
      getReplicaNodes.execute
      val replicaNodesStr: String = getReplicaNodes.getString(2)
      val allNetUrls = setReplicasToServerMappingInfo(replicaNodesStr)
      allNetUrls
    }
  }

  private def setReplicasToServerMappingInfo(replicaNodesStr: String): Array[ArrayBuffer[(String, String)]]  = {
    val urlPrefix = "jdbc:" + Constant.JDBC_URL_PREFIX
    // no query routing or load-balancing
    val urlSuffix = "/" + ClientAttribute.ROUTE_QUERY + "=false;" +
        ClientAttribute.LOAD_BALANCE + "=false"
    val arr = replicaNodesStr.split("\\|")
    if (arr(0).toInt > 0) {
      val hosts = arr(1).split(";")
      val allNetUrls = new Array[ArrayBuffer[(String, String)]](arr(0).toInt)
      var i = 0
      for (host <- hosts) {
        val hostAddressPort = returnHostPortFromServerString(arr(1))
        val netUrls = ArrayBuffer.empty[(String, String)]
        netUrls += hostAddressPort._1 -> (urlPrefix + hostAddressPort._2 + "[" + hostAddressPort._3 + "]" + urlSuffix)
        allNetUrls(i) = netUrls
        i += 1
      }
     return allNetUrls
    }
    Array.empty
  }

  private def setBucketToServerMappingInfo(bucketToServerMappingStr: String): Array[ArrayBuffer[(String, String)]]  = {
    val urlPrefix = "jdbc:" + Constant.JDBC_URL_PREFIX
    // no query routing or load-balancing
    val urlSuffix = "/" + ClientAttribute.ROUTE_QUERY + "=false;" +
        ClientAttribute.LOAD_BALANCE + "=false"
    if (bucketToServerMappingStr != null) {
      val arr: Array[String] = bucketToServerMappingStr.split(":")
      val noOfBuckets = arr(0).toInt
//    val redundancy = arr(1).toInt
      val allNetUrls = new Array[ArrayBuffer[(String, String)]](noOfBuckets)
      val bucketsServers: String = arr(2)
      val newarr: Array[String] = bucketsServers.split("\\|")
      for (x <- newarr) {
        val aBucketInfo: Array[String] = x.split(";")
        val bid: Int = aBucketInfo(0).toInt
        if (!(aBucketInfo(1) == "null")) {
          // get (addr,host,port)
          val hostAddressPort = returnHostPortFromServerString(aBucketInfo(1))
          val netUrls = ArrayBuffer.empty[(String, String)]
          netUrls += hostAddressPort._1 -> (urlPrefix + hostAddressPort._2 + "[" + hostAddressPort._3 + "]" + urlSuffix)
          allNetUrls(bid) = netUrls
        }
      }
      return allNetUrls
    }
    Array.empty
  }

  /*
  * The pattern to extract addresses from the result of
  * GET_ALLSERVERS_AND_PREFSERVER2 procedure; format is:
  *
  * host1/addr1[port1]{kind1},host2/addr2[port2]{kind2},...
  */
  private lazy val addrPattern: java.util.regex.Pattern = java.util.regex.Pattern.compile("([^,/]*)(/[^,\\[]+)?\\[([\\d]+)\\](\\{[^}]+\\})?")

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
      val addr: String = matcher.group(2)
      val portStr: String = matcher.group(3)
      (addr, host, portStr)
    }
  }


}
