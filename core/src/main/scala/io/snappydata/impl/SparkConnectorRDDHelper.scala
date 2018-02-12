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
import java.util.Collections

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.gemstone.gemfire.internal.SocketCreator
import com.pivotal.gemfirexd.jdbc.ClientAttribute
import io.snappydata.Constant
import io.snappydata.collection.ObjectObjectHashMap
import io.snappydata.thrift.internal.ClientStatement

import org.apache.spark.Partition
import org.apache.spark.sql.collection.SmartExecutorBucketPartition
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.columnar.impl.{ColumnDelta, ColumnFormatEntry}
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.row.GemFireXDClientDialect
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types.StructType

final class SparkConnectorRDDHelper {

  var useLocatorURL: Boolean = false

  def getSQLStatement(resolvedTableName: String,
      partitionId: Int, requiredColumns: Array[String],
      schema: StructType): (String, String) = {

    val schemaWithIndex = schema.zipWithIndex
    // To fetch columns create an IN query on columnIndex
    // with other two columns fixed (uuid and partitionId).
    // (select data, columnIndex from table where
    //  partitionId = 1 and uuid = ? and columnIndex in (1, 2, -3, ...))
    // Store QueryInfo has been enhanced to convert such queries
    // to getAll for best performance (and also enable remote
    //   fetch if required).
    // Note that partitionId is required in where clause even though it
    // is fixed and already set by BUCKETIDs list set for the scan
    // to enable conversion to getAll since it is part of PK.
    // Also the queries use a "select *" instead of projection since
    // store getAll with ProjectionRow does not work for object tables
    // and the additional columns are minuscule in size compared to data blob.
    val fetchCols = requiredColumns.toSeq.map(col => {
      schemaWithIndex.filter(_._1.name.equalsIgnoreCase(col)).last._2 + 1
    })
    val fetchColString = (fetchCols ++ fetchCols.flatMap { col =>
      val deltaCol = ColumnDelta.deltaColumnIndex(col - 1 /* zero based */, 0)
      deltaCol until(deltaCol - ColumnDelta.MAX_DEPTH, -1)
    } :+ ColumnFormatEntry.DELETE_MASK_COL_INDEX).mkString(
      s"select * from $resolvedTableName where " +
          s"partitionId = $partitionId and uuid = ? and columnIndex in (", ",", ")")
    // fetch stats query and fetch columns query
    (s"select * from $resolvedTableName where columnIndex = " +
        s"${ColumnFormatEntry.STATROW_COL_INDEX}", fetchColString)
  }

  def executeQuery(conn: Connection, tableName: String, partition: SmartExecutorBucketPartition,
      query: String, relDestroyVersion: Int): (Statement, ResultSet, String) = {
    val statement = conn.createStatement()
    val txId = SparkConnectorRDDHelper.snapshotTxIdForRead.get() match {
      case "" => null
      case id => id
    }
    statement match {
      case clientStmt: ClientStatement =>
        val bucketSet = Collections.singleton(Int.box(partition.bucketId))
        clientStmt.setLocalExecutionBucketIds(bucketSet, tableName, true)
        clientStmt.setMetadataVersion(relDestroyVersion)
        clientStmt.setSnapshotTransactionId(txId)
      case _ =>
        statement.execute("call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION(" +
            s"'$tableName', '${partition.bucketId}', $relDestroyVersion)")
        if (txId ne null) {
          statement.execute(s"call sys.USE_SNAPSHOT_TXID('$txId')")
        }
    }

    val rs = statement.executeQuery(query)
    (statement, rs, txId)
  }

  def getConnection(connectionProperties: ConnectionProperties,
      part: SmartExecutorBucketPartition): Connection = {
    val urlsOfNetServerHost = part.hostList
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

  DriverRegistry.register(Constant.JDBC_CLIENT_DRIVER)

  var snapshotTxIdForRead: ThreadLocal[String] = new ThreadLocal[String]
  var snapshotTxIdForWrite: ThreadLocal[String] = new ThreadLocal[String]

  def getPartitions(bucketToServerList: Array[ArrayBuffer[(String, String)]]): Array[Partition] = {
    val numPartitions = bucketToServerList.length
    val partitions = new Array[Partition](numPartitions)
    for (p <- 0 until numPartitions) {
      if (StoreUtils.TEST_RANDOM_BUCKETID_ASSIGNMENT) {
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

  def setBucketToServerMappingInfo(
      bucketToServerMappingStr: String): Array[ArrayBuffer[(String, String)]] = {
    val urlPrefix = "jdbc:" + Constant.JDBC_URL_PREFIX
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
          // get (addr,host,port)
          val hostAddressPort = returnHostPortFromServerString(aBucketInfo(1))
          val host = hostAddressPort._1
          val netUrl = urlPrefix + hostAddressPort._2 + "[" + hostAddressPort._3 + "]" + urlSuffix
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
