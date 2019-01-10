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
package io.snappydata.sql.catalog

import java.net.URL
import java.nio.file.{Files, Paths}
import java.sql.{CallableStatement, Connection, SQLException}
import java.util.Collections

import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.iapi.types.HarmonySerialBlob
import com.pivotal.gemfirexd.jdbc.ClientAttribute
import io.snappydata.{Constant, Property}
import io.snappydata.thrift.{BucketOwners, CatalogMetadataDetails, CatalogMetadataRequest}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.collection.{SharedUtils, SmartExecutorBucketPartition}
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, JDBCOptions, JdbcUtils}
import org.apache.spark.{Logging, Partition, SparkContext}
import org.eclipse.collections.impl.map.mutable.UnifiedMap

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class SmartConnectorHelper(session: SparkSession, jdbcUrl: String) extends Logging {

  private val conn: Connection = {
    val jdbcOptions = new JDBCOptions(jdbcUrl + getSecurePart + ";route-query=false;", "",
      Map("driver" -> Constant.JDBC_CLIENT_DRIVER))
    JdbcUtils.createConnectionFactory(jdbcOptions)()
  }

  private lazy val getCatalogMetaDataStmt: CallableStatement =
    conn.prepareCall("call sys.GET_CATALOG_METADATA(?, ?, ?)")

  private lazy val updateCatalogMetaDataStmt: CallableStatement =
    conn.prepareCall("call sys.UPDATE_CATALOG_METADATA(?, ?)")

  {
    val stmt = conn.prepareCall("call sys.GET_DEPLOYED_JARS(?)")
    val sc = session.sparkContext
    if ((sc ne null) && System.getProperty("pull-deployed-jars", "true").toBoolean) {
      try {
        executeGetJarsStmt(sc, stmt)
      } catch {
        case sqle: SQLException => logWarning(s"could not get jar and" +
          s" package information from SnappyData cluster", sqle)
      }
    }
  }

  private def getSecurePart: String = {
    var securePart = ""
    val user = session.conf.get(Constant.SPARK_STORE_PREFIX + Attribute
      .USERNAME_ATTR, "")
    if (!user.isEmpty) {
      val pass = session.conf.get(Constant.SPARK_STORE_PREFIX + Attribute
        .PASSWORD_ATTR, "")
      securePart = s";user=$user;password=$pass"
      logInfo(s"Using $user credentials to securely connect to SnappyData cluster")
    }
    securePart
  }

  def setCurrentSchema(schema: String): Unit = {
    conn.setSchema(schema)
  }

  private def executeGetJarsStmt(sc: SparkContext, stmt: CallableStatement): Unit = {
    stmt.registerOutParameter(1, java.sql.Types.VARCHAR)
    stmt.execute()
    val jarsString = stmt.getString(1)
    var mutableList = new mutable.MutableList[URL]
    if (jarsString != null && jarsString.nonEmpty) {
      // comma separated list of file urls will be obtained
      jarsString.split(",").foreach(f => {
        mutableList.+=(new URL(f))
        val jarpath = f.substring(5)
        if (Files.isReadable(Paths.get(jarpath))) {
          try {
            sc.addJar(jarpath)
          } catch {
            // warn
            case ex: Exception => logWarning(s"could not add path $jarpath to SparkContext", ex)
          }
        } else {
          // May be the smart connector app does not care about the deployed jars or
          // the path is not readable so just log warning
          logWarning(s"could not add path $jarpath to SparkContext as the file is not readable")
        }
      })
      val newClassLoader = SharedUtils.newMutableURLClassLoader(mutableList.toArray)
      Thread.currentThread().setContextClassLoader(newClassLoader)
    }
  }

  def getCatalogMetadata(operation: Int,
                         request: CatalogMetadataRequest): CatalogMetadataDetails = {
    getCatalogMetaDataStmt.setInt(1, operation)
    val requestBytes = GemFireXDUtils.writeThriftObject(request)
    getCatalogMetaDataStmt.setBlob(2, new HarmonySerialBlob(requestBytes))
    getCatalogMetaDataStmt.registerOutParameter(3, java.sql.Types.BLOB)
    assert(!getCatalogMetaDataStmt.execute())
    val resultBlob = getCatalogMetaDataStmt.getBlob(3)
    val resultLen = resultBlob.length().toInt
    val result = new CatalogMetadataDetails()
    assert(GemFireXDUtils.readThriftObject(result, resultBlob.getBytes(1, resultLen)) == 0)
    resultBlob.free()
    result
  }

  def updateCatalogMetadata(operation: Int, request: CatalogMetadataDetails): Unit = {
    updateCatalogMetaDataStmt.setInt(1, operation)
    val requestBytes = GemFireXDUtils.writeThriftObject(request)
    updateCatalogMetaDataStmt.setBlob(2, new HarmonySerialBlob(requestBytes))
    assert(!updateCatalogMetaDataStmt.execute())
  }

  def close(): Unit = {
    try {
      getCatalogMetaDataStmt.close()
    } catch {
      case _: SQLException => // ignore
    }
    try {
      conn.close()
    } catch {
      case _: SQLException => // ignore
    }
  }
}

object SmartConnectorHelper {

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
      if (SharedUtils.TEST_RANDOM_BUCKETID_ASSIGNMENT) {
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

  def preferHostName(session: SparkSession): Boolean = {
    // check if Spark executors are using IP addresses or host names
    SharedUtils.executorsListener(session.sparkContext) match {
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
                        urlSuffix: String,
                        availableNetUrls: UnifiedMap[String, String]): (String, String) = {
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
                                   preferHost: Boolean, preferPrimaries: Boolean):
  Array[ArrayBuffer[(String, String)]] = {
    if (!buckets.isEmpty) {
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
                                     preferHost: Boolean):
  Array[ArrayBuffer[(String, String)]] = {
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
