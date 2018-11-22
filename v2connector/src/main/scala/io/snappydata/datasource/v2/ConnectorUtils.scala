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
package io.snappydata.datasource.v2

import scala.collection.mutable.ArrayBuffer

import com.pivotal.gemfirexd.jdbc.ClientAttribute
import io.snappydata.Constant
import io.snappydata.collection.ObjectObjectHashMap

/**
 * Contains utility methods required by connectors
 */

/*
TODO: These methods are copied from SmartConnectorRDDHelper, move this file to a common jar accessible from both Smart connector and V2 connector
 */
object ConnectorUtils {
  /*
  TODO: check SmartConnectorRDDHelper for implementation
  NOTE: JavaDoc for DataReaderFactory.preferredLocations says
  that preferredLocations should return host name, so currently
  this returns true
   */
  private def preferHostName(): Boolean = {
    true
  }

  def setBucketToServerMappingInfo(bucketToServerMappingStr: String):
  Array[ArrayBuffer[(String, String)]] = {
    val urlPrefix = Constant.DEFAULT_THIN_CLIENT_URL
    // no query routing or load-balancing
    val urlSuffix = "/" + ClientAttribute.ROUTE_QUERY + "=false;" +
        ClientAttribute.LOAD_BALANCE + "=false"
    if (bucketToServerMappingStr != null) {
      // check if Spark executors are using IP addresses or host names
      val preferHost = preferHostName()
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


  def setReplicasToServerMappingInfo(replicaNodesStr: String):
  Array[ArrayBuffer[(String, String)]] = {
    // check if Spark executors are using IP addresses or host names
    val preferHost = preferHostName()
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
   *
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
