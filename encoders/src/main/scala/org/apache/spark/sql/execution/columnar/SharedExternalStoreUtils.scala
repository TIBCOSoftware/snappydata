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

import java.sql.{Connection, PreparedStatement, SQLException, Types}
import java.util.Properties

import com.gemstone.gemfire.internal.shared.ClientSharedUtils
import com.pivotal.gemfirexd.jdbc.ClientAttribute

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import org.apache.spark.sql.{AnalysisException, SnappyStoreClientDialect}
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.sql.types.{Decimal, StructType}

object SharedExternalStoreUtils {

  private var useLocatorURL: Boolean = _

  final val COLUMN_BATCH_SIZE = "COLUMN_BATCH_SIZE"
  final val COLUMN_MAX_DELTA_ROWS = "COLUMN_MAX_DELTA_ROWS"

  private def useLocatorUrl(hostList: ArrayBuffer[(String, String)]): Boolean =
    hostList.isEmpty

  def getTableSchema(schemaAsJson: String): StructType = StructType.fromString(schemaAsJson)

  def getConnection(connectionProperties: ConnectionProperties,
      hostList: ArrayBuffer[(String, String)]): Connection = {
    useLocatorURL = useLocatorUrl(hostList)
    createConnection(connectionProperties, hostList)
  }

  private def createConnection(connProperties: ConnectionProperties,
      hostList: ArrayBuffer[(String, String)]): Connection = {
    val localhost = ClientSharedUtils.getLocalHost
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
    val props = getAllPoolProperties(jdbcUrl, null,
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

  private def addProperty(props: mutable.Map[String, String], key: String,
      default: String): Unit = {
    if (!props.contains(key)) props.put(key, default)
  }

  private def defaultMaxEmbeddedPoolSize: String =
    String.valueOf(math.max(256, Runtime.getRuntime.availableProcessors() * 16))

  private def defaultMaxExternalPoolSize: String =
    String.valueOf(math.max(256, Runtime.getRuntime.availableProcessors() * 8))

  def getAllPoolProperties(url: String, driver: String,
      poolProps: Map[String, String], hikariCP: Boolean,
      isEmbedded: Boolean): Map[String, String] = {
    // setup default pool properties
    val props = new mutable.HashMap[String, String]()
    if (poolProps.nonEmpty) props ++= poolProps
    if (driver != null && !driver.isEmpty) {
      addProperty(props, "driverClassName", driver)
    }
    val defaultMaxPoolSize = if (isEmbedded) defaultMaxEmbeddedPoolSize
    else defaultMaxExternalPoolSize
    if (hikariCP) {
      props.put("jdbcUrl", url)
      addProperty(props, "maximumPoolSize", defaultMaxPoolSize)
      addProperty(props, "minimumIdle", "10")
      addProperty(props, "idleTimeout", "120000")
    } else {
      props.put("url", url)
      addProperty(props, "maxActive", defaultMaxPoolSize)
      addProperty(props, "maxIdle", defaultMaxPoolSize)
      addProperty(props, "initialSize", "4")
      addProperty(props, "testOnBorrow", "true")
      // embedded validation check is cheap
      if (isEmbedded) addProperty(props, "validationInterval", "0")
      else addProperty(props, "validationInterval", "10000")
    }
    props.toMap
  }

  def setStatementParameters(stmt: PreparedStatement,
      row: mutable.ArrayBuffer[Any]): Unit = {
    var col = 1
    val len = row.length
    while (col <= len) {
      val colVal = row(col - 1)
      if (colVal != null) {
        colVal match {
          case s: String => stmt.setString(col, s)
          case i: Int => stmt.setInt(col, i)
          case l: Long => stmt.setLong(col, l)
          case d: Double => stmt.setDouble(col, d)
          case f: Float => stmt.setFloat(col, f)
          case s: Short => stmt.setInt(col, s)
          case b: Byte => stmt.setInt(col, b)
          case b: Boolean => stmt.setBoolean(col, b)
          case b: Array[Byte] => stmt.setBytes(col, b)
          case ts: java.sql.Timestamp => stmt.setTimestamp(col, ts)
          case d: java.sql.Date => stmt.setDate(col, d)
          case t: java.sql.Time => stmt.setTime(col, t)
          case d: Decimal => stmt.setBigDecimal(col, d.toJavaBigDecimal)
          case bd: java.math.BigDecimal => stmt.setBigDecimal(col, bd)
          case _ => stmt.setObject(col, colVal)
        }
      } else {
        stmt.setNull(col, Types.NULL)
      }
      col += 1
    }
  }

  /*
 *
 */
  def connectionProperties(hostList: ArrayBuffer[(String, String)]): ConnectionProperties = {

    // TODO: Check how to make properties Dynamic
    val map: Map[String, String] = HashMap[String, String](("maxActive", "256"),
      ("testOnBorrow", "true"), ("maxIdle", "256"), ("validationInterval", "10000"),
      ("initialSize", "4"), ("driverClassName", "io.snappydata.jdbc.ClientDriver"))

    val poolProperties = new Properties
    poolProperties.setProperty("driver", "io.snappydata.jdbc.ClientDriver")
    poolProperties.setProperty("route-query", "false")

    val executorConnProps = new Properties
    executorConnProps.setProperty("lob-chunk-size", "33554432")
    executorConnProps.setProperty("driver", "io.snappydata.jdbc.ClientDriver")
    executorConnProps.setProperty("route-query", "false")
    executorConnProps.setProperty("lob-direct-buffers", "true")

    ConnectionProperties(hostList(0)._2,
      "io.snappydata.jdbc.ClientDriver", SnappyStoreClientDialect, map,
      poolProperties, executorConnProps, false)

  }
}

class TableNotFoundException(schema: String, table: String, cause: Option[Throwable] = None)
    extends AnalysisException(s"Table or view '$table' not found in schema '$schema'",
      cause = cause)

