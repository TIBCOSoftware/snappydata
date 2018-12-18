/*
 *
 */
package org.apache.spark.sql.execution.columnar

import java.sql.{Connection, PreparedStatement, SQLException, Types}
import java.util.Properties

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.row.SnappyStoreClientDialect
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.sql.types.Decimal

object SharedExternalStoreUtils {

  private var useLocatorURL: Boolean = _

  private def useLocatorUrl(hostList: ArrayBuffer[(String, String)]): Boolean =
    hostList.isEmpty

  def getConnection(connectionProperties: ConnectionProperties,
      hostList: ArrayBuffer[(String, String)]): Connection = {
    useLocatorURL = useLocatorUrl(hostList)
    createConnection(connectionProperties, hostList)
  }

  private def createConnection(connProperties: ConnectionProperties,
      hostList: ArrayBuffer[(String, String)]): Connection = {
    // TODO CHECK THIS SOCKETCREATOR DEPENDENCY RESOLUTION
    // val localhost = SocketCreator.getLocalHost
    var index = -1

    val jdbcUrl = if (useLocatorURL) {
      connProperties.url
    } else {
      // TODO NEEDS TO BE HANDLED=
      // if (index < 0) index = hostList.indexWhere(_._1.contains(localhost.getHostAddress))
      if (index < 0) index = Random.nextInt(hostList.size)
      hostList(index)._2
    }

    // TODO : REMOVE THIS AND ADD DEPENDENCY TO ACCESS ClientAttribute.THRIFT_LOB_DIRECT_BUFFERS
    /**
     * Use direct ByteBuffers when reading BLOBs. This will provide higher
     * performance avoiding a copy but caller must take care to free the BLOB
     * after use else cleanup may happen only in a GC cycle which may be delayed
     * due to no particular GC pressure due to direct ByteBuffers.
     */
    val THRIFT_LOB_DIRECT_BUFFERS = "lob-direct-buffers"
    // -

    // enable direct ByteBuffers for best performance
    val executorProps = connProperties.executorConnProps
    // executorProps.setProperty(ClientAttribute.THRIFT_LOB_DIRECT_BUFFERS, "true")
    executorProps.setProperty(THRIFT_LOB_DIRECT_BUFFERS, "true")

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
}
