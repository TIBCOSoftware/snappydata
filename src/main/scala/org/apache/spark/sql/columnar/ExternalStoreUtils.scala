package org.apache.spark.sql.columnar

import java.sql.{DriverManager, Connection}
import java.util.Properties

import com.pivotal.gemfirexd.internal.client.net.NetConnection
import com.pivotal.gemfirexd.internal.impl.jdbc.EmbedConnection
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.row.{GemFireXDClientDialect, GemFireXDDialect, JDBCUpdatableRelation}
import org.apache.spark.sql.jdbc.{JdbcDialects, JdbcUtils, DriverRegistry}

import scala.collection.mutable

/**
 * Created by kneeraj on 27/7/15.
 */
private[sql] object ExternalStoreUtils {
  // TODO: KN: copy of the method JDBCUpdatableSource.createStreamTable. Need to merge the two code.
  // Need to understand whether that class can have some utils method also

  def validateAndGetAllProps(options: Map[String, String]) = {
    val parameters = new mutable.HashMap[String, String]
    parameters ++= options
    val url = parameters.remove("url").getOrElse(
      sys.error("Option 'url' not specified"))
    val driver = parameters.remove("driver")
    val poolImpl = parameters.remove("poolImpl")
    val poolProperties = parameters.remove("poolproperties")

    driver.foreach(DriverRegistry.register)

    val hikariCP = poolImpl.map(Utils.normalizeId) match {
      case Some("hikari") => true
      case Some("tomcat") => false
      case Some(p) =>
        throw new IllegalArgumentException("JDBCUpdatableRelation: " +
          s"unsupported pool implementation '$p' " +
          s"(supported values: tomcat, hikari)")
      case None => false
    }
    val poolProps = poolProperties.map(p => Map(p.split(",").map { s =>
      val eqIndex = s.indexOf('=')
      if (eqIndex >= 0) {
        (s.substring(0, eqIndex).trim, s.substring(eqIndex + 1).trim)
      } else {
        // assume a boolean property to be enabled
        (s.trim, "true")
      }
    }: _*)).getOrElse(Map.empty)

    // remaining parameters are passed as properties to getConnection
    val connProps = new Properties()
    parameters.foreach(kv => connProps.setProperty(kv._1, kv._2))
    val allPoolProps = JDBCUpdatableRelation.getAllPoolProperties(url, driver.getOrElse(null), poolProps, hikariCP)
    (url, driver, allPoolProps, connProps, hikariCP)
  }

  def getPoolConnection(id: String, driver: Option[String], poolProps: Map[String, String],
                        connProps: Properties, hikariCP: Boolean): Connection = {
    try {
      if (driver.isDefined) DriverRegistry.register(driver.get)
    } catch {
      case cnfe: ClassNotFoundException =>
        throw new IllegalArgumentException(s"Couldn't find driver class $driver", cnfe)
    }

    ConnectionPool.getPoolConnection(id, poolProps, connProps, hikariCP)
  }

  def getConnection(url: String, connProperties: Properties) = {
    connProperties.remove("poolProps")
    JdbcUtils.createConnection(url, connProperties)
    //DriverManager.getConnection(url)
  }

  def getConnectionType(url: String) = {
      JdbcDialects.get(url) match {
        case GemFireXDDialect => ConnectionType.Embedded
        case GemFireXDClientDialect => ConnectionType.Net
        case _ => ConnectionType.Unknown
      }
  }
}

object ConnectionType extends Enumeration {
  type ConnectionType = Value
  val Embedded, Net, Unknown = Value
}

