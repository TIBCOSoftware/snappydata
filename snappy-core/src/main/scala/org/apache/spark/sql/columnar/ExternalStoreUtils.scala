package org.apache.spark.sql.columnar

import java.util.Properties

import scala.collection.mutable

import io.snappydata.Constant

import org.apache.spark.SparkContext
import org.apache.spark.sql.collection.{ToolsCallbackInit, Utils}
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, JdbcUtils}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects}
import org.apache.spark.sql.row.{GemFireXDClientDialect, GemFireXDDialect}
import org.apache.spark.sql.sources.JdbcExtendedUtils
import org.apache.spark.sql._

/**
 * Utility methods used by external storage layers.
 */
private[sql] object ExternalStoreUtils {

  def getAllPoolProperties(url: String, driver: String,
      poolProps: Map[String, String], hikariCP: Boolean) = {
    val urlProp = if (hikariCP) "jdbcUrl" else "url"
    val driverClassProp = "driverClassName"
    if (driver == null || driver.isEmpty) {
      if (poolProps.isEmpty) {
        Map(urlProp -> url)
      } else {
        poolProps + (urlProp -> url)
      }
    } else if (poolProps.isEmpty) {
      Map(urlProp -> url, driverClassProp -> driver)
    } else {
      poolProps + (urlProp -> url) + (driverClassProp -> driver)
    }
  }

  def getDriver(url: String, dialect: JdbcDialect): String = {
    dialect match {
      case GemFireXDDialect => "com.pivotal.gemfirexd.jdbc.EmbeddedDriver"
      case GemFireXDClientDialect => "com.pivotal.gemfirexd.jdbc.ClientDriver"
      case _ => DriverRegistry.getDriverClassName(url)
    }
  }

  class CaseInsensitiveMutableHashMap(map: Map[String, String])
      extends mutable.Map[String, String] with Serializable {

    val baseMap = new mutable.HashMap[String, String]
    baseMap ++= map.map(kv => kv.copy(_1 = kv._1.toLowerCase))

    override def get(k: String): Option[String] = baseMap.get(k.toLowerCase)

    override def remove(k: String): Option[String] = baseMap.remove(k.toLowerCase)

    override def iterator: Iterator[(String, String)] = baseMap.iterator

    override def +=(kv: (String, String)) = {
      baseMap += kv
      this
    }

    override def -=(key: String) = {
      baseMap -= key
      this
    }
  }

  def removeInternalProps(parameters: mutable.Map[String, String]): String = {
    val dbtableProp = JdbcExtendedUtils.DBTABLE_PROPERTY
    val table = parameters.remove(dbtableProp)
        .getOrElse(sys.error(s"Option '$dbtableProp' not specified"))
    parameters.remove(JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY)
    parameters.remove(JdbcExtendedUtils.SCHEMA_PROPERTY)
    parameters.remove("serialization.format")
    table
  }

  def defaultStoreURL(sc: SparkContext): String = {
    val modeUrl = SnappyContext.getClusterMode(sc) match {
      case SnappyEmbeddedMode(_, _) =>
        // Already connected to SnappyData in embedded mode.
        Constant.DEFAULT_EMBEDDED_URL + ";host-data=false;mcast-port=0"
      case SnappyShellMode(_, _) =>
        ToolsCallbackInit.toolsCallback.getLocatorJDBCURL(sc)
      case ExternalEmbeddedMode(_, url) =>
        Constant.DEFAULT_EMBEDDED_URL + ";host-data=false;" + url
      case LonerMode(_, url) => Constant.DEFAULT_EMBEDDED_URL +
          (if (url == null) ";mcast-port=0" else ";" + url)
      case ExternalClusterMode(_, _) =>
        throw new AnalysisException("Option 'url' not specified")
    }
    modeUrl + ";route-query=false"
  }

  def isExternalShellMode(sparkContext: SparkContext): Boolean = {
    SnappyContext.getClusterMode(sparkContext) match {
      case SnappyShellMode(_, _) => true
      case _ => false
    }
  }

  def validateAndGetAllProps(sc : SparkContext,
      parameters: mutable.Map[String, String]) = {

    val url = parameters.remove("url").getOrElse(defaultStoreURL(sc))

    val dialect = JdbcDialects.get(url)
    val driver = parameters.remove("driver").getOrElse(getDriver(url, dialect))

    DriverRegistry.register(driver)

    val poolImpl = parameters.remove("poolimpl")
    val poolProperties = parameters.remove("poolproperties")

    val hikariCP = poolImpl.map(Utils.normalizeId) match {
      case Some("hikari") => true
      case Some("tomcat") => false
      case Some(p) =>
        throw new IllegalArgumentException("ExternalStoreUtils: " +
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
    dialect match {
      case GemFireXDDialect | GemFireXDClientDialect =>
        connProps.setProperty("route-query", "false")
      case _ =>
    }
    val allPoolProps = getAllPoolProperties(url, driver,
      poolProps, hikariCP)
    (url, driver, allPoolProps, connProps, hikariCP)
  }

  def getConnection(url: String, connProperties: Properties) = {
    connProperties.remove("poolProperties")
    JdbcUtils.createConnection(url, connProperties)
  }

  def getConnectionType(url: String) = {
    JdbcDialects.get(url) match {
      case GemFireXDDialect => ConnectionType.Embedded
      case GemFireXDClientDialect =>   ConnectionType.Net
      case _ => ConnectionType.Unknown
    }
  }
}

object ConnectionType extends Enumeration {
  type ConnectionType = Value
  val Embedded, Net, Unknown = Value
}
