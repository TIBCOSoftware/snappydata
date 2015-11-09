package org.apache.spark.sql.columnar

import java.sql.Connection
import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.cluster.SnappyCoarseGrainedSchedulerBackend
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.sql.execution.datasources.ResolvedDataSource
import org.apache.spark.sql.store.StoreProperties




import scala.collection.mutable

import org.apache.spark.sql.collection.{UUIDRegionKey, Utils}
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.sql.execution.datasources.jdbc.{DriverRegistry, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.row.{GemFireXDClientDialect, GemFireXDDialect}

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

  def getDriver(url : String): Option[String] = {
    val dialect = JdbcDialects.get(url)
    dialect match {
      case  GemFireXDDialect => Option("com.pivotal.gemfirexd.jdbc.EmbeddedDriver")
      case  GemFireXDClientDialect => Option("com.pivotal.gemfirexd.jdbc.ClientDriver")
      case _=> Option(DriverRegistry.getDriverClassName(url))
    }

  }

  def validateAndGetAllProps(sc : SparkContext, options: Map[String, String]) = {
    val parameters = new mutable.HashMap[String, String]
    parameters ++= options

    // First priority is for URL specified in SparkConf. If not we will check if user has provided that in
    // options, else we will default it to Snappy peer connection URL
    val snappyUrl = sc.getConf.get(StoreProperties.SNAPPY_STORE_JDBC_URL, "")

    val url = if (ExternalStoreUtils.isExternalShellMode(sc)) {
      val clazz = ResolvedDataSource.lookupDataSource("io.snappydata.Utils")
      clazz.getMethod("getLocatorClientURL").invoke(clazz).asInstanceOf[String] }
    else
      parameters.remove("url").getOrElse {
        if (snappyUrl.isEmpty) StoreProperties.DEFAULT_SNAPPY_STORE_JDBC_URL else snappyUrl
      }

    val driver = parameters.remove("driver").map { d =>
      // register for this case
      DriverRegistry.register(d)
      d
    }.orElse(getDriver(url))

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
    val allPoolProps = getAllPoolProperties(url, driver.get,
      poolProps, hikariCP)
    (url, driver.get, allPoolProps, connProps, hikariCP)
  }

  def getPoolConnection(id: String, driver: Option[String],
      poolProps: Map[String, String], connProps: Properties,
      hikariCP: Boolean): Connection = {
    try {
      if (driver.isDefined) DriverRegistry.register(driver.get)
    } catch {
      case cnfe: ClassNotFoundException => throw new IllegalArgumentException(
        s"Couldn't find driver class $driver", cnfe)
    }
    ConnectionPool.getPoolConnection(id, poolProps, connProps, hikariCP)
  }

  def getConnection(url: String, connProperties: Properties) = {
    connProperties.remove("poolProperties")
    JdbcUtils.createConnection(url, connProperties)
    //DriverManager.getConnection(url)
  }

  def getConnectionType(url: String) = {
    JdbcDialects.get(url) match {
      case GemFireXDDialect => ConnectionType.Embedded
      case GemFireXDClientDialect =>   ConnectionType.Net
      case _ => ConnectionType.Unknown
    }
  }

  def isExternalShellMode (sparkContext: SparkContext): Boolean ={
    if (!sparkContext.schedulerBackend.isInstanceOf[LocalBackend] &&
        !sparkContext.schedulerBackend.isInstanceOf[SnappyCoarseGrainedSchedulerBackend])
       true
    else
      false
  }
}

object ConnectionType extends Enumeration {
  type ConnectionType = Value
  val Embedded, Net, ExternalShell, Unknown = Value
}
