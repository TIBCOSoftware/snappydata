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
package org.apache.spark.sql.hive

import java.util.Properties

import java.nio.file.Paths

import com.gemstone.gemfire.internal.shared.SystemProperties
import com.pivotal.gemfirexd.Attribute.{PASSWORD_ATTR, USERNAME_ATTR}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.impl.sql.catalog.GfxdDataDictionary
import io.snappydata.Constant
import io.snappydata.Constant.{SPARK_STORE_PREFIX, STORE_PROPERTY_PREFIX}
import io.snappydata.impl.SnappyHiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.hive.execution.HiveTableScanExec
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH
import org.apache.spark.sql.{ClusterMode, SnappyContext, ThinClientConnectorMode}
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * A utility class to get hive meta-store connection to underlying SnappyData store.
 * The main use of this class is to setup hive metadata client properties appropriate
 * for connection to embedded store.
 */
object HiveClientUtil extends Logging {

  val HIVE_TMPDIR = "./hive"

  val HIVE_DEFAULT_SETTINGS = Map(ConfVars.SCRATCHDIR -> hivePath("scratch"),
    ConfVars.LOCALSCRATCHDIR -> hivePath("local_scratch"),
    ConfVars.DOWNLOADED_RESOURCES_DIR -> hivePath("resources"),
    ConfVars.HIVEHISTORYFILELOC -> hivePath("query_logs"),
    ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION -> hivePath("operation_logs"))

  ExternalStoreUtils.registerBuiltinDrivers()

  private def hivePath(name: String): String =
    Paths.get(s"$HIVE_TMPDIR/$name").toAbsolutePath.toString

  /**
   * Create a SnappyHiveExternalCatalog appropriate for the cluster.
   * The catalog internally initializes a hive client that is used to retrieve metadata from
   * the in-built Hive MetaStore.
   */
  def getOrCreateExternalCatalog(sparkContext: SparkContext,
      conf: SparkConf): SnappyHiveExternalCatalog = synchronized {
    val (dbURL, dbDriver) = resolveMetaStoreDBProps(SnappyContext.getClusterMode(sparkContext))
    val metadataConf = new SnappyHiveConf
    // make a copy of SparkConf since it is to be updated later
    val sparkConf = conf.clone()
    var user = sparkConf.getOption(SPARK_STORE_PREFIX + USERNAME_ATTR)
    var password = sparkConf.getOption(SPARK_STORE_PREFIX + PASSWORD_ATTR)
    if (user.isEmpty) {
      user = sparkConf.getOption(STORE_PROPERTY_PREFIX + USERNAME_ATTR)
      password = sparkConf.getOption(STORE_PROPERTY_PREFIX + PASSWORD_ATTR)
    }
    // check store boot properties
    val bootProperties = Misc.getMemStoreBooting.getBootProperties
    if (user.isEmpty) user = Option(bootProperties.get(USERNAME_ATTR).asInstanceOf[String])
    if (password.isEmpty) password = Option(bootProperties.get(PASSWORD_ATTR).asInstanceOf[String])
    var logURL = dbURL
    val secureDbURL = if (user.isDefined && password.isDefined) {
      logURL = dbURL + ";user=" + user.get
      logURL + ";password=" + password.get + ";"
    } else {
      metadataConf.setVar(ConfVars.METASTORE_CONNECTION_USER_NAME,
        SystemProperties.SNAPPY_HIVE_METASTORE)
      dbURL
    }
    if (SnappyHiveExternalCatalog.getInstance eq null) {
      logInfo(s"Using dbURL = $logURL for Hive metastore initialization")
    }
    metadataConf.setVar(ConfVars.METASTORECONNECTURLKEY, secureDbURL)
    metadataConf.setVar(ConfVars.METASTORE_CONNECTION_DRIVER, dbDriver)

    val props = initCommonHiveMetaStoreProperties(metadataConf)

    // set warehouse directory as per Spark's default
    val warehouseDir = sparkConf.get(WAREHOUSE_PATH)
    sparkConf.set(ConfVars.METASTOREWAREHOUSE.varname, warehouseDir)
    metadataConf.setVar(ConfVars.METASTOREWAREHOUSE, warehouseDir)

    // remove all custom hive settings and add defaults needed for access to in-built meta-store
    val hiveSettings = sparkConf.getAll.filter(_._1.startsWith("spark.sql.hive"))
    if (hiveSettings.nonEmpty) hiveSettings.foreach(k => sparkConf.remove(k._1))
    // always use builtin classes with the base class loader without isolation
    sparkConf.set(HiveUtils.HIVE_METASTORE_JARS, "builtin")
    sparkConf.set("spark.sql.hive.metastore.isolation", "false")
    sparkConf.set(HiveUtils.HIVE_METASTORE_SHARED_PREFIXES, Seq(
      "io.snappydata.jdbc", "com.pivotal.gemfirexd.jdbc"))
    for ((hiveVar, dirName) <- HiveClientUtil.HIVE_DEFAULT_SETTINGS) {
      sparkConf.set(hiveVar.varname, dirName)
    }

    val skipFlags = GfxdDataDictionary.SKIP_CATALOG_OPS.get()
    val oldSkipCatalogCalls = skipFlags.skipHiveCatalogCalls
    skipFlags.skipHiveCatalogCalls = true
    try {
      SnappyHiveExternalCatalog.getInstance(sparkConf, metadataConf)
    } finally {
      skipFlags.skipHiveCatalogCalls = oldSkipCatalogCalls
      // clear the system properties set for hive
      val propertyNames = props.stringPropertyNames.iterator()
      while (propertyNames.hasNext) {
        System.clearProperty(propertyNames.next())
      }
    }
  }

  /**
   * Set the common hive metastore properties and also invoke
   * the static initialization for Hive with system properties
   * which tries booting default derby otherwise (SNAP-1956, SNAP-1961).
   * <p>
   * Should be called after all other properties have been filled in.
   */
  private def initCommonHiveMetaStoreProperties(metadataConf: SnappyHiveConf): Properties = {
    metadataConf.set("datanucleus.mapping.Schema", Misc.SNAPPY_HIVE_METASTORE)
    // Tomcat pool has been shown to work best but does not work in split mode
    // because upstream spark does not ship with it (and the one in snappydata-core
    //   cannot be loaded by datanucleus which should be in system CLASSPATH).
    // Using inbuilt DBCP pool which allows setting the max time to wait
    // for a pool connection else BoneCP hangs if network servers are down, for example,
    // and the thrift JDBC connection fails since its default timeout is infinite.
    // The DBCP 1.x versions are thoroughly outdated and should not be used but
    // the expectation is that the one bundled in datanucleus will be in better shape.
    metadataConf.setVar(ConfVars.METASTORE_CONNECTION_POOLING_TYPE, "dbcp-builtin")
    metadataConf.setVar(ConfVars.HADOOPFS, "file:///")
    metadataConf.set("datanucleus.connectionPool.testSQL", "VALUES(1)")

    // ensure no other Hive instance is alive for this thread but also
    // set the system properties because this can initialize Hive static
    // instance that will try to boot default derby otherwise
    val props = metadataConf.getAllProperties
    val propertyNames = props.stringPropertyNames.iterator()
    while (propertyNames.hasNext) {
      val name = propertyNames.next()
      val value = props.getProperty(name)
      if (value ne null) System.setProperty(name, value)
    }

    // set integer properties after the system properties have been used by
    // Hive static initialization so that these never go into system properties

    // a small pool of connections for the shared hive client
    // metadataConf.set("datanucleus.connectionPool.maxPoolSize", "4");
    // metadataConf.set("datanucleus.connectionPool.minPoolSize", "0");
    metadataConf.set("datanucleus.connectionPool.maxActive", "4")
    metadataConf.set("datanucleus.connectionPool.maxIdle", "2")
    metadataConf.set("datanucleus.connectionPool.minIdle", "0")
    // throw pool exhausted exception after 30s
    metadataConf.set("datanucleus.connectionPool.maxWait", "30000")

    props
  }

  private def resolveMetaStoreDBProps(clusterMode: ClusterMode): (String, String) = {
    clusterMode match {
      case ThinClientConnectorMode(_, _) =>
        throw new IllegalStateException("Hive client should not be used in smart connector")
      case _ => (ExternalStoreUtils.defaultStoreURL(clusterMode) + getCommonJDBCSuffix,
          Constant.JDBC_EMBEDDED_DRIVER)
    }
  }

  /**
   * Common connection properties set on embedded metastore JDBC connections.
   * Smart connector no longer uses a hive client rather generic SYS.GET_CATALOG_METADATA
   * and SYS.UPDATE_CATALOG_METADATA procedures for reads and writes to hive meta-store.
   */
  private def getCommonJDBCSuffix: String = {
    ";default-schema=" + SystemProperties.SNAPPY_HIVE_METASTORE +
        ";disable-streaming=true;default-persistent=true;" +
        "sync-commits=true;internal-connection=true;skip-constraint-checks=true"
  }

  def isHiveExecPlan(plan: SparkPlan): Boolean = plan.isInstanceOf[HiveTableScanExec]
}
