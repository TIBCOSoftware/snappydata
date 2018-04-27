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
package org.apache.spark.sql.hive

import java.io.File
import java.net.{URL, URLClassLoader}
import java.util.Properties

import scala.collection.JavaConverters._
import com.gemstone.gemfire.internal.shared.{ClientSharedUtils, SystemProperties}
import com.pivotal.gemfirexd.Attribute.{PASSWORD_ATTR, USERNAME_ATTR}
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.Constant
import io.snappydata.Constant.{SPARK_STORE_PREFIX, STORE_PROPERTY_PREFIX}
import io.snappydata.impl.SnappyHiveCatalog
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.util.VersionInfo
import org.apache.log4j.LogManager
import org.apache.spark.sql._
import org.apache.spark.sql.collection.ToolsCallbackInit
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.hive.client.{HiveClient, IsolatedClientLoader}
import org.apache.spark.sql.hive.execution.HiveTableScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.{Logging, SparkContext}

/**
 * A utility class to get hive connection to underlying metastore.
 * A lot of code is similar to org.apache.spark.sql.hive.HiveUtils.
 * Difference being we take a connection to underlying GemXD store.
 * TODO We need to investigate if we can phase out this class and use HiveUtils directly.
 */
private class HiveClientUtil(sparkContext: SparkContext) extends Logging {

  /** The version of hive used internally by Spark SQL. */
  private val hiveExecutionVersion = HiveUtils.hiveExecutionVersion

  val HIVE_METASTORE_VERSION = HiveUtils.HIVE_METASTORE_VERSION
  val HIVE_METASTORE_JARS = HiveUtils.HIVE_METASTORE_JARS
  val HIVE_METASTORE_SHARED_PREFIXES =
    HiveUtils.HIVE_METASTORE_SHARED_PREFIXES
  val HIVE_METASTORE_BARRIER_PREFIXES =
    HiveUtils.HIVE_METASTORE_BARRIER_PREFIXES

  private val sparkConf = sparkContext.conf

  val sqlConf = new SQLConf
  sqlConf.setConf(SQLContext.getSQLProperties(sparkConf))

  private val hadoopConf = sparkContext.hadoopConfiguration

  /**
   * The version of the hive client that will be used to communicate
   * with the meta-store forL catalog.
   */
  protected[sql] val hiveMetastoreVersion: String =
    sqlConf.getConf(HIVE_METASTORE_VERSION, hiveExecutionVersion)

  /**
   * The location of the jars that should be used to instantiate the Hive
   * meta-store client.  This property can be one of three options:
   *
   * a classpath in the standard format for both hive and hadoop.
   *
   * builtin - attempt to discover the jars that were used to load Spark SQL
   * and use those. This option is only valid when using the
   * execution version of Hive.
   *
   * maven - download the correct version of hive on demand from maven.
   */
  protected[sql] def hiveMetastoreJars(): String =
    sqlConf.getConf(HIVE_METASTORE_JARS)

  /**
   * A comma separated list of class prefixes that should be loaded using the
   * ClassLoader that is shared between Spark SQL and a specific version of
   * Hive. An example of classes that should be shared is JDBC drivers that
   * are needed to talk to the meta-store. Other classes that need to be
   * shared are those that interact with classes that are already shared.
   * For example, custom appender used by log4j.
   */
  protected[sql] def hiveMetastoreSharedPrefixes(): Seq[String] =
    sqlConf.getConf(HIVE_METASTORE_SHARED_PREFIXES, snappyPrefixes())
        .filterNot(_ == "")

  /**
   * Add any other classes which has already been loaded by base loader. As Hive Clients creates
   * another class loader to load classes , it sometimes can give incorrect behaviour
   */
  private def snappyPrefixes() = Seq("com.pivotal.gemfirexd", "com.mysql.jdbc",
    "org.postgresql", "com.microsoft.sqlserver", "oracle.jdbc", "com.mapr")

  /**
   * A comma separated list of class prefixes that should explicitly be
   * reloaded for each version of Hive that Spark SQL is communicating with.
   * For example, Hive UDFs that are declared in a prefix that typically
   * would be shared (i.e. org.apache.spark.*)
   */
  protected[sql] def hiveMetastoreBarrierPrefixes(): Seq[String] =
    sqlConf.getConf(HIVE_METASTORE_BARRIER_PREFIXES).filterNot(_ == "")

  /**
   * Overridden by child classes that need to set configuration before
   * client init (but after hive-site.xml).
   */
  protected def configure(): Map[String, String] = Map.empty

  /**
   * Hive client that is used to retrieve metadata from the Hive MetaStore.
   * The version of the Hive client that is used here must match the
   * meta-store that is configured in the hive-site.xml file.
   */
  private def newClientWithLogSetting(): HiveClient = {
    val currentLevel = ClientSharedUtils.convertToJavaLogLevel(
      LogManager.getRootLogger.getLevel)
    try {
      ifSmartConn(() => {
        val props = new Properties()
        props.setProperty("log4j.logger.DataNucleus.Datastore", "ERROR")
        props.setProperty("log4j.logger.DataNucleus.Query", "ERROR")
        ClientSharedUtils.initLog4J(null, props, currentLevel)
      })
      // wait for store hive client to initialize first
      val store = Misc.getMemStoreBootingNoThrow
      if (store ne null) {
        val storeCatalog = store.getExternalCatalog
        if (storeCatalog ne null) {
          // explicit wait though it should already be done by the getter above
          storeCatalog.waitForInitialization()
        }
      }
      val hc = newClient()
      // Perform some action to hit other paths that could throw warning messages.
      ifSmartConn(() => {hc.getTableOption(SystemProperties.SNAPPY_HIVE_METASTORE, "DBS")})
      hc
    } finally { // reset log config
      ifSmartConn(() => {
        ClientSharedUtils.initLog4J(null, currentLevel)
      })
    }
  }

  private def ifSmartConn(func: () => Unit): Unit = {
    SnappyContext.getClusterMode(sparkContext) match {
      case ThinClientConnectorMode(_, _) => func()
      case _ =>
    }
  }

  private def newClient(): HiveClient = SnappyHiveCatalog.hiveClientSync.synchronized {

    val metaVersion = IsolatedClientLoader.hiveVersion(hiveMetastoreVersion)
    // We instantiate a HiveConf here to read in the hive-site.xml file and
    // then pass the options into the isolated client loader
    val metadataConf = new HiveConf()
    SnappyContext.getClusterMode(sparkContext) match {
      case _: ThinClientConnectorMode =>
        metadataConf.setBoolVar(HiveConf.ConfVars.METASTORE_AUTO_CREATE_SCHEMA, false)
        metadataConf.set("datanucleus.generateSchema.database.mode", "none")
      case _ =>
    }

    val (dbURL, dbDriver) = resolveMetaStoreDBProps()
    var user = sparkConf.getOption(SPARK_STORE_PREFIX + USERNAME_ATTR)
    var password = sparkConf.getOption(SPARK_STORE_PREFIX + PASSWORD_ATTR)
    if (user.isEmpty && password.isEmpty) {
      user = sparkConf.getOption(STORE_PROPERTY_PREFIX + USERNAME_ATTR)
      password = sparkConf.getOption(STORE_PROPERTY_PREFIX + PASSWORD_ATTR)
    }
    var logURL = dbURL
    val secureDbURL = if (user.isDefined && password.isDefined) {
      logURL = dbURL + ";default-schema=" + SystemProperties.SNAPPY_HIVE_METASTORE +
          ";user=" + user.get
      logURL + ";password=" + password.get + ";"
    } else {
      metadataConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME,
        SystemProperties.SNAPPY_HIVE_METASTORE)
      dbURL
    }
    logInfo(s"Using dbURL = $logURL for Hive metastore initialization")
    metadataConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, secureDbURL)
    metadataConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER, dbDriver)

    val warehouse = SnappyHiveCatalog.initCommonHiveMetaStoreProperties(metadataConf)
    logInfo("Default warehouse location is " + warehouse)

    val allConfig = metadataConf.asScala.map(e =>
      e.getKey -> e.getValue).toMap ++ configure

    val hiveMetastoreJars = this.hiveMetastoreJars()
    if (hiveMetastoreJars == "builtin") {
      if (hiveExecutionVersion != hiveMetastoreVersion) {
        throw new IllegalArgumentException("Builtin jars can only be used " +
            "when hive default version == hive metastore version. Execution: " +
            s"$hiveExecutionVersion != Metastore: $hiveMetastoreVersion. " +
            "Specify a vaild path to the correct hive jars using " +
            s"$HIVE_METASTORE_JARS or change " +
            s"$HIVE_METASTORE_VERSION to $hiveExecutionVersion.")
      }

      // We recursively find all jars in the class loader chain,
      // starting from the given classLoader.
      def allJars(classLoader: ClassLoader): Array[URL] = classLoader match {
        case null => Array.empty[URL]
        case urlClassLoader: URLClassLoader =>
          urlClassLoader.getURLs ++ allJars(urlClassLoader.getParent)
        case other => allJars(other.getParent)
      }

      val classLoader = org.apache.spark.util.Utils.getContextOrSparkClassLoader
      val jars = allJars(classLoader)
      if (jars.length == 0) {
        throw new IllegalArgumentException(
          "Unable to locate hive jars to connect to metastore. " +
              "Please set spark.sql.hive.metastore.jars.")
      }

      logInfo("Initializing HiveMetastoreConnection version " +
          s"$hiveMetastoreVersion using Spark classes.")
      // new ClientWrapper(metaVersion, allConfig, classLoader)
      new IsolatedClientLoader(
        version = metaVersion,
        sparkConf = sparkConf,
        hadoopConf = hadoopConf,
        execJars = jars.toSeq,
        config = allConfig,
        isolationOn = false,
        barrierPrefixes = hiveMetastoreBarrierPrefixes(),
        sharedPrefixes = hiveMetastoreSharedPrefixes()).createClient()
    } else if (hiveMetastoreJars == "maven") {
      logInfo("Initializing HiveMetastoreConnection version " +
          s"$hiveMetastoreVersion using maven.")

      IsolatedClientLoader.forVersion(
        hiveMetastoreVersion,
        hadoopVersion = VersionInfo.getVersion,
        sparkConf = sparkConf,
        hadoopConf = hadoopConf,
        config = allConfig,
        barrierPrefixes = hiveMetastoreBarrierPrefixes(),
        sharedPrefixes = hiveMetastoreSharedPrefixes()).createClient()
    } else {
      // Convert to files and expand any directories.
      val jars = hiveMetastoreJars.split(File.pathSeparator).flatMap {
        case path if new File(path).getName == "*" =>
          val files = new File(path).getParentFile.listFiles()
          if (files == null) {
            logWarning(s"Hive jar path '$path' does not exist.")
            Nil
          } else {
            files.filter(_.getName.toLowerCase.endsWith(".jar"))
          }
        case path =>
          new File(path) :: Nil
      }.map(_.toURI.toURL)

      logInfo("Initializing HiveMetastoreConnection version " +
          s"$hiveMetastoreVersion using $jars")
      new IsolatedClientLoader(
        version = metaVersion,
        sparkConf = sparkConf,
        hadoopConf = hadoopConf,
        execJars = jars.toSeq,
        config = allConfig,
        isolationOn = true,
        barrierPrefixes = hiveMetastoreBarrierPrefixes(),
        sharedPrefixes = hiveMetastoreSharedPrefixes()).createClient()
    }
  }

  private def resolveMetaStoreDBProps(): (String, String) = {
    SnappyContext.getClusterMode(sparkContext) match {
      case SnappyEmbeddedMode(_, _) | LocalMode(_, _) =>
        (ExternalStoreUtils.defaultStoreURL(Some(sparkContext)) +
            SnappyHiveCatalog.getCommonJDBCSuffix + ";skip-constraint-checks=true",
            Constant.JDBC_EMBEDDED_DRIVER)
      case ThinClientConnectorMode(_, url) =>
        (url + ";route-query=false;skip-constraint-checks=true;", Constant.JDBC_CLIENT_DRIVER)
    }
  }
}

object HiveClientUtil {

  ExternalStoreUtils.registerBuiltinDrivers()

  def newClient(sparkContext: SparkContext): HiveClient = synchronized {
    val client = new HiveClientUtil(sparkContext).newClientWithLogSetting()
    // replay global sql commands
    val deployCmds = ToolsCallbackInit.toolsCallback.getAllGlobalCmnds()
    // logInfo(s"deploycmnds size = ${deployCmds.size}")
    // deployCmds.foreach(s => logDebug(s"s"))
    deployCmds.foreach(d => {
      val cmdFields = d.split('|')
      val coordinate = cmdFields(0)
      val repos = if (cmdFields(1).isEmpty) None else Some(cmdFields(1))
      val cache = if (cmdFields(2).isEmpty) None else Some(cmdFields(2))
      val session = SparkSession.builder().getOrCreate()
      DeployCommand(coordinate, null, repos, cache, false).run(session)
    })
    client
  }

  def isHiveExecPlan(plan: SparkPlan): Boolean = plan.isInstanceOf[HiveTableScanExec]
}
