/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
import java.net.{URLClassLoader, URL}

import scala.collection.JavaConverters._

import io.snappydata.{Constant, Property}
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.hadoop.util.VersionInfo

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.{ExternalClusterMode, SplitClusterMode, LocalMode, ExternalEmbeddedMode, SnappyEmbeddedMode, SnappyContext, SQLContext}
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.hive.client.{IsolatedClientLoader, HiveClient}
import org.apache.spark.sql.internal.SQLConf

/**
 * A utility class to get hive connection to underlying metastore.
 * A lot of code is similar to org.apache.spark.sql.hive.HiveUtils.
 * Difference being we take a connection to underlying GemXD store.
 * TODO We need to investigate if we can phase out this class and use HiveUtils directly.
 */
class HiveClientUtil(val sparkContext: SparkContext) extends Logging {

  /** The version of hive used internally by Spark SQL. */
  val hiveExecutionVersion = HiveUtils.hiveExecutionVersion

  val HIVE_METASTORE_VERSION = HiveUtils.HIVE_METASTORE_VERSION
  val HIVE_METASTORE_JARS = HiveUtils.HIVE_METASTORE_JARS
  val HIVE_METASTORE_SHARED_PREFIXES =
    HiveUtils.HIVE_METASTORE_SHARED_PREFIXES
  val HIVE_METASTORE_BARRIER_PREFIXES =
    HiveUtils.HIVE_METASTORE_BARRIER_PREFIXES

  val HIVE_PROVIDER = "spark.sql.sources.provider"
  val HIVE_SCHEMA_NUMPARTS = "spark.sql.sources.schema.numParts"
  val HIVE_SCHEMA_PART = "spark.sql.sources.schema.part"
  val HIVE_METASTORE = "SNAPPY_HIVE_METASTORE"

  val sparkConf = sparkContext.conf

  val sqlConf = new SQLConf
  sqlConf.setConf(SQLContext.getSQLProperties(sparkConf))

  val hadoopConf = sparkContext.hadoopConfiguration


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
  @transient
  protected[sql] var client: HiveClient = newClient()


  def closeCurrent(): Unit = {
    Hive.closeCurrent()
  }

  private def newClient(): HiveClient = synchronized {

    val metaVersion = IsolatedClientLoader.hiveVersion(hiveMetastoreVersion)
    // We instantiate a HiveConf here to read in the hive-site.xml file and
    // then pass the options into the isolated client loader
    val metadataConf = new HiveConf()
    var warehouse = metadataConf.get(
      HiveConf.ConfVars.METASTOREWAREHOUSE.varname)
    if (warehouse == null || warehouse.isEmpty ||
        warehouse == HiveConf.ConfVars.METASTOREWAREHOUSE.getDefaultExpr) {
      // append warehouse to current directory
      warehouse = new java.io.File("./warehouse").getCanonicalPath
      metadataConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, warehouse)
    }
    logInfo("Default warehouse location is " + warehouse)
    metadataConf.setVar(HiveConf.ConfVars.HADOOPFS, "file:///")

    val (useSnappyStore, dbURL, dbDriver) = resolveMetaStoreDBProps()
    if (useSnappyStore) {
      logInfo(s"Using SnappyStore as metastore database, dbURL = $dbURL")
      metadataConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, dbURL)
      metadataConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER,
        dbDriver)
      metadataConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME,
        HIVE_METASTORE)
    } else if (dbURL != null) {
      logInfo(s"Using specified metastore database, dbURL = $dbURL")
      metadataConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, dbURL)
      if (dbDriver != null) {
        metadataConf.setVar(HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER,
          dbDriver)
      } else {
        metadataConf.unset(
          HiveConf.ConfVars.METASTORE_CONNECTION_DRIVER.varname)
      }
      metadataConf.unset(
        HiveConf.ConfVars.METASTORE_CONNECTION_USER_NAME.varname)
    } else {
      logInfo("Using Hive metastore database, dbURL = " +
          metadataConf.getVar(HiveConf.ConfVars.METASTORECONNECTURLKEY))
    }
    metadataConf.setVar(HiveConf.ConfVars.METASTORE_EVENT_LISTENERS,
      "org.apache.spark.sql.hive.SnappyHiveMetaStoreEventListener")

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

      DriverRegistry.register("io.snappydata.jdbc.EmbeddedDriver")
      DriverRegistry.register("io.snappydata.jdbc.ClientDriver")

      // set as system properties for default HiveConf's
      val props = metadataConf.getAllProperties
      val propNames = props.stringPropertyNames().iterator()
      while (propNames.hasNext) {
        val name = propNames.next()
        System.setProperty(name, props.getProperty(name))
      }

      closeCurrent() // Just to ensure no other HiveDB is alive for this thread.

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

  private def resolveMetaStoreDBProps(): (Boolean, String, String) = {
    val sc = sparkContext
    Property.MetaStoreDBURL.getOption(sparkConf) match {
      case Some(url) =>
        val driver = Property.MetaStoreDriver.getOption(sparkConf).orNull
        (false, url, driver)
      case None => SnappyContext.getClusterMode(sc) match {
        case SnappyEmbeddedMode(_, _) | ExternalEmbeddedMode(_, _) |
             LocalMode(_, _) =>
          (true, ExternalStoreUtils.defaultStoreURL(sc) +
              ";disable-streaming=true;default-persistent=true",
              Constant.JDBC_EMBEDDED_DRIVER)
        case SplitClusterMode(_, props) =>
          (true, Constant.DEFAULT_EMBEDDED_URL +
              ";host-data=false;disable-streaming=true;default-persistent=true;" +
              props, Constant.JDBC_EMBEDDED_DRIVER)
        case ExternalClusterMode(_, _) =>
          (false, null, null)
      }
    }
  }
}
