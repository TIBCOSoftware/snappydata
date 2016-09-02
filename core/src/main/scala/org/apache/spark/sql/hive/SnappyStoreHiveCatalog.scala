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
import java.net.{URL, URLClassLoader}
import java.util.concurrent.ExecutionException
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.hadoop.fs.FileSystem

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.control.NonFatal

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.util.concurrent.UncheckedExecutionException
import io.snappydata.{Constant, Property}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.hadoop.hive.ql.metadata.{Hive, HiveException}
import org.apache.hadoop.util.VersionInfo

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, NoSuchDatabaseException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.impl.IndexColumnFormatRelation
import org.apache.spark.sql.execution.columnar.{ExternalStoreUtils, JDBCAppendableRelation}
import org.apache.spark.sql.execution.datasources.jdbc.DriverRegistry
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog._
import org.apache.spark.sql.hive.client._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.row.JDBCMutableRelation
import org.apache.spark.sql.sources.{BaseRelation, DependentRelation, JdbcExtendedUtils, ParentRelation}
import org.apache.spark.sql.streaming.StreamPlan
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructType}
/**
 * Catalog using Hive for persistence and adding Snappy extensions like
 * stream/topK tables and returning LogicalPlan to materialize these entities.
 */
class SnappyStoreHiveCatalog(externalCatalog: ExternalCatalog,
    val snappySession: SnappySession,
    functionResourceLoader: FunctionResourceLoader,
    functionRegistry: FunctionRegistry,
    sqlConf: SQLConf,
    hadoopConf: Configuration)
    extends SessionCatalog(
      externalCatalog,
      functionResourceLoader,
      functionRegistry,
      sqlConf,
      hadoopConf) with Logging {

  val sparkConf = snappySession.sparkContext.getConf


  // Overriding SessionCatalog values and methods, this will ensure any catalyst layer access to
  // catalog will hit our catalog rather than the SessionCatalog. Some of the methods might look
  // not needed . @TODO will clean up once we have our own seggregation for SessionCatalog and
  // ExternalCatalog
  // override val tempTables = new ConcurrentHashMap[QualifiedTableName, LogicalPlan]().asScala

  // private val sessionTables = new ConcurrentHashMap[QualifiedTableName, LogicalPlan]().asScala

  override def dropTable(name: TableIdentifier,
      ignoreIfNotExists: Boolean): Unit = synchronized {
    snappySession.dropTable(newQualifiedTableName(name), ignoreIfNotExists)
  }

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


  protected var currentSchema: String = {
    val defaultName = Constant.DEFAULT_SCHEMA
    val defaultDbDefinition =
      CatalogDatabase(defaultName, "app database", sqlConf.warehousePath, Map())
    // Initialize default database if it doesn't already exist
    client.createDatabase(defaultDbDefinition, ignoreIfExists = true)
    client.setCurrentDatabase(defaultName)
    formatDatabaseName(defaultName)
  }

  override def setCurrentDatabase(db: String): Unit = {
    val dbName = formatTableName(db)
    requireDbExists(dbName)
    synchronized {
      currentSchema = dbName
      client.setCurrentDatabase(db)
    }
  }

  /**
    * Format table name, taking into account case sensitivity.
    */
  override def formatTableName(name: String): String = {
    SnappyStoreHiveCatalog.processTableIdentifier(name, sqlConf)
  }

  /**
    * Format database name, taking into account case sensitivity.
    */
  override def formatDatabaseName(name: String): String = {
    SnappyStoreHiveCatalog.processTableIdentifier(name, sqlConf)
  }

  // TODO: SW: cleanup this schema/database stuff
  override def databaseExists(db: String): Boolean = {
    val dbName = formatTableName(db)
    externalCatalog.databaseExists(dbName) ||
        client.getDatabaseOption(dbName).isDefined ||
        currentSchema == dbName || currentSchema == db
  }

  private def requireDbExists(db: String): Unit = {
    if (!databaseExists(db)) {
      throw new NoSuchDatabaseException(db)
    }
  }

  override def getCurrentDatabase: String = synchronized {
    formatTableName(currentSchema)
  }

  def getCurrentSchema: String = currentSchema

  // As long as threads using same IsolatedClientLoader, they should
  // use the cached Hive client and not create a new one. we will be setting the cached client as
  // soon as the ClientWrapper is initialized, so that any thread accessing Hive client after
  // that will get the cached client. This change will be redundant after Spark 2.0 merge. But
  // for the time being it will avoid ThreadLocal access to set SessionState.
  // protected val internalHiveclient = this.client.client


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

      DriverRegistry.register("com.pivotal.gemfirexd.jdbc.EmbeddedDriver")
      DriverRegistry.register("com.pivotal.gemfirexd.jdbc.ClientDriver")

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
    val sc = snappySession.sparkContext
    Property.MetastoreDBURL.getOption(sparkConf) match {
      case Some(url) =>
        val driver = Property.MetastoreDriver.getOption(sparkConf).orNull
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

  /** A cache of Spark SQL data source tables that have been accessed. */
  private val cachedDataSourceTables: LoadingCache[QualifiedTableName,
      LogicalRelation] = {
    val cacheLoader = new CacheLoader[QualifiedTableName, LogicalRelation]() {
      override def load(in: QualifiedTableName): LogicalRelation = {
        logDebug(s"Creating new cached data source for $in")
        val table = in.getTable(client)
        val schemaString = getSchemaString(table.properties)
        val userSpecifiedSchema = schemaString.map(s =>
          DataType.fromJson(s).asInstanceOf[StructType])
        val partitionColumns = table.partitionColumns.map(_.name)
        val provider = table.properties(HIVE_PROVIDER)
        val options = table.storage.serdeProperties

        val relation = options.get(JdbcExtendedUtils.SCHEMA_PROPERTY) match {
          case Some(schema) => JdbcExtendedUtils.externalResolvedDataSource(
            snappySession, schema, provider, SaveMode.Ignore, options)

          case None =>
            // add allowExisting in properties used by some implementations
            DataSource(snappySession, provider, userSpecifiedSchema = userSpecifiedSchema,
              partitionColumns = partitionColumns, options = options +
                  (JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY -> "true")).resolveRelation()
        }

        LogicalRelation(relation)
      }
    }

    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  private var relationDestroyVersion = 0

  def getCachedHiveTable(table: QualifiedTableName): LogicalRelation = {
    val sync = SnappyStoreHiveCatalog.relationDestroyLock.readLock()
    sync.lock()
    try {
      // if a relation has been destroyed (e.g. by another instance of catalog),
      // then the cached ones can be stale, so check and clear entire cache
      val globalVersion = SnappyStoreHiveCatalog.getRelationDestroyVersion
      if (globalVersion != this.relationDestroyVersion) {
        cachedDataSourceTables.invalidateAll()
        this.relationDestroyVersion = globalVersion
      }

      cachedDataSourceTables(table)
    } catch {
      case e@(_: UncheckedExecutionException | _: ExecutionException) =>
        throw e.getCause
    } finally {
      sync.unlock()
    }
  }

  private def registerRelationDestroy(): Unit = {
    val globalVersion = SnappyStoreHiveCatalog.registerRelationDestroy()
    if (globalVersion != this.relationDestroyVersion) {
      cachedDataSourceTables.invalidateAll()
    }
    this.relationDestroyVersion = globalVersion + 1
  }

  def normalizeSchema(schema: StructType): StructType = {
    if (sqlConf.caseSensitiveAnalysis) {
      schema
    } else {
      val fields = schema.fields
      if (fields.exists(f => Utils.hasLowerCase(Utils.fieldName(f)))) {
        StructType(fields.map { f =>
          val name = Utils.toUpperCase(Utils.fieldName(f))
          val metadata = if (f.metadata.contains("name")) {
            val builder = new MetadataBuilder
            builder.withMetadata(f.metadata).putString("name", name).build()
          } else {
            f.metadata
          }
          f.copy(name = name, metadata = metadata)
        })
      } else {
        schema
      }
    }
  }

  def compatibleSchema(schema1: StructType, schema2: StructType): Boolean = {
    schema1.fields.length == schema2.fields.length &&
        !schema1.zip(schema2).exists { case (f1, f2) =>
          !f1.dataType.sameType(f2.dataType)
        }
  }

  def newQualifiedTableName(tableIdent: TableIdentifier): QualifiedTableName = {
    tableIdent match {
      case q: QualifiedTableName => q
      case _ => new QualifiedTableName(formatTableName(
          tableIdent.database.getOrElse(currentSchema)),
          formatTableName(tableIdent.table))
    }
  }

  def newQualifiedTableName(tableIdent: String): QualifiedTableName = {
    val tableName = formatTableName(tableIdent)
    val dotIndex = tableName.indexOf('.')
    if (dotIndex > 0) {
      new QualifiedTableName(tableName.substring(0, dotIndex),
        tableName.substring(dotIndex + 1))
    } else {
      new QualifiedTableName(currentSchema, tableName)
    }
  }

  def newQualifiedTempTableName(tableIdent: String): QualifiedTableName = {
    val tableName = formatTableName(tableIdent)
    val dotIndex = tableName.indexOf('.')
    if (dotIndex > 0) {
      throw new AnalysisException(" temp table name can not have db prefix")
    } else {
      new QualifiedTableName(currentSchema, tableName)
    }
  }

  override def refreshTable(tableIdent: TableIdentifier): Unit = {
    // refreshTable does not eagerly reload the cache. It just invalidates
    // the cache. it is better at here to invalidate the cache to avoid
    // confusing warning logs from the cache loader (e.g. cannot find data
    // source provider, which is only defined for data source table).
    invalidateTable(newQualifiedTableName(tableIdent))
  }

  def invalidateTable(tableIdent: QualifiedTableName): Unit = {
    cachedDataSourceTables.invalidate(tableIdent)
  }

  def unregisterAllTables(): Unit = synchronized {
    tempTables.clear()
  }

  def unregisterTable(tableIdent: QualifiedTableName): Unit = synchronized {
    val tableName = tableIdent.table
    if (tempTables.contains(tableName)) {
      snappySession.truncateTable(tableIdent, ignoreIfUnsupported = true)
      tempTables -= tableName
    }
  }

  final def setSchema(schema: String): Unit = {
    this.currentSchema = schema
  }

  final def lookupRelation(tableIdent: QualifiedTableName): LogicalPlan = {
    tableIdent.getTableOption(client) match {
      case Some(table) =>
        if (table.properties.contains(HIVE_PROVIDER)) {
          getCachedHiveTable(tableIdent)
        } else if (table.tableType == CatalogTableType.VIEW) {
          // @TODO Confirm from Sumedh
          // Difference between VirtualView & View
          val viewText = table.viewText
              .getOrElse(sys.error("Invalid view without text."))
          snappySession.sessionState.sqlParser.parsePlan(viewText)
        } else {
          throw new IllegalStateException(
            s"Unsupported table type ${table.tableType}")
        }

      case None => synchronized {
        tempTables.getOrElse(tableIdent.table,
          throw new TableNotFoundException(s"Table '$tableIdent' not found"))
      }
    }
  }

  override def lookupRelation(tableIdent: TableIdentifier,
      alias: Option[String]): LogicalPlan = {
    // If an alias was specified by the lookup, wrap the plan in a
    // sub-query so that attributes are properly qualified with this alias
    SubqueryAlias(alias.getOrElse(tableIdent.table),
      lookupRelation(newQualifiedTableName(tableIdent)))
  }

  override def tableExists(tableIdentifier: TableIdentifier): Boolean = {
    tableExists(newQualifiedTableName(tableIdentifier))
  }

  def tableExists(tableIdentifier: String): Boolean = {
    tableExists(newQualifiedTableName(tableIdentifier))
  }

  def tableExists(tableName: QualifiedTableName): Boolean = {
    tableName.getTableOption(client).isDefined || synchronized {
      tempTables.contains(tableName.table)
    }
  }

  // TODO: SW: cleanup the tempTables handling to error for schema
  def registerTable(tableName: QualifiedTableName,
      plan: LogicalPlan): Unit = synchronized {
    tempTables += (tableName.table -> plan)
  }

  private def clientDropTable(dbName: String,
      tableName: String): Unit = client.withHiveState {
    client.dropTable(dbName, tableName, ignoreIfNotExists = false)
  }

  /**
   * Drops a data source table from Hive's meta-store.
   */
  def unregisterDataSourceTable(tableIdent: QualifiedTableName,
      relation: Option[BaseRelation]): Unit = {
    // remove from parent relation, if any
    relation.foreach {
      case dep: DependentRelation => dep.baseTable.foreach { t =>
        try {
          lookupRelation(newQualifiedTableName(t)) match {
            case LogicalRelation(p: ParentRelation, _, _) =>
              p.removeDependent(dep, this)
            case _ => // ignore
          }
        } catch {
          case NonFatal(_) => // ignore at this point
        }
      }
      case _ => // nothing for others
    }

    cachedDataSourceTables.invalidate(tableIdent)
    registerRelationDestroy()

    val schemaName = tableIdent.schemaName
    withHiveExceptionHandling(clientDropTable(schemaName, tableIdent.table))
  }

  /**
   * Creates a data source table (a table created with USING clause)
   * in Hive's meta-store.
   */
  def registerDataSourceTable(
      tableIdent: QualifiedTableName,
      userSpecifiedSchema: Option[StructType],
      partitionColumns: Array[String],
      provider: String,
      options: Map[String, String],
      relation: BaseRelation): Unit = {

    // invalidate any cached plan for the table
    cachedDataSourceTables.invalidate(tableIdent)

    val tableProperties = new mutable.HashMap[String, String]
    tableProperties.put(HIVE_PROVIDER, provider)

    // Saves optional user specified schema.  Serialized JSON schema string
    // may be too long to be stored into a single meta-store SerDe property.
    // In this case, we split the JSON string and store each part as a
    // separate SerDe property.
    if (userSpecifiedSchema.isDefined) {
      val threshold = sqlConf.schemaStringLengthThreshold
      val schemaJsonString = userSpecifiedSchema.get.json
      // Split the JSON string.
      val parts = schemaJsonString.grouped(threshold).toSeq
      tableProperties.put(HIVE_SCHEMA_NUMPARTS, parts.size.toString)
      parts.zipWithIndex.foreach { case (part, index) =>
        tableProperties.put(s"$HIVE_SCHEMA_PART.$index", part)
      }
    }

    // get the tableType
    val tableType = getTableType(relation)
    tableProperties.put(JdbcExtendedUtils.TABLETYPE_PROPERTY, tableType.toString)
    // add baseTable property if required
    relation match {
      case dep: DependentRelation => dep.baseTable.foreach { t =>
        lookupRelation(newQualifiedTableName(t)) match {
          case LogicalRelation(p: ParentRelation, _, _) =>
            p.addDependent(dep, this)
          case _ => // ignore
        }
        tableProperties.put(JdbcExtendedUtils.BASETABLE_PROPERTY, t)
      }
      case _ => // ignore baseTable for others
    }

    val schemaName = tableIdent.schemaName
    val dbInHive = client.getDatabaseOption(schemaName)
    dbInHive match {
      case Some(x) => // We are all good
      case None => client.createDatabase(CatalogDatabase(
        schemaName,
        description = schemaName,
        getDefaultDBPath(schemaName),
        Map.empty[String, String]),
        ignoreIfExists = true)
      // Path is empty String for now @TODO for parquet & hadoop relation
      // handle path correctly
    }

    val hiveTable = CatalogTable(
      identifier = tableIdent,
      // Can not inherit from this class. Ideally we should
      // be extending from this case class
      tableType = CatalogTableType.EXTERNAL,
      schema = Nil,
      storage = CatalogStorageFormat(
        locationUri = None,
        inputFormat = None,
        outputFormat = None,
        serde = None,
        compressed = false,
        serdeProperties = options
      ),
      properties = tableProperties.toMap)

    withHiveExceptionHandling(client.createTable(hiveTable, ignoreIfExists = true))
  }

  private def addIndexProp(inTable: QualifiedTableName,
      index: QualifiedTableName): Unit = {
    val hiveTable = inTable.getTable(client)
    var indexes = ""
    try {
      indexes = hiveTable.properties(ExternalStoreUtils.INDEX_NAME) + ","
    } catch {
      case e: scala.NoSuchElementException =>
    }

    client.alterTable(
      hiveTable.copy(properties = hiveTable.properties +
          (ExternalStoreUtils.INDEX_NAME -> (indexes + index.toString())))
    )
  }

  def withHiveExceptionHandling[T](function: => T): T = {
    try {
      function
    } catch {
      case he: HiveException if isDisconnectException(he) =>
        // stale GemXD connection
        Hive.closeCurrent()
        client = newClient()
        function
    }
  }

  def alterTableToAddIndexProp(inTable: QualifiedTableName,
      index: QualifiedTableName): Unit = {
    alterTableLock.synchronized {
      withHiveExceptionHandling(addIndexProp(inTable, index))
    }
    cachedDataSourceTables.invalidate(inTable)
  }

  def removeIndexProp(inTable: QualifiedTableName,
      index: QualifiedTableName): Unit = {
    val hiveTable = inTable.getTable(client)
    val indexes = hiveTable.properties(ExternalStoreUtils.INDEX_NAME)
    val indexArray = indexes.split(",")
    // indexes are stored in lower case
    val newindexes = indexArray.filter(_ != index.toString()).mkString(",")
    if (newindexes.isEmpty) {
      client.alterTable(
        hiveTable.copy(
          properties = hiveTable.properties - ExternalStoreUtils.INDEX_NAME)
      )
    } else {
      client.alterTable(
        hiveTable.copy(properties = hiveTable.properties +
            (ExternalStoreUtils.INDEX_NAME -> newindexes))
      )
    }
  }

  def alterTableToRemoveIndexProp(inTable: QualifiedTableName,
      index: QualifiedTableName): Unit = {
    alterTableLock.synchronized {
      withHiveExceptionHandling(removeIndexProp(inTable, index))
    }
    cachedDataSourceTables.invalidate(inTable)
  }

  private def isDisconnectException(t: Throwable): Boolean = {
    if (t != null) {
      val tClass = t.getClass.getName
      tClass.contains("DisconnectedException") ||
          tClass.contains("DisconnectException") ||
          isDisconnectException(t.getCause)
    } else {
      false
    }
  }

  def getTables(db: Option[String]): Seq[(String, Boolean)] = {
    val client = this.client
    val schemaName = db.map(formatTableName)
        .getOrElse(currentSchema)
    synchronized(tempTables.collect {
      case (tableIdent, _) if db.isEmpty || currentSchema == schemaName =>
        (tableIdent, true)
    }).toSeq ++
        (if (db.isEmpty) allTables() else client.listTables(schemaName)).map { t =>
          if (db.isDefined) {
            (schemaName + '.' + formatTableName(t), false)
          } else {
            (formatTableName(t), false)
          }
        }
  }

  def getDataSourceTables(tableTypes: Seq[ExternalTableType],
      baseTable: Option[String] = None): Seq[QualifiedTableName] = {
    val client = this.client
    val tables = new ArrayBuffer[QualifiedTableName](4)
    allTables().foreach { t =>
      val tableIdent = newQualifiedTableName(formatTableName(t))
      val table = tableIdent.getTable(client)
      if (tableTypes.isEmpty || table.properties.get(JdbcExtendedUtils
          .TABLETYPE_PROPERTY).exists(tableType => tableTypes.exists(_
          .toString == tableType))) {
        if (baseTable.isEmpty || table.properties.get(
          JdbcExtendedUtils.BASETABLE_PROPERTY).contains(baseTable.get)) {
          tables += tableIdent
        }
      }
    }
    tables
  }

  private def allTables(): Seq[String] = {
    val allTables = new ArrayBuffer[String]()
    val currentSchemaName = this.currentSchema
    var hasCurrentDb = false
    val client = this.client
    val databases = client.listDatabases("*").iterator
    while (databases.hasNext) {
      val db = databases.next()
      if (!hasCurrentDb && db == currentSchemaName) {
        allTables ++= client.listTables(db)
        hasCurrentDb = true
      } else {
        allTables ++= client.listTables(db).map(db + '.' + _)
      }
    }
    if (!hasCurrentDb) {
      allTables ++= client.listTables(currentSchemaName)
    }
    allTables
  }

  def getDataSourceRelations[T](tableTypes: Seq[ExternalTableType],
      baseTable: Option[String] = None): Seq[T] = {
    getDataSourceTables(tableTypes, baseTable).map(
      getCachedHiveTable(_).relation.asInstanceOf[T])
  }

  def getTableType(relation: BaseRelation): ExternalTableType = {
    relation match {
      case x: JDBCMutableRelation => ExternalTableType.Row
      case x: IndexColumnFormatRelation => ExternalTableType.Index
      case x: JDBCAppendableRelation => ExternalTableType.Column
      case x: StreamPlan => ExternalTableType.Stream
      case _ => ExternalTableType.Row
    }
  }

}

object SnappyStoreHiveCatalog {

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

  def processTableIdentifier(tableIdentifier: String, conf: SQLConf): String = {
    if (conf.caseSensitiveAnalysis) {
      tableIdentifier
    } else {
      Utils.toUpperCase(tableIdentifier)
    }
  }

  private[this] var relationDestroyVersion = 0
  private val relationDestroyLock = new ReentrantReadWriteLock()
  private val alterTableLock = new Object

  private[sql] def getRelationDestroyVersion: Int = relationDestroyVersion

  private[sql] def registerRelationDestroy(): Int = {
    val sync = relationDestroyLock.writeLock()
    sync.lock()
    try {
      val globalVersion = relationDestroyVersion
      relationDestroyVersion += 1
      globalVersion
    } finally {
      sync.unlock()
    }
  }

  private def getSchemaString(
      tableProps: scala.collection.Map[String, String]): Option[String] = {
    tableProps.get(HIVE_SCHEMA_NUMPARTS).map { numParts =>
      (0 until numParts.toInt).map { index =>
        val partProp = s"$HIVE_SCHEMA_PART.$index"
        tableProps.get(partProp) match {
          case Some(part) => part
          case None => throw new AnalysisException("Could not read " +
              "schema from metastore because it is corrupted (missing " +
              s"part $index of the schema, $numParts parts expected).")
        }
        // Stick all parts back to a single schema string.
      }.mkString
    }
  }

  def getSchemaStringFromHiveTable(table: Table): String =
    getSchemaString(table.getParameters.asScala).orNull

  def closeCurrent(): Unit = {
    Hive.closeCurrent()
  }
}

/** A fully qualified identifier for a table (i.e. [schema.]tableName) */
final class QualifiedTableName(val schemaName: String, _tableIdent: String)
    extends TableIdentifier(_tableIdent, Some(schemaName)) {

  @transient private[this] var _table: Option[CatalogTable] = None

  def getTableOption(client: HiveClient): Option[CatalogTable] = _table.orElse {
    _table = client.getTableOption(schemaName, table)
    _table
  }

  def getTable(client: HiveClient): CatalogTable =
    getTableOption(client).getOrElse(throw new TableNotFoundException(
      s"Table '$schemaName.$table' not found"))

  override def toString(): String = schemaName + '.' + table
}

case class ExternalTableType(name: String)

object ExternalTableType {
  val Row = ExternalTableType("ROW")
  val Column = ExternalTableType("COLUMN")
  val Index = ExternalTableType("INDEX")
  val Stream = ExternalTableType("STREAM")
  val Sample = ExternalTableType("SAMPLE")
  val TopK = ExternalTableType("TOPK")
}
