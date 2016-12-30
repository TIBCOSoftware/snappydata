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

import java.util.concurrent.ExecutionException
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.language.implicitConversions
import scala.util.control.NonFatal

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.util.concurrent.UncheckedExecutionException
import io.snappydata.Constant
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.metastore.api.Table
import org.apache.hadoop.hive.ql.metadata.{Hive, HiveException}

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, NoSuchDatabaseException}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.impl.IndexColumnFormatRelation
import org.apache.spark.sql.execution.columnar.{ExternalStoreUtils, JDBCAppendableRelation}
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog._
import org.apache.spark.sql.hive.client._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.row.JDBCMutableRelation
import org.apache.spark.sql.sources.{BaseRelation, DependencyCatalog, DependentRelation, JdbcExtendedUtils, ParentRelation}
import org.apache.spark.sql.streaming.{StreamBaseRelation, StreamPlan}
import org.apache.spark.sql.types.{StringType, DataType, MetadataBuilder, StructType}

/**
 * Catalog using Hive for persistence and adding Snappy extensions like
 * stream/topK tables and returning LogicalPlan to materialize these entities.
 */
class SnappyStoreHiveCatalog(externalCatalog: SnappyExternalCatalog,
    val snappySession: SnappySession,
    metadataHive: HiveClient,
    functionResourceLoader: FunctionResourceLoader,
    functionRegistry: FunctionRegistry,
    sqlConf: SQLConf,
    hadoopConf: Configuration)
    extends SessionCatalog(
      externalCatalog,
      functionResourceLoader,
      functionRegistry,
      sqlConf,
      hadoopConf) {

  val sparkConf = snappySession.sparkContext.getConf

  private[sql] var client = metadataHive


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


  /** A cache of Spark SQL data source tables that have been accessed. */
  protected val cachedDataSourceTables: LoadingCache[QualifiedTableName,
      (LogicalRelation, CatalogTable)] = {
    val cacheLoader = new CacheLoader[QualifiedTableName,
        (LogicalRelation, CatalogTable)]() {
      override def load(in: QualifiedTableName): (LogicalRelation, CatalogTable) = {
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
            snappySession, schema, userSpecifiedSchema.get, provider, SaveMode.Ignore, options)

          case None =>
            // add allowExisting in properties used by some implementations
            DataSource(snappySession, provider, userSpecifiedSchema = userSpecifiedSchema,
              partitionColumns = partitionColumns, options = options +
                  (JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY -> "true")).resolveRelation()
        }
        relation match {
          case sr: StreamBaseRelation => // Do Nothing as it is not supported for stream relation
          case pr: ParentRelation =>
            var dependentRelations: Array[String] = Array()
            if (table.properties.get(ExternalStoreUtils.DEPENDENT_RELATIONS).isDefined) {
              dependentRelations = table.properties(ExternalStoreUtils.DEPENDENT_RELATIONS)
                  .split(",")
            }

            dependentRelations.foreach(rel => {
              DependencyCatalog.addDependent(in.toString, rel)
            })
          case _ => // Do nothing
        }

        (LogicalRelation(relation), table)
      }
    }

    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  val cachedSampleTables: LoadingCache[QualifiedTableName,
      Seq[(LogicalPlan, String)]] = createCachedSampleTables

  protected def createCachedSampleTables =
    SnappyStoreHiveCatalog.cachedSampleTables

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

      cachedDataSourceTables(table)._1
    } catch {
      case e@(_: UncheckedExecutionException | _: ExecutionException) =>
        throw e.getCause
    } finally {
      sync.unlock()
    }
  }

  def getCachedHiveTableProperties(table: QualifiedTableName): Map[String, String] = {
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

      cachedDataSourceTables(table)._2.properties
    } catch {
      case e@(_: UncheckedExecutionException | _: ExecutionException) =>
        throw e.getCause
    } finally {
      sync.unlock()
    }
  }

  def getCachedSampledRelations(table: QualifiedTableName): Seq[(LogicalPlan, String)] = {
    val sync = SnappyStoreHiveCatalog.relationDestroyLock.readLock()
    sync.lock()
    try {
      // if a relation has been destroyed (e.g. by another instance of catalog),
      // then the cached ones can be stale, so check and clear entire cache
      val globalVersion = SnappyStoreHiveCatalog.getRelationDestroyVersion
      if (globalVersion != this.relationDestroyVersion) {
        cachedSampleTables.invalidateAll()
        this.relationDestroyVersion = globalVersion
      }
      cachedSampleTables(table)
    } catch {
      case e@(_: UncheckedExecutionException | _: ExecutionException) =>
        throw e.getCause
    } finally {
      sync.unlock()
    }
  }

  def getCachedCatalogTable(table: QualifiedTableName): Option[CatalogTable] = {
    val sync = SnappyStoreHiveCatalog.relationDestroyLock.readLock()
    sync.lock()
    try {
      val globalVersion = SnappyStoreHiveCatalog.getRelationDestroyVersion
      if (globalVersion == this.relationDestroyVersion) {
        val cachedTable = cachedDataSourceTables.getIfPresent(table)
        if (cachedTable != null) Some(cachedTable._2) else None
      } else {
        // if a relation has been destroyed (e.g. by another instance of
        // catalog), then the cached ones can be stale so invalidate the cache
        cachedDataSourceTables.invalidateAll()
        this.relationDestroyVersion = globalVersion
        None
      }
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
            f.dataType match {
              case s: StringType =>
                if (!f.metadata.contains(Constant.CHAR_TYPE_BASE_PROP)) {
                  val builder = new MetadataBuilder
                  builder.withMetadata(f.metadata).putString(Constant.CHAR_TYPE_BASE_PROP,
                    "STRING").build()
                } else if (f.metadata.getString(Constant.CHAR_TYPE_BASE_PROP)
                    .equalsIgnoreCase("CLOB")) {
                  // Remove the CharType properties from metadata
                  val builder = new MetadataBuilder
                  builder.withMetadata(f.metadata).remove(Constant.CHAR_TYPE_BASE_PROP)
                      .remove(Constant.CHAR_TYPE_SIZE_PROP).build()
                } else {
                  f.metadata
                }
              case _ => f.metadata
            }
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
    tableIdent.invalidate()
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

  /**
   * Return whether a table with the specified name is a temporary table.
   */
  def isTemporaryTable(tableIdent: QualifiedTableName): Boolean = synchronized {
    tempTables.contains(tableIdent.table)
  }

  final def lookupRelation(tableIdent: QualifiedTableName): LogicalPlan = {
    tableIdent.getTableOption(this) match {
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
          throw new TableNotFoundException(s"Table '$tableIdent' not found")) match {
          case lr: LogicalRelation =>
            lr.copy(metastoreTableIdentifier = Some(tableIdent))
          case x => x
        }
      }
    }
  }

  final def lookupRelationOption(tableIdent: QualifiedTableName): Option[LogicalPlan] = {
    tableIdent.getTableOption(this) match {
      case Some(table) =>
        if (table.properties.contains(HIVE_PROVIDER)) {
          Some(getCachedHiveTable(tableIdent))
        } else if (table.tableType == CatalogTableType.VIEW) {
          // @TODO Confirm from Sumedh
          // Difference between VirtualView & View
          val viewText = table.viewText
              .getOrElse(sys.error("Invalid view without text."))
          Some(snappySession.sessionState.sqlParser.parsePlan(viewText))
        } else {
          None
        }

      case None => synchronized {
        tempTables.get(tableIdent.table).orElse(None)
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
    tableName.getTableOption(this).isDefined || synchronized {
      tempTables.contains(tableName.table)
    }
  }

  // TODO: SW: cleanup the tempTables handling to error for schema
  def registerTable(tableName: QualifiedTableName,
      plan: LogicalPlan): Unit = synchronized {
    tempTables += (tableName.table -> plan)
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
              removeDependentRelation(newQualifiedTableName(t),
                newQualifiedTableName(dep.name))
            case _ => // ignore
          }
        } catch {
          case NonFatal(_) => // ignore at this point
        }
      }
      case _ => // nothing for others
    }

    tableIdent.invalidate()
    cachedDataSourceTables.invalidate(tableIdent)

    registerRelationDestroy()

    val schemaName = tableIdent.schemaName
    withHiveExceptionHandling(externalCatalog.dropTable(schemaName,
      tableIdent.table, ignoreIfNotExists = false))
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
    tableIdent.invalidate()
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
            addDependentRelation(newQualifiedTableName(t),
              newQualifiedTableName(dep.name))
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
    SnappySession.clearAllCache()
  }


  def withHiveExceptionHandling[T](function: => T): T = {
    try {
      function
    } catch {
      case he: HiveException if isDisconnectException(he) =>
        // stale JDBC connection
        Hive.closeCurrent()
        client = externalCatalog.client.newSession()
        function
    }
  }

  def removeDependentRelationFromHive(table: QualifiedTableName,
      dependentRelation: QualifiedTableName): Unit = {
    val hiveTable = table.getTable(this)
    if (hiveTable.properties.contains(ExternalStoreUtils.DEPENDENT_RELATIONS)) {
      val dependentRelations = hiveTable.properties(ExternalStoreUtils.DEPENDENT_RELATIONS)
      val relationsArray = dependentRelations.split(",")
      val newindexes = relationsArray.filter(_ != dependentRelation.toString()).mkString(",")
      if (newindexes.isEmpty) {
        client.alterTable(
          hiveTable.copy(
            properties = hiveTable.properties - ExternalStoreUtils.DEPENDENT_RELATIONS)
        )
      } else {
        client.alterTable(
          hiveTable.copy(properties = hiveTable.properties +
              (ExternalStoreUtils.DEPENDENT_RELATIONS -> newindexes))
        )
      }
    }
  }

  def removeDependentRelation(table: QualifiedTableName,
      dependentRelation: QualifiedTableName): Unit = {
    alterTableLock.synchronized {
      withHiveExceptionHandling(removeDependentRelationFromHive(table, dependentRelation))
    }
    table.invalidate()
    cachedDataSourceTables.invalidate(table)
  }

  private def isDisconnectException(t: Throwable): Boolean = {
    if (t != null) {
      val tClass = t.getClass.getName
      tClass.contains("DisconnectedException") ||
          tClass.contains("DisconnectException") ||
          (tClass.contains("MetaException") && t.getMessage.contains("retries")) ||
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
    val tables = new ArrayBuffer[QualifiedTableName](4)
    allTables().foreach { t =>
      val tableIdent = newQualifiedTableName(formatTableName(t))
      tableIdent.getTableOption(this) match {
        case Some(table) =>
          if (tableTypes.isEmpty || table.properties.get(JdbcExtendedUtils
              .TABLETYPE_PROPERTY).exists(tableType => tableTypes.exists(_
              .toString == tableType))) {
            if (baseTable.isEmpty || table.properties.get(
              JdbcExtendedUtils.BASETABLE_PROPERTY).contains(baseTable.get)) {
              tables += tableIdent
            }
          }
        case None =>
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
      case _ => ExternalTableType.External
    }
  }

  // -----------------
  // | Other methods |
  // -----------------

  /**
   * Drop all existing databases (except "default"), tables, partitions and functions,
   * and set the current database to "default".
   *
   * This is mainly used for tests.
   */
  override def reset(): Unit = synchronized {
    setCurrentDatabase(Constant.DEFAULT_SCHEMA)
    listDatabases().map(s => s.toUpperCase).
        filter(_ != Constant.DEFAULT_SCHEMA).
        filter(_ != DEFAULT_DATABASE.toUpperCase).foreach { db =>
      dropDatabase(db, ignoreIfNotExists = false, cascade = true)
    }

    listTables(Constant.DEFAULT_SCHEMA).foreach { table =>
      dropTable(table, ignoreIfNotExists = false)
    }
    listFunctions(Constant.DEFAULT_SCHEMA).map(_._1).foreach { func =>
      if (func.database.isDefined) {
        dropFunction(func, ignoreIfNotExists = false)
      } else {
        dropTempFunction(func.funcName, ignoreIfNotExists = false)
      }
    }
    tempTables.clear()
    functionRegistry.clear()
    // restore built-in functions
    FunctionRegistry.builtin.listFunction().foreach { f =>
      val expressionInfo = FunctionRegistry.builtin.lookupFunction(f)
      val functionBuilder = FunctionRegistry.builtin.lookupFunctionBuilder(f)
      require(expressionInfo.isDefined, s"built-in function '$f' is missing expression info")
      require(functionBuilder.isDefined, s"built-in function '$f' is missing function builder")
      functionRegistry.registerFunction(f, expressionInfo.get, functionBuilder.get)
    }
  }

  private def addDependentRelationToHive(inTable: QualifiedTableName,
      index: QualifiedTableName): Unit = {
    val hiveTable = inTable.getTable(this)
    var indexes = ""
    try {
      indexes = hiveTable.properties(ExternalStoreUtils.DEPENDENT_RELATIONS) + ","
    } catch {
      case e: scala.NoSuchElementException =>
    }

    client.alterTable(
      hiveTable.copy(properties = hiveTable.properties +
          (ExternalStoreUtils.DEPENDENT_RELATIONS -> (indexes + index.toString())))
    )
  }

  def addDependentRelation(inTable: QualifiedTableName,
      dependentRelation: QualifiedTableName): Unit = {
    alterTableLock.synchronized {
      withHiveExceptionHandling(addDependentRelationToHive(inTable, dependentRelation))
    }
    cachedDataSourceTables.invalidate(inTable)
  }

}

object SnappyStoreHiveCatalog {

  val HIVE_PROVIDER = "spark.sql.sources.provider"
  val HIVE_SCHEMA_NUMPARTS = "spark.sql.sources.schema.numParts"
  val HIVE_SCHEMA_PART = "spark.sql.sources.schema.part"
  val HIVE_METASTORE = "SNAPPY_HIVE_METASTORE"
  val cachedSampleTables: LoadingCache[QualifiedTableName,
      Seq[(LogicalPlan, String)]] = CacheBuilder.newBuilder().maximumSize(1).build(
    new CacheLoader[QualifiedTableName, Seq[(LogicalPlan, String)]]() {
      override def load(in: QualifiedTableName): Seq[(LogicalPlan, String)] = {
        Seq.empty
      }
    })


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

  def getTableOption(
      catalog: SnappyStoreHiveCatalog): Option[CatalogTable] = _table.orElse {
    _table = catalog.getCachedCatalogTable(this).orElse(
      catalog.client.getTableOption(schemaName, table))
    _table
  }

  def getTable(catalog: SnappyStoreHiveCatalog): CatalogTable =
    getTableOption(catalog).getOrElse(throw new TableNotFoundException(
      s"Table '$schemaName.$table' not found"))

  def getTable(client: HiveClient): CatalogTable = _table.orElse {
    _table = client.getTableOption(schemaName, table)
    _table
  }.getOrElse(throw new TableNotFoundException(
    s"Table '$schemaName.$table' not found"))

  def invalidate(): Unit = _table = None

  override def toString: String = schemaName + '.' + table
}

case class ExternalTableType(name: String)

object ExternalTableType {
  val Row = ExternalTableType("ROW")
  val Column = ExternalTableType("COLUMN")
  val Index = ExternalTableType("INDEX")
  val Stream = ExternalTableType("STREAM")
  val Sample = ExternalTableType("SAMPLE")
  val TopK = ExternalTableType("TOPK")
  val External = ExternalTableType("EXTERNAL")
}
