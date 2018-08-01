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
import java.net.URL
import java.util.concurrent.ExecutionException
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions
import scala.util.control.NonFatal

import com.gemstone.gemfire.internal.shared.SystemProperties
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.util.concurrent.UncheckedExecutionException
import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.diag.HiveTablesVTI
import com.pivotal.gemfirexd.internal.engine.distributed.GfxdDistributionAdvisor.GfxdProfile
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.iapi.util.IdUtil
import io.snappydata.Constant
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.TableType
import org.apache.hadoop.hive.ql.metadata.{Hive, HiveException, Table}

import org.apache.spark.sql.internal
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.analysis.{FunctionAlreadyExistsException, FunctionRegistry, NoSuchDatabaseException, NoSuchFunctionException, NoSuchPermanentFunctionException, NoSuchTableException, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.collection.{ToolsCallbackInit, Utils}
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.execution.columnar.impl.{DefaultSource => ColumnSource}
import org.apache.spark.sql.execution.columnar.impl.IndexColumnFormatRelation
import org.apache.spark.sql.execution.columnar.{ExternalStoreUtils, JDBCAppendableRelation}
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog._
import org.apache.spark.sql.hive.client._
import org.apache.spark.sql.internal.{BypassRowLevelSecurity, ContextJarUtils, SQLConf, UDFFunction}
import org.apache.spark.sql.policy.PolicyProperties
import org.apache.spark.sql.row.JDBCMutableRelation
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.{StreamBaseRelation, StreamPlan}
import org.apache.spark.sql.types._
import org.apache.spark.util.MutableURLClassLoader

/**
 * Catalog using Hive for persistence and adding Snappy extensions like
 * stream/topK tables and returning LogicalPlan to materialize these entities.
 */
class SnappyStoreHiveCatalog(externalCatalog: SnappyExternalCatalog,
    val snappySession: SnappySession,
    metadataHive: HiveClient,
    globalTempViewManager: GlobalTempViewManager,
    functionResourceLoader: FunctionResourceLoader,
    functionRegistry: FunctionRegistry,
    sqlConf: SQLConf,
    hadoopConf: Configuration)
    extends SessionCatalog(
      externalCatalog,
      globalTempViewManager,
      functionResourceLoader,
      functionRegistry,
      sqlConf,
      hadoopConf) {

  val sparkConf: SparkConf = snappySession.sparkContext.getConf

  private var _client = metadataHive

  private[sql] def client = {
    // check initialized meta-store (including initial consistency check)
    val memStore = Misc.getMemStoreBootingNoThrow
    if (memStore ne null) {
      memStore.getExistingExternalCatalog
    }
    _client
  }

  // Overriding SessionCatalog values and methods, this will ensure any catalyst layer access to
  // catalog will hit our catalog rather than the SessionCatalog. Some of the methods might look
  // not needed . @TODO will clean up once we have our own seggregation for SessionCatalog and
  // ExternalCatalog
  // override val tempTables = new ConcurrentHashMap[QualifiedTableName, LogicalPlan]().asScala

  // private val sessionTables = new ConcurrentHashMap[QualifiedTableName, LogicalPlan]().asScala

//  override def dropTable(name: TableIdentifier,
//      ignoreIfNotExists: Boolean): Unit = synchronized {
//    snappySession.dropTable(newQualifiedTableName(name), ignoreIfNotExists)
//  }

  protected[sql] var currentSchema: String = {
    var user = snappySession.conf.get(Attribute.USERNAME_ATTR, "")
    if (user.isEmpty) {
      // In smart connector, property name is different.
      user = snappySession.conf.get(Constant.SPARK_STORE_PREFIX + Attribute.USERNAME_ATTR, "")
    }
    val defaultName = IdUtil.getUserAuthorizationId(
      if (user.isEmpty) Constant.DEFAULT_SCHEMA else formatDatabaseName(user)).
      replace('-', '_')

    SnappyContext.getClusterMode(snappySession.sparkContext) match {
      case ThinClientConnectorMode(_, _) =>
      case _ =>
        // Initialize default database if it doesn't already exist
        val defaultDbDefinition =
          CatalogDatabase(defaultName, "app database", sqlConf.warehousePath, Map())
        externalCatalog.createDatabase(defaultDbDefinition, ignoreIfExists = true)
        client.setCurrentDatabase(defaultName)
    }
    defaultName
  }


  override def setCurrentDatabase(db: String): Unit = {
    val dbName = formatDatabaseName(db)
    requireDbExists(dbName)
    synchronized {
      currentSchema = dbName
      client.setCurrentDatabase(db)
    }
  }

  /**
   * Format table name, taking into account case sensitivity.
   */
  override def formatTableName(name: String): String = formatName(name)

  /**
   * Format database name, taking into account case sensitivity.
   */
  override def formatDatabaseName(name: String): String = formatName(name)

  private[sql] def formatName(name: String): String = {
    SnappyStoreHiveCatalog.processIdentifier(name, sqlConf)
  }

  // TODO: SW: cleanup this schema/database stuff
  override def databaseExists(db: String): Boolean = {
    val dbName = formatTableName(db)
    externalCatalog.databaseExists(dbName) ||
        withHiveExceptionHandling(getDatabaseOption(client, dbName)).isDefined ||
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

  /** A cache of Spark SQL data source tables that have been accessed. */
  protected val cachedDataSourceTables: LoadingCache[QualifiedTableName,
      (LogicalRelation, CatalogTable, RelationInfo)] = {
    val cacheLoader = new CacheLoader[QualifiedTableName,
        (LogicalRelation, CatalogTable, RelationInfo)]() {
      override def load(in: QualifiedTableName): (LogicalRelation, CatalogTable, RelationInfo) = {
        // table names are always case-insensitive in hive
        val qualifiedName = Utils.toUpperCase(in.toString)
        logDebug(s"Creating new cached data source for $qualifiedName")
        val table = withHiveExceptionHandling(in.getTable(client))
        val partitionColumns = table.partitionSchema.map(_.name)
        val provider = table.properties(HIVE_PROVIDER)
        var options: Map[String, String] = new CaseInsensitiveMap(table.storage.properties)
        // add dbtable property if not present
        val dbtableProp = JdbcExtendedUtils.DBTABLE_PROPERTY
        if (!options.contains(dbtableProp)) {
          options += dbtableProp -> qualifiedName
        }
        val userSpecifiedSchema = if (table.properties.contains(
          ExternalStoreUtils.USER_SPECIFIED_SCHEMA)) {
          ExternalStoreUtils.getTableSchema(table.properties)
        } else None
        val relation = JdbcExtendedUtils.readSplitProperty(
          JdbcExtendedUtils.SCHEMADDL_PROPERTY, options) match {
          case Some(schema) => JdbcExtendedUtils.externalResolvedDataSource(
            snappySession, schema, provider, SaveMode.Ignore, options)

          case None =>
            // add allowExisting in properties used by some implementations
            DataSource(snappySession, provider, userSpecifiedSchema = userSpecifiedSchema,
              partitionColumns = partitionColumns, options = options +
                  (JdbcExtendedUtils.ALLOW_EXISTING_PROPERTY -> "true")).resolveRelation()
        }
        relation match {
          case _: StreamBaseRelation => // Do Nothing as it is not supported for stream relation
          case _: ParentRelation =>
            var dependentRelations: Array[String] = Array()
            if (table.properties.get(ExternalStoreUtils.DEPENDENT_RELATIONS).isDefined) {
              dependentRelations = table.properties(ExternalStoreUtils.DEPENDENT_RELATIONS)
                  .split(",")
            }

            dependentRelations.foreach(rel => {
              DependencyCatalog.addDependent(qualifiedName, rel)
            })
          case _ => // Do nothing
        }

        (LogicalRelation(relation, catalogTable = Some(table)), table, RelationInfo(
          0, isPartitioned = false, Nil, Array.empty, Array.empty, Array.empty, -1))
      }
    }

    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  val cachedSampleTables: LoadingCache[QualifiedTableName,
      Seq[(LogicalPlan, String)]] = createCachedSampleTables

  protected def createCachedSampleTables: LoadingCache[QualifiedTableName,
      Seq[(LogicalPlan, String)]] = {
    SnappyStoreHiveCatalog.cachedSampleTables
  }

  var relationDestroyVersion = 0

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

  protected def registerRelationDestroy(): Unit = {
    val globalVersion = SnappyStoreHiveCatalog.registerRelationDestroy()
    if (globalVersion != this.relationDestroyVersion) {
      cachedDataSourceTables.invalidateAll()
    }
    this.relationDestroyVersion = globalVersion + 1
  }

  private def normalizeType(dataType: DataType): DataType = dataType match {
    case a: ArrayType => a.copy(elementType = normalizeType(a.elementType))
    case m: MapType => m.copy(keyType = normalizeType(m.keyType),
      valueType = normalizeType(m.valueType))
    case s: StructType => normalizeSchema(s)
    case _ => dataType
  }

  private def normalizeSchemaField(f: StructField): StructField =
    normalizeField(f, Utils.fieldName(f))

  def normalizeField(f: StructField, fieldName: String): StructField = {
    val name = Utils.toUpperCase(fieldName)
    val dataType = normalizeType(f.dataType)
    val metadata = if (f.metadata.contains("name")) {
      val builder = new MetadataBuilder
      builder.withMetadata(f.metadata).putString("name", name).build()
    } else {
      dataType match {
        case StringType =>
          if (!f.metadata.contains(Constant.CHAR_TYPE_BASE_PROP)) {
            val builder = new MetadataBuilder
            builder.withMetadata(f.metadata).putString(Constant.CHAR_TYPE_BASE_PROP,
              "STRING").build()
          } else if (f.metadata.getString(Constant.CHAR_TYPE_BASE_PROP)
              .equalsIgnoreCase("CLOB")) {
            // Remove the CharStringType properties from metadata
            val builder = new MetadataBuilder
            builder.withMetadata(f.metadata).remove(Constant.CHAR_TYPE_BASE_PROP)
                .remove(Constant.CHAR_TYPE_SIZE_PROP).build()
          } else {
            f.metadata
          }
        case _ => f.metadata
      }
    }
    f.copy(name = name, dataType = dataType, metadata = metadata)
  }

  def caseSensitiveAnalysis: Boolean = sqlConf.caseSensitiveAnalysis

  def normalizeSchema(schema: StructType): StructType = {
    if (caseSensitiveAnalysis) {
      schema
    } else {
      val fields = schema.fields
      if (fields.exists(f => Utils.hasLowerCase(Utils.fieldName(f)))) {
        StructType(fields.map(normalizeSchemaField))
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
      case _ => new QualifiedTableName(formatDatabaseName(
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

  def invalidateAll(): Unit = {
    cachedDataSourceTables.invalidateAll()
  }

  def unregisterAllTables(): Unit = synchronized {
    tempTables.clear()
  }

  def unregisterTable(tableIdent: QualifiedTableName): Unit = synchronized {
    val tableName = tableIdent.table
    if (tempTables.contains(tableName)) {
      snappySession.truncateTable(tableIdent, ifExists = false,
        ignoreIfUnsupported = true)
      tempTables -= tableName
    }
  }

  def unregisterPolicy(policyIdent: QualifiedTableName, ct: CatalogTable): Unit = {
    val client = this.client
    policyIdent.invalidate()
    cachedDataSourceTables.invalidate(policyIdent)
    registerRelationDestroy()
    val schemaName = policyIdent.schemaName
    withHiveExceptionHandling(externalCatalog.dropTable(schemaName,
      policyIdent.table, ignoreIfNotExists = false, purge = false))
  }

  def unregisterGlobalView(tableIdent: QualifiedTableName): Boolean = synchronized {
    val schema = tableIdent.schemaName
    if ((schema eq null) || schema == currentSchema || schema == globalTempViewManager.database) {
      dropGlobalTempView(tableIdent.table)
    } else false
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

  /**
   * Return whether a table with the specified name is a global temporary or persistent view.
   */
  private[sql] def isView(tableIdent: QualifiedTableName): Boolean = synchronized {
    val schema = tableIdent.schemaName
    if (((schema eq null) || schema == currentSchema || schema == globalTempViewManager.database)
        && globalTempViewManager.get(tableIdent.table).isDefined) {
      true
    } else tableIdent.getTableOption(this) match {
      case Some(t) if t.tableType == CatalogTableType.VIEW => true
      case _ => false
    }
  }

  final def getCombinedPolicyFilterForTable(rlsRelation: RowLevelSecurityRelation):
  Option[Filter] = {
    // filter out policy rows
    if (!rlsRelation.isRowLevelSecurityEnabled) {
      None
    } else {
      val policyFilters = getAllTablesIncludingPolicies.flatMap(name => {
        val qt = newQualifiedTableName(name)
        qt.getTableOption(this) match {
          case Some(ct) if ct.tableType == CatalogTableType.EXTERNAL &&
              ct.properties.getOrElse(JdbcExtendedUtils.TABLETYPE_PROPERTY, "").
                  equals(ExternalTableType.Policy.name) &&
              ct.properties.getOrElse(PolicyProperties.targetTable, "").
                  equals(rlsRelation.resolvedName) =>
            Seq(this.lookupRelation(ct.identifier).asInstanceOf[SubqueryAlias].
                child.asInstanceOf[BypassRowLevelSecurity].child)
          case _ => Seq.empty
        }
      })
      if (policyFilters.isEmpty) None
      else {
        Some(policyFilters.foldLeft[Filter](null) {
          case (result, filter) =>
            if (result == null) {
              filter
            } else {
              result.copy(condition = org.apache.spark.sql.catalyst.expressions.And(
                result.condition, filter.condition))
            }
        })
      }
    }
  }

  final def lookupRelation(tableIdent: QualifiedTableName): LogicalPlan = {
    tableIdent.getTableOption(this) match {
      case Some(table) =>
        if (table.properties.contains(HIVE_PROVIDER)) {
          getCachedHiveTable(tableIdent)
        } else if (table.tableType == CatalogTableType.VIEW) {
          // val viewText = table.viewText
          //     .getOrElse(sys.error("Invalid view without text."))
          val viewText = JdbcExtendedUtils.readSplitProperty(
            Constant.SPLIT_VIEW_TEXT_PROPERTY, table.properties).getOrElse(table.viewText
              .getOrElse(sys.error("Invalid view without text.")))
          snappySession.sessionState.sqlParser.parsePlan(viewText)
        } else if (table.tableType == CatalogTableType.EXTERNAL &&
            (table.properties.get(JdbcExtendedUtils.TABLETYPE_PROPERTY) match {
              case Some(tt) => tt.equals(ExternalTableType.Policy.name)
              case None => false
            })
            ) {
          val filterExpression = snappySession.sessionState.sqlParser.parseExpression(
            table.properties.getOrElse(PolicyProperties.filterString,
              throw new IllegalStateException("Filter for the policy not found")))
          val tableIdent = newQualifiedTableName(table.properties.getOrElse(
            PolicyProperties.targetTable,
            throw new IllegalStateException("Target Table for the policy not found")))
          val plan = PolicyProperties.createFilterPlan(filterExpression, tableIdent,
            table.properties.getOrElse(PolicyProperties.policyOwner, ""),
            table.properties.getOrElse(PolicyProperties.expandedPolicyApplyTo, "").split(",").
                toSeq.filterNot(_.isEmpty))
          val resolvedPlan = snappySession.sessionState.analyzer.execute(plan)
          snappySession.sessionState.analyzer.checkAnalysis(resolvedPlan)
          resolvedPlan
        }
        else {
          throw new IllegalStateException(
            s"Unsupported table type ${table.tableType} with properties: ${table.properties}")
        }

      case None => synchronized {
        val schema = tableIdent.schemaName
        val table = tableIdent.table
        val plan = if (schema == globalTempViewManager.database) {
          globalTempViewManager.get(table)
        } else if ((schema == null) || schema.isEmpty || schema == currentSchema) {
          tempTables.get(table).orElse(globalTempViewManager.get(table))
        } else None
        plan match {
          case Some(lr: LogicalRelation) => lr.catalogTable match {
            case Some(_) => lr
            case None => lr.copy(catalogTable = Some(CatalogTable(tableIdent,
              CatalogTableType.VIEW, null, lr.schema)))
          }
          case Some(p) => p
          case None =>
            throw new TableNotFoundException(s"Table '$tableIdent' not found")
        }
      }
    }
  }

  final def lookupRelationOption(tableIdent: QualifiedTableName): Option[LogicalPlan] = {
    try {
      Some(lookupRelation(tableIdent))
    } catch {
      case _: TableNotFoundException | _: NoSuchTableException => None
    }
  }

  override def lookupRelation(tableIdent: TableIdentifier,
      alias: Option[String]): LogicalPlan = {
    // If an alias was specified by the lookup, wrap the plan in a
    // sub-query so that attributes are properly qualified with this alias
    SubqueryAlias(alias.getOrElse(tableIdent.table),
      lookupRelation(newQualifiedTableName(tableIdent)), None)
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
    withHiveExceptionHandling(
      client.getTableOption(tableIdent.schemaName, tableIdent.table)) match {
      case Some(_) =>
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
          tableIdent.table, ignoreIfNotExists = false, purge = false))
      case None =>
    }
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
                               relation: Option[BaseRelation]): Unit = {
    val client = this.client
    withHiveExceptionHandling(
      client.getTableOption(tableIdent.schemaName, tableIdent.table)) match {
      case None =>

        val newOptions = new CaseInsensitiveMutableHashMap(options)
        // add default batchSize and maxDeltaRows options for column tables
        if (SnappyParserConsts.COLUMN_SOURCE.equalsIgnoreCase(provider) ||
            classOf[ColumnSource].getCanonicalName == provider ||
            SnappyContext.SAMPLE_SOURCE.equalsIgnoreCase(provider) ||
            SnappyContext.SAMPLE_SOURCE_CLASS == provider) {
          newOptions.get(ExternalStoreUtils.COLUMN_BATCH_SIZE) match {
            case Some(_) =>
            case None => newOptions += (ExternalStoreUtils.COLUMN_BATCH_SIZE ->
                ExternalStoreUtils.defaultColumnBatchSize(snappySession).toString)
              // mark this as transient since can change as per session configuration later
              newOptions += (ExternalStoreUtils.COLUMN_BATCH_SIZE_TRANSIENT -> "true")
          }
          newOptions.get(ExternalStoreUtils.COLUMN_MAX_DELTA_ROWS) match {
            case Some(_) =>
            case None => newOptions += (ExternalStoreUtils.COLUMN_MAX_DELTA_ROWS ->
                ExternalStoreUtils.defaultColumnMaxDeltaRows(snappySession).toString)
              // mark this as transient since can change as per session configuration later
              newOptions += (ExternalStoreUtils.COLUMN_MAX_DELTA_ROWS_TRANSIENT -> "true")
          }
        }
        // invalidate any cached plan for the table
        tableIdent.invalidate()
        cachedDataSourceTables.invalidate(tableIdent)

        val tableProperties = new mutable.HashMap[String, String]
        tableProperties.put(HIVE_PROVIDER, provider)

        // Saves optional user specified schema.  Serialized JSON schema string
        // may be too long to be stored into a single meta-store SerDe property.
        // In this case, we split the JSON string and store each part as a
        // separate SerDe property.
        val tableSchema = userSpecifiedSchema match {
          case Some(schema) =>
            tableProperties.put(ExternalStoreUtils.USER_SPECIFIED_SCHEMA, "true")
            schema
          case None => relation match {
            case Some(r) => r.schema
            case _ => StructType(Nil)
          }
        }
        val schemaJsonString = tableSchema.json
        // Split the JSON string.
        JdbcExtendedUtils.addSplitProperty(schemaJsonString,
          HIVE_SCHEMA_PROP, tableProperties)

        // get the tableType
        val tableType = relation match {
          case Some(r) => getTableType(r)
          case None => ExternalTableType.External
        }
        tableProperties.put(JdbcExtendedUtils.TABLETYPE_PROPERTY, tableType.name)
        // add baseTable property if required
        relation match {
          case Some(dep: DependentRelation) => dep.baseTable.foreach { t =>
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
        withHiveExceptionHandling(getDatabaseOption(client, schemaName)) match {
          case Some(_) => // We are all good
          case None => withHiveExceptionHandling(client.createDatabase(CatalogDatabase(
            schemaName,
            description = schemaName,
            getDefaultDBPath(schemaName),
            Map.empty[String, String]),
            ignoreIfExists = true))
          // Path is empty String for now @TODO for parquet & hadoop relation
          // handle path correctly
        }

        val hiveTable = CatalogTable(
          identifier = tableIdent,
          // Can not inherit from this class. Ideally we should
          // be extending from this case class
          tableType = CatalogTableType.EXTERNAL,
          schema = tableSchema,
          storage = CatalogStorageFormat(
            locationUri = None,
            inputFormat = None,
            outputFormat = None,
            serde = None,
            compressed = false,
            properties = newOptions.toMap
          ),
          properties = tableProperties.toMap)

        withHiveExceptionHandling(client.createTable(hiveTable, ignoreIfExists = true))
        SnappySession.clearAllCache()
      case Some(_) =>  // Do nothing
    }
  }

  def registerPolicy(
      policyNameX: TableIdentifier,
      targetTableX: TableIdentifier,
      policyFor: String,
      policyApplyTo: Seq[String],
      expandedPolicyApplyTo: Seq[String],
      owner: String,
      filterString: String,
      filterPlan: BypassRowLevelSecurity
  ): Unit = {
    val client = this.client
    val policyName = newQualifiedTableName(policyNameX)
    val targetTable = newQualifiedTableName(targetTableX)
    withHiveExceptionHandling(
      client.getTableOption(policyName.schemaName, policyName.table)) match {
      case None =>

        // invalidate any cached plan for the table
        targetTable.invalidate()
        cachedDataSourceTables.invalidate(targetTable)
        policyName.invalidate()
        cachedDataSourceTables.invalidate(policyName)
        val policyProperties = new mutable.HashMap[String, String]
        policyProperties.put(PolicyProperties.targetTable, targetTable.toString)
        policyProperties.put(PolicyProperties.filterString, filterString)
        policyProperties.put(PolicyProperties.policyFor, policyFor)
        policyProperties.put(PolicyProperties.policyApplyTo, policyApplyTo.mkString(","))
        policyProperties.put(PolicyProperties.expandedPolicyApplyTo,
          expandedPolicyApplyTo.mkString(","))
        policyProperties.put(PolicyProperties.policyOwner, owner)
        policyProperties.put(JdbcExtendedUtils.TABLETYPE_PROPERTY,
          ExternalTableType.Policy.name)
        val hiveTable = CatalogTable(
          identifier = policyName,
          tableType = CatalogTableType.EXTERNAL,
          schema = StructType.apply(Seq.empty),
          storage = CatalogStorageFormat(
            locationUri = None,
            inputFormat = None,
            outputFormat = None,
            serde = None,
            compressed = false,
            properties = Map.empty
          ),
          properties = policyProperties.toMap)

        withHiveExceptionHandling(client.createTable(hiveTable, ignoreIfExists = true))
        SnappySession.clearAllCache()
      case Some(catalogTable) => {
        val policyProperties = new mutable.HashMap[String, String]
        policyProperties.put(PolicyProperties.targetTable, targetTable.toString)
        policyProperties.put(PolicyProperties.filterString, filterString)
        policyProperties.put(PolicyProperties.policyFor, policyFor)
        policyProperties.put(PolicyProperties.policyApplyTo, policyApplyTo.mkString(","))
        policyProperties.put(PolicyProperties.policyOwner, owner)
        val cloneCatalogProps = catalogTable.properties.filterNot(keyVal =>
          keyVal._1.equalsIgnoreCase(PolicyProperties.expandedPolicyApplyTo))
        if (!(cloneCatalogProps.size == policyProperties.size &&
            cloneCatalogProps.forall {
              case (key, value) => value == policyProperties.getOrElse(key, "")
            })) {
          throw new AnalysisException(s"A policy with same name " +
              s"but different attributes {${catalogTable.properties.toSeq.mkString(",")}}" +
              s" already exists")
        }

      }

    }
  }


  def withHiveExceptionHandling[T](function: => T): T = {
    val oldFlag = HiveTablesVTI.SKIP_HIVE_TABLE_CALLS.get
    if (oldFlag ne java.lang.Boolean.TRUE) {
      HiveTablesVTI.SKIP_HIVE_TABLE_CALLS.set(java.lang.Boolean.TRUE)
    }
    try {
      function
    } catch {
      case he: HiveException if isDisconnectException(he) =>
        // stale JDBC connection
        SnappyStoreHiveCatalog.closeHive(client)
        SnappyStoreHiveCatalog.suspendActiveSession {
          _client = externalCatalog.client.newSession()
        }
        function
    } finally {
      if (oldFlag ne java.lang.Boolean.TRUE) {
        HiveTablesVTI.SKIP_HIVE_TABLE_CALLS.set(oldFlag)
      }
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
        withHiveExceptionHandling(client.alterTable(
          hiveTable.copy(
            properties = hiveTable.properties - ExternalStoreUtils.DEPENDENT_RELATIONS)
        ))
      } else {
        withHiveExceptionHandling(client.alterTable(
          hiveTable.copy(properties = hiveTable.properties +
              (ExternalStoreUtils.DEPENDENT_RELATIONS -> newindexes))
        ))
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
    val schemaName = db.map(formatTableName)
        .getOrElse(currentSchema)
    synchronized(tempTables.collect {
      case (tableIdent, _) if db.isEmpty || currentSchema == schemaName =>
        (tableIdent, true)
    }).toSeq ++
        (if (db.isEmpty) allTables() else withHiveExceptionHandling(
          client.listTables(schemaName))).map { t =>
          if (db.isDefined) {
            (schemaName + '.' + formatTableName(t), false)
          } else {
            (formatTableName(t), false)
          }
        }
  }

  def getDataSourceTables(tableTypes: Seq[ExternalTableType],
      baseTable: Option[String] = None): Seq[QualifiedTableName] = {
    val tables = new mutable.ArrayBuffer[QualifiedTableName](4)
    allTables().foreach { t =>
      val tableIdent = newQualifiedTableName(formatTableName(t))
      tableIdent.getTableOption(this) match {
        case Some(table) =>
          if (tableTypes.isEmpty || table.properties.get(JdbcExtendedUtils
              .TABLETYPE_PROPERTY).exists(tableType => tableTypes.exists(_.name
              == tableType))) {
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



  private def getAllTablesIncludingPolicies(): Seq[String] = {
    val allTables = new mutable.ArrayBuffer[String]()
    val currentSchemaName = this.currentSchema
    var hasCurrentDb = false
    // Why am I seeing lowercase as well as uppercase database?
    val databases = withHiveExceptionHandling(client.listDatabases("*")).iterator.
        map(_.toUpperCase).toSet.iterator
    while (databases.hasNext) {
      val db = databases.next()
      if (!hasCurrentDb && db == currentSchemaName) {
        allTables ++= withHiveExceptionHandling(client.listTables(db))
        hasCurrentDb = true
      } else {
        allTables ++= withHiveExceptionHandling(client.listTables(db)).map(db + '.' + _)
      }
    }
    if (!hasCurrentDb) {
      allTables ++= withHiveExceptionHandling(client.listTables(currentSchemaName))
    }
    allTables
  }

  // without policy tables
  private def allTables(): Seq[String] = {
    // filter out policy rows
    getAllTablesIncludingPolicies.filterNot(name => {
      val qt = newQualifiedTableName(name)
      qt.getTableOption(this) match {
        case Some(ct) => ct.tableType == CatalogTableType.EXTERNAL &&
            ct.properties.getOrElse(JdbcExtendedUtils.TABLETYPE_PROPERTY, "").
                equals(ExternalTableType.Policy.name)
        case _ => false
      }
    })
  }

  def getDataSourceRelations[T](tableTypes: Seq[ExternalTableType],
      baseTable: Option[String] = None): Seq[T] = {
    getDataSourceTables(tableTypes, baseTable).map(
      getCachedHiveTable(_).relation.asInstanceOf[T])
  }

  def getTableType(relation: BaseRelation): ExternalTableType = {
    relation match {
      case _: JDBCMutableRelation => ExternalTableType.Row
      case _: IndexColumnFormatRelation => ExternalTableType.Index
      case _: JDBCAppendableRelation => ExternalTableType.Column
      case _: StreamPlan => ExternalTableType.Stream
      case _ => ExternalTableType.External
    }
  }

  private def toUrl(resource: FunctionResource): URL = {
    val path = resource.uri
    val uri = new Path(path).toUri
    if (uri.getScheme == null) {
      // `path` is a local file path without a URL scheme
      new File(path).toURI.toURL
    } else {
      // `path` is a URL with a scheme
      uri.toURL
    }
  }

  private def addToFuncJars(funcDefinition: CatalogFunction,
      qualifiedName: FunctionIdentifier): Unit = {
    val parentLoader = org.apache.spark.util.Utils.getContextOrSparkClassLoader
    val callbacks = ToolsCallbackInit.toolsCallback
    val newClassLoader = ContextJarUtils.getDriverJar(qualifiedName.unquotedString).getOrElse({
      val urls = if (callbacks != null) {
        funcDefinition.resources.map { r =>
          ContextJarUtils.fetchFile(funcDefinition.identifier.toString(), r.uri)
        }
      } else {
        funcDefinition.resources.map { r =>
          toUrl(r)
        }
      }
      val newClassLoader = new MutableURLClassLoader(urls.toArray, parentLoader)
      ContextJarUtils.addDriverJar(qualifiedName.unquotedString, newClassLoader)
      newClassLoader
    })

    SnappyContext.getClusterMode(snappySession.sparkContext) match {
      case SnappyEmbeddedMode(_, _) =>
        callbacks.setSessionDependencies(snappySession.sparkContext,
          qualifiedName.unquotedString,
          newClassLoader)
      case _ =>
        newClassLoader.getURLs.foreach(url =>
          snappySession.sparkContext.addJar(url.getFile))
    }
  }

  private def removeFromFuncJars(funcDefinition: CatalogFunction,
      qualifiedName: FunctionIdentifier): Unit = {
    funcDefinition.resources.foreach { r =>
      ContextJarUtils.deleteFile(funcDefinition.identifier.toString(), r.uri)
    }
    ContextJarUtils.removeDriverJar(qualifiedName.unquotedString)
  }

  override def dropFunction(name: FunctionIdentifier, ignoreIfNotExists: Boolean): Unit = {
    // If the name itself is not qualified, add the current database to it.
    val database = formatDatabaseName(name.database.getOrElse(currentSchema))
    val qualifiedName = name.copy(database = Some(database))
    ContextJarUtils.getDriverJar(qualifiedName.unquotedString) match {
      case Some(_) =>
        val catalogFunction = try {
          externalCatalog.getFunction(database, qualifiedName.funcName)
        } catch {
          case _: AnalysisException => failFunctionLookup(qualifiedName.funcName)
          case _: NoSuchPermanentFunctionException => failFunctionLookup(qualifiedName.funcName)
        }
        removeFromFuncJars(catalogFunction, qualifiedName)
      case _ =>
    }
    super.dropFunction(name, ignoreIfNotExists)
  }

  /**
    * Create a metastore function in the database specified in `funcDefinition`.
    * If no such database is specified, create it in the current database.
    * If the specified database is not present in catalog, create that database.
    * @ TODO Ideally create schema from gfxd should get routed to create the database in
    * the Hive catalog.
    */
  override def createFunction(funcDefinition: CatalogFunction, ignoreIfExists: Boolean): Unit = {
    val db = formatDatabaseName(funcDefinition.identifier.database.getOrElse(currentSchema))
    withHiveExceptionHandling(getDatabaseOption(client, db)) match {
      case Some(_) => // We are all good
      case None => withHiveExceptionHandling(client.createDatabase(CatalogDatabase(
        db,
        description = db,
        getDefaultDBPath(db),
        Map.empty[String, String]),
        ignoreIfExists = true))
      // Path is empty String for now @TODO for parquet & hadoop relation
      // handle path correctly
    }

    val identifier = FunctionIdentifier(funcDefinition.identifier.funcName, Some(db))
    val newFuncDefinition = funcDefinition.copy(identifier = identifier)
    if (!functionExists(identifier)) {
      externalCatalog.createFunction(db, newFuncDefinition)
    } else if (!ignoreIfExists) {
      throw new FunctionAlreadyExistsException(db = db, func = identifier.toString)
    }
  }

  override def makeFunctionBuilder(funcName: String, className: String): FunctionBuilder = {
    val uRLClassLoader = ContextJarUtils.getDriverJar(funcName).getOrElse(
      org.apache.spark.util.Utils.getContextOrSparkClassLoader)
    val (actualClassName, typeName) = className.splitAt(className.lastIndexOf("__"))
    UDFFunction.makeFunctionBuilder(funcName,
      uRLClassLoader.loadClass(actualClassName),
      snappySession.sessionState.sqlParser.parseDataType(typeName.stripPrefix("__")))
  }

  /**
   * Look up the [[ExpressionInfo]] associated with the specified function, assuming it exists.
   */
  override def lookupFunctionInfo(name: FunctionIdentifier): ExpressionInfo = synchronized {
    // TODO: just make function registry take in FunctionIdentifier instead of duplicating this
    val database = name.database.orElse(Some(currentSchema)).map(formatDatabaseName)
    val qualifiedName = name.copy(database = database)
    functionRegistry.lookupFunction(name.funcName)
        .orElse(functionRegistry.lookupFunction(qualifiedName.unquotedString))
        .getOrElse {
          val db = qualifiedName.database.get
          requireDbExists(db)
          if (externalCatalog.functionExists(db, name.funcName)) {
            val metadata = externalCatalog.getFunction(db, name.funcName)
            new ExpressionInfo(metadata.className, qualifiedName.unquotedString)
          } else {
            failFunctionLookup(name.funcName)
          }
        }
  }

  /**
   * Return an [[Expression]] that represents the specified function, assuming it exists.
   *
   * For a temporary function or a permanent function that has been loaded,
   * this method will simply lookup the function through the
   * FunctionRegistry and create an expression based on the builder.
   *
   * For a permanent function that has not been loaded, we will first fetch its metadata
   * from the underlying external catalog. Then, we will load all resources associated
   * with this function (i.e. jars and files). Finally, we create a function builder
   * based on the function class and put the builder into the FunctionRegistry.
   * The name of this function in the FunctionRegistry will be `databaseName.functionName`.
   */
  override def lookupFunction(
      name: FunctionIdentifier,
      children: Seq[Expression]): Expression = synchronized {
    // Note: the implementation of this function is a little bit convoluted.
    // We probably shouldn't use a single FunctionRegistry to register all three kinds of functions
    // (built-in, temp, and external).
    if (name.database.isEmpty && functionRegistry.functionExists(name.funcName)) {
      // This function has been already loaded into the function registry.
      return functionRegistry.lookupFunction(name.funcName, children)
    }

    // If the name itself is not qualified, add the current database to it.
    val database = formatDatabaseName(name.database.getOrElse(currentSchema))
    val qualifiedName = name.copy(database = Some(database))

    if (functionRegistry.functionExists(qualifiedName.unquotedString)) {
      // This function has been already loaded into the function registry.
      // Unlike the above block, we find this function by using the qualified name.
      return functionRegistry.lookupFunction(qualifiedName.unquotedString, children)
    }

    // The function has not been loaded to the function registry, which means
    // that the function is a permanent function (if it actually has been registered
    // in the metastore). We need to first put the function in the FunctionRegistry.
    // TODO: why not just check whether the function exists first?
    val catalogFunction = try {
      externalCatalog.getFunction(database, qualifiedName.funcName)
    } catch {
      case _: AnalysisException =>
        throw new NoSuchFunctionException(db = database, func = qualifiedName.funcName)
      case _: NoSuchPermanentFunctionException =>
        throw new NoSuchFunctionException(db = database, func = qualifiedName.funcName)
    }
    // loadFunctionResources(catalogFunction.resources) // Not needed for Snappy use case

    // Please note that qualifiedName is provided by the user. However,
    // catalogFunction.identifier.unquotedString is returned by the underlying
    // catalog. So, it is possible that qualifiedName is not exactly the same as
    // catalogFunction.identifier.unquotedString (difference is on case-sensitivity).
    // At here, we preserve the input from the user.
    val info = new ExpressionInfo(catalogFunction.className, qualifiedName.unquotedString)

    addToFuncJars(catalogFunction, qualifiedName)

    val builder = makeFunctionBuilder(qualifiedName.unquotedString, catalogFunction.className)
    createTempFunction(qualifiedName.unquotedString, info, builder, ignoreIfExists = false)
    // Now, we need to create the Expression.
    functionRegistry.lookupFunction(qualifiedName.unquotedString, children)
  }



  // -----------------
  // | Other methods |
  // -----------------

  /**
   * Drop all existing databases (except "default"), tables, partitions and functions,
   * and set the current database to "default".
   * This method will only remove tables from hive catalog.Don't use this method if you want to
   * delete physical tables
   * This is mainly used for tests.
   */
  override def reset(): Unit = synchronized {
    setCurrentDatabase(Constant.DEFAULT_SCHEMA)
    listDatabases().map(Utils.toUpperCase).
        filter(_ != Constant.DEFAULT_SCHEMA).
        filter(_ != Utils.toUpperCase(DEFAULT_DATABASE)).foreach { db =>
      dropDatabase(db, ignoreIfNotExists = false, cascade = true)
    }

    listTables(Constant.DEFAULT_SCHEMA).foreach { table =>
      dropTable(table, ignoreIfNotExists = false, purge = false)
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

  /**
   * Test only method
   */
  def destroyAndRegisterBuiltInFunctions(): Unit = {
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
      case _: scala.NoSuchElementException =>
    }

    withHiveExceptionHandling(client.alterTable(
      hiveTable.copy(properties = hiveTable.properties +
          (ExternalStoreUtils.DEPENDENT_RELATIONS -> (indexes + index.toString()))))
    )
  }

  def addDependentRelation(inTable: QualifiedTableName,
      dependentRelation: QualifiedTableName): Unit = {
    alterTableLock.synchronized {
      withHiveExceptionHandling(addDependentRelationToHive(inTable, dependentRelation))
    }
    cachedDataSourceTables.invalidate(inTable)
  }

  def getTableOption(qtn: QualifiedTableName): Option[CatalogTable] = {
    withHiveExceptionHandling(client.getTableOption(qtn.schemaName, qtn.table))
  }

  def close(): Unit = synchronized {
    closeHive(client)
  }
}

object SnappyStoreHiveCatalog {

  val HIVE_PROVIDER = "spark.sql.sources.provider"
  val HIVE_SCHEMA_PROP = "spark.sql.sources.schema"
  val HIVE_METASTORE = SystemProperties.SNAPPY_HIVE_METASTORE
  val cachedSampleTables: LoadingCache[QualifiedTableName,
      Seq[(LogicalPlan, String)]] = CacheBuilder.newBuilder().maximumSize(1).build(
    new CacheLoader[QualifiedTableName, Seq[(LogicalPlan, String)]]() {
      override def load(in: QualifiedTableName): Seq[(LogicalPlan, String)] = {
        Nil
      }
    })


  def processIdentifier(identifier: String, conf: SQLConf): String = {
    if (conf.caseSensitiveAnalysis) {
      identifier
    } else {
      Utils.toUpperCase(identifier)
    }
  }

  private[this] var relationDestroyVersion = 0
  val relationDestroyLock = new ReentrantReadWriteLock()
  private val alterTableLock = new Object

  private[sql] def getRelationDestroyVersion: Int = relationDestroyVersion

  private[sql] def registerRelationDestroy(): Int = {
    val sync = relationDestroyLock.writeLock()
    sync.lock()
    try {
      val globalVersion = relationDestroyVersion
      relationDestroyVersion += 1
      setRelationDestroyVersionOnAllMembers()
      globalVersion
    } finally {
      sync.unlock()
    }
  }

  def setRelationDestroyVersionOnAllMembers(): Unit = {
    SparkSession.getDefaultSession.foreach(session =>
      SnappyContext.getClusterMode(session.sparkContext) match {
        case SnappyEmbeddedMode(_, _) =>
          val version = getRelationDestroyVersion
          Utils.mapExecutors[Unit](session.sparkContext,
            () => {
              val profile: GfxdProfile =
                GemFireXDUtils.getGfxdProfile(Misc.getGemFireCache.getMyId)
              Option(profile).foreach(gp => gp.setRelationDestroyVersion(version))
              Iterator.empty
            })
        case _ =>
      }
    )
  }

  def getSchemaStringFromHiveTable(table: Table): String =
    JdbcExtendedUtils.readSplitProperty(HIVE_SCHEMA_PROP,
      table.getParameters.asScala).orNull

  def getDatabaseOption(client: HiveClient, db: String): Option[CatalogDatabase] = try {
    Some(client.getDatabase(db))
  } catch {
    case NonFatal(_) => None
  }

  /**
   * Suspend the active SparkSession in case "function" creates new threads
   * that can end up inheriting it. Currently used during hive client creation
   * otherwise the BoneCP background threads hold on to old sessions
   * (even after a restart) due to the InheritableThreadLocal. Shows up as
   * leaks in unit tests where lead JVM size keeps on increasing with new tests.
   */
  def suspendActiveSession[T](function: => T): T = {
    SparkSession.getActiveSession match {
      case Some(activeSession) =>
        SparkSession.clearActiveSession()
        try {
          function
        } finally {
          SparkSession.setActiveSession(activeSession)
        }
      case None => function
    }
  }

  def closeHive(client: HiveClient): Unit = {
    if (client ne null) {
      val loader = client.asInstanceOf[HiveClientImpl].clientLoader
      val hive = loader.cachedHive
      if (hive != null) {
        loader.cachedHive = null
        Hive.set(hive.asInstanceOf[Hive])
        Hive.closeCurrent()
      }
    }
  }
}

/** A fully qualified identifier for a table (i.e. [schema.]tableName) */
final class QualifiedTableName(val schemaName: String, _tableIdent: String)
    extends TableIdentifier(_tableIdent, Some(schemaName)) {

  @transient private[this] var _table: Option[CatalogTable] = None

  def getTableOption(
      catalog: SnappyStoreHiveCatalog): Option[CatalogTable] = _table.orElse {
    _table = catalog.getCachedCatalogTable(this).orElse(
      catalog.getTableOption(this))
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
  val Policy = ExternalTableType("POLICY")

  def getTableType(t: Table): String = {
    if (t ne null) {
      // check for VIEW types
      if (TableType.VIRTUAL_VIEW.name.equalsIgnoreCase(t.getTableType.name())) {
        return "VIEW"
      }
      else {
        val tableType = t.getParameters.get(JdbcExtendedUtils.TABLETYPE_PROPERTY)
        if (tableType ne null) return tableType
      }
    }
    // assume EXTERNAL type
    ExternalTableType.External.name
  }

  def isTableBackedByRegion(tableType: String): Boolean = {
    tableType.equalsIgnoreCase(ExternalTableType.Row.name) ||
        tableType.equalsIgnoreCase(ExternalTableType.Column.name) ||
        tableType.equalsIgnoreCase(ExternalTableType.Sample.name) ||
        tableType.equalsIgnoreCase(ExternalTableType.Index.name)
  }
}
