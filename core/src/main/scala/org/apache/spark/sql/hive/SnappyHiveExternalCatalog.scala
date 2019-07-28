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

import java.lang.reflect.InvocationTargetException
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionException

import com.gemstone.gemfire.cache.CacheClosedException
import com.gemstone.gemfire.internal.cache.{LocalRegion, PartitionedRegion}
import com.gemstone.gemfire.internal.{GFToSlf4jBridge, LogWriterImpl}
import com.google.common.cache.{Cache, CacheBuilder, CacheLoader, LoadingCache}
import com.pivotal.gemfirexd.Constants
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.ddl.catalog.GfxdSystemProcedures
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionByExpressionResolver
import com.pivotal.gemfirexd.internal.engine.diag.SysVTIs
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.impl.sql.catalog.GfxdDataDictionary
import io.snappydata.sql.catalog.SnappyExternalCatalog._
import io.snappydata.sql.catalog.{CatalogObjectType, ConnectorExternalCatalog, RelationInfo, SnappyExternalCatalog}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException
import org.apache.hadoop.hive.ql.metadata.Hive
import org.apache.log4j.{Level, LogManager}

import org.apache.spark.SparkConf
import org.apache.spark.jdbc.{ConnectionConf, ConnectionUtil}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.collection.Utils.EMPTY_STRING_ARRAY
import org.apache.spark.sql.collection.{ToolsCallbackInit, Utils}
import org.apache.spark.sql.execution.RefreshMetadata
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.hive.client.HiveClientImpl
import org.apache.spark.sql.internal.StaticSQLConf.SCHEMA_STRING_LENGTH_THRESHOLD
import org.apache.spark.sql.policy.PolicyProperties
import org.apache.spark.sql.sources.JdbcExtendedUtils
import org.apache.spark.sql.sources.JdbcExtendedUtils.normalizeSchema
import org.apache.spark.sql.types.LongType

class SnappyHiveExternalCatalog private[hive](val conf: SparkConf,
    val hadoopConf: Configuration, val createTime: Long)
    extends SnappyHiveCatalogBase(conf, hadoopConf) with SnappyExternalCatalog {

  {
    // fire dummy queries to initialize more components of hive meta-store
    withHiveExceptionHandling {
      assert(!client.tableExists(SYS_SCHEMA, "dbs"))
      assert(!client.functionExists(SYS_SCHEMA, "funcs"))
    }
  }

  /** A cache of Spark SQL data source tables that have been accessed. */
  protected val cachedCatalogTables: LoadingCache[(String, String), CatalogTable] = {
    val cacheLoader = new CacheLoader[(String, String), CatalogTable]() {
      override def load(name: (String, String)): CatalogTable = {
        logDebug(s"Looking up data source for ${name._1}.${name._2}")
        try {
          withHiveExceptionHandling(SnappyHiveExternalCatalog.super.getTableOption(
            name._1, name._2)) match {
            case None =>
              nonExistentTables.put(name, java.lang.Boolean.TRUE)
              throw new TableNotFoundException(name._1, name._2)
            case Some(catalogTable) => finalizeCatalogTable(catalogTable)
          }
        } catch {
          case _: NullPointerException =>
            // dropTableUnsafe() searches for below exception message. check before changing.
            throw new AnalysisException(
              s"Table ${name._1}.${name._2} might be inconsistent in hive catalog. " +
                  "Use system procedure SYS.REMOVE_METASTORE_ENTRY to remove inconsistency. " +
                  "Refer to troubleshooting section of documentation for more details")
        }
      }
    }
    CacheBuilder.newBuilder().maximumSize(ConnectorExternalCatalog.cacheSize).build(cacheLoader)
  }

  /** A cache of SQL data source tables that are missing in catalog. */
  protected val nonExistentTables: Cache[(String, String), java.lang.Boolean] = {
    CacheBuilder.newBuilder().maximumSize(ConnectorExternalCatalog.cacheSize).build()
  }

  private def isDisconnectException(t: Throwable): Boolean = {
    if (t ne null) {
      val tClass = t.getClass.getName
      tClass.contains("DisconnectedException") ||
          // NPE can be seen if catalog object is being dropped concurrently
          t.isInstanceOf[NullPointerException] ||
          tClass.contains("DisconnectException") ||
          (tClass.contains("MetaException") && t.getMessage.contains("retries")) ||
          isDisconnectException(t.getCause)
    } else {
      false
    }
  }

  /**
   * Retries on transient disconnect exceptions.
   */
  private[sql] def withHiveExceptionHandling[T](function: => T,
      handleDisconnects: Boolean = true): T = synchronized {
    val skipFlags = GfxdDataDictionary.SKIP_CATALOG_OPS.get()
    val oldSkipCatalogCalls = skipFlags.skipHiveCatalogCalls
    val oldSkipLocks = skipFlags.skipDDLocks
    skipFlags.skipHiveCatalogCalls = true
    skipFlags.skipDDLocks = true
    try {
      function
    } catch {
      case he: Exception if isDisconnectException(he) =>
        // stale JDBC connection
        closeHive(clearCache = false)
        suspendActiveSession {
          hiveClient = hiveClient.newSession()
        }
        function
      case e: InvocationTargetException =>
        if (e.getCause ne null) throw e.getCause else throw e
      case e: ExecutionException =>
        if (e.getCause ne null) throw e.getCause else throw e
    } finally {
      skipFlags.skipDDLocks = oldSkipLocks
      skipFlags.skipHiveCatalogCalls = oldSkipCatalogCalls
    }
  }

  def getCatalogSchemaVersion: Long = {
    // schema version is now always stored in the profile to be able to exchange with other
    // nodes and update from incoming nodes easily
    GemFireXDUtils.getMyProfile(true).getCatalogSchemaVersion
  }

  def registerCatalogSchemaChange(relations: Seq[(String, String)]): Unit = {
    GemFireXDUtils.getMyProfile(true).incrementCatalogSchemaVersion()
    invalidateCaches(relations)
  }

  override def invalidateCaches(relations: Seq[(String, String)]): Unit = {
    RefreshMetadata.executeOnAll(SnappyContext.globalSparkContext,
      RefreshMetadata.UPDATE_CATALOG_SCHEMA_VERSION, getCatalogSchemaVersion -> relations,
      executeInConnector = false)
  }

  override def invalidate(name: (String, String)): Unit = {
    cachedCatalogTables.invalidate(name)
    nonExistentTables.invalidate(name)
    // also clear "isRowBuffer" since it may have been cached incorrectly
    // when column store was still being created
    Misc.getRegion(Misc.getRegionPath(JdbcExtendedUtils.toUpperCase(name._1),
      JdbcExtendedUtils.toUpperCase(name._2), null), false, true)
        .asInstanceOf[LocalRegion] match {
      case pr: PartitionedRegion => pr.clearIsRowBuffer()
      case _ =>
    }
  }

  override def invalidateAll(): Unit = {
    cachedCatalogTables.invalidateAll()
    nonExistentTables.invalidateAll()
    // also clear "isRowBuffer" for all regions
    for (pr <- PartitionedRegion.getAllPartitionedRegions.asScala) {
      pr.clearIsRowBuffer()
    }
  }

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  override def createDatabase(schemaDefinition: CatalogDatabase,
      ignoreIfExists: Boolean): Unit = {
    // dot is used for schema, name separation and will cause many problems if present
    if (schemaDefinition.name.indexOf('.') != -1) {
      throw new AnalysisException(
        s"Schema '${schemaDefinition.name}' cannot contain dot in its name")
    }
    // dependent tables are store comma separated so don't allow commas in schema names
    if (schemaDefinition.name.indexOf(',') != -1) {
      throw new AnalysisException(
        s"Schema '${schemaDefinition.name}' cannot contain comma in its name")
    }
    if (databaseExists(schemaDefinition.name)) {
      if (ignoreIfExists) return
      else throw new AnalysisException(s"Schema ${schemaDefinition.name} already exists")
    }
    withHiveExceptionHandling(super.createDatabase(schemaDefinition, ignoreIfExists))
  }

  override def dropDatabase(schema: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    if (schema == SYS_SCHEMA) {
      throw new AnalysisException(s"$schema is a system preserved database/schema")
    }
    try {
      withHiveExceptionHandling(super.dropDatabase(schema, ignoreIfNotExists, cascade))
    } catch {
      case _: NoSuchDatabaseException | _: NoSuchObjectException =>
        throw SnappyExternalCatalog.schemaNotFoundException(schema)
    }
  }

  // Special in-built SYS schema does not have hive catalog entry so the methods below
  // add that specifically to the existing schemas.

  override def getDatabase(schema: String): CatalogDatabase = {
    try {
      if (schema == SYS_SCHEMA) systemSchemaDefinition
      else withHiveExceptionHandling(super.getDatabase(schema).copy(name = schema))
    } catch {
      case _: NoSuchDatabaseException | _: NoSuchObjectException =>
        throw SnappyExternalCatalog.schemaNotFoundException(schema)
    }
  }

  override def databaseExists(schema: String): Boolean = {
    schema == SYS_SCHEMA || withHiveExceptionHandling(super.databaseExists(schema))
  }

  override def listDatabases(): Seq[String] = {
    (withHiveExceptionHandling(super.listDatabases().toSet) + SYS_SCHEMA)
        .toSeq.sorted
  }

  override def listDatabases(pattern: String): Seq[String] = {
    (withHiveExceptionHandling(super.listDatabases(pattern).toSet) ++
        StringUtils.filterPattern(Seq(SYS_SCHEMA), pattern)).toSeq.sorted
  }

  override def setCurrentDatabase(schema: String): Unit = {
    try {
      withHiveExceptionHandling(super.setCurrentDatabase(schema))
    } catch {
      case _: NoSuchDatabaseException | _: NoSuchObjectException =>
        throw SnappyExternalCatalog.schemaNotFoundException(schema)
    }
  }

  override def alterDatabase(schemaDefinition: CatalogDatabase): Unit = {
    try {
      withHiveExceptionHandling(super.alterDatabase(schemaDefinition))
    } catch {
      case _: NoSuchDatabaseException | _: NoSuchObjectException =>
        throw SnappyExternalCatalog.schemaNotFoundException(schemaDefinition.name)
    }
  }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  private def addDependentToBase(name: (String, String), dependent: String): Unit = {
    withHiveExceptionHandling(client.getTableOption(name._1, name._2)) match {
      case None => // ignore, can be a temporary table
      case Some(baseTable) =>
        val dependents = SnappyExternalCatalog.getDependentsValue(baseTable.properties) match {
          case None => dependent
          case Some(deps) =>
            // add only if it doesn't exist
            if (deps.split(',').contains(dependent)) return else deps + "," + dependent
        }
        val newProps = baseTable.properties.filterNot(
          _._1.equalsIgnoreCase(DEPENDENT_RELATIONS))
        withHiveExceptionHandling(client.alterTable(baseTable.copy(properties =
            newProps + (DEPENDENT_RELATIONS -> dependents))))
    }
  }

  private def addViewProperties(tableDefinition: CatalogTable): CatalogTable = {
    // add split VIEW properties for large view strings
    var catalogTable = tableDefinition
    if (catalogTable.tableType == CatalogTableType.VIEW) {
      val maxLen = conf.get(SCHEMA_STRING_LENGTH_THRESHOLD)
      var props: scala.collection.Map[String, String] = catalogTable.properties
      catalogTable.viewText match {
        case Some(v) if v.length > maxLen =>
          catalogTable = catalogTable.copy(viewText = Some(v.substring(0, maxLen)))
          props = JdbcExtendedUtils.addSplitProperty(v, SPLIT_VIEW_TEXT_PROPERTY, props, maxLen)
        case _ =>
      }
      if (catalogTable.viewOriginalText.isEmpty && catalogTable.viewText.isDefined) {
        catalogTable = catalogTable.copy(viewOriginalText = catalogTable.viewText)
      }
      catalogTable.viewOriginalText match {
        case Some(v) if v.length > maxLen =>
          catalogTable = catalogTable.copy(viewOriginalText = Some(v.substring(0, maxLen)))
          props = JdbcExtendedUtils.addSplitProperty(v, SPLIT_VIEW_ORIGINAL_TEXT_PROPERTY,
            props, maxLen)
        case _ =>
      }
      // add the schema as JSON string to restore with full properties
      props = JdbcExtendedUtils.addSplitProperty(catalogTable.schema.json,
        SnappyExternalCatalog.SPLIT_VIEW_SCHEMA, props, maxLen)
      catalogTable = catalogTable.copy(properties = props.toMap)
    }
    catalogTable
  }

  private def removeDependentFromBase(name: (String, String),
      dependent: String): Unit = withHiveExceptionHandling {
    withHiveExceptionHandling(client.getTableOption(name._1, name._2)) match {
      case None => // ignore, can be a temporary table
      case Some(baseTable) =>
        SnappyExternalCatalog.getDependents(baseTable.properties) match {
          case deps if deps.length > 0 =>
            // remove all instances in case there are multiple coming from older releases
            val dependents = deps.toSet
            if (dependents.contains(dependent)) withHiveExceptionHandling {
              val newProps = baseTable.properties.filterNot(
                _._1.equalsIgnoreCase(DEPENDENT_RELATIONS))
              if (dependents.size == 1) {
                client.alterTable(baseTable.copy(properties = newProps))
              } else {
                val newDependents = (dependents - dependent).mkString(",")
                client.alterTable(baseTable.copy(properties = newProps +
                    (DEPENDENT_RELATIONS -> newDependents)))
              }
            }
          case _ =>
        }
    }
  }

  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {
    val catalogTable = addViewProperties(tableDefinition)
    var ifExists = ignoreIfExists
    // Add dependency on base table if required. This is done before actual table
    // entry so that if there is a cluster failure between the two steps, then
    // table will still not be in catalog and base table will simply ignore
    // the dependents not present during reads in getDependents.
    val refreshRelations = getTableWithBaseTable(catalogTable)
    if (refreshRelations.length > 1) {
      val dependent = catalogTable.identifier.unquotedString
      addDependentToBase(refreshRelations.head, dependent)
    }

    val isGemFireTable = catalogTable.provider.isDefined &&
        CatalogObjectType.isGemFireProvider(catalogTable.provider.get)
    // for tables with backing region using CTAS to create, the catalog entry has already been
    // made before to enable inserts, so alter it with the final definition here
    if (!ifExists && (CatalogObjectType.isTableBackedByRegion(
      CatalogObjectType.getTableType(catalogTable)) || isGemFireTable)) {
      withHiveExceptionHandling {
        val schemaName = catalogTable.database
        val tableName = catalogTable.identifier.table
        if (client.tableExists(schemaName, tableName)) {
          // This is the case of CTAS or GemFire. With CTAS ignoreIfExists is always false at this
          // point and any non-CTAS case with ignoreIfExists=false would have failed much earlier
          // when creating the backing region since table already exists for the Some(.) case.
          // Recreate the catalog table because final properties may be slightly different.
          if (isGemFireTable) ifExists = true
          else client.dropTable(schemaName, tableName, ignoreIfNotExists = true, purge = false)
          invalidate(schemaName -> tableName)
        }
      }
    }

    try {
      withHiveExceptionHandling(super.createTable(catalogTable, ifExists))
    } catch {
      case _: TableAlreadyExistsException =>
        val objectType = CatalogObjectType.getTableType(tableDefinition)
        throw new AnalysisException(s"Object '${tableDefinition.identifier.table}' of type " +
            s"$objectType already exists in schema '${tableDefinition.database}'")
    }

    // refresh cache for required tables
    registerCatalogSchemaChange(refreshRelations)
  }

  def dropTableUnsafe(schema: String, table: String, forceDrop: Boolean): Unit = {
    try {
      super.getTable(schema, table)
      // no exception raised while getting catalogTable
      if (forceDrop) {
        // parameter to force drop entry from metastore is set
        withHiveExceptionHandling(super.dropTable(schema, table,
          ignoreIfNotExists = true, purge = true))
      } else {
        // AnalysisException not thrown while getting table. suspecting that wrong table
        // name is passed. throwing exception as a precaution.
        throw new AnalysisException("Table retrieved successfully. To " +
            "continue to drop this table change FORCE_DROP argument in procedure to true")
      }
    } catch {
      case a: AnalysisException if a.message.contains("might be inconsistent in hive catalog") =>
        // exception is expected as table might be inconsistent. continuing to drop
        withHiveExceptionHandling(super.dropTable(schema, table,
          ignoreIfNotExists = true, purge = true))
    }
  }

  override def dropTable(schema: String, table: String, ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = {
    val tableDefinition = getTableOption(schema, table) match {
      case None =>
        if (ignoreIfNotExists) return else throw new TableNotFoundException(schema, table)
      case Some(t) => t
    }
    withHiveExceptionHandling(super.dropTable(schema, table, ignoreIfNotExists = false, purge))

    // drop all policies for the table
    if (Misc.getMemStoreBooting.isRLSEnabled) {
      val policies = getPolicies(schema, table, tableDefinition.properties)
      if (policies.nonEmpty) for (policy <- policies) {
        val schemaName = policy.database
        val policyName = policy.identifier.table
        withHiveExceptionHandling(super.dropTable(schemaName, policyName,
          ignoreIfNotExists = true, purge = false))
        invalidate(schemaName -> policyName)
      }
    }

    // remove from base table if this is a dependent relation
    val refreshRelations = getTableWithBaseTable(tableDefinition)
    if (refreshRelations.length > 1) {
      val dependent = s"$schema.$table"
      removeDependentFromBase(refreshRelations.head, dependent)
    }

    // refresh cache for required tables
    registerCatalogSchemaChange(refreshRelations)
  }

  override def alterTable(tableDefinition: CatalogTable): Unit = {
    val catalogTable = addViewProperties(tableDefinition)
    val schemaName = catalogTable.database
    val tableName = catalogTable.identifier.table

    // if schema has changed then assume only that has to be changed and add the schema
    // property separately because Spark's HiveExternalCatalog does not support schema changes
    if (catalogTable.tableType == CatalogTableType.EXTERNAL) {
      val oldRawDefinition = withHiveExceptionHandling(client.getTable(
        catalogTable.database, catalogTable.identifier.table))
      val oldSchema = ExternalStoreUtils.getTableSchema(
        oldRawDefinition.properties, forView = false) match {
        case None => oldRawDefinition.schema
        case Some(s) => s
      }
      if (oldSchema.length != catalogTable.schema.length) {
        val maxLen = conf.get(SCHEMA_STRING_LENGTH_THRESHOLD)
        // only change to new schema with corresponding properties and assume
        // rest is same since this can only come through SnappySession.alterTable
        val newProps = JdbcExtendedUtils.addSplitProperty(catalogTable.schema.json,
          SnappyExternalCatalog.TABLE_SCHEMA, oldRawDefinition.properties, maxLen)
        withHiveExceptionHandling(client.alterTable(oldRawDefinition.copy(
          schema = catalogTable.schema, properties = newProps.toMap)))

        registerCatalogSchemaChange(schemaName -> tableName :: Nil)
        return
      }
    }

    withHiveExceptionHandling(super.alterTable(catalogTable))

    registerCatalogSchemaChange(schemaName -> tableName :: Nil)
  }

  override def renameTable(schemaName: String, oldName: String, newName: String): Unit = {
    withHiveExceptionHandling(super.renameTable(schemaName, oldName, newName))

    registerCatalogSchemaChange(schemaName -> oldName :: schemaName -> newName :: Nil)
  }

  /**
   * Transform given CatalogTable to final form filling in viewText and other fields
   * using the properties if required.
   */
  protected def finalizeCatalogTable(table: CatalogTable): CatalogTable = {
    val tableIdent = table.identifier
    // VIEW text is stored as split text for large view strings,
    // so restore its full text and schema from properties if present
    val newTable = if (table.tableType == CatalogTableType.VIEW) {
      // update the meta-data from properties
      val viewText = JdbcExtendedUtils.readSplitProperty(SPLIT_VIEW_TEXT_PROPERTY,
        table.properties).orElse(table.viewText)
      val viewOriginalText = JdbcExtendedUtils.readSplitProperty(SPLIT_VIEW_ORIGINAL_TEXT_PROPERTY,
        table.properties).orElse(table.viewOriginalText)
      // schema is "normalized" to deal with upgrade from previous
      // releases that store column names in upper-case (SNAP-3090)
      ExternalStoreUtils.getTableSchema(table.properties, forView = true) match {
        case Some(s) => table.copy(identifier = tableIdent, schema = normalizeSchema(s),
          viewText = viewText, viewOriginalText = viewOriginalText)
        case None => table.copy(identifier = tableIdent, schema = normalizeSchema(table.schema),
          viewText = viewText, viewOriginalText = viewOriginalText)
      }
    } else if (CatalogObjectType.isPolicy(table)) {
      // explicitly change table name in policy properties to lower-case
      // to deal with older releases that store the name in upper-case
      table.copy(identifier = tableIdent, schema = normalizeSchema(table.schema),
        properties = table.properties.updated(PolicyProperties.targetTable,
          JdbcExtendedUtils.toLowerCase(table.properties(PolicyProperties.targetTable))))
    } else table.provider match {
      case Some(provider) if SnappyContext.isBuiltInProvider(provider) ||
          CatalogObjectType.isGemFireProvider(provider) =>
        // add dbtable property which is not present in old releases
        val storageFormat =
          if (table.storage.properties.contains(DBTABLE_PROPERTY)) table.storage
          else {
            table.storage.copy(properties = table.storage.properties +
                (DBTABLE_PROPERTY -> tableIdent.unquotedString))
          }
        // schema is "normalized" to deal with upgrade from previous
        // releases that store column names in upper-case (SNAP-3090)
        table.copy(identifier = tableIdent, schema = normalizeSchema(table.schema),
          storage = storageFormat)
      case _ => table.copy(identifier = tableIdent)
    }
    // explicitly add weightage column to sample tables for old catalog data
    if (CatalogObjectType.getTableType(newTable) == CatalogObjectType.Sample &&
        !newTable.schema(table.schema.length - 1).name.equalsIgnoreCase(
          Utils.WEIGHTAGE_COLUMN_NAME)) {
      newTable.copy(schema = newTable.schema.add(Utils.WEIGHTAGE_COLUMN_NAME, LongType,
        nullable = false))
    } else newTable
  }

  override protected def getCachedCatalogTable(schema: String, table: String): CatalogTable = {
    val name = schema -> table
    if (nonExistentTables.getIfPresent(name) eq java.lang.Boolean.TRUE) {
      throw new TableNotFoundException(schema, table)
    }
    // need to do the load under a sync block to avoid deadlock due to lock inversion
    // (sync block and map loader future) so do a get separately first
    val catalogTable = cachedCatalogTables.getIfPresent(name)
    if (catalogTable ne null) catalogTable
    else withHiveExceptionHandling(cachedCatalogTables.get(name))
  }

  override def getTableOption(schema: String, table: String): Option[CatalogTable] = {
    try {
      Some(getTable(schema, table))
    } catch {
      case _: TableNotFoundException | _: NoSuchTableException => None
    }
  }

  private def toLowerCase(s: Array[String]): Array[String] = {
    val r = new Array[String](s.length)
    for (i <- s.indices) {
      r(i) = JdbcExtendedUtils.toLowerCase(s(i))
    }
    r
  }

  override def getRelationInfo(schema: String, table: String,
      rowTable: Boolean): (RelationInfo, Option[LocalRegion]) = {
    if (SYS_SCHEMA.equalsIgnoreCase(schema)) {
      RelationInfo(1, isPartitioned = false) -> None
    } else {
      val r = Misc.getRegion(Misc.getRegionPath(JdbcExtendedUtils.toUpperCase(schema),
        JdbcExtendedUtils.toUpperCase(table), null),
        true, false).asInstanceOf[LocalRegion]
      val indexCols = if (rowTable) {
        toLowerCase(GfxdSystemProcedures.getIndexColumns(r).asScala.toArray)
      } else EMPTY_STRING_ARRAY
      val pkCols = if (rowTable) {
        toLowerCase(GfxdSystemProcedures.getPKColumns(r).asScala.toArray)
      } else EMPTY_STRING_ARRAY
      r match {
        case pr: PartitionedRegion =>
          val resolver = pr.getPartitionResolver.asInstanceOf[GfxdPartitionByExpressionResolver]
          val partCols = toLowerCase(resolver.getColumnNames)
          RelationInfo(pr.getTotalNumberOfBuckets, isPartitioned = true, partCols,
            indexCols, pkCols) -> Some(pr)
        case _ => RelationInfo(1, isPartitioned = false, EMPTY_STRING_ARRAY,
          indexCols, pkCols) -> Some(r)
      }
    }
  }

  override def createPolicy(schemaName: String, policyName: String, targetTable: String,
      policyFor: String, policyApplyTo: Seq[String], expandedPolicyApplyTo: Seq[String],
      owner: String, filterString: String): Unit = {

    val policyProperties = new mutable.HashMap[String, String]
    policyProperties.put(PolicyProperties.targetTable, targetTable)
    policyProperties.put(PolicyProperties.filterString, filterString)
    policyProperties.put(PolicyProperties.policyFor, policyFor)
    policyProperties.put(PolicyProperties.policyApplyTo, policyApplyTo.mkString(","))
    policyProperties.put(PolicyProperties.expandedPolicyApplyTo,
      expandedPolicyApplyTo.mkString(","))
    policyProperties.put(PolicyProperties.policyOwner, owner)
    val tableDefinition = CatalogTable(
      identifier = TableIdentifier(policyName, Some(schemaName)),
      tableType = CatalogTableType.EXTERNAL,
      schema = JdbcExtendedUtils.EMPTY_SCHEMA,
      provider = Some("policy"),
      storage = CatalogStorageFormat(
        locationUri = None,
        inputFormat = None,
        outputFormat = None,
        serde = None,
        compressed = false,
        properties = Map(BASETABLE_PROPERTY -> targetTable)
      ),
      properties = policyProperties.toMap)

    createTable(tableDefinition, ignoreIfExists = false)
  }

  def refreshPolicies(ldapGroup: String): Unit = {
    val qualifiedLdapGroup = Constants.LDAP_GROUP_PREFIX + ldapGroup
    getAllTables().filter(_.provider.exists(_.equalsIgnoreCase("policy"))).foreach { table =>
      val applyToStr = table.properties(PolicyProperties.policyApplyTo)
      if (applyToStr.nonEmpty) {
        val applyTo = applyToStr.split(',')
        if (applyTo.contains(qualifiedLdapGroup)) {
          val expandedApplyTo = ExternalStoreUtils.getExpandedGranteesIterator(applyTo).toSeq
          val newProperties = table.properties +
              (PolicyProperties.expandedPolicyApplyTo -> expandedApplyTo.mkString(","))
          withHiveExceptionHandling(super.alterTable(table.copy(properties = newProperties)))
        }
      }
    }
  }

  override def tableExists(schema: String, table: String): Boolean = {
    try {
      getTable(schema, table) ne null
    } catch {
      case _: TableNotFoundException | _: NoSuchTableException => false
    }
  }

  override def listTables(schema: String): Seq[String] = {
    if (SYS_SCHEMA.equalsIgnoreCase(schema)) listTables(schema, "*")
    else withHiveExceptionHandling(super.listTables(schema))
  }

  override def listTables(schema: String, pattern: String): Seq[String] = {
    if (SYS_SCHEMA.equalsIgnoreCase(schema)) {
      // check for a system table/VTI in store
      val session = SparkSession.getActiveSession
      val conn = ConnectionUtil.getPooledConnection(schema, new ConnectionConf(
        ExternalStoreUtils.validateAndGetAllProps(session, ExternalStoreUtils.emptyCIMutableMap)))
      try {
        // hive compatible filter patterns are different from JDBC ones
        // so get all tables in the schema and apply filter separately
        val rs = conn.getMetaData.getTables(null, JdbcExtendedUtils.toUpperCase(schema), "%", null)
        val buffer = new mutable.ArrayBuffer[String]()
        // add special case sys.members which is a distributed VTI but used by
        // SnappyData layer as a replicated one
        buffer += MEMBERS_VTI
        while (rs.next()) {
          // skip distributed VTIs
          if (rs.getString(4) != SysVTIs.LOCAL_VTI) {
            buffer += JdbcExtendedUtils.toLowerCase(rs.getString(3))
          }
        }
        rs.close()
        if (pattern == "*") buffer else StringUtils.filterPattern(buffer, pattern)
      } finally {
        conn.close()
      }
    } else withHiveExceptionHandling(super.listTables(schema, pattern))
  }

  override def loadTable(schema: String, table: String, loadPath: String,
      isOverwrite: Boolean, holdDDLTime: Boolean): Unit = {
    withHiveExceptionHandling(super.loadTable(schema, table, loadPath, isOverwrite, holdDDLTime))
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  override def createPartitions(schema: String, table: String, parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    withHiveExceptionHandling(super.createPartitions(schema, table, parts, ignoreIfExists))
  }

  override def dropPartitions(schema: String, table: String, parts: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean, purge: Boolean, retainData: Boolean): Unit = {
    withHiveExceptionHandling(super.dropPartitions(schema, table, parts, ignoreIfNotExists,
      purge, retainData))
  }

  override def renamePartitions(schema: String, table: String, specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = {
    withHiveExceptionHandling(super.renamePartitions(schema, table, specs, newSpecs))
  }

  override def alterPartitions(schema: String, table: String,
      parts: Seq[CatalogTablePartition]): Unit = {
    withHiveExceptionHandling(super.alterPartitions(schema, table, parts))
  }

  override def loadPartition(schema: String, table: String, loadPath: String,
      partition: TablePartitionSpec, isOverwrite: Boolean, holdDDLTime: Boolean,
      inheritTableSpecs: Boolean): Unit = {
    withHiveExceptionHandling(super.loadPartition(schema, table, loadPath, partition,
      isOverwrite, holdDDLTime, inheritTableSpecs))
  }

  override def loadDynamicPartitions(schema: String, table: String, loadPath: String,
      partition: TablePartitionSpec, replace: Boolean, numDP: Int, holdDDLTime: Boolean): Unit = {
    withHiveExceptionHandling(super.loadDynamicPartitions(schema, table, loadPath, partition,
      replace, numDP, holdDDLTime))
  }

  override def getPartition(schema: String, table: String,
      spec: TablePartitionSpec): CatalogTablePartition = {
    withHiveExceptionHandling(super.getPartition(schema, table, spec))
  }

  override def getPartitionOption(schema: String, table: String,
      spec: TablePartitionSpec): Option[CatalogTablePartition] = {
    withHiveExceptionHandling(super.getPartitionOption(schema, table, spec))
  }

  override def listPartitionNames(schema: String, table: String,
      partialSpec: Option[TablePartitionSpec]): Seq[String] = {
    withHiveExceptionHandling(super.listPartitionNames(schema, table, partialSpec))
  }

  override def listPartitions(schema: String, table: String,
      partialSpec: Option[TablePartitionSpec] = None): Seq[CatalogTablePartition] = {
    withHiveExceptionHandling(super.listPartitions(schema, table, partialSpec))
  }

  override def listPartitionsByFilter(schema: String, table: String,
      predicates: Seq[Expression]): Seq[CatalogTablePartition] = {
    withHiveExceptionHandling(super.listPartitionsByFilter(schema, table, predicates))
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  override def createFunction(schema: String, funcDefinition: CatalogFunction): Unit = {
    withHiveExceptionHandling(super.createFunction(schema, funcDefinition))
    SnappySession.clearAllCache()
  }

  override def dropFunction(schema: String, name: String): Unit = {
    withHiveExceptionHandling(super.dropFunction(schema, name))
    SnappySession.clearAllCache()
  }

  override def renameFunction(schema: String, oldName: String, newName: String): Unit = {
    withHiveExceptionHandling(super.renameFunction(schema, oldName, newName))
    SnappySession.clearAllCache()
  }

  override def getFunction(schema: String, funcName: String): CatalogFunction = {
    withHiveExceptionHandling(super.getFunction(schema, funcName))
  }

  override def functionExists(schema: String, funcName: String): Boolean = {
    withHiveExceptionHandling(super.functionExists(schema, funcName))
  }

  override def listFunctions(schema: String, pattern: String): Seq[String] = {
    withHiveExceptionHandling(super.listFunctions(schema, pattern))
  }

  /**
   * Suspend the active SparkSession in case "function" creates new threads
   * that can end up inheriting it. Currently used during hive client creation
   * otherwise the BoneCP background threads hold on to old sessions
   * (even after a restart) due to the InheritableThreadLocal. Shows up as
   * leaks in unit tests where lead JVM size keeps on increasing with new tests.
   */
  private def suspendActiveSession[T](function: => T): T = {
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

  override def close(): Unit = {}

  private[hive] def closeHive(clearCache: Boolean): Unit = synchronized {
    if (clearCache) invalidateAll()
    // Non-isolated client can be closed here directly which is only present in cluster mode
    // using the new property HiveUtils.HIVE_METASTORE_ISOLATION not present in upstream.
    // Isolated loader would require reflection but that case is only in snappy-core
    // unit tests and will never happen in actual usage so ignored here.
    if (ToolsCallbackInit.toolsCallback ne null) {
      val client = hiveClient.asInstanceOf[HiveClientImpl]
      if (client ne null) {
        val loader = client.clientLoader
        val hive = loader.cachedHive
        if (hive != null) {
          loader.cachedHive = null
          Hive.set(hive.asInstanceOf[Hive])
          Hive.closeCurrent()
        }
      }
    }
  }
}

object SnappyHiveExternalCatalog {

  @GuardedBy("this")
  private[this] var instance: SnappyHiveExternalCatalog = _

  def getInstance(sparkConf: SparkConf,
      hadoopConf: Configuration): SnappyHiveExternalCatalog = synchronized {
    val catalog = instance
    val createTime = Misc.getMemStoreBooting.getCreateTime
    if (catalog ne null) {
      // Check if it is being invoked for the same instance of GemFireStore.
      // We don't store the store instance itself to avoid a dangling reference to
      // entire store even after shutdown, rather compare its creation time.
      if (createTime == catalog.createTime) return catalog
      close()
    }

    // Reduce log level to error during hive client initialization
    // as it generates hundreds of lines of logs which are of no use.
    // Once the initialization is done, restore the logging level.
    val logger = Misc.getI18NLogWriter.asInstanceOf[GFToSlf4jBridge]
    val previousLevel = logger.getLevel
    val log4jLogger = LogManager.getRootLogger
    val log4jLevel = log4jLogger.getEffectiveLevel
    logger.info("Starting hive meta-store initialization")
    val reduceLog = previousLevel == LogWriterImpl.CONFIG_LEVEL ||
        previousLevel == LogWriterImpl.INFO_LEVEL
    if (reduceLog) {
      logger.setLevel(LogWriterImpl.ERROR_LEVEL)
      log4jLogger.setLevel(Level.ERROR)
    }
    try {
      // delete the hive scratch directory if it exists
      FileUtils.deleteDirectory(new java.io.File("./hive"))
      instance = new SnappyHiveExternalCatalog(sparkConf, hadoopConf, createTime)
    } finally {
      logger.setLevel(previousLevel)
      log4jLogger.setLevel(log4jLevel)
      logger.info("Done hive meta-store initialization")
    }
    instance
  }

  private[sql] def getExistingInstance: SnappyHiveExternalCatalog = synchronized {
    if (instance ne null) instance
    else throw new CacheClosedException("No external catalog instance available")
  }

  private[sql] def getInstance: SnappyHiveExternalCatalog = synchronized(instance)

  def close(): Unit = synchronized {
    if (instance ne null) {
      instance.withHiveExceptionHandling(instance.closeHive(clearCache = true),
        handleDisconnects = false)
      instance = null
    }
  }
}
