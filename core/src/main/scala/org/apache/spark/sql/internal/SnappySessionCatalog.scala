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
package org.apache.spark.sql.internal

import java.io.File
import java.net.URL

import scala.util.control.NonFatal

import com.pivotal.gemfirexd.Attribute
import com.pivotal.gemfirexd.internal.iapi.util.IdUtil
import io.snappydata.Constant
import io.snappydata.sql.catalog.CatalogObjectType.getTableType
import io.snappydata.sql.catalog.SnappyExternalCatalog.{DBTABLE_PROPERTY, getTableWithSchema}
import io.snappydata.sql.catalog.{CatalogObjectType, SnappyExternalCatalog}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql._
import org.apache.spark.sql.catalog.Column
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, NoSuchFunctionException, NoSuchPartitionException}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, IdentifierWithDatabase, TableIdentifier}
import org.apache.spark.sql.collection.{ToolsCallbackInit, Utils}
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.execution.datasources.{DataSource, FindDataSourceTable, LogicalRelation}
import org.apache.spark.sql.hive.HiveSessionCatalog
import org.apache.spark.sql.policy.PolicyProperties
import org.apache.spark.sql.sources.{DestroyRelation, MutableRelation, RowLevelSecurityRelation}
import org.apache.spark.sql.types._
import org.apache.spark.util.MutableURLClassLoader

/**
 * ::DeveloperApi::
 * Catalog using Hive for persistence and adding Snappy extensions like
 * stream/topK tables and returning LogicalPlan to materialize these entities.
 */
@DeveloperApi
class SnappySessionCatalog(val externalCatalog: SnappyExternalCatalog,
    val snappySession: SnappySession,
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

  /**
   * Can be used to temporarily switch the metadata returned by catalog
   * to use CharType and VarcharTypes. Is to be used for only temporary
   * change by a caller that wishes the consume the result because rest
   * of Spark cannot deal with those types.
   */
  protected[sql] var convertCharTypesInMetadata = false

  private[this] var skipDefaultSchemas = false

  // initialize default schema
  val defaultSchemaName: String = {
    var defaultName = snappySession.conf.get(Attribute.USERNAME_ATTR, "")
    if (defaultName.isEmpty) {
      // In smart connector, property name is different.
      defaultName = snappySession.conf.get(Constant.SPARK_STORE_PREFIX + Attribute.USERNAME_ATTR,
        Constant.DEFAULT_SCHEMA)
    }
    defaultName = formatDatabaseName(IdUtil.getUserAuthorizationId(defaultName).replace('-', '_'))
    createSchema(defaultName, ignoreIfExists = true)
    setCurrentDatabase(defaultName)
    defaultName
  }

  final def getCurrentSchema: String = getCurrentDatabase

  /**
   * Format table name. Hive meta-store is case-insensitive so always convert to upper case.
   */
  override def formatTableName(name: String): String = Utils.toUpperCase(name)

  /**
   * Format schema name. Hive meta-store is case-insensitive so always convert to upper case.
   */
  override def formatDatabaseName(name: String): String = Utils.toUpperCase(name)

  final def caseSensitiveAnalysis: Boolean = sqlConf.caseSensitiveAnalysis

  /**
   * Fallback session state to lookup from external hive catalog in case
   * "snappydata.sql.enableHiveSupport" is set on the session.
   */
  private def hiveSessionCatalog: HiveSessionCatalog =
    snappySession.snappySessionState.hiveSessionCatalog

  def formatName(name: String): String = {
    if (caseSensitiveAnalysis) name else Utils.toUpperCase(name)
  }

  protected def schemaNotFoundException(schema: String): AnalysisException =
    new AnalysisException(s"Schema '$schema' not found")

  /** API to get primary key or Key Columns of a SnappyData table */
  def getKeyColumns(table: String): Seq[Column] = getKeyColumnsAndPositions(table).map(_._1)

  /** API to get primary key or Key Columns of a SnappyData table */
  def getKeyColumnsAndPositions(table: String): Seq[(Column, Int)] = {
    val tableIdent = snappySession.tableIdentifier(table)
    val relation = resolveRelation(tableIdent)
    val keyColumns = relation match {
      case LogicalRelation(mutable: MutableRelation, _, _) =>
        val keyCols = mutable.getPrimaryKeyColumns
        if (keyCols.isEmpty) {
          Nil
        } else {
          val tableMetadata = this.getTempViewOrPermanentTableMetadata(tableIdent)
          val tableSchema = tableMetadata.schema.zipWithIndex
          val fieldsInMetadata =
            keyCols.map(k => tableSchema.find(p => p._1.name.equalsIgnoreCase(k)) match {
              case None => throw new AnalysisException(s"Invalid key column name $k")
              case Some(p) => p
            })
          fieldsInMetadata.map { p =>
            val c = p._1
            new Column(
              name = Utils.toUpperCase(c.name),
              description = c.getComment().orNull,
              dataType = c.dataType.catalogString,
              nullable = c.nullable,
              isPartition = false, // Setting it to false for SD tables
              isBucket = false) -> p._2
          }
        }
      case _ => Nil
    }
    keyColumns
  }

  private def normalizeType(dataType: DataType): DataType = dataType match {
    case a: ArrayType => a.copy(elementType = normalizeType(a.elementType))
    case m: MapType => m.copy(keyType = normalizeType(m.keyType),
      valueType = normalizeType(m.valueType))
    case s: StructType => normalizeSchema(s)
    case _ => dataType
  }

  protected[sql] def normalizeField(f: StructField): StructField = {
    val name = formatName(f.name)
    val dataType = normalizeType(f.dataType)
    f.copy(name = name, dataType = dataType)
  }

  def normalizeSchema(schema: StructType): StructType = {
    if (caseSensitiveAnalysis) {
      schema
    } else {
      val fields = schema.fields
      if (fields.exists(f => Utils.hasLowerCase(f.name))) {
        StructType(fields.map(normalizeField))
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

  final def getCombinedPolicyFilterForExternalTable(rlsRelation: RowLevelSecurityRelation,
      wrappingLogicalRelation: Option[LogicalRelation],
      currentUser: Option[String]): Option[Filter] = {
    // filter out policy rows
    // getCombinedPolicyFilter(rlsRelation, wrappingLogicalRelation, currentUser)
    None
  }

  final def getCombinedPolicyFilterForNativeTable(rlsRelation: RowLevelSecurityRelation,
      wrappingLogicalRelation: Option[LogicalRelation]): Option[Filter] = {
    // filter out policy rows
    getCombinedPolicyFilter(rlsRelation, wrappingLogicalRelation)
  }

  private def getCombinedPolicyFilter(rlsRelation: RowLevelSecurityRelation,
      wrappingLogicalRelation: Option[LogicalRelation]): Option[Filter] = {
    if (!rlsRelation.isRowLevelSecurityEnabled) {
      None
    } else {
      val catalogTable = getTableMetadata(new TableIdentifier(
        rlsRelation.tableName, Some(rlsRelation.schemaName)))
      val policyFilters = externalCatalog.getPolicies(rlsRelation.schemaName,
        rlsRelation.tableName, catalogTable.properties).map { ct =>
        resolveRelation(ct.identifier).asInstanceOf[BypassRowLevelSecurity].child
      }
      if (policyFilters.isEmpty) None
      else {
        val combinedPolicyFilters = policyFilters.foldLeft[Filter](null) {
          case (result, filter) =>
            if (result == null) {
              filter
            } else {
              result.copy(condition = org.apache.spark.sql.catalyst.expressions.And(
                filter.condition, result.condition))
            }
        }
        val storedLogicalRelation = resolveRelation(snappySession.tableIdentifier(
          rlsRelation.resolvedName)).find {
          case _: LogicalRelation => true
          case _ => false
        }.get.asInstanceOf[LogicalRelation]

        Some(remapFilterIfNeeded(combinedPolicyFilters, wrappingLogicalRelation,
          storedLogicalRelation))
      }
    }
  }

  private def remapFilterIfNeeded(filter: Filter, queryLR: Option[LogicalRelation],
      storedLR: LogicalRelation): Filter = {
    if (queryLR.isEmpty || queryLR.get.output.
        corresponds(storedLR.output)((a1, a2) => a1.exprId == a2.exprId)) {
      filter
    } else {
      // remap filter
      val mappingInfo = storedLR.output.map(_.exprId).zip(
        queryLR.get.output.map(_.exprId)).toMap
      filter.transformAllExpressions {
        case ar: AttributeReference if mappingInfo.contains(ar.exprId) =>
          AttributeReference(ar.name, ar.dataType, ar.nullable,
            ar.metadata)(mappingInfo(ar.exprId), ar.qualifier, ar.isGenerated)
      }
    }
  }

  final def getSchemaName(identifier: IdentifierWithDatabase): String = identifier.database match {
    case None => getCurrentSchema
    case Some(s) => formatDatabaseName(s)
  }

  /** Add schema to TableIdentifier if missing and format the name. */
  final def resolveTableIdentifier(identifier: TableIdentifier): TableIdentifier = {
    TableIdentifier(formatTableName(identifier.table), Some(getSchemaName(identifier)))
  }

  /**
   * Lookup relation and resolve to a LogicalRelation if not a temporary view.
   */
  final def resolveRelationWithAlias(tableIdent: TableIdentifier): LogicalPlan = {
    // resolve the relation right away with alias around
    new FindDataSourceTable(snappySession)(lookupRelation(tableIdent))
  }

  /**
   * Lookup relation and resolve to a LogicalRelation if not a temporary view.
   */
  final def resolveRelation(tableIdent: TableIdentifier): LogicalPlan = {
    // resolve the relation right away
    resolveRelationWithAlias(tableIdent).asInstanceOf[SubqueryAlias].child
  }

  // NOTE: Many of the overrides below are due to SnappyData allowing absence of
  // "global_temp" schema to access global temporary views. Secondly checking for
  // schema access permissions and creating schema implicitly if required.

  /**
   * SnappyData allows the schema for global temporary views to be optional so this method
   * adds it to TableIdentifier if required so that super methods can be invoked directly.
   */
  protected def addMissingGlobalTempSchema(name: TableIdentifier): TableIdentifier = {
    if (name.database.isEmpty) {
      val tableName = formatTableName(name.table)
      if (globalTempViewManager.get(tableName).isDefined) {
        name.copy(table = tableName, database = Some(globalTempViewManager.database))
      } else name
    } else name
  }

  private[sql] def checkSchemaPermission(schema: String, table: String,
      defaultUser: String, ignoreIfNotExists: Boolean = false): String = {
    SnappyExternalCatalog.checkSchemaPermission(schema, table, defaultUser,
      snappySession.conf, ignoreIfNotExists)
  }

  protected[sql] def validateSchemaName(schemaName: String, checkForDefault: Boolean): Unit = {
    if (schemaName == globalTempViewManager.database) {
      throw new AnalysisException(s"$schemaName is a system reserved schema for global " +
          s"temporary tables. You cannot create, drop or set a schema with this name.")
    }
    if (checkForDefault && schemaName == SnappyExternalCatalog.SPARK_DEFAULT_SCHEMA) {
      throw new AnalysisException(s"$schemaName is a system reserved schema.")
    }
  }

  def isLocalTemporaryView(name: TableIdentifier): Boolean = synchronized {
    name.database.isEmpty && tempTables.contains(formatTableName(name.table))
  }

  /**
   * Same as createDatabase but uses pre-defined defaults for CatalogDatabase.
   * The passed schemaName should already be formatted by a call to [[formatDatabaseName]].
   */
  private[sql] def createSchema(schemaName: String, ignoreIfExists: Boolean): Unit = {
    validateSchemaName(schemaName, checkForDefault = false)
    if (externalCatalog.databaseExists(schemaName)) {
      if (!ignoreIfExists) throw new AnalysisException(s"Schema '$schemaName' already exists")
    } else {
      createDatabase(CatalogDatabase(schemaName, s"User $schemaName schema",
        getDefaultDBPath(schemaName), Map.empty), ignoreIfExists)
    }
  }

  private[sql] def setCurrentSchema(schema: String): Unit = {
    val schemaName = formatDatabaseName(schema)
    if (schemaName != getCurrentSchema) {
      // create the schema implicitly if not present
      createSchema(schemaName, ignoreIfExists = true)
      setCurrentDatabase(schemaName)
      // invalidate cached plans which could be referring to old current schema
      snappySession.clearPlanCache()
    }
  }

  override def createDatabase(schemaDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    validateSchemaName(formatDatabaseName(schemaDefinition.name), checkForDefault = false)
    super.createDatabase(schemaDefinition, ignoreIfExists)
  }

  /**
   * Drop all the objects in a schema. The provided schema must already be formatted
   * with a call to [[formatDatabaseName]].
   */
  def dropAllSchemaObjects(schemaName: String, ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = {
    if (schemaName == SnappyExternalCatalog.SYS_SCHEMA) {
      throw new AnalysisException(s"$schemaName is a system reserved schema")
    }

    if (!externalCatalog.databaseExists(schemaName)) {
      if (ignoreIfNotExists) return
      else throw schemaNotFoundException(schemaName)
    }
    checkSchemaPermission(schemaName, table = "", defaultUser = null, ignoreIfNotExists)

    if (cascade) {
      // drop all the tables in order first, dependents followed by others
      val allTables = externalCatalog.listTables(schemaName).flatMap(
        table => externalCatalog.getTableOption(schemaName, table))
      if (allTables.nonEmpty) {
        val (dependents, others) = allTables.partition(t => t.tableType ==
            CatalogTableType.VIEW || externalCatalog.getBaseTable(t).isDefined)
        dependents.foreach(d => snappySession.dropTable(d.identifier, ifExists = true,
          d.tableType == CatalogTableType.VIEW))
        // drop streams at last
        val (streams, tables) = others.partition(getTableType(_) == CatalogObjectType.Stream)
        tables.foreach(t => snappySession.dropTable(t.identifier.unquotedString, ifExists = true))
        if (streams.nonEmpty) {
          streams.foreach(s => snappySession.dropTable(
            s.identifier.unquotedString, ifExists = true))
        }
      }

      // drop all the functions
      val allFunctions = listFunctions(schemaName)
      if (allFunctions.nonEmpty) {
        allFunctions.foreach(f => dropFunction(f._1, ignoreIfNotExists = true))
      }
    }
  }

  override def dropDatabase(schema: String, ignoreIfNotExists: Boolean,
      cascade: Boolean): Unit = {
    val schemaName = formatDatabaseName(schema)
    // user cannot drop own schema
    if (schemaName == defaultSchemaName) {
      throw new AnalysisException(s"Cannot drop own schema $schemaName")
    }
    validateSchemaName(formatDatabaseName(schemaName), checkForDefault = true)
    dropAllSchemaObjects(schemaName, ignoreIfNotExists, cascade)

    super.dropDatabase(schemaName, ignoreIfNotExists, cascade)
  }

  override def setCurrentDatabase(schema: String): Unit = {
    val schemaName = formatDatabaseName(schema)
    validateSchemaName(schemaName, checkForDefault = false)
    super.setCurrentDatabase(schemaName)
    externalCatalog.setCurrentDatabase(schemaName)
  }

  override def getDatabaseMetadata(schema: String): CatalogDatabase = {
    val schemaName = formatDatabaseName(schema)
    if (!databaseExists(schemaName)) {
      if (snappySession.enableHiveSupport) {
        if (!hiveSessionCatalog.databaseExists(schema)) throw schemaNotFoundException(schema)
        hiveSessionCatalog.getDatabaseMetadata(schema)
      } else throw schemaNotFoundException(schema)
    }
    externalCatalog.getDatabase(schemaName)
  }

  override def listDatabases(): Seq[String] = synchronized {
    if (skipDefaultSchemas) {
      listAllDatabases().filter(s => s != SnappyExternalCatalog.SPARK_DEFAULT_SCHEMA &&
          s != SnappyExternalCatalog.SYS_SCHEMA && s != defaultSchemaName)
    } else listAllDatabases()
  }

  private def listAllDatabases(): Seq[String] = {
    if (snappySession.enableHiveSupport) {
      (super.listDatabases().toSet ++
          hiveSessionCatalog.listDatabases().map(formatDatabaseName).toSet).toSeq
    } else super.listDatabases()
  }

  override def listDatabases(pattern: String): Seq[String] = {
    if (snappySession.enableHiveSupport) {
      (super.listDatabases(pattern).toSet ++
          hiveSessionCatalog.listDatabases(pattern).map(formatDatabaseName).toSet).toSeq
    } else super.listDatabases(pattern)
  }

  override def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit = {
    // first check required permission to create objects in a schema
    val schemaName = getSchemaName(table.identifier)
    checkSchemaPermission(schemaName, table.identifier.table, defaultUser = null)
    createSchema(schemaName, ignoreIfExists = true)

    super.createTable(table, ignoreIfExists)
  }

  /**
   * Create catalog object for a BaseRelation backed by a Region in store or GemFire.
   *
   * This method is to be used for pre-entry into the catalog during a CTAS execution
   * for the inserts to proceed (which themselves may require the catalog entry
   * on executors). The GemFire provider uses it in a special way to update
   * the options stored for the catalog entry.
   */
  private[sql] def createTableForBuiltin(fullTableName: String, provider: String,
      schema: StructType, options: Map[String, String], ignoreIfExists: Boolean): Unit = {
    assert(CatalogObjectType.isTableBackedByRegion(SnappyContext.getProviderType(provider)) ||
        CatalogObjectType.isGemFireProvider(provider))
    val (schemaName, tableName) = getTableWithSchema(fullTableName, getCurrentSchema)
    assert(schemaName.length > 0)
    val catalogTable = CatalogTable(new TableIdentifier(tableName, Some(schemaName)),
      CatalogTableType.EXTERNAL, DataSource.buildStorageFormatFromOptions(
        options + (DBTABLE_PROPERTY -> fullTableName)), schema, Some(provider))
    createTable(catalogTable, ignoreIfExists)
  }

  private def convertCharTypes(table: CatalogTable): CatalogTable = {
    if (convertCharTypesInMetadata) table.copy(schema = StructType(table.schema.map { field =>
      field.dataType match {
        case StringType if field.metadata.contains(Constant.CHAR_TYPE_BASE_PROP) =>
          val md = field.metadata
          md.getString(Constant.CHAR_TYPE_BASE_PROP) match {
            case "CHAR" =>
              field.copy(dataType = CharType(md.getLong(Constant.CHAR_TYPE_SIZE_PROP).toInt))
            case "VARCHAR" =>
              field.copy(dataType = VarcharType(md.getLong(Constant.CHAR_TYPE_SIZE_PROP).toInt))
            case _ => field
          }
        case _ => field
      }
    })) else table
  }

  override def getTableMetadata(name: TableIdentifier): CatalogTable = {
    super.getTableMetadataOption(name) match {
      case None =>
        if (snappySession.enableHiveSupport) hiveSessionCatalog.getTableMetadata(name)
        else throw new TableNotFoundException(getSchemaName(name), name.table)
      case Some(table) => convertCharTypes(table)
    }
  }

  override def getTableMetadataOption(name: TableIdentifier): Option[CatalogTable] = {
    super.getTableMetadataOption(name) match {
      case None =>
        if (snappySession.enableHiveSupport) hiveSessionCatalog.getTableMetadataOption(name)
        else None
      case Some(table) => Some(convertCharTypes(table))
    }
  }

  override def dropTable(tableIdent: TableIdentifier, ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = synchronized {
    val name = addMissingGlobalTempSchema(tableIdent)

    if (isTemporaryTable(name)) {
      dropTemporaryTable(name)
    } else {
      val schema = getSchemaName(name)
      val table = formatTableName(name.table)
      checkSchemaPermission(schema, table, defaultUser = null)
      // resolve the table and destroy underlying storage if possible
      externalCatalog.getTableOption(schema, table) match {
        case None =>
          if (ignoreIfNotExists) return else throw new TableNotFoundException(schema, table)
        case Some(metadata) =>
          // fail if there are any existing dependents except policies
          val dependents = externalCatalog.getDependents(schema, table,
            externalCatalog.getTable(schema, table), Nil, CatalogObjectType.Policy :: Nil)
          if (dependents.nonEmpty) {
            throw new AnalysisException(s"Object $schema.$table cannot be dropped because of " +
                s"dependent objects: ${dependents.map(_.identifier.unquotedString).mkString(",")}")
          }
          // remove from temporary base table if applicable
          dropFromTemporaryBaseTable(metadata)
          metadata.provider match {
            case Some(provider) if provider != DDLUtils.HIVE_PROVIDER =>
              val relation = try {
                DataSource(snappySession, provider, userSpecifiedSchema = Some(metadata.schema),
                  partitionColumns = metadata.partitionColumnNames,
                  bucketSpec = metadata.bucketSpec,
                  options = metadata.storage.properties).resolveRelation()
              } catch {
                case NonFatal(_) => null // ignore any exception in class lookup
              }
              relation match {
                case d: DestroyRelation => d.destroy(ignoreIfNotExists)
                case _ =>
              }
            case _ =>
          }
      }
    }
    super.dropTable(name, ignoreIfNotExists, purge)
  }

  protected def dropTemporaryTable(tableIdent: TableIdentifier): Unit = {}

  protected def dropFromTemporaryBaseTable(table: CatalogTable): Unit = {}

  override def alterTable(table: CatalogTable): Unit = {
    // first check required permission to alter objects in a schema
    val schemaName = getSchemaName(table.identifier)
    checkSchemaPermission(schemaName, table.identifier.table, defaultUser = null)

    super.alterTable(table)
  }

  override def alterTempViewDefinition(name: TableIdentifier,
      viewDefinition: LogicalPlan): Boolean = {
    super.alterTempViewDefinition(addMissingGlobalTempSchema(name), viewDefinition)
  }

  override def getTempViewOrPermanentTableMetadata(name: TableIdentifier): CatalogTable =
    convertCharTypes(super.getTempViewOrPermanentTableMetadata(addMissingGlobalTempSchema(name)))

  override def renameTable(oldName: TableIdentifier, newName: TableIdentifier): Unit = {
    // first check required permission to alter objects in a schema
    val oldSchemaName = getSchemaName(oldName)
    checkSchemaPermission(oldSchemaName, oldName.table, defaultUser = null)
    val newSchemaName = getSchemaName(newName)
    checkSchemaPermission(newSchemaName, newName.table, defaultUser = null)

    // in-built tables don't support rename yet
    super.getTableMetadataOption(oldName) match {
      case Some(table) if DDLUtils.isDatasourceTable(table) &&
          SnappyContext.isBuiltInProvider(table.provider.get) =>
        throw new UnsupportedOperationException(
          s"Table $oldName having provider '${table.provider.get}' does not support rename")
      case _ =>
    }
    super.renameTable(addMissingGlobalTempSchema(oldName), newName)
  }

  override def loadTable(table: TableIdentifier, loadPath: String, isOverwrite: Boolean,
      holdDDLTime: Boolean): Unit = {
    // first check required permission to alter objects in a schema
    val schemaName = getSchemaName(table)
    checkSchemaPermission(schemaName, table.table, defaultUser = null)

    super.loadTable(table, loadPath, isOverwrite, holdDDLTime)
  }

  def createPolicy(
      policyIdent: TableIdentifier,
      targetTable: TableIdentifier,
      policyFor: String,
      policyApplyTo: Seq[String],
      expandedPolicyApplyTo: Seq[String],
      currentUser: String,
      filterString: String): Unit = {

    // first check required permission to create objects in a schema
    val schemaName = getSchemaName(policyIdent)
    val policyName = formatTableName(policyIdent.table)
    val targetIdent = resolveTableIdentifier(targetTable)
    val targetSchema = targetIdent.database.get
    // Target table schema should be writable as well as own.
    // Owner of the target table schema has full permissions on it so becomes
    // the policy owner too (can be an ldap group).
    val owner = checkSchemaPermission(targetSchema, policyName, currentUser)
    if (targetSchema != schemaName) {
      checkSchemaPermission(schemaName, policyName, currentUser)
    }
    createSchema(schemaName, ignoreIfExists = true)

    externalCatalog.createPolicy(schemaName, policyName, targetIdent.unquotedString,
      policyFor, policyApplyTo, expandedPolicyApplyTo, owner, filterString)
  }

  private def getPolicyPlan(table: CatalogTable): LogicalPlan = {
    val parser = snappySession.sessionState.sqlParser
    val filterExpression = table.properties.get(PolicyProperties.filterString) match {
      case Some(e) => parser.parseExpression(e)
      case None => throw new IllegalStateException("Filter for the policy not found")
    }
    val tableIdent = table.properties.get(PolicyProperties.targetTable) match {
      case Some(t) => snappySession.tableIdentifier(t)
      case None => throw new IllegalStateException("Target Table for the policy not found")
    }
    /* val targetRelation = snappySession.sessionState.catalog.lookupRelation(tableIdent)
     val isTargetExternalRelation = targetRelation.find(x => x match {
       case _: ExternalRelation => true
       case _ => false
     }).isDefined
     */
    PolicyProperties.createFilterPlan(filterExpression, tableIdent,
      table.properties(PolicyProperties.policyOwner),
      table.properties(PolicyProperties.expandedPolicyApplyTo).split(",").
          toSeq.filterNot(_.isEmpty))
  }

  override def lookupRelation(name: TableIdentifier, alias: Option[String]): LogicalPlan = {
    synchronized {
      val tableName = formatTableName(name.table)
      var view: Option[TableIdentifier] = Some(name)
      val relationPlan = (if (name.database.isEmpty) {
        tempTables.get(tableName) match {
          case None => globalTempViewManager.get(tableName)
          case s => s
        }
      } else None) match {
        case None =>
          val schemaName =
            if (name.database.isEmpty) currentDb else formatDatabaseName(name.database.get)
          if (schemaName == globalTempViewManager.database) {
            globalTempViewManager.get(tableName) match {
              case None => throw new TableNotFoundException(schemaName, tableName)
              case Some(p) => p
            }
          } else {
            val table = externalCatalog.getTableOption(schemaName, tableName) match {
              case None =>
                if (snappySession.enableHiveSupport) {
                  return hiveSessionCatalog.lookupRelation(name, alias)
                } else throw new TableNotFoundException(schemaName, tableName)
              case Some(t) => t
            }
            if (table.tableType == CatalogTableType.VIEW) {
              if (table.viewText.isEmpty) sys.error("Invalid view without text.")
              snappySession.sessionState.sqlParser.parsePlan(table.viewText.get)
            } else if (CatalogObjectType.isPolicy(table)) {
              getPolicyPlan(table)
            } else {
              view = None
              SimpleCatalogRelation(schemaName, table)
            }
          }
        case Some(p) => p
      }
      SubqueryAlias(if (alias.isEmpty) tableName else alias.get, relationPlan, view)
    }
  }

  override def isTemporaryTable(name: TableIdentifier): Boolean = {
    if (name.database.isEmpty) synchronized {
      // check both local and global temporary tables
      val tableName = formatTableName(name.table)
      tempTables.contains(tableName) || globalTempViewManager.get(tableName).isDefined
    } else if (formatDatabaseName(name.database.get) == globalTempViewManager.database) {
      globalTempViewManager.get(formatTableName(name.table)).isDefined
    } else false
  }

  override def listTables(schema: String, pattern: String): Seq[TableIdentifier] = {
    val schemaName = formatDatabaseName(schema)
    if (schemaName != globalTempViewManager.database &&
        !externalCatalog.databaseExists(schemaName)) {
      throw schemaNotFoundException(schema)
    }
    if (snappySession.enableHiveSupport) {
      (super.listTables(schema, pattern).toSet ++
          hiveSessionCatalog.listTables(schema, pattern).map(id => TableIdentifier(
            formatTableName(id.table), id.database.map(formatDatabaseName))).toSet).toSeq
    } else super.listTables(schema, pattern)
  }

  override def refreshTable(name: TableIdentifier): Unit = {
    val table = addMissingGlobalTempSchema(name)
    if (isTemporaryTable(table)) {
      super.refreshTable(table)
    } else {
      val resolved = resolveTableIdentifier(table)
      externalCatalog.invalidate(resolved.database.get -> resolved.table)
    }
  }

  def getDataSourceRelations[T](tableType: CatalogObjectType.Type): Seq[T] = {
    externalCatalog.getAllTables().collect {
      case table if tableType == CatalogObjectType.getTableType(table) =>
        resolveRelation(table.identifier).asInstanceOf[LogicalRelation].relation.asInstanceOf[T]
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

  override def createPartitions(tableName: TableIdentifier, parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    // first check required permission to create objects in a schema
    val schemaName = getSchemaName(tableName)
    checkSchemaPermission(schemaName, tableName.table, defaultUser = null)

    super.createPartitions(tableName, parts, ignoreIfExists)
  }

  override def dropPartitions(tableName: TableIdentifier, specs: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean, purge: Boolean, retainData: Boolean): Unit = {
    // first check required permission to drop objects in a schema
    val schemaName = getSchemaName(tableName)
    checkSchemaPermission(schemaName, tableName.table, defaultUser = null)

    super.dropPartitions(tableName, specs, ignoreIfNotExists, purge, retainData)
  }

  override def alterPartitions(tableName: TableIdentifier,
      parts: Seq[CatalogTablePartition]): Unit = {
    // first check required permission to alter objects in a schema
    val schemaName = getSchemaName(tableName)
    checkSchemaPermission(schemaName, tableName.table, defaultUser = null)

    super.alterPartitions(tableName, parts)
  }

  override def renamePartitions(tableName: TableIdentifier, specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = {
    // first check required permission to alter objects in a schema
    val schemaName = getSchemaName(tableName)
    checkSchemaPermission(schemaName, tableName.table, defaultUser = null)

    super.renamePartitions(tableName, specs, newSpecs)
  }

  override def loadPartition(table: TableIdentifier, loadPath: String, spec: TablePartitionSpec,
      isOverwrite: Boolean, holdDDLTime: Boolean, inheritTableSpecs: Boolean): Unit = {
    // first check required permission to alter objects in a schema
    val schemaName = getSchemaName(table)
    checkSchemaPermission(schemaName, table.table, defaultUser = null)

    super.loadPartition(table, loadPath, spec, isOverwrite, holdDDLTime, inheritTableSpecs)
  }

  override def getPartition(tableName: TableIdentifier,
      spec: TablePartitionSpec): CatalogTablePartition = {
    if (snappySession.enableHiveSupport) {
      try {
        super.getPartition(tableName, spec)
      } catch {
        case _: NoSuchPartitionException => hiveSessionCatalog.getPartition(tableName, spec)
      }
    } else super.getPartition(tableName, spec)
  }

  override def listPartitionNames(tableName: TableIdentifier,
      partialSpec: Option[TablePartitionSpec] = None): Seq[String] = {
    if (snappySession.enableHiveSupport) {
      (super.listPartitionNames(tableName, partialSpec).toSet ++
          hiveSessionCatalog.listPartitionNames(tableName, partialSpec).toSet).toSeq
    } else super.listPartitionNames(tableName, partialSpec)
  }

  override def listPartitions(tableName: TableIdentifier,
      partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] = {
    if (snappySession.enableHiveSupport) {
      (super.listPartitions(tableName, partialSpec).toSet ++
          hiveSessionCatalog.listPartitions(tableName, partialSpec).toSet).toSeq
    } else super.listPartitions(tableName, partialSpec)
  }

  override def listPartitionsByFilter(tableName: TableIdentifier,
      predicates: Seq[Expression]): Seq[CatalogTablePartition] = {
    if (snappySession.enableHiveSupport) {
      (super.listPartitionsByFilter(tableName, predicates).toSet ++
          hiveSessionCatalog.listPartitionsByFilter(tableName, predicates).toSet).toSeq
    } else super.listPartitionsByFilter(tableName, predicates)
  }

  // TODO: SW: clean up function creation to be like Spark with backward compatibility

  override def loadFunctionResources(resources: Seq[FunctionResource]): Unit = {
    val qualifiedName = SnappyExternalCatalog.currentFunctionIdentifier.get()
    val functionQualifiedName = qualifiedName.unquotedString
    val parentLoader = org.apache.spark.util.Utils.getContextOrSparkClassLoader
    val callbacks = ToolsCallbackInit.toolsCallback
    val newClassLoader = ContextJarUtils.getDriverJar(functionQualifiedName) match {
      case None =>
        val urls = if (callbacks != null) {
          resources.map { r =>
            ContextJarUtils.fetchFile(functionQualifiedName, r.uri)
          }
        } else {
          resources.map { r =>
            toUrl(r)
          }
        }
        val newClassLoader = new MutableURLClassLoader(urls.toArray, parentLoader)
        ContextJarUtils.addDriverJar(functionQualifiedName, newClassLoader)
        newClassLoader

      case Some(c) => c
    }

    SnappyContext.getClusterMode(snappySession.sparkContext) match {
      case SnappyEmbeddedMode(_, _) =>
        callbacks.setSessionDependencies(snappySession.sparkContext,
          functionQualifiedName, newClassLoader)
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
    val schemaName = getSchemaName(name)
    // first check required permission to create objects in a schema
    checkSchemaPermission(schemaName, name.funcName, defaultUser = null)

    val qualifiedName = name.copy(database = Some(schemaName))
    ContextJarUtils.getDriverJar(qualifiedName.unquotedString) match {
      case Some(_) =>
        val catalogFunction = try {
          externalCatalog.getFunction(schemaName, qualifiedName.funcName)
        } catch {
          case _: AnalysisException => failFunctionLookup(qualifiedName.funcName)
        }
        removeFromFuncJars(catalogFunction, qualifiedName)
      case _ =>
    }
    super.dropFunction(name, ignoreIfNotExists)
  }

  override def createFunction(funcDefinition: CatalogFunction, ignoreIfExists: Boolean): Unit = {
    val schemaName = getSchemaName(funcDefinition.identifier)
    // first check required permission to create objects in a schema
    checkSchemaPermission(schemaName, funcDefinition.identifier.funcName, defaultUser = null)
    createSchema(schemaName, ignoreIfExists = true)

    super.createFunction(funcDefinition, ignoreIfExists)
  }

  override def makeFunctionBuilder(funcName: String, className: String): FunctionBuilder = {
    val uRLClassLoader = ContextJarUtils.getDriverJar(funcName) match {
      case None => org.apache.spark.util.Utils.getContextOrSparkClassLoader
      case Some(c) => c
    }
    val (actualClassName, typeName) = className.splitAt(className.lastIndexOf("__"))
    UDFFunction.makeFunctionBuilder(funcName,
      uRLClassLoader.loadClass(actualClassName),
      snappySession.snappySessionState.sqlParser.parseDataType(typeName.stripPrefix("__")))
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
  override def lookupFunction(name: FunctionIdentifier,
      children: Seq[Expression]): Expression = synchronized {
    // If the name itself is not qualified, add the current database to it.
    val database = getSchemaName(name)
    val qualifiedName = name.copy(database = Some(database))
    // for some reason Spark's lookup uses current schema rather than the schema of function
    val currentSchema = currentDb
    currentDb = database
    SnappyExternalCatalog.currentFunctionIdentifier.set(qualifiedName)
    try {
      super.lookupFunction(name, children)
    } catch {
      case _: NoSuchFunctionException if snappySession.enableHiveSupport =>
        // lookup in external hive catalog
      hiveSessionCatalog.lookupFunction(name, children)
    } finally {
      SnappyExternalCatalog.currentFunctionIdentifier.set(null)
      currentDb = currentSchema
    }
  }

  override def lookupFunctionInfo(name: FunctionIdentifier): ExpressionInfo = {
    // If the name itself is not qualified, add the current database to it.
    val database = getSchemaName(name)
    // for some reason Spark's lookup uses current schema rather than the schema of function
    val currentSchema = currentDb
    currentDb = database
    try {
      super.lookupFunctionInfo(name)
    } catch {
      case _: NoSuchFunctionException if snappySession.enableHiveSupport =>
        // lookup in external hive catalog
        hiveSessionCatalog.lookupFunctionInfo(name)
    } finally {
      currentDb = currentSchema
    }
  }

  override def getFunctionMetadata(name: FunctionIdentifier): CatalogFunction = {
    try {
      super.getFunctionMetadata(name)
    } catch {
      case _: NoSuchFunctionException if snappySession.enableHiveSupport =>
        // lookup in external hive catalog
        hiveSessionCatalog.getFunctionMetadata(name)
    }
  }

  override def listFunctions(schema: String,
      pattern: String): Seq[(FunctionIdentifier, String)] = {
    if (snappySession.enableHiveSupport) {
      (super.listFunctions(schema, pattern).toSet ++
          hiveSessionCatalog.listFunctions(schema, pattern).toSet).toSeq
    } else super.listFunctions(schema, pattern)
  }

  // -----------------
  // | Other methods |
  // -----------------

  /**
   * Test only method
   */
  def destroyAndRegisterBuiltInFunctionsForTests(): Unit = {
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

  override def reset(): Unit = synchronized {
    // flag to avoid listing the DEFAULT and SYS schemas to avoid attempting to drop them
    skipDefaultSchemas = true
    try {
      super.reset()
    } finally {
      skipDefaultSchemas = false
    }
  }
}
