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
package io.snappydata.sql.catalog

import java.sql.SQLException
import java.util.Collections
import javax.annotation.concurrent.GuardedBy

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.internal.cache.LocalRegion
import com.google.common.cache.{Cache, CacheBuilder}
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import io.snappydata.impl.SmartConnectorRDDHelper
import io.snappydata.thrift._

import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionException, NoSuchPermanentFunctionException}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BoundReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Statistics}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.collection.Utils.EMPTY_STRING_ARRAY
import org.apache.spark.sql.collection.{SmartExecutorBucketPartition, Utils}
import org.apache.spark.sql.execution.RefreshMetadata
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.{SnappySession, TableNotFoundException}
import org.apache.spark.{Logging, Partition}

/**
 * An ExternalCatalog implementation for the smart connector mode.
 *
 * Note that unlike other ExternalCatalog implementations, this is created one for each session
 * rather than being a singleton in the SharedState because each request needs to be authenticated
 * independently using the credentials of the user that created the session. Consequently calls
 * to "sharedState.externalCatalog()" will return null in smart connector mode and should never
 * be used. For internal code paths in Spark that use it, an alternative dummy global might
 * be added later that switches the user authentication using thread-locals or similar, but as
 * of now it is used only by some hive insert paths which are not used in SnappySessionState.
 */
class ConnectorExternalCatalog(val session: SnappySession)
    extends SnappyExternalCatalog with Logging {

  @GuardedBy("this")
  protected var connectorHelper: SmartConnectorHelper = new SmartConnectorHelper(session)

  protected def withExceptionHandling[T](function: => T): T = synchronized {
    try {
      function
    } catch {
      case e: SQLException if isConnectionException(e) =>
        // attempt to create a new connection
        connectorHelper.close()
        connectorHelper = new SmartConnectorHelper(session)
        function
    }
  }

  protected def isConnectionException(e: SQLException): Boolean = {
    e.getSQLState.startsWith(SQLState.CONNECTIVITY_PREFIX) ||
        e.getSQLState.startsWith(SQLState.LANG_DEAD_STATEMENT) ||
        e.getSQLState.startsWith(SQLState.GFXD_NODE_SHUTDOWN_PREFIX)
  }

  override def invalidateCaches(relations: Seq[(String, String)]): Unit = {
    // invalidation of a single table can result in all cached RelationInfo being
    // out of date due to lower schema version, so always invalidate all
    invalidateAll()
    // there is no version update in this call here, rather only the caches are cleared
    RefreshMetadata.executeLocal(RefreshMetadata.UPDATE_CATALOG_SCHEMA_VERSION, args = null)
  }

  override def invalidate(name: (String, String)): Unit = {
    // invalidation of a single table can result in all cached RelationInfo being
    // out of date due to lower schema version, so always invalidate all
    invalidateAll()
  }

  override def invalidateAll(): Unit = {
    // invalidate all the RelationInfo objects inside as well as the cache itself
    val iter = ConnectorExternalCatalog.cachedCatalogTables.asMap().values().iterator()
    while (iter.hasNext) {
      iter.next()._2 match {
        case Some(info) => info.invalid = true
        case None =>
      }
    }
    ConnectorExternalCatalog.cachedCatalogTables.invalidateAll()
  }

  // Using a common procedure to update catalog meta-data for create/drop/alter methods
  // and likewise a common procedure to get catalog meta-data for get/exists/list methods

  override def createDatabase(schemaDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    val request = new CatalogMetadataDetails()
    request.setCatalogSchema(new CatalogSchemaObject(schemaDefinition.name,
      schemaDefinition.description, schemaDefinition.locationUri,
      schemaDefinition.properties.asJava))
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_CREATE_SCHEMA, request))
  }

  override def dropDatabase(schema: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames(Collections.singletonList(schema)).setExists(ignoreIfNotExists)
        .setOtherFlags(Collections.singletonList(flag(cascade)))
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_DROP_SCHEMA, request))
  }

  override def getDatabase(schema: String): CatalogDatabase = {
    if (schema == SnappyExternalCatalog.SYS_SCHEMA) return systemSchemaDefinition
    val request = new CatalogMetadataRequest()
    request.setSchemaName(schema)
    val result = withExceptionHandling(connectorHelper.getCatalogMetadata(
      snappydataConstants.CATALOG_GET_SCHEMA, request))
    if (result.isSetCatalogSchema) {
      val schemaObj = result.getCatalogSchema
      CatalogDatabase(name = schemaObj.getName, description = schemaObj.getDescription,
        locationUri = schemaObj.getLocationUri, properties = schemaObj.getProperties.asScala.toMap)
    } else throw schemaNotFoundException(schema)
  }

  override def databaseExists(schema: String): Boolean = {
    // this is invoked frequently so instead of messaging right away, check the common
    // case if there exists a cached table in the schema
    if (schema == SnappyExternalCatalog.SYS_SCHEMA) true
    else {
      val itr = ConnectorExternalCatalog.cachedCatalogTables.asMap().keySet().iterator()
      while (itr.hasNext) {
        val tableWithSchema = itr.next()
        if (tableWithSchema._1 == schema) return true
      }
      val request = new CatalogMetadataRequest()
      request.setSchemaName(schema)
      withExceptionHandling(connectorHelper.getCatalogMetadata(
        snappydataConstants.CATALOG_SCHEMA_EXISTS, request)).exists
    }
  }

  override def listDatabases(): Seq[String] = listDatabases("*")

  override def listDatabases(pattern: String): Seq[String] = {
    val request = new CatalogMetadataRequest()
    request.setNameOrPattern(pattern)
    withExceptionHandling(connectorHelper.getCatalogMetadata(
      snappydataConstants.CATALOG_LIST_SCHEMAS, request)).getNames.asScala
  }

  override def setCurrentDatabase(schema: String): Unit = {
    connectorHelper.setCurrentSchema(schema)
  }

  override def createTable(table: CatalogTable, ignoreIfExists: Boolean): Unit = {
    val request = new CatalogMetadataDetails()
    request.setCatalogTable(ConnectorExternalCatalog.convertFromCatalogTable(table))
        .setExists(ignoreIfExists)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_CREATE_TABLE, request))

    // version stored in RelationInfo will be out-of-date now for all tables so clear everything
    invalidateCaches(Nil)
  }

  override def dropTable(schema: String, table: String, ignoreIfNotExists: Boolean,
      purge: Boolean): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames((schema :: table :: Nil).asJava).setExists(ignoreIfNotExists)
        .setOtherFlags(Collections.singletonList(flag(purge)))
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_DROP_TABLE, request))

    // version stored in RelationInfo will be out-of-date now for all tables so clear everything
    invalidateCaches(Nil)
  }

  override def alterTable(table: CatalogTable): Unit = {
    val request = new CatalogMetadataDetails()
    request.setCatalogTable(ConnectorExternalCatalog.convertFromCatalogTable(table))
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_ALTER_TABLE, request))

    // version stored in RelationInfo will be out-of-date now for all tables so clear everything
    invalidateCaches(Nil)
  }

  override def renameTable(schemaName: String, oldName: String, newName: String): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames((schemaName :: oldName :: newName :: Nil).asJava)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_RENAME_TABLE, request))

    // version stored in RelationInfo will be out-of-date now for all tables so clear everything
    invalidateCaches(Nil)
  }

  override def createPolicy(schemaName: String, policyName: String, targetTable: String,
      policyFor: String, policyApplyTo: Seq[String], expandedPolicyApplyTo: Seq[String],
      owner: String, filterString: String): Unit = {
    throw Utils.analysisException("CREATE POLICY for Row Level Security " +
        "not supported for smart connector mode")
  }

  override protected def getCachedCatalogTable(schema: String, table: String): CatalogTable = {
    ConnectorExternalCatalog.getCatalogTable(schema -> table, catalog = this)
  }

  override def getTableOption(schema: String, table: String): Option[CatalogTable] = {
    try {
      Some(getTable(schema, table))
    } catch {
      case _: TableNotFoundException => None
    }
  }

  override def getRelationInfo(schema: String, table: String,
      rowTable: Boolean): (RelationInfo, Option[LocalRegion]) = {
    if (schema == SnappyExternalCatalog.SYS_SCHEMA) {
      // SYS tables are treated as single partition replicated tables visible
      // from all executors using the JDBC connection
      RelationInfo(1, isPartitioned = false, partitions = Array(
        new SmartExecutorBucketPartition(0, 0, ArrayBuffer.empty))) -> None
    } else {
      assert(schema.length > 0)
      ConnectorExternalCatalog.getRelationInfo(schema -> table, catalog = this) match {
        case None => throw new TableNotFoundException(schema, table, Some(new RuntimeException(
          "RelationInfo for the table is missing. Its region may have been destroyed.")))
        case Some(r) => r -> None
      }
    }
  }

  override def tableExists(schema: String, table: String): Boolean = {
    if (ConnectorExternalCatalog.cachedCatalogTables.getIfPresent(schema -> table) ne null) true
    else {
      val request = new CatalogMetadataRequest()
      request.setSchemaName(schema).setNameOrPattern(table)
      withExceptionHandling(connectorHelper.getCatalogMetadata(
        snappydataConstants.CATALOG_TABLE_EXISTS, request)).exists
    }
  }

  override def listTables(schema: String): Seq[String] = listTables(schema, "*")

  override def listTables(schema: String, pattern: String): Seq[String] = {
    val request = new CatalogMetadataRequest()
    request.setSchemaName(schema).setNameOrPattern(pattern)
    withExceptionHandling(connectorHelper.getCatalogMetadata(
      snappydataConstants.CATALOG_LIST_TABLES, request)).getNames.asScala
  }

  private def flag(b: Boolean): java.lang.Integer = if (b) 1 else 0

  override def loadTable(schema: String, table: String, loadPath: String,
      isOverwrite: Boolean, holdDDLTime: Boolean): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames((schema :: table :: loadPath :: Nil).asJava)
        .setOtherFlags((flag(isOverwrite) :: flag(holdDDLTime) :: Nil).asJava)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_LOAD_TABLE, request))

    invalidateCaches(schema -> table :: Nil)
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  override def createPartitions(schema: String, table: String, parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames((schema :: table :: Nil).asJava).setCatalogPartitions(parts.map(
      ConnectorExternalCatalog.convertFromCatalogPartition).asJava).setExists(ignoreIfExists)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_CREATE_PARTITIONS, request))

    invalidateCaches(schema -> table :: Nil)
  }

  override def dropPartitions(schema: String, table: String, parts: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean, purge: Boolean, retainData: Boolean): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames((schema :: table :: Nil).asJava).setProperties(parts.map(_.asJava).asJava)
        .setExists(ignoreIfNotExists)
        .setOtherFlags((flag(purge) :: flag(retainData) :: Nil).asJava)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_DROP_PARTITIONS, request))

    invalidateCaches(schema -> table :: Nil)
  }

  override def alterPartitions(schema: String, table: String,
      parts: Seq[CatalogTablePartition]): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames((schema :: table :: Nil).asJava).setCatalogPartitions(parts.map(
      ConnectorExternalCatalog.convertFromCatalogPartition).asJava)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_ALTER_PARTITIONS, request))

    invalidateCaches(schema -> table :: Nil)
  }

  override def renamePartitions(schema: String, table: String, specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames((schema :: table :: Nil).asJava).setProperties(specs.map(_.asJava).asJava)
        .setNewProperties(newSpecs.map(_.asJava).asJava)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_RENAME_PARTITIONS, request))

    invalidateCaches(schema -> table :: Nil)
  }

  override def loadPartition(schema: String, table: String, loadPath: String,
      partition: TablePartitionSpec, isOverwrite: Boolean, holdDDLTime: Boolean,
      inheritTableSpecs: Boolean): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames((schema :: table :: loadPath :: Nil).asJava)
        .setProperties(Collections.singletonList(partition.asJava)).setOtherFlags(
      (flag(isOverwrite) :: flag(holdDDLTime) :: flag(inheritTableSpecs) :: Nil).asJava)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_LOAD_PARTITION, request))

    invalidateCaches(schema -> table :: Nil)
  }

  override def loadDynamicPartitions(schema: String, table: String, loadPath: String,
      partition: TablePartitionSpec, replace: Boolean, numDP: Int, holdDDLTime: Boolean): Unit = {
    val request = new CatalogMetadataDetails()
    request.setNames((schema :: table :: loadPath :: Nil).asJava)
        .setProperties(Collections.singletonList(partition.asJava)).setOtherFlags(
      (flag(replace) :: Int.box(numDP) :: flag(holdDDLTime) :: Nil).asJava)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_LOAD_DYNAMIC_PARTITIONS, request))

    invalidateCaches(schema -> table :: Nil)
  }

  override def getPartition(schema: String, table: String,
      spec: TablePartitionSpec): CatalogTablePartition = {
    getPartitionOption(schema, table, spec) match {
      case Some(p) => p
      case None => throw new NoSuchPartitionException(schema, table, spec)
    }
  }

  override def getPartitionOption(schema: String, table: String,
      spec: TablePartitionSpec): Option[CatalogTablePartition] = {
    val request = new CatalogMetadataRequest()
    request.setSchemaName(schema).setNameOrPattern(table).setProperties(spec.asJava)
    val result = withExceptionHandling(connectorHelper.getCatalogMetadata(
      snappydataConstants.CATALOG_GET_PARTITION, request))
    if (result.getCatalogPartitionsSize == 1) {
      Some(ConnectorExternalCatalog.convertToCatalogPartition(result.getCatalogPartitions.get(0)))
    } else None
  }

  override def listPartitionNames(schema: String, table: String,
      partialSpec: Option[TablePartitionSpec]): Seq[String] = {
    val request = new CatalogMetadataRequest()
    request.setSchemaName(schema).setNameOrPattern(table)
    if (partialSpec.isDefined) request.setProperties(partialSpec.get.asJava)
    val result = withExceptionHandling(connectorHelper.getCatalogMetadata(
      snappydataConstants.CATALOG_LIST_PARTITION_NAMES, request))
    result.getNames.asScala
  }

  override def listPartitions(schema: String, table: String,
      partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] = {
    val request = new CatalogMetadataRequest()
    request.setSchemaName(schema).setNameOrPattern(table)
    if (partialSpec.isDefined) request.setProperties(partialSpec.get.asJava)
    val result = withExceptionHandling(connectorHelper.getCatalogMetadata(
      snappydataConstants.CATALOG_LIST_PARTITIONS, request))
    if (result.getCatalogPartitionsSize > 0) {
      result.getCatalogPartitions.asScala.map(ConnectorExternalCatalog.convertToCatalogPartition)
    } else Nil
  }

  override def listPartitionsByFilter(schema: String, table: String,
      predicates: Seq[Expression]): Seq[CatalogTablePartition] = {
    // taken from HiveExternalCatalog.listPartitionsByFilter
    val catalogTable = getTable(schema, table)
    val partitionColumnNames = catalogTable.partitionColumnNames.toSet
    val nonPartitionPruningPredicates = predicates.filterNot {
      _.references.map(_.name).toSet.subsetOf(partitionColumnNames)
    }
    if (nonPartitionPruningPredicates.nonEmpty) {
      throw new IllegalArgumentException("Expected only partition pruning predicates: " +
          predicates.reduceLeft(And))
    }

    val partitionSchema = catalogTable.partitionSchema
    val partitions = listPartitions(schema, table, None)
    if (predicates.nonEmpty) {
      val boundPredicate = predicates.reduce(And).transform {
        case attr: AttributeReference =>
          val index = partitionSchema.indexWhere(_.name == attr.name)
          BoundReference(index, partitionSchema(index).dataType, nullable = true)
      }
      partitions.filter(p => boundPredicate.eval(p.toRow(partitionSchema)).asInstanceOf[Boolean])
    } else partitions
  }

  override def createFunction(schema: String, function: CatalogFunction): Unit = {
    val request = new CatalogMetadataDetails()
    request.setCatalogFunction(ConnectorExternalCatalog.convertFromCatalogFunction(function))
        .setNames(Collections.singletonList(schema))
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_CREATE_FUNCTION, request))
  }

  override def dropFunction(schema: String, funcName: String): Unit = {
    val request = new CatalogMetadataDetails().setNames((schema :: funcName :: Nil).asJava)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_DROP_FUNCTION, request))
  }

  override def renameFunction(schema: String, oldName: String, newName: String): Unit = {
    val request = new CatalogMetadataDetails()
        .setNames((schema :: oldName :: newName :: Nil).asJava)
    withExceptionHandling(connectorHelper.updateCatalogMetadata(
      snappydataConstants.CATALOG_RENAME_FUNCTION, request))
  }

  override def getFunction(schema: String, funcName: String): CatalogFunction = {
    val request = new CatalogMetadataRequest()
    request.setSchemaName(schema).setNameOrPattern(funcName)
    val result = withExceptionHandling(connectorHelper.getCatalogMetadata(
      snappydataConstants.CATALOG_GET_FUNCTION, request))
    if (result.isSetCatalogFunction) {
      ConnectorExternalCatalog.convertToCatalogFunction(result.getCatalogFunction)
    } else throw new NoSuchPermanentFunctionException(schema, funcName)
  }

  override def functionExists(schema: String, funcName: String): Boolean = {
    val request = new CatalogMetadataRequest()
    request.setSchemaName(schema).setNameOrPattern(funcName)
    withExceptionHandling(connectorHelper.getCatalogMetadata(
      snappydataConstants.CATALOG_FUNCTION_EXISTS, request)).exists
  }

  override def listFunctions(schema: String, pattern: String): Seq[String] = {
    val request = new CatalogMetadataRequest()
    request.setSchemaName(schema).setNameOrPattern(pattern)
    withExceptionHandling(connectorHelper.getCatalogMetadata(
      snappydataConstants.CATALOG_LIST_FUNCTIONS, request)).getNames.asScala
  }

  override def close(): Unit = connectorHelper.close()
}

object ConnectorExternalCatalog extends Logging {

  /** A cache of Spark SQL data source tables that have been accessed. */
  private lazy val cachedCatalogTables: Cache[(String, String),
      (CatalogTable, Option[RelationInfo])] = {
    CacheBuilder.newBuilder().maximumSize(SnappyExternalCatalog.cacheSize).build()
  }

  private def toArray(columns: java.util.List[String]): Array[String] =
    columns.toArray(new Array[String](columns.size()))

  private def convertToCatalogStorage(storage: CatalogStorage,
      storageProps: Map[String, String]): CatalogStorageFormat = {
    CatalogStorageFormat(Option(storage.getLocationUri), Option(storage.getInputFormat),
      Option(storage.getOutputFormat), Option(storage.getSerde), storage.compressed, storageProps)
  }

  private[snappydata] def convertToCatalogTable(request: CatalogMetadataDetails,
      session: SnappySession): (CatalogTable, Option[RelationInfo]) = {
    val tableObj = request.getCatalogTable
    val identifier = TableIdentifier(tableObj.getTableName, Option(tableObj.getSchemaName))
    val tableType = tableObj.getTableType match {
      case "EXTERNAL" => CatalogTableType.EXTERNAL
      case "MANAGED" => CatalogTableType.MANAGED
      case "VIEW" => CatalogTableType.VIEW
    }
    val tableProps = tableObj.getProperties.asScala.toMap
    val storage = tableObj.getStorage
    val storageProps = storage.properties.asScala.toMap
    val schema = ExternalStoreUtils.getTableSchema(tableObj.getTableSchema)
    // SnappyData tables have bucketOwners while hive managed tables have bucketColumns
    // The bucketSpec below is only for hive managed tables.
    val bucketSpec = if (tableObj.getBucketColumns.isEmpty) None
    else {
      Some(BucketSpec(tableObj.getNumBuckets, tableObj.getBucketColumns.asScala,
        tableObj.getSortColumns.asScala))
    }
    val stats = if (tableObj.isSetSizeInBytes) {
      val colStatMaps = tableObj.getColStats.asScala
      val colStats = schema.indices.flatMap { i =>
        val f = schema(i)
        val colStatsMap = colStatMaps(i)
        if (colStatsMap.isEmpty) None
        else ColumnStat.fromMap(identifier.unquotedString, f, colStatsMap.asScala.toMap) match {
          case None => None
          case Some(s) => Some(f.name -> s)
        }
      }.toMap
      Some(Statistics(tableObj.getSizeInBytes,
        if (tableObj.isSetRowCount) Some(tableObj.getRowCount) else None,
        colStats, tableObj.isBroadcastable))
    } else None
    val bucketOwners = tableObj.getBucketOwners
    // remove partitioning columns from CatalogTable for row/column tables
    val partitionCols = if (bucketOwners.isEmpty) Utils.EMPTY_STRING_ARRAY
    else {
      val cols = tableObj.getPartitionColumns
      tableObj.setPartitionColumns(Collections.emptyList())
      toArray(cols)
    }
    val table = CatalogTable(identifier, tableType, ConnectorExternalCatalog
        .convertToCatalogStorage(storage, storageProps), schema, Option(tableObj.getProvider),
      tableObj.getPartitionColumns.asScala, bucketSpec, tableObj.getOwner, tableObj.createTime,
      tableObj.lastAccessTime, tableProps, stats, Option(tableObj.getViewOriginalText),
      Option(tableObj.getViewText), Option(tableObj.getComment),
      tableObj.getUnsupportedFeatures.asScala, tableObj.tracksPartitionsInCatalog,
      tableObj.schemaPreservesCase)

    // if catalog schema version is not set then it indicates that RelationInfo was not filled
    // in due to region being destroyed or similar exception
    if (!request.isSetCatalogSchemaVersion) {
      return table -> None
    }
    val catalogSchemaVersion = request.getCatalogSchemaVersion
    if (bucketOwners.isEmpty) {
      // external tables (with source as csv, parquet etc.)
      table -> Some(RelationInfo(1, isPartitioned = false, EMPTY_STRING_ARRAY, EMPTY_STRING_ARRAY,
        EMPTY_STRING_ARRAY, Array.empty[Partition], catalogSchemaVersion))
    } else {
      val bucketCount = tableObj.getNumBuckets
      val indexCols = toArray(tableObj.getIndexColumns)
      val pkCols = toArray(tableObj.getPrimaryKeyColumns)
      if (bucketCount > 0) {
        val allNetUrls = SmartConnectorRDDHelper.setBucketToServerMappingInfo(
          bucketCount, bucketOwners, session)
        val partitions = SmartConnectorRDDHelper.getPartitions(allNetUrls)
        table -> Some(RelationInfo(bucketCount, isPartitioned = true, partitionCols,
          indexCols, pkCols, partitions, catalogSchemaVersion))
      } else {
        val allNetUrls = SmartConnectorRDDHelper.setReplicasToServerMappingInfo(
          tableObj.getBucketOwners.get(0).getSecondaries, session)
        val partitions = SmartConnectorRDDHelper.getPartitions(allNetUrls)
        table -> Some(RelationInfo(1, isPartitioned = false, EMPTY_STRING_ARRAY, indexCols,
          pkCols, partitions, catalogSchemaVersion))
      }
    }
  }

  private[snappydata] def convertToCatalogPartition(
      partitionObj: CatalogPartitionObject): CatalogTablePartition = {
    val storage = partitionObj.getStorage
    CatalogTablePartition(partitionObj.getSpec.asScala.toMap,
      convertToCatalogStorage(storage, storage.getProperties.asScala.toMap))
  }

  /**
   * Convention in thrift CatalogFunctionObject.resources is list of "resourceType:uri" strings.
   */
  private def functionResource(fullName: String): FunctionResource = {
    val sepIndex = fullName.indexOf(':')
    val resourceType = FunctionResourceType.fromString(fullName.substring(0, sepIndex))
    FunctionResource(resourceType, fullName.substring(sepIndex + 1))
  }

  private[snappydata] def convertToCatalogFunction(
      functionObj: CatalogFunctionObject): CatalogFunction = {
    CatalogFunction(FunctionIdentifier(functionObj.getFunctionName,
      Option(functionObj.getSchemaName)), functionObj.getClassName,
      functionObj.getResources.asScala.map(functionResource))
  }

  private def convertFromCatalogStorage(storage: CatalogStorageFormat): CatalogStorage = {
    val storageObj = new CatalogStorage(storage.properties.asJava, storage.compressed)
    if (storage.locationUri.isDefined) storageObj.setLocationUri(storage.locationUri.get)
    if (storage.inputFormat.isDefined) storageObj.setInputFormat(storage.inputFormat.get)
    if (storage.outputFormat.isDefined) storageObj.setOutputFormat(storage.outputFormat.get)
    if (storage.serde.isDefined) storageObj.setSerde(storage.serde.get)
    storageObj
  }

  private def getOrNull(option: Option[String]): String = option match {
    case None => null
    case Some(v) => v
  }

  private[snappydata] def convertFromCatalogTable(table: CatalogTable): CatalogTableObject = {
    val storageObj = convertFromCatalogStorage(table.storage)
    // non CatalogTable attributes like indexColumns, buckets will be set by caller
    // in the GET_CATALOG_SCHEMA system procedure hence filled as empty here
    val (numBuckets, bucketColumns, sortColumns) = table.bucketSpec match {
      case None => (-1, Collections.emptyList[String](), Collections.emptyList[String]())
      case Some(spec) => (spec.numBuckets, spec.bucketColumnNames.asJava,
          spec.sortColumnNames.asJava)
    }
    val (sizeInBytes, rowCount, colStats, canBroadcast) = table.stats match {
      case None =>
        (Long.MinValue, None, Collections.emptyList[java.util.Map[String, String]](), false)
      case Some(stats) =>
        val colStats = table.schema.map { f =>
          stats.colStats.get(f.name) match {
            case None => Collections.emptyMap[String, String]()
            case Some(stat) => stat.toMap.asJava
          }
        }.asJava
        (stats.sizeInBytes.toLong, stats.rowCount, colStats, stats.isBroadcastable)
    }
    val tableObj = new CatalogTableObject(table.identifier.table, table.tableType.name,
      storageObj, table.schema.json, table.partitionColumnNames.asJava, Collections.emptyList(),
      Collections.emptyList(), Collections.emptyList(), bucketColumns, sortColumns,
      table.owner, table.createTime, table.lastAccessTime, table.properties.asJava,
      colStats, canBroadcast, table.unsupportedFeatures.asJava,
      table.tracksPartitionsInCatalog, table.schemaPreservesCase)
    tableObj.setSchemaName(getOrNull(table.identifier.database))
        .setProvider(getOrNull(table.provider))
        .setViewText(getOrNull(table.viewText))
        .setViewOriginalText(getOrNull(table.viewOriginalText))
        .setComment(getOrNull(table.comment))
    if (numBuckets != -1) tableObj.setNumBuckets(numBuckets)
    if (sizeInBytes != Long.MinValue) tableObj.setSizeInBytes(sizeInBytes)
    rowCount match {
      case None => tableObj
      case Some(c) => tableObj.setRowCount(c.toLong)
    }
  }

  private[snappydata] def convertFromCatalogPartition(
      partition: CatalogTablePartition): CatalogPartitionObject = {
    new CatalogPartitionObject(partition.spec.asJava,
      convertFromCatalogStorage(partition.storage), partition.parameters.asJava)
  }

  private[snappydata] def convertFromCatalogFunction(
      function: CatalogFunction): CatalogFunctionObject = {
    val resources = function.resources.map(r => s"${r.resourceType.resourceType}:${r.uri}")
    new CatalogFunctionObject(function.identifier.funcName,
      function.className, resources.asJava).setSchemaName(getOrNull(
      function.identifier.database))
  }

  private def loadFromCache(name: (String, String),
      catalog: ConnectorExternalCatalog): (CatalogTable, Option[RelationInfo]) = {
    cachedCatalogTables.getIfPresent(name) match {
      case null => synchronized {
        cachedCatalogTables.getIfPresent(name) match {
          case null =>
            logDebug(s"Looking up data source for $name")
            val request = new CatalogMetadataRequest()
            request.setSchemaName(name._1).setNameOrPattern(name._2)
            val result = catalog.withExceptionHandling(catalog.connectorHelper.getCatalogMetadata(
              snappydataConstants.CATALOG_GET_TABLE, request))
            if (!result.isSetCatalogTable) throw new TableNotFoundException(name._1, name._2)
            val (table, relationInfo) = convertToCatalogTable(result, catalog.session)
            val tableMetadata = table -> relationInfo
            cachedCatalogTables.put(name, tableMetadata)
            tableMetadata

          case result => result
        }
      }
      case result => result
    }
  }

  def getCatalogTable(name: (String, String), catalog: ConnectorExternalCatalog): CatalogTable = {
    loadFromCache(name, catalog)._1
  }

  def getRelationInfo(name: (String, String),
      catalog: ConnectorExternalCatalog): Option[RelationInfo] = {
    loadFromCache(name, catalog)._2
  }

  def close(): Unit = synchronized {
    cachedCatalogTables.invalidateAll()
  }
}

case class RelationInfo(numBuckets: Int,
    isPartitioned: Boolean,
    partitioningCols: Array[String] = Utils.EMPTY_STRING_ARRAY,
    indexCols: Array[String] = Utils.EMPTY_STRING_ARRAY,
    pkCols: Array[String] = Utils.EMPTY_STRING_ARRAY,
    partitions: Array[org.apache.spark.Partition] = Array.empty,
    catalogSchemaVersion: Long = -1) {

  @transient
  @volatile var invalid: Boolean = _
}
