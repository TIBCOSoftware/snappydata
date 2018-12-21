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
package io.snappydata.sql.catalog.impl

import java.util.Collections

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.internal.cache.LocalRegion
import io.snappydata.sql.catalog.{ConnectorExternalCatalog, RelationInfo, SnappyExternalCatalog}
import io.snappydata.thrift.{CatalogMetadataDetails, CatalogMetadataRequest, CatalogSchemaObject, snappydataConstants}

import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionException, NoSuchPermanentFunctionException}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogFunction, CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BoundReference, Expression}
import org.apache.spark.sql.collection.{SmartExecutorBucketPartition, Utils}
import org.apache.spark.sql.execution.RefreshMetadata
import org.apache.spark.sql.{SnappyContext, SparkSession, TableNotFoundException, ThinClientConnectorMode}

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
class SmartConnectorExternalCatalog(override val session: SparkSession)
    extends SnappyExternalCatalog with ConnectorExternalCatalog {

  override val jdbcUrl: String = SnappyContext.getClusterMode(session.sparkContext)
      .asInstanceOf[ThinClientConnectorMode].url

  override def invalidate(name: (String, String)): Unit = {
    // invalidation of a single table can result in all cached RelationInfo being
    // out of date due to lower schema version, so always invalidate all
    invalidateAll()
  }

  override def invalidateCaches(relations: Seq[(String, String)]): Unit = {
    // invalidation of a single table can result in all cached RelationInfo being
    // out of date due to lower schema version, so always invalidate all
    invalidateAll()
    // there is no version update in this call here, rather only the caches are cleared
    RefreshMetadata.executeLocal(RefreshMetadata.UPDATE_CATALOG_SCHEMA_VERSION, args = null)
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
}
