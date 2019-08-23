/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

import com.google.common.cache.{Cache, CacheBuilder}
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import io.snappydata.Property
import io.snappydata.thrift._

import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Statistics}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.collection.Utils.EMPTY_STRING_ARRAY
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.{SparkSession, TableNotFoundException}
import org.apache.spark.{Logging, Partition, SparkEnv}

/**
 * Base class for catalog implementations for connector modes. This is either used as basis
 * for ExternalCatalog implementation (in smart connector) or as a helper class for catalog
 * queries like in connector v2 implementation.
 */
trait ConnectorExternalCatalog {

  def session: SparkSession

  def jdbcUrl: String

  @GuardedBy("this")
  protected var connectorHelper: SmartConnectorHelper = new SmartConnectorHelper(session, jdbcUrl)

  protected def withExceptionHandling[T](function: => T): T = synchronized {
    try {
      function
    } catch {
      case e: SQLException if isConnectionException(e) =>
        // attempt to create a new connection
        connectorHelper.close()
        connectorHelper = new SmartConnectorHelper(session, jdbcUrl)
        function
    }
  }

  protected def isConnectionException(e: SQLException): Boolean = {
    e.getSQLState.startsWith(SQLState.CONNECTIVITY_PREFIX) ||
        e.getSQLState.startsWith(SQLState.LANG_DEAD_STATEMENT) ||
        e.getSQLState.startsWith(SQLState.GFXD_NODE_SHUTDOWN_PREFIX)
  }

  def invalidateAll(): Unit = {
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

  def close(): Unit = synchronized(connectorHelper.close())
}

object ConnectorExternalCatalog extends Logging {

  def cacheSize: Int = {
    SparkEnv.get match {
      case null => Property.CatalogCacheSize.defaultValue.get
      case env => Property.CatalogCacheSize.get(env.conf)
    }
  }

  /** A cache of Spark SQL data source tables that have been accessed. */
  private[catalog] lazy val cachedCatalogTables: Cache[(String, String),
      (CatalogTable, Option[RelationInfo])] = {
    CacheBuilder.newBuilder().maximumSize(cacheSize).build()
  }

  private def toArray(columns: java.util.List[String]): Array[String] =
    columns.toArray(new Array[String](columns.size()))

  private def convertToCatalogStorage(storage: CatalogStorage,
      storageProps: Map[String, String]): CatalogStorageFormat = {
    CatalogStorageFormat(Option(storage.getLocationUri), Option(storage.getInputFormat),
      Option(storage.getOutputFormat), Option(storage.getSerde), storage.compressed, storageProps)
  }

  private[snappydata] def convertToCatalogTable(request: CatalogMetadataDetails,
      session: SparkSession): (CatalogTable, Option[RelationInfo]) = {
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
        val allNetUrls = SmartConnectorHelper.setBucketToServerMappingInfo(
          bucketCount, bucketOwners, session)
        val partitions = SmartConnectorHelper.getPartitions(allNetUrls)
        table -> Some(RelationInfo(bucketCount, isPartitioned = true, partitionCols,
          indexCols, pkCols, partitions, catalogSchemaVersion))
      } else {
        val allNetUrls = SmartConnectorHelper.setReplicasToServerMappingInfo(
          tableObj.getBucketOwners.get(0).getSecondaries, session)
        val partitions = SmartConnectorHelper.getPartitions(allNetUrls)
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
