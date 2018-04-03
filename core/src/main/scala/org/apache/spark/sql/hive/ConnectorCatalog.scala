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

import java.util.concurrent.ExecutionException

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.util.concurrent.UncheckedExecutionException
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.ql.metadata.Table

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.sources.{BaseRelation, DependencyCatalog, JdbcExtendedUtils, ParentRelation}
import org.apache.spark.sql.streaming.StreamBaseRelation
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{AnalysisException, SaveMode, SmartConnectorHelper}

trait ConnectorCatalog extends SnappyStoreHiveCatalog {

  lazy val connectorHelper = new SmartConnectorHelper(snappySession)

  def getCachedRelationInfo(table: QualifiedTableName): RelationInfo = {
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

      cachedDataSourceTables(table)._3
    } catch {
      case e@(_: UncheckedExecutionException | _: ExecutionException) =>
        throw e.getCause
    } finally {
      sync.unlock()
    }
  }

  /** A cache of Spark SQL data source tables that have been accessed. */
  override protected val cachedDataSourceTables: LoadingCache[QualifiedTableName,
      (LogicalRelation, CatalogTable, RelationInfo)] = {
    val cacheLoader = new CacheLoader[QualifiedTableName,
        (LogicalRelation, CatalogTable, RelationInfo)]() {
      override def load(in: QualifiedTableName): (LogicalRelation, CatalogTable, RelationInfo) = {
        // table names are always case-insensitive in hive
        val qualifiedName = Utils.toUpperCase(in.toString)
        logDebug(s"Creating new cached data source for $qualifiedName")

        // val (hiveTable: Table, relationInfo: RelationInfo) =
        //   SmartConnectorHelper.getHiveTableAndMetadata(in)
        val (hiveTable: Table, relationInfo: RelationInfo) =
          connectorHelper.getHiveTableAndMetadata(in)

        //        val table: CatalogTable = in.getTable(client)
        val table: CatalogTable = getCatalogTable(hiveTable).get

        val userSpecifiedSchema = ExternalStoreUtils.getTableSchema(
          table.properties)
        val partitionColumns = table.partitionSchema.map(_.name)
        val provider = table.properties(SnappyStoreHiveCatalog.HIVE_PROVIDER)
        var options: Map[String, String] = new CaseInsensitiveMap(table.storage.properties)
        // add dbtable property if not present
        val dbtableProp = JdbcExtendedUtils.DBTABLE_PROPERTY
        if (!options.contains(dbtableProp)) {
          options += dbtableProp -> qualifiedName
        }
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


        (LogicalRelation(relation), table, relationInfo)
      }
    }

    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  /*
  * Code copied from org.apache.spark.sql.hive.client.HiveClientImpl.getTableOption
   */
  def getCatalogTable(table: org.apache.hadoop.hive.ql.metadata.Table): Option[CatalogTable] = {
    Option(table).map { h =>
      // Note: Hive separates partition columns and the schema, but for us the
      // partition columns are part of the schema
      val partCols = h.getPartCols.asScala.map(fromHiveColumn)
      val schema = StructType(h.getCols.asScala.map(fromHiveColumn) ++ partCols)

      // Skew spec, storage handler, and bucketing info can't be mapped to CatalogTable (yet)
      val unsupportedFeatures = ArrayBuffer.empty[String]

      if (!h.getSkewedColNames.isEmpty) {
        unsupportedFeatures += "skewed columns"
      }

      if (h.getStorageHandler != null) {
        unsupportedFeatures += "storage handler"
      }

      if (!h.getBucketCols.isEmpty) {
        unsupportedFeatures += "bucketing"
      }

      val properties = Option(h.getParameters).map(_.asScala.toMap).orNull

      CatalogTable(
        identifier = TableIdentifier(h.getTableName, Option(h.getDbName)),
        tableType = h.getTableType match {
          case org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE =>
            org.apache.spark.sql.catalyst.catalog.CatalogTableType.EXTERNAL
          case org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE =>
            org.apache.spark.sql.catalyst.catalog.CatalogTableType.MANAGED
          case org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW =>
            org.apache.spark.sql.catalyst.catalog.CatalogTableType.VIEW
          case org.apache.hadoop.hive.metastore.TableType.INDEX_TABLE =>
            throw new AnalysisException("Hive index table is not supported.")
        },
        schema = schema,
        partitionColumnNames = partCols.map(_.name),
        // We can not populate bucketing information for Hive tables as Spark SQL has a different
        // implementation of hash function from Hive.
        bucketSpec = None,
        owner = h.getOwner,
        createTime = h.getTTable.getCreateTime.toLong * 1000,
        lastAccessTime = h.getLastAccessTime.toLong * 1000,
        storage = CatalogStorageFormat(
          locationUri = Option(h.getTTable.getSd.getLocation),
          inputFormat = Option(h.getInputFormatClass).map(_.getName),
          outputFormat = Option(h.getOutputFormatClass).map(_.getName),
          serde = Option(h.getSerializationLib),
          compressed = h.getTTable.getSd.isCompressed,
          properties = Option(h.getTTable.getSd.getSerdeInfo.getParameters)
            .map(_.asScala.toMap).orNull
        ),
        // For EXTERNAL_TABLE, the table properties has a particular field "EXTERNAL". This is added
        // in the function toHiveTable.
        properties = properties.filter(kv => kv._1 != "comment" && kv._1 != "EXTERNAL"),
        comment = properties.get("comment"),
        viewOriginalText = Option(h.getViewOriginalText),
        viewText = Option(h.getViewExpandedText),
        unsupportedFeatures = unsupportedFeatures)
    }
  }

  private def fromHiveColumn(hc: FieldSchema): StructField = {
    val columnType = try {
      snappySession.sessionState.sqlParser.parseDataType(hc.getType)
    } catch {
      case e: ParseException =>
        throw new SparkException("Cannot recognize hive type string: " + hc.getType, e)
    }

    // the key below should match the key used by HiveClientImpl in MetadataBuilder
    val metadata = new MetadataBuilder().putString("HIVE_TYPE_STRING", hc.getType).build()
    val field = StructField(
      name = hc.getName,
      dataType = columnType,
      nullable = true,
      metadata = metadata)
    Option(hc.getComment).map(field.withComment).getOrElse(field)
  }

  override def registerDataSourceTable(
      tableIdent: QualifiedTableName,
      userSpecifiedSchema: Option[StructType],
      partitionColumns: Array[String],
      provider: String,
      options: Map[String, String],
      relation: Option[BaseRelation]): Unit = {
    // no op
  }

  /**
   * Drops a data source table from Hive's meta-store.
   */
  override def unregisterDataSourceTable(tableIdent: QualifiedTableName,
      relation: Option[BaseRelation]): Unit = {
    // no op
  }
}

case class RelationInfo(numBuckets: Int = 1,
    isPartitioned: Boolean = false,
    partitioningCols: Seq[String] = Nil,
    indexCols: Array[String] = Array.empty,
    pkCols: Array[String] = Array.empty,
    partitions: Array[org.apache.spark.Partition] = Array.empty,
    embdClusterRelDestroyVersion: Int = -1) {
}
