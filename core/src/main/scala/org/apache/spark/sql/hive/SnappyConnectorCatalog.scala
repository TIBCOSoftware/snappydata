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

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util.Properties
import java.util.concurrent.ExecutionException

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.util.concurrent.UncheckedExecutionException
import io.snappydata.Property
import io.snappydata.impl.SparkShellRDDHelper
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Table}

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.catalog.{CatalogColumn, CatalogStorageFormat, CatalogTable, CatalogTableType, FunctionResourceLoader}
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.execution.datasources.{DataSource, LogicalRelation}
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{BaseRelation, DependencyCatalog, JdbcExtendedUtils, ParentRelation}
import org.apache.spark.sql.streaming.StreamBaseRelation
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{AnalysisException, SaveMode, SnappyContext, SnappySession, TableNotFoundException, ThinClientConnectorMode}

/**
 * Catalog used when SnappyData Connector mode is used over thin client JDBC connection.
 * This cannot directly update Hive metastore but will forward DDLs to the SnappyData
 * cluster to which this connector is connected to
 */
class SnappyConnectorCatalog(externalCatalog: SnappyExternalCatalog,
    snappySession: SnappySession,
    metadataHive: HiveClient,
    functionResourceLoader: FunctionResourceLoader,
    functionRegistry: FunctionRegistry,
    sqlConf: SQLConf,
    hadoopConf: Configuration)
    extends SnappyStoreHiveCatalog(
      externalCatalog: SnappyExternalCatalog,
      snappySession: SnappySession,
      metadataHive: HiveClient,
      functionResourceLoader: FunctionResourceLoader,
      functionRegistry: FunctionRegistry,
      sqlConf: SQLConf,
      hadoopConf: Configuration) {

  private lazy val clusterMode = SnappyContext.getClusterMode(snappySession.sparkContext)

  private lazy val connFactory = {
    clusterMode match {
      case ThinClientConnectorMode(_, props) =>
        JdbcUtils.createConnectionFactory(
          Property.ClusterURL.getOption(snappySession.sparkContext.conf).get +
              ";route-query=false;" + props, new Properties())
      case _ =>
        throw new AnalysisException("Not expected to be called for " + clusterMode)
    }
  }

  private var conn = connFactory()
  private val metadataStmtString = "call sys.GET_TABLE_METADATA(?, ?, ?, ?, ?, ?)"
  private var metadataStmt = conn.prepareCall(metadataStmtString)

  def executeMetaDataStatement(tableName: String): Unit = {
    metadataStmt.setString(1, tableName)
    metadataStmt.registerOutParameter(2, java.sql.Types.BLOB) /*Hive table object*/
    metadataStmt.registerOutParameter(3, java.sql.Types.INTEGER) /*bucket count*/
    metadataStmt.registerOutParameter(4, java.sql.Types.VARCHAR) /*partitioning columns*/
    metadataStmt.registerOutParameter(5, java.sql.Types.VARCHAR) /*index columns*/
    metadataStmt.registerOutParameter(6, java.sql.Types.CLOB) /*bucket to server or replica to server mapping*/
    metadataStmt.execute
  }

  def runMetaDataStmtWithExceptionHandling(tableName: String): Unit = {
    try {
      executeMetaDataStatement(tableName)
    } catch {
      case e: Exception if isDisconnectException(e) =>
        // stale JDBC connection
        conn.close()
        conn = connFactory()
        metadataStmt = conn.prepareCall(metadataStmtString)
        executeMetaDataStatement(tableName)
    }
  }

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
        logDebug(s"Creating new cached data source for $in")

        val (hiveTable: Table, relationInfo: RelationInfo) = getHiveTableAndMetadata(in)

//        val table: CatalogTable = in.getTable(client)
        val table: CatalogTable = getCatalogTable(new org.apache.hadoop.hive.ql.metadata.Table(hiveTable)).get

        val schemaString = SnappyStoreHiveCatalog.getSchemaString(table.properties)
        val userSpecifiedSchema = schemaString.map(s =>
          DataType.fromJson(s).asInstanceOf[StructType])
        val partitionColumns = table.partitionColumns.map(_.name)
        val provider = table.properties(SnappyStoreHiveCatalog.HIVE_PROVIDER)
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


        (LogicalRelation(relation), table, relationInfo)
      }
    }

    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  def getHiveTableAndMetadata(in: QualifiedTableName): (Table, RelationInfo) = {
    runMetaDataStmtWithExceptionHandling(in.toString)

    val tableObjectBlob = Option(metadataStmt.getBlob(2)).
        getOrElse(throw new TableNotFoundException(s"Table ${in} not found"))

    val t: Table = {
      val tableObjectBytes = tableObjectBlob.getBytes(1, tableObjectBlob.length().toInt)
      val baip = new ByteArrayInputStream(tableObjectBytes)
      val ois = new ObjectInputStream(baip)
      ois.readObject().asInstanceOf[Table]
    }
    val bucketCount = metadataStmt.getInt(3)
    val indexColsString = metadataStmt.getString(5)
    val indexCols = Option(indexColsString) match {
      case Some(str) => str.split(":")
      case None => Array.empty[String]
    }
    if (bucketCount > 0) {
      val partitionCols = metadataStmt.getString(4).split(":")
      val bucketToServerMappingStr = metadataStmt.getString(6)
      val allNetUrls = SparkShellRDDHelper.setBucketToServerMappingInfo(bucketToServerMappingStr)
      val partitions = SparkShellRDDHelper.getPartitions(allNetUrls)
      (t, new RelationInfo(bucketCount, partitionCols.toSeq, indexCols, partitions))
    } else {
      val replicaToNodesInfo = metadataStmt.getString(6)
      val allNetUrls = SparkShellRDDHelper.setReplicasToServerMappingInfo(replicaToNodesInfo)
      val partitions = SparkShellRDDHelper.getPartitions(allNetUrls)
      (t, new RelationInfo(1, Seq.empty[String], indexCols, partitions))
    }
  }

  /*
  * Code copied from org.apache.spark.sql.hive.client.HiveClientImpl.getTableOption
   */
  def getCatalogTable(table: org.apache.hadoop.hive.ql.metadata.Table): Option[CatalogTable] = {
    Option(table).map { h =>
      // Note: Hive separates partition columns and the schema, but for us the
      // partition columns are part of the schema
      val partCols = h.getPartCols.asScala.map(fromHiveColumn)
      val schema = h.getCols.asScala.map(fromHiveColumn) ++ partCols

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
          case org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE => CatalogTableType.EXTERNAL
          case org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE => CatalogTableType.MANAGED
          case org.apache.hadoop.hive.metastore.TableType.INDEX_TABLE => CatalogTableType.INDEX
          case org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW => CatalogTableType.VIEW
        },
        schema = schema,
        partitionColumnNames = partCols.map(_.name),
        sortColumnNames = Seq(), // TODO: populate this
        bucketColumnNames = h.getBucketCols.asScala,
        numBuckets = h.getNumBuckets,
        owner = h.getOwner,
        createTime = h.getTTable.getCreateTime.toLong * 1000,
        lastAccessTime = h.getLastAccessTime.toLong * 1000,
        storage = CatalogStorageFormat(
          locationUri = Option(h.getTTable.getSd.getLocation),
          inputFormat = Option(h.getInputFormatClass).map(_.getName),
          outputFormat = Option(h.getOutputFormatClass).map(_.getName),
          serde = Option(h.getSerializationLib),
          compressed = h.getTTable.getSd.isCompressed,
          serdeProperties = Option(h.getTTable.getSd.getSerdeInfo.getParameters)
              .map(_.asScala.toMap).orNull
        ),
        properties = properties,
        viewOriginalText = Option(h.getViewOriginalText),
        viewText = Option(h.getViewExpandedText),
        unsupportedFeatures = unsupportedFeatures)
    }
  }

  private def fromHiveColumn(hc: FieldSchema): CatalogColumn = {
    new CatalogColumn(
      name = hc.getName,
      dataType = hc.getType,
      nullable = true,
      comment = Option(hc.getComment))
  }

  override def registerDataSourceTable(
      tableIdent: QualifiedTableName,
      userSpecifiedSchema: Option[StructType],
      partitionColumns: Array[String],
      provider: String,
      options: Map[String, String],
      relation: BaseRelation): Unit = {

    logInfo(s"sdeshmukh registerDataSourceTable = tableIdent = $tableIdent , " +
        s"userSpecifiedSchema = $userSpecifiedSchema, partitionColumns = $partitionColumns, " +
        s"provider = $provider, options = $options, relation = $relation ")

    super.registerDataSourceTable(tableIdent, userSpecifiedSchema, partitionColumns, provider, options, relation )

  }

}

case class RelationInfo(val numBuckets: Int, val partitioningCols: Seq[String],
    val indexCols: Array[String],
    val partitions: Array[org.apache.spark.Partition]) {
}

