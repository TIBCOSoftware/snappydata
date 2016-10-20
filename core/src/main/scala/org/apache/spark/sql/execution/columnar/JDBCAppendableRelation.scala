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
package org.apache.spark.sql.execution.columnar

import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable

import _root_.io.snappydata.{Constant, SnappyTableStatsProviderService}

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SortDirection
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.hive.{QualifiedTableName, SnappyStoreHiveCatalog}
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.row.GemFireXDBaseDialect
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType


/**
 * A LogicalPlan implementation for an external column table whose contents
 * are retrieved using a JDBC URL or DataSource.
 */
case class JDBCAppendableRelation(
    table: String,
    provider: String,
    mode: SaveMode,
    override val schema: StructType,
    origOptions: Map[String, String],
    externalStore: ExternalStore,
    @transient override val sqlContext: SQLContext)
  extends BaseRelation
  with PrunedUnsafeFilteredScan
  with InsertableRelation
  with DestroyRelation
  with IndexableRelation
  with Logging
  with Serializable {

  self =>

  override val needConversion: Boolean = false

  var tableExists: Boolean = _

  protected final val connProperties = externalStore.connProperties

  protected final val connFactory = JdbcUtils.createConnectionFactory(
    connProperties.url, connProperties.connProps)

  val resolvedName: String = externalStore.tryExecute(table, conn => {
    ExternalStoreUtils.lookupName(table, conn.getSchema)
  })

  override def sizeInBytes: Long = {
    val stats = SnappyTableStatsProviderService.getTableStatsFromService(table)
    if (stats.isDefined) stats.get.getTotalSize
    else super.sizeInBytes
  }


  protected final def dialect = connProperties.dialect

  val schemaFields = Utils.getSchemaFields(schema)

  final lazy val executorConnector = ExternalStoreUtils.getConnector(table,
    connProperties, forExecutor = true)

  private val bufferLock = new ReentrantReadWriteLock()

  /** Acquires a read lock on the cache for the duration of `f`. */
  private[sql] def readLock[A](f: => A): A = {
    val lock = bufferLock.readLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  /** Acquires a write lock on the cache for the duration of `f`. */
  private[sql] def writeLock[A](f: => A): A = {
    val lock = bufferLock.writeLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  // TODO: Suranjan currently doesn't apply any filters.
  // will see that later.
  override def buildUnsafeScan(requiredColumns: Array[String],
      filters: Array[Filter]): (RDD[Any], Seq[RDD[InternalRow]]) = {
    val (cachedColumnBuffers, requestedColumns) = scanTable(table,
      requiredColumns, filters)
    val rdd = cachedColumnBuffers.mapPartitionsPreserve { cachedBatchIterator =>
      // Find the ordinals and data types of the requested columns.
      // If none are requested, use the narrowest (the field with
      // minimum default element size).

      ExternalStoreUtils.cachedBatchesToRows(cachedBatchIterator,
        requestedColumns, schema, forScan = true)
    }
    (rdd.asInstanceOf[RDD[Any]], Nil)
  }

  def scanTable(tableName: String, requiredColumns: Array[String],
      filters: Array[Filter]): (RDD[CachedBatch], Array[String]) = {

    val requestedColumns = if (requiredColumns.isEmpty) {
      val narrowField =
        schema.fields.minBy { a =>
          ColumnType(a.dataType).defaultSize
        }

      Array(narrowField.name)
    } else {
      requiredColumns
    }

    val cachedColumnBuffers: RDD[CachedBatch] = readLock {
      externalStore.getCachedBatchRDD(tableName,
        requestedColumns.map(column => externalStore.columnPrefix + column),
        sqlContext.sparkSession)
    }
    (cachedColumnBuffers, requestedColumns)
  }

  override def insert(df: DataFrame, overwrite: Boolean = true): Unit = {
    insert(df.queryExecution.toRdd, df, overwrite)
  }

  def cachedBatchAggregate(batch: CachedBatch): Unit = {
    externalStore.storeCachedBatch(table, batch)
  }

  protected def insert(rdd: RDD[InternalRow], df: DataFrame,
      overwrite: Boolean): Unit = {

    // We need to truncate the table
    if (overwrite) {
      truncate()
    }

    val useCompression = sqlContext.conf.useCompression
    val columnBatchSize = sqlContext.conf.columnBatchSize

    val output = df.logicalPlan.output
    val cached = rdd.mapPartitionsPreserve(rowIterator => {

      def columnBuilders = output.map { attribute =>
        val columnType = ColumnType(attribute.dataType)
        val initialBufferSize = columnType.defaultSize * columnBatchSize
        ColumnBuilder(attribute.dataType, initialBufferSize,
          attribute.name, useCompression)
      }.toArray

      val holder = new CachedBatchHolder(columnBuilders, 0, columnBatchSize,
        cachedBatchAggregate)

      rowIterator.foreach(holder.appendRow)
      holder.forceEndOfBatch()
      Iterator.empty
    }, preservesPartitioning = true)
    // trigger an Action to materialize 'cached' batch
    cached.count()
  }

  // truncate both actual and shadow table
  def truncate(): Unit = writeLock {
    externalStore.tryExecute(table, conn => {
      JdbcExtendedUtils.truncateTable(conn, table, dialect)
    })
  }

  def createTable(mode: SaveMode): Unit = {
    val conn = connFactory()
    try {
      tableExists = JdbcExtendedUtils.tableExists(table, conn,
        dialect, sqlContext)
      if (mode == SaveMode.Ignore && tableExists) {
        dialect match {
          case d: JdbcExtendedDialect =>
            d.initializeTable(table,
              sqlContext.conf.caseSensitiveAnalysis, conn)
          case _ => // do nothing
        }
      }
      else if (mode == SaveMode.ErrorIfExists && tableExists) {
        sys.error(s"Table $table already exists.")
      }
    } finally {
      conn.close()
    }
    createExternalTableForCachedBatches(table, externalStore)
  }

  protected def createExternalTableForCachedBatches(tableName: String,
      externalStore: ExternalStore): Unit = {
    require(tableName != null && tableName.length > 0,
      "createExternalTableForCachedBatches: expected non-empty table name")

    val (primarykey, partitionStrategy) = dialect match {
      // The driver if not a loner should be an accesor only
      case d: JdbcExtendedDialect =>
        (s"constraint ${tableName}_partitionCheck check (partitionId != -1), " +
            "primary key (uuid, partitionId)",
            d.getPartitionByClause("partitionId"))
      case _ => ("primary key (uuid)", "")
    }

    createTable(externalStore, s"create table $tableName (uuid varchar(36) " +
        "not null, partitionId integer not null, numRows integer not null, " +
        "stats blob, " + schema.fields.map(structField =>
        externalStore.columnPrefix + structField.name + " blob")
        .mkString(", ") + s", $primarykey) $partitionStrategy",
      tableName, dropIfExists = false) // for test make it false
  }

  def createTable(externalStore: ExternalStore, tableStr: String,
      tableName: String, dropIfExists: Boolean): Unit = {

    externalStore.tryExecute(tableName,
      conn => {
        if (dropIfExists) {
          JdbcExtendedUtils.dropTable(conn, tableName, dialect, sqlContext,
            ifExists = true)
        }
        val tableExists = JdbcExtendedUtils.tableExists(tableName, conn,
          dialect, sqlContext)
        if (!tableExists) {
          logInfo(s"Applying DDL (url=${connProperties.url}; " +
              s"props=${connProperties.connProps}): $tableStr")
          JdbcExtendedUtils.executeUpdate(tableStr, conn)
          dialect match {
            case d: JdbcExtendedDialect => d.initializeTable(tableName,
              sqlContext.conf.caseSensitiveAnalysis, conn)
            case _ => // do nothing
          }
        }
      })
  }

  /**
   * Destroy and cleanup this relation. It may include, but not limited to,
   * dropping the external table that this relation represents.
   */
  override def destroy(ifExists: Boolean): Unit = {
    // drop the external table using a non-pool connection
    val conn = connFactory()
    try {
      // clean up the connection pool and caches
      ExternalStoreUtils.removeCachedObjects(sqlContext, table)
    } finally {
      try {
        JdbcExtendedUtils.dropTable(conn, table, dialect, sqlContext, ifExists)
      } finally {
        conn.close()
      }
    }
  }

  def flushRowBuffer(): Unit = {
    // nothing by default
  }

  override def createIndex(indexIdent: QualifiedTableName,
      tableIdent: QualifiedTableName,
      indexColumns: Map[String, Option[SortDirection]],
      options: Map[String, String]): Unit = {
    throw new UnsupportedOperationException("Indexes are not supported")
  }

  override def dropIndex(indexIdent: QualifiedTableName,
      tableIdent: QualifiedTableName,
      ifExists: Boolean): Unit = {
    throw new UnsupportedOperationException("Indexes are not supported")
  }

  private[sql] def externalColumnTableName: String = JDBCAppendableRelation.
      cachedBatchTableName(table)
}

object JDBCAppendableRelation extends Logging {

  private[sql] final def cachedBatchTableName(table: String): String = {
    val tableName = if (table.indexOf('.') > 0) {
      table.replace(".", "__")
    } else {
      Constant.DEFAULT_SCHEMA + "__" + table
    }
    Constant.INTERNAL_SCHEMA_NAME + "." +  tableName + Constant.SHADOW_TABLE_SUFFIX
  }
}

final class DefaultSource extends ColumnarRelationProvider

class ColumnarRelationProvider
    extends SchemaRelationProvider
    with CreatableRelationProvider {

  def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schema: StructType): JDBCAppendableRelation = {
    val parameters = new mutable.HashMap[String, String]
    parameters ++= options

    val table = ExternalStoreUtils.removeInternalProps(parameters)
    val sc = sqlContext.sparkContext

    val connectionProperties =
      ExternalStoreUtils.validateAndGetAllProps(sc, parameters)

    val partitions = ExternalStoreUtils.getTotalPartitions(sc, parameters,
      forManagedTable = false)

    val externalStore = getExternalSource(sqlContext, connectionProperties,
      partitions)

    var success = false
    val relation = new JDBCAppendableRelation(SnappyStoreHiveCatalog
        .processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName, mode, schema, options,
      externalStore, sqlContext)
    try {
      relation.createTable(mode)
      success = true
      relation
    } finally {
      if (!success && !relation.tableExists) {
        // destroy the relation
        relation.destroy(ifExists = true)
      }
    }
  }

  override def createRelation(sqlContext: SQLContext,
      options: Map[String, String], schema: StructType): JDBCAppendableRelation = {

    val allowExisting = options.get(JdbcExtendedUtils
        .ALLOW_EXISTING_PROPERTY).exists(_.toBoolean)
    val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists

    val rel = getRelation(sqlContext, options)
    rel.createRelation(sqlContext, mode, options, schema)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], data: DataFrame): JDBCAppendableRelation = {
    val rel = getRelation(sqlContext, options)
    val relation = rel.createRelation(sqlContext, mode, options, data.schema)
    var success = false
    try {
      relation.insert(data, mode == SaveMode.Overwrite)
      success = true
      relation
    } finally {
      if (!success && !relation.tableExists) {
        // destroy the relation
        relation.destroy(ifExists = true)
      }
    }
  }

  def getRelation(sqlContext: SQLContext,
      options: Map[String, String]): ColumnarRelationProvider = {

    val url = options.getOrElse("url",
      ExternalStoreUtils.defaultStoreURL(sqlContext.sparkContext))
    val clazz = JdbcDialects.get(url) match {
      case d: GemFireXDBaseDialect =>
        DataSource(sqlContext.sparkSession, classOf[impl.DefaultSource]
            .getCanonicalName).providingClass

      case _ => classOf[org.apache.spark.sql.execution.columnar.DefaultSource]
    }
    clazz.newInstance().asInstanceOf[ColumnarRelationProvider]
  }

  def getExternalSource(sqlContext: SQLContext,
      connProperties: ConnectionProperties,
      numPartitions: Int): ExternalStore = {
    new JDBCSourceAsStore(connProperties, numPartitions)
  }
}
