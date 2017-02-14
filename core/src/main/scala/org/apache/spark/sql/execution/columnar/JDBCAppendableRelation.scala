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

import java.sql.Connection
import java.util.concurrent.locks.ReentrantReadWriteLock

import _root_.io.snappydata.{Constant, SnappyTableStatsProviderService}

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SortDirection
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.hive.{QualifiedTableName}
import org.apache.spark.sql.jdbc.{JdbcDialect}
import org.apache.spark.sql.snappy._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{StructField, StructType}


/**
 * A LogicalPlan implementation for an external column table whose contents
 * are retrieved using a JDBC URL or DataSource.
 */
abstract case class JDBCAppendableRelation(
    table: String,
    provider: String,
    mode: SaveMode,
    override val schema: StructType,
    origOptions: Map[String, String],
    externalStore: ExternalStore,
    @transient override val sqlContext: SQLContext) extends BaseRelation
    with PrunedUnsafeFilteredScan
    with InsertableRelation
    with DestroyRelation
    with IndexableRelation
    with Logging
    with Serializable {

  self =>

  override val needConversion: Boolean = false

  var tableExists: Boolean = _

  protected final val connProperties: ConnectionProperties =
    externalStore.connProperties

  protected final val connFactory: () => Connection = JdbcUtils
      .createConnectionFactory(connProperties.url, connProperties.connProps)

  val resolvedName: String = externalStore.tryExecute(table, conn => {
    ExternalStoreUtils.lookupName(table, conn.getSchema)
  })

  override def sizeInBytes: Long = {
    SnappyTableStatsProviderService.getTableStatsFromService(table) match {
      case Some(s) => s.getTotalSize
      case None => super.sizeInBytes
    }
  }


  protected final def dialect: JdbcDialect = connProperties.dialect

  val schemaFields: Map[String, StructField] = Utils.getSchemaFields(schema)

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

  def scanTable(tableName: String, requiredColumns: Array[String],
      filters: Array[Filter]): (RDD[Any], Array[String]) = {

    val requestedColumns = if (requiredColumns.isEmpty) {
      val narrowField =
        schema.fields.minBy { a =>
          ColumnType(a.dataType).defaultSize
        }

      Array(narrowField.name)
    } else {
      requiredColumns
    }

    val cachedColumnBuffers: RDD[Any] = readLock {
      externalStore.getCachedBatchRDD(tableName,
        requestedColumns.map(column => externalStore.columnPrefix + column),
        sqlContext.sparkSession, schema)
    }
    (cachedColumnBuffers, requestedColumns)
  }

  override def insert(df: DataFrame, overwrite: Boolean = true): Unit = {
    insert(df.queryExecution.toRdd, df, overwrite)
  }

  def cachedBatchAggregate(batch: CachedBatch): Unit = {
    externalStore.storeCachedBatch(table, batch)
  }

  def getCachedBatchParams: (Integer, Boolean) = {
    val columnBatchSize = origOptions.get(ExternalStoreUtils.COLUMN_BATCH_SIZE) match {
      case Some(cb) => Integer.parseInt(cb)
      case None => ExternalStoreUtils.getDefaultCachedBatchSize()
    }
    val useCompression = origOptions.get(ExternalStoreUtils.USE_COMPRESSION) match {
      case Some(uc) => java.lang.Boolean.parseBoolean(uc)
      case None => true
    }
    (columnBatchSize, useCompression)
  }

  protected def insert(rdd: RDD[InternalRow], df: DataFrame,
      overwrite: Boolean): Unit = {

    // We need to truncate the table
    if (overwrite) {
      truncate()
    }
    val (columnBatchSize, useCompression) = getCachedBatchParams
    val output = df.logicalPlan.output
    val cached = rdd.mapPartitionsPreserve(rowIterator => {

      def columnBuilders = output.map { attribute =>
        val columnType = ColumnType(attribute.dataType)
        val initialBufferSize = columnType.defaultSize * columnBatchSize
        ColumnBuilder(attribute.dataType, initialBufferSize,
          attribute.name, useCompression)
      }.toArray

      val holder = new CachedBatchHolder(columnBuilders, 0, columnBatchSize,
        schema, cachedBatchAggregate)
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
    Constant.INTERNAL_SCHEMA_NAME + "." + tableName + Constant.SHADOW_TABLE_SUFFIX
  }

  final def getTableName(cachedBatchTablename: String): String = {
    cachedBatchTablename.substring(Constant.INTERNAL_SCHEMA_NAME.length + 1,
      cachedBatchTablename.indexOf(Constant.SHADOW_TABLE_SUFFIX)).replace("__", ".")
  }
}
