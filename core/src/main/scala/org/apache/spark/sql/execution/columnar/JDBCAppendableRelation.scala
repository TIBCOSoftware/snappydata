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
package org.apache.spark.sql.execution.columnar

import java.sql.Connection
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.JavaConverters._

import com.pivotal.gemfirexd.Attribute
import io.snappydata.SnappyTableStatsProviderService

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.SortDirection
import org.apache.spark.sql.catalyst.plans.logical.OverwriteOptions
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.hive.QualifiedTableName
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType


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
    with PlanInsertableRelation
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
      .createConnectionFactory(new JDBCOptions(connProperties.url,
        table, connProperties.connProps.asScala.toMap))

  val resolvedName: String = table

  def numBuckets: Int = -1

  def isPartitioned: Boolean = true

  override def sizeInBytes: Long = {
    SnappyTableStatsProviderService.getService.getTableStatsFromService(table) match {
      case Some(s) => s.getTotalSize
      case None => super.sizeInBytes
    }
  }

  protected final def dialect: JdbcDialect = connProperties.dialect

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
      filters: Array[Filter], prunePartitions: => Int): RDD[Any] = {

    val requestedColumns = if (requiredColumns.isEmpty) {
      val narrowField =
        schema.fields.minBy { a =>
          ColumnType(a.dataType).defaultSize
        }

      Array(narrowField.name)
    } else {
      requiredColumns
    }

    readLock {
      externalStore.getColumnBatchRDD(tableName, rowBuffer = table,
        requestedColumns.map(column => externalStore.columnPrefix + column),
        prunePartitions,
        sqlContext.sparkSession, schema)
    }
  }

  override def getInsertPlan(relation: LogicalRelation,
      child: SparkPlan): SparkPlan = {
    new ColumnInsertExec(child, Seq.empty, Seq.empty, this, table)
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    // use the Insert plan for best performance
    // that will use the getInsertPlan above (in StoreStrategy)
    sqlContext.sessionState.executePlan(
      new Insert(
        table = LogicalRelation(this),
        partition = Map.empty[String, Option[String]],
        child = data.logicalPlan,
        OverwriteOptions(overwrite),
        ifNotExists = false)).toRdd
  }

  def getColumnBatchParams: (Int, Int, String) = {
    val session = sqlContext.sparkSession
    val columnBatchSize = origOptions.get(
      ExternalStoreUtils.COLUMN_BATCH_SIZE) match {
      case Some(cb) if !origOptions.contains(ExternalStoreUtils.COLUMN_BATCH_SIZE_TRANSIENT) =>
        Integer.parseInt(cb)
      case _ => ExternalStoreUtils.defaultColumnBatchSize(session)
    }
    val columnMaxDeltaRows = origOptions.get(
      ExternalStoreUtils.COLUMN_MAX_DELTA_ROWS) match {
      case Some(cd) if !origOptions.contains(ExternalStoreUtils.COLUMN_MAX_DELTA_ROWS_TRANSIENT) =>
        Integer.parseInt(cd)
      case _ => ExternalStoreUtils.defaultColumnMaxDeltaRows(session)
    }
    val compressionCodec = origOptions.get(
      ExternalStoreUtils.COMPRESSION_CODEC) match {
      case Some(codec) => codec
      case None => ExternalStoreUtils.defaultCompressionCodec(session)
    }
    (columnBatchSize, columnMaxDeltaRows, compressionCodec)
  }

  // truncate both actual and shadow table
  def truncate(): Unit = writeLock {
    externalStore.tryExecute(table) { conn =>
      JdbcExtendedUtils.truncateTable(conn, table, dialect)
    }
  }

  def createTable(mode: SaveMode): Unit = {
    val conn = connFactory()
    try {
      tableExists = JdbcExtendedUtils.tableExists(table, conn,
        dialect, sqlContext)
      if (mode == SaveMode.Ignore && tableExists) {
//        dialect match {
//          case d: JdbcExtendedDialect =>
//            d.initializeTable(table,
//              sqlContext.conf.caseSensitiveAnalysis, conn)
//          case _ => // do nothing
//        }
      }
      else if (mode == SaveMode.ErrorIfExists && tableExists) {
        sys.error(s"Table $table already exists.")
      }
    } finally {
      conn.commit()
      conn.close()
    }
    createExternalTableForColumnBatches(table, externalStore)
  }

  protected def createExternalTableForColumnBatches(tableName: String,
      externalStore: ExternalStore): Unit = {
    require(tableName != null && tableName.length > 0,
      "createExternalTableForColumnBatches: expected non-empty table name")

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

    externalStore.tryExecute(tableName)
      { conn =>
        if (dropIfExists) {
          JdbcExtendedUtils.dropTable(conn, tableName, dialect, sqlContext,
            ifExists = true)
        }
        val tableExists = JdbcExtendedUtils.tableExists(tableName, conn,
          dialect, sqlContext)
        if (!tableExists) {
          val pass = connProperties.connProps.remove(Attribute.PASSWORD_ATTR)
          logInfo(s"Applying DDL (url=${connProperties.url}; " +
              s"props=${connProperties.connProps}): $tableStr")
          if (pass != null) {
            connProperties.connProps.setProperty(Attribute.PASSWORD_ATTR, pass.asInstanceOf[String])
          }
          JdbcExtendedUtils.executeUpdate(tableStr, conn)
          dialect match {
            case d: JdbcExtendedDialect => d.initializeTable(tableName,
              sqlContext.conf.caseSensitiveAnalysis, conn)
            case _ => // do nothing
          }
        }
      }
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
        conn.commit()
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

  private[sql] def externalColumnTableName: String
}
