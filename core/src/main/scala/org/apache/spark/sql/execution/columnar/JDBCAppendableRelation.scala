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
package org.apache.spark.sql.execution.columnar

import java.sql.Connection
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.JavaConverters._

import com.pivotal.gemfirexd.Attribute
import io.snappydata.{Constant, SnappyTableStatsProviderService}
import org.eclipse.collections.impl.map.mutable.primitive.ObjectLongHashMap

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Expression, SortDirection}
import org.apache.spark.sql.catalyst.plans.logical.OverwriteOptions
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcUtils}
import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType


/**
 * A LogicalPlan implementation for an external column table whose contents
 * are retrieved using a JDBC URL or DataSource.
 */
abstract case class JDBCAppendableRelation(
    override val table: String,
    provider: String,
    mode: SaveMode,
    userSchema: StructType,
    origOptions: CaseInsensitiveMap,
    externalStore: ExternalStore,
    @transient override val sqlContext: SQLContext) extends BaseRelation
    with PrunedUnsafeFilteredScan
    with InsertableRelation
    with PlanInsertableRelation
    with DestroyRelation
    with IndexableRelation
    with Logging
    with NativeTableRowLevelSecurityRelation
    with Serializable {

  self =>

  override val needConversion: Boolean = false

  private[sql] var tableCreated: Boolean = _

  override final def connProperties: ConnectionProperties = externalStore.connProperties

  override protected final def isRowTable: Boolean = false

  override final val connFactory: () => Connection = JdbcUtils
      .createConnectionFactory(new JDBCOptions(connProperties.url,
        table, connProperties.connProps.asScala.toMap))

  override final def resolvedName: String = table

  protected var delayRollover = false

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
      filters: Array[Expression], prunePartitions: () => Int): (RDD[Any], Array[Int]) = {

    val fieldNames = new ObjectLongHashMap[String](schema.length)
    (0 until schema.length).foreach(i =>
      fieldNames.put(Utils.toLowerCase(schema(i).name), i + 1))
    val projection = requiredColumns.map { c =>
      val index = fieldNames.get(Utils.toLowerCase(c))
      if (index == 0) Utils.analysisException(s"Column $c does not exist in $tableName")
      index.toInt
    }
    readLock {
      externalStore.getColumnBatchRDD(tableName, rowBuffer = table, projection,
        filters, prunePartitions, sqlContext.sparkSession, schema, delayRollover) -> projection
    }
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
        ExternalStoreUtils.sizeAsBytes(cb, ExternalStoreUtils.COLUMN_BATCH_SIZE)
      case _ => ExternalStoreUtils.defaultColumnBatchSize(session)
    }
    val columnMaxDeltaRows = origOptions.get(
      ExternalStoreUtils.COLUMN_MAX_DELTA_ROWS) match {
      case Some(cd) if !origOptions.contains(ExternalStoreUtils.COLUMN_MAX_DELTA_ROWS_TRANSIENT) =>
        ExternalStoreUtils.checkPositiveNum(Integer.parseInt(cd),
          ExternalStoreUtils.COLUMN_MAX_DELTA_ROWS)
      case _ => ExternalStoreUtils.defaultColumnMaxDeltaRows(session)
    }
    (columnBatchSize, columnMaxDeltaRows, getCompressionCodec)
  }

  def getCompressionCodec: String = {
    origOptions.get(ExternalStoreUtils.COMPRESSION_CODEC) match {
      case Some(codec) => codec
      case None => Constant.DEFAULT_CODEC
    }
  }

  protected def createTable(conn: Connection, tableStr: String, tableName: String): Unit = {
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

  def flushRowBuffer(): Unit = {
    // nothing by default
  }

  override def createIndex(indexIdent: TableIdentifier,
      tableIdent: TableIdentifier,
      indexColumns: Seq[(String, Option[SortDirection])],
      options: Map[String, String]): Unit = {
    throw new UnsupportedOperationException(s"Indexes are not supported for $toString")
  }

  override def dropIndex(indexIdent: TableIdentifier,
      tableIdent: TableIdentifier,
      ifExists: Boolean): Unit = {
    throw new UnsupportedOperationException(s"Indexes are not supported for $toString")
  }

  private[sql] def externalColumnTableName: String
}
