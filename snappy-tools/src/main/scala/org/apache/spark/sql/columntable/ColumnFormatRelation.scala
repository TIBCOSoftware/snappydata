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
package org.apache.spark.sql.columntable

import java.sql.Connection

import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.PartitionedRegion
import com.pivotal.gemfirexd.internal.engine.Misc

import org.apache.spark.rdd.RDD

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.scheduler.SparkListenerUnpersistRDD
import org.apache.spark.sql.collection.{UUIDRegionKey, Utils}
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.execution.columnar.{ColumnarRelationProvider, ExternalStoreUtils, JDBCAppendableRelation, _}
import org.apache.spark.sql.execution.{ConnectionPool, PartitionedDataSourceScan}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.row.GemFireXDDialect
import org.apache.spark.sql.rowtable.RowFormatScanRDD
import org.apache.spark.sql.sources.{JdbcExtendedDialect, _}
import org.apache.spark.sql.store.impl.{JDBCSourceAsColumnarStore, SparkShellRowRDD}
import org.apache.spark.sql.store.{CodeGeneration, ExternalStore, StoreUtils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, _}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{Logging, Partition}
import io.snappydata.Constant._
/**
 * This class acts as a DataSource provider for column format tables provided Snappy.
 * It uses GemFireXD as actual datastore to physically locate the tables.
 * Column tables can be used for storing data in columnar compressed format.
 * A example usage is given below.
 *
 * val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(tableName, "column", dataDF.schema, props)
    dataDF.write.insertInto(tableName)

    This provider scans underlying tables in parallel and is aware of the data partition.
    It does not introduces a shuffle if simple table query is fired.
    One can insert a single or multiple rows into this table as well as do a bulk insert by a Spark DataFrame.
    Bulk insert example is shown above.
 */
class ColumnFormatRelation(
    _table: String,
    _provider: String,
    _mode: SaveMode,
    _userSchema: StructType,
    schemaExtensions: String,
    ddlExtensionForShadowTable: String,
    _origOptions: Map[String, String],
    _externalStore: ExternalStore,
    blockMap: Map[InternalDistributedMember, BlockManagerId],
    partitioningColumns: Seq[String],
    _context: SQLContext)
    extends JDBCAppendableRelation(_table, _provider, _mode, _userSchema,
     _origOptions, _externalStore, _context)
    with PartitionedDataSourceScan with RowInsertableRelation {

  override def toString: String = s"ColumnFormatRelation[$table]"

  override def sizeInBytes: Long = SnappyAnalyticsService.getValueSize(table , true)

  val columnBatchSize = sqlContext.conf.columnBatchSize

  lazy val connectionType = ExternalStoreUtils.getConnectionType(dialect)

  private val resolvedName: String = externalStore.tryExecute(table, conn => {
    StoreUtils.lookupName(table, conn.getSchema)
  })

  val rowInsertStr = ExternalStoreUtils.getInsertStringWithColumnName(
    resolvedName, userSchema)

  override lazy val numPartitions: Int = {
    val region = Misc.getRegionForTable(resolvedName, true).
        asInstanceOf[PartitionedRegion]
    region.getTotalNumberOfBuckets
  }

  override def partitionColumns: Seq[String] = {
    connectionType match {
      case ConnectionType.Embedded => partitioningColumns
      // TODO: [sumedh] is the issue in comment below being tracked somewhere??
      case _ =>   Seq.empty[String] // Temporary fix till we fix Non-EMbededed join
    }
  }

  override def scanTable(tableName: String, requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {
    super.scanTable(ColumnFormatRelation.cachedBatchTableName(tableName),
      requiredColumns, filters)
  }

  // TODO: Suranjan currently doesn't apply any filters.
  // will see that later.
  override def buildScan(requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {
    val colRdd = scanTable(table, requiredColumns, filters)
    // TODO: Suranjan scanning over column rdd before row will make sure
    // that we don't have duplicates; we may miss some results though
    // [sumedh] In the absence of snapshot isolation, one option is to
    // use increasing cached batch IDs and note the IDs at the start, then
    // scan row buffer first and delay cached batch creation till that is done,
    // finally skipping any IDs greater than the noted ones.
    // However, with plans for mutability in column store (via row buffer) need
    // to re-think in any case and provide proper snapshot isolation in store.
    val zipped = connectionType match {
      case ConnectionType.Embedded =>
        val rowRdd = new RowFormatScanRDD(
          sqlContext.sparkContext,
          executorConnector,
          ExternalStoreUtils.pruneSchema(schemaFields, requiredColumns),
          table,
          requiredColumns,
          connProperties,
          filters,
          Array.empty[Partition],
          blockMap
        ).asInstanceOf[RDD[Row]]

        rowRdd.zipPartitions(colRdd) { (leftItr, rightItr) =>
          leftItr ++ rightItr
        }

      case _ =>
        val rowRdd = new SparkShellRowRDD(
          sqlContext.sparkContext,
          executorConnector,
          ExternalStoreUtils.pruneSchema(schemaFields, requiredColumns),
          table,
          requiredColumns,
          connProperties,
          externalStore,
          filters
        ).asInstanceOf[RDD[Row]]

        rowRdd.zipPartitions(colRdd) { (leftItr, rightItr) =>
          leftItr ++ rightItr
        }
    }
    zipped
  }

  override def uuidBatchAggregate(accumulated: ArrayBuffer[UUIDRegionKey],
      batch: CachedBatch): ArrayBuffer[UUIDRegionKey] = {
    //TODO - currently using the length from the part Object but it needs to be handled more generically
    //in order to replace UUID
    // if number of rows are greater than columnBatchSize then store otherwise store locally
    if (batch.numRows >= columnBatchSize) {
      val uuid = externalStore.storeCachedBatch(ColumnFormatRelation.
          cachedBatchTableName(table), batch)
      accumulated += uuid
    } else {
      //TODO: can we do it before compressing. Might save a bit
      val unCachedRows = ExternalStoreUtils.cachedBatchesToRows(
        Iterator(batch), schema.map(_.name).toArray, schema)
      insert(unCachedRows)
    }
    accumulated
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    partitionColumns match {
      case Nil => super.insert(data, overwrite)
      case _ => insert(data, if (overwrite) SaveMode.Overwrite else SaveMode.Append)
    }
  }

  def insert(data: DataFrame, mode: SaveMode): Unit = {
    if (mode == SaveMode.Overwrite) {
      truncate()
    }
    JdbcExtendedUtils.saveTable(data, table, connProperties)
  }

  /**
   * Insert a sequence of rows into the table represented by this relation.
   *
   * @param rows the rows to be inserted
    * @return number of rows inserted
   */
  override def insert(rows: Seq[Row]): Int = {
    val numRows = rows.length
    if (numRows == 0) {
      throw new IllegalArgumentException(
        "ColumnFormatRelation.insert: no rows provided")
    }
    val connProps = connProperties.connProps
    val batchSize = connProps.getProperty("batchsize", "1000").toInt
    val connection = ConnectionPool.getPoolConnection(table, dialect,
      connProperties.poolProps, connProps, connProperties.hikariCP)
    try {
      val stmt = connection.prepareStatement(rowInsertStr)
      val result = CodeGeneration.executeUpdate(table, stmt,
        rows, numRows > 1, batchSize, userSchema.fields, dialect)
      stmt.close()
      result
    } finally {
      connection.close()
    }
  }

  /**
   * Insert a sequence of rows into the table represented by this relation.
   *
   * @param rows the rows to be inserted
    * @return number of rows inserted
   */
  def insert(rows: Iterator[InternalRow]): Int = {
    if (rows.hasNext) {
      val connProps = connProperties.connProps
      val batchSize = connProps.getProperty("batchsize", "1000").toInt
      val connection = ConnectionPool.getPoolConnection(table, dialect,
        connProperties.poolProps, connProps, connProperties.hikariCP)
      try {
        val stmt = connection.prepareStatement(rowInsertStr)
        val result = CodeGeneration.executeUpdate(table, stmt,
          rows, multipleRows = true, batchSize, userSchema.fields, dialect)
        stmt.close()
        result
      } finally {
        connection.close()
      }
    } else 0
  }

  // truncate both actual and shadow table
  override def truncate() = writeLock {
    try {
      val columnTable = ColumnFormatRelation.cachedBatchTableName(table)
      externalStore.tryExecute(columnTable, conn => {
        JdbcExtendedUtils.truncateTable(conn, ColumnFormatRelation.
            cachedBatchTableName(table), dialect)
      })
    } finally {
      externalStore.tryExecute(table, conn => {
        JdbcExtendedUtils.truncateTable(conn, table, dialect)
      })
    }
  }

  /**
   * Destroy and cleanup this relation. It may include, but not limited to,
   * dropping the external table that this relation represents.
   */
  override def destroy(ifExists: Boolean): Unit = {
    // use a non-pool connection for operations
    val conn = connFactory()
    try {
      // clean up the connection pool and caches
      StoreUtils.removeCachedObjects(sqlContext, table, numPartitions)

    } finally {
      try {
        try {
          JdbcExtendedUtils.dropTable(conn, ColumnFormatRelation.
              cachedBatchTableName(table), dialect, sqlContext, ifExists)
        } finally {
          JdbcExtendedUtils.dropTable(conn, table, dialect, sqlContext,
            ifExists)
        }
      } finally {
        conn.close()
      }
    }
  }

  override def createTable(mode: SaveMode): Unit = {
    val conn = connFactory()
    try {
      val tableExists = JdbcExtendedUtils.tableExists(table, conn,
        dialect, sqlContext)
      if (mode == SaveMode.Ignore && tableExists) {
        dialect match {
          case GemFireXDDialect =>
            GemFireXDDialect.initializeTable(table,
              sqlContext.conf.caseSensitiveAnalysis, conn)
            GemFireXDDialect.initializeTable(ColumnFormatRelation.
                cachedBatchTableName(table),
              sqlContext.conf.caseSensitiveAnalysis, conn)
          case _ => // Do nothing
        }
        return
      }

      if (mode == SaveMode.ErrorIfExists && tableExists) {
        sys.error(s"Table $table already exists.")
      }
    } finally {
      conn.close()
    }
    createActualTable(table, externalStore)
  }

  override def createExternalTableForCachedBatches(tableName: String,
      externalStore: ExternalStore): Unit = {
    require(tableName != null && tableName.length > 0,
      "createExternalTableForCachedBatches: expected non-empty table name")


    val (primarykey, partitionStrategy) = dialect match {
      // The driver if not a loner should be an accessor only
      case d: JdbcExtendedDialect =>
        (s"constraint ${tableName}_bucketCheck check (bucketId != -1), " +
            "primary key (uuid, bucketId) ", d.getPartitionByClause("bucketId"))
      case _ => ("primary key (uuid)", "") //TODO. How to get primary key contraint from each DB
    }
    val colocationClause = s"COLOCATE WITH ($table)"

    createTable(externalStore, s"create table $tableName (uuid varchar(36) " +
        "not null, bucketId integer, numRows integer not null, stats blob, " +
        userSchema.fields.map(structField => externalStore.columnPrefix +
        structField.name + " blob").mkString(" ", ",", " ") +
        s", $primarykey) $partitionStrategy $colocationClause $ddlExtensionForShadowTable",
        tableName, dropIfExists = false)
  }

  //TODO: Suranjan make sure that this table doesn't evict to disk by
  // setting some property, may be MemLRU?
  def createActualTable(tableName: String, externalStore: ExternalStore) = {

    // Create the table if the table didn't exist.
    var conn: Connection = null
    try {
      conn = connFactory()
      val tableExists = JdbcExtendedUtils.tableExists(tableName, conn,
        dialect, sqlContext)
      if (!tableExists) {
        val sql = s"CREATE TABLE $tableName $schemaExtensions "
        logInfo(s"Applying DDL (url=${connProperties.url}; " +
            s"props=${connProperties.connProps}): $sql")
        JdbcExtendedUtils.executeUpdate(sql, conn)
        dialect match {
          case d: JdbcExtendedDialect => d.initializeTable(tableName,
            sqlContext.conf.caseSensitiveAnalysis, conn)
        }
        createExternalTableForCachedBatches(ColumnFormatRelation.
            cachedBatchTableName(table), externalStore)
      }
    }
    catch {
      case sqle: java.sql.SQLException =>
        if (sqle.getMessage.contains("No suitable driver found")) {
          throw new java.sql.SQLException(s"${sqle.getMessage}\n" +
              "Ensure that the 'driver' option is set appropriately and " +
              "the driver jars available (--jars option in spark-submit).", sqle.getSQLState)
        } else {
          throw sqle
        }
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
  }

  /**
   * Execute a DML SQL and return the number of rows affected.
   */
  override def executeUpdate(sql: String): Int = {
    val connection = ConnectionPool.getPoolConnection(table, dialect,
      connProperties.poolProps, connProperties.connProps,
      connProperties.hikariCP)
    try {
      val stmt = connection.prepareStatement(sql)
      //stmt.setSt
      val result = stmt.executeUpdate()
      stmt.close()
      result
    } finally {
      connection.close()
    }
  }

  override def flushRowBuffer(): Unit = {
    // force flush all the buckets into the column store
    Utils.mapExecutors(sqlContext, () => {
      ColumnFormatRelation.flushLocalBuckets(resolvedName)
      Iterator.empty
    })
    ColumnFormatRelation.flushLocalBuckets(resolvedName)
  }
}


object ColumnFormatRelation extends Logging with StoreCallback {
  // register the call backs with the JDBCSource so that
  // bucket region can insert into the column table

  def flushLocalBuckets(resolvedName: String): Unit = {
    val pr = Misc.getRegionForTable(resolvedName, false)
        .asInstanceOf[PartitionedRegion]
    if (pr != null) {
      val ds = pr.getDataStore
      if (ds != null) {
        val itr = ds.getAllLocalPrimaryBucketRegions.iterator()
        while (itr.hasNext) {
          itr.next().createAndInsertCachedBatch(true)
        }
      }
    }
  }

  def registerStoreCallbacks(sqlContext: SQLContext, table: String,
      userSchema: StructType, externalStore: ExternalStore) = {
    StoreCallbacksImpl.registerExternalStoreAndSchema(sqlContext, table, userSchema,
      externalStore, sqlContext.conf.columnBatchSize, sqlContext.conf.useCompression)
  }

  final def cachedBatchTableName(table: String) = {

    val tableName = if(table.indexOf('.') > 0){
      table.replace(".","__")
    } else {
      table
    }
    INTERNAL_SCHEMA_NAME + "." + tableName +
        SHADOW_TABLE_SUFFIX
  }
}

final class DefaultSource extends ColumnarRelationProvider {

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schema: StructType) = {
    val parameters = new CaseInsensitiveMutableHashMap(options)

    val table = ExternalStoreUtils.removeInternalProps(parameters)
    val sc = sqlContext.sparkContext
    val partitions = ExternalStoreUtils.getTotalPartitions(sc, parameters,
      forManagedTable = true, forColumnTable = true)
    val parametersForShadowTable = new CaseInsensitiveMutableHashMap(parameters)

    val partitioningColumn = StoreUtils.getPartitioningColumn(parameters)
    val primaryKeyClause = StoreUtils.getPrimaryKeyClause(parameters)
    val ddlExtension = StoreUtils.ddlExtensionString(parameters,
      isRowTable = false, isShadowTable = false)

    val ddlExtensionForShadowTable = StoreUtils.ddlExtensionString(
      parametersForShadowTable, isRowTable = false, isShadowTable = true)

    val connProperties =
      ExternalStoreUtils.validateAndGetAllProps(sc, parameters)

    StoreUtils.validateConnProps(parameters)

    val blockMap = connProperties.dialect match {
      case GemFireXDDialect => StoreUtils.initStore(sqlContext, table,
        Some(schema), partitions, connProperties)
      case _ => Map.empty[InternalDistributedMember, BlockManagerId]
    }
    val schemaString = JdbcExtendedUtils.schemaString(schema,
      connProperties.dialect)
    val schemaExtension = if (schemaString.length > 0) {
      val temp = schemaString.substring(0, schemaString.length - 1).
          concat(s", ${StoreUtils.SHADOW_COLUMN}, $primaryKeyClause )")
      s"$temp $ddlExtension"
    } else {
      s"$schemaString $ddlExtension"
    }

    val externalStore = new JDBCSourceAsColumnarStore(connProperties,
      partitions, blockMap)

    ColumnFormatRelation.registerStoreCallbacks(sqlContext, table,
      schema, externalStore)

    var success = false
    val relation = new ColumnFormatRelation(SnappyStoreHiveCatalog.
        processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName,
      mode,
      schema,
      schemaExtension,
      ddlExtensionForShadowTable,
      options,
      externalStore,
      blockMap,
      partitioningColumn,
      sqlContext)
    try {
      relation.createTable(mode)
      success = true
      relation
    } finally {
      if (!success) {
        // destroy the relation
        relation.destroy(ifExists = true)
      }
    }
  }
}
