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
package org.apache.spark.sql.execution.columnar.impl

import java.sql.{Connection, PreparedStatement}

import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.internal.cache.{ExternalTableMetaData, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.Constant

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SortDirection}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.execution.columnar._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.row.RowFormatScanRDD
import org.apache.spark.sql.execution.{ConnectionPool, PartitionedDataSourceScan, columnar}
import org.apache.spark.sql.hive.{QualifiedTableName, SnappyStoreHiveCatalog}
import org.apache.spark.sql.row.GemFireXDDialect
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.{CodeGeneration, StoreUtils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Logging, Partition}

/**
 * This class acts as a DataSource provider for column format tables provided Snappy.
 * It uses GemFireXD as actual datastore to physically locate the tables.
 * Column tables can be used for storing data in columnar compressed format.
 * A example usage is given below.
 *
 * val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
 * val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
 * val dataDF = snc.createDataFrame(rdd)
 * snc.createTable(tableName, "column", dataDF.schema, props)
 * dataDF.write.insertInto(tableName)
 *
 * This provider scans underlying tables in parallel and is aware of the data partition.
 * It does not introduces a shuffle if simple table query is fired.
 * One can insert a single or multiple rows into this table as well
 * as do a bulk insert by a Spark DataFrame.
 * Bulk insert example is shown above.
 */
class BaseColumnFormatRelation(
    _table: String,
    _provider: String,
    _mode: SaveMode,
    _userSchema: StructType,
    schemaExtensions: String,
    ddlExtensionForShadowTable: String,
    _origOptions: Map[String, String],
    _externalStore: ExternalStore,
    partitioningColumns: Seq[String],
    _context: SQLContext)
    extends JDBCAppendableRelation(_table, _provider, _mode, _userSchema,
      _origOptions, _externalStore, _context)
    with PartitionedDataSourceScan
    with RowInsertableRelation {

  override def toString: String = s"${getClass.getSimpleName}[$table]"

  override val connectionType: ConnectionType.Value =
    ExternalStoreUtils.getConnectionType(dialect)

  lazy val rowInsertStr: String = ExternalStoreUtils
      .getInsertStringWithColumnName(resolvedName, schema)

  @transient protected lazy val region: PartitionedRegion =
    Misc.getRegionForTable(resolvedName, true).asInstanceOf[PartitionedRegion]

  def getColumnBatchStatistics(schema: Seq[AttributeReference]): PartitionStatistics = {
    new PartitionStatistics(schema)
  }

  override lazy val numBuckets: Int = {
    region.getTotalNumberOfBuckets
  }

  override def partitionColumns: Seq[String] = {
    partitioningColumns
  }

  override def scanTable(tableName: String, requiredColumns: Array[String],
      filters: Array[Filter]): (RDD[CachedBatch], Array[String]) = {
    super.scanTable(ColumnFormatRelation.cachedBatchTableName(tableName),
      requiredColumns, filters)
  }

  override def buildUnsafeScan(requiredColumns: Array[String],
      filters: Array[Filter]): (RDD[Any], Seq[RDD[InternalRow]]) = {
    val (rdd, _) = scanTable(table, requiredColumns, filters)
    val zipped = buildRowBufferRDD(rdd.partitions, requiredColumns, filters,
      useResultSet = true).zipPartitions(rdd) { (leftItr, rightItr) =>
      Iterator[Any](leftItr, rightItr)
    }
    (zipped, Nil)
  }


  def buildUnsafeScanForSampledRelation(requiredColumns: Array[String],
      filters: Array[Filter]): (RDD[Any], RDD[Any],
      Seq[RDD[InternalRow]]) = {
    val (rdd, _) = scanTable(table, requiredColumns, filters)
    val rowRDD = buildRowBufferRDD(rdd.partitions, requiredColumns, filters,
      useResultSet = true)
    (rdd.asInstanceOf[RDD[Any]], rowRDD.asInstanceOf[RDD[Any]], Nil)
  }

  def buildRowBufferRDD(partitions: Array[Partition],
      requiredColumns: Array[String], filters: Array[Filter],
      useResultSet: Boolean): RDD[Any] = {
    // TODO: Suranjan scanning over column rdd before row will make sure
    // that we don't have duplicates; we may miss some results though
    // [sumedh] In the absence of snapshot isolation, one option is to
    // use increasing cached batch IDs and note the IDs at the start, then
    // scan row buffer first and delay cached batch creation till that is done,
    // finally skipping any IDs greater than the noted ones.
    // However, with plans for mutability in column store (via row buffer) need
    // to re-think in any case and provide proper snapshot isolation in store.
    val isPartitioned = region.getPartitionAttributes != null
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    connectionType match {
      case ConnectionType.Embedded =>
        new RowFormatScanRDD(
          session,
          resolvedName,
          isPartitioned,
          requiredColumns,
          pushProjections = false,
          useResultSet = useResultSet,
          connProperties,
          Array.empty[Filter],
          // use same partitions as the column store (SNAP-1083)
          partitions
        )
      case _ =>
        new SparkShellRowRDD(
          session,
          resolvedName,
          isPartitioned,
          requiredColumns,
          connProperties,
          filters,
          // use same partitions as the column store (SNAP-1083)
          partitions
        )
    }
  }

  private[this] val forceFlush = java.lang.Boolean.getBoolean(
    "snappydata.testForceFlush")


  override def cachedBatchAggregate(batch: CachedBatch): Unit = {
    // if number of rows are greater than columnBatchSize then store
    // otherwise store locally
    val (columnBatchSize, useCompression) = getCachedBatchParams
    if (batch.numRows >= Constant.COLUMN_MIN_BATCH_SIZE || forceFlush ||
        batch.numRows <= math.max(1, columnBatchSize)) {
      externalStore.storeCachedBatch(ColumnFormatRelation.
          cachedBatchTableName(table), batch)
    } else {
      // TODO: can we do it before compressing. Might save a bit.
      // [sumedh] instead we should add it to a separate CachedBatch
      // which will be appended with such small pieces in future.
      val unCachedRows = ExternalStoreUtils.cachedBatchesToRows(
        Iterator(batch), schema.map(_.name).toArray, schema, forScan = false)
      insert(unCachedRows)
    }
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
    JdbcExtendedUtils.saveTable(data, table, schema, connProperties)
    flushRowBuffer()
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
        rows, numRows > 1, batchSize, schema.fields, dialect)
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
        val result = CodeGeneration.executeUpdate(table, stmt, rows,
          multipleRows = true, batchSize, schema.fields.map(_.dataType), dialect)
        stmt.close()
        result
      } finally {
        connection.close()
      }
    } else 0
  }

  // truncate both actual and shadow table
  override def truncate(): Unit = writeLock {
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
      StoreUtils.removeCachedObjects(sqlContext, table)
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
      tableExists = JdbcExtendedUtils.tableExists(table, conn,
        dialect, sqlContext)

      if (tableExists) {
        mode match {
          case SaveMode.Ignore =>
            // TODO: Suranjan for split mode when driver acts as client
            // we will need to change this code and add a flag to
            // CREATE_ALL_BUCKETS to create only when no buckets created
            if (region.getRegionAdvisor().getCreatedBucketsCount == 0) {
              dialect match {
                case GemFireXDDialect =>
                  GemFireXDDialect.initializeTable(table,
                    sqlContext.conf.caseSensitiveAnalysis, conn)
                  GemFireXDDialect.initializeTable(ColumnFormatRelation.
                      cachedBatchTableName(table),
                    sqlContext.conf.caseSensitiveAnalysis, conn)
                case _ => // Do nothing
              }
            }
            return
          case SaveMode.ErrorIfExists =>
            // sys.error(s"Table $table already exists.") TODO: Why so?
            return
          case _ => // Ignore
        }
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


    val (primaryKey, partitionStrategy, concurrency) = dialect match {
      // The driver if not a loner should be an accessor only
      case d: JdbcExtendedDialect =>
        (s"constraint ${tableName}_partitionCheck check (partitionId != -1), " +
            "primary key (uuid, partitionId) ",
            d.getPartitionByClause("partitionId"),
            "  DISABLE CONCURRENCY CHECKS ")
      case _ => ("primary key (uuid)", "", "")
    }
    val colocationClause = s"COLOCATE WITH ($table)"

    // if the numRows or other columns are ever changed here, then change
    // the hardcoded positions in insert and PartitionedPhysicalRDD.CT_*
    createTable(externalStore, s"create table $tableName (uuid varchar(36) " +
        "not null, partitionId integer, numRows integer not null, stats blob, " +
        schema.fields.map(structField => externalStore.columnPrefix +
            structField.name + " blob").mkString(", ") +
        s", $primaryKey) $partitionStrategy $colocationClause " +
        s" $concurrency $ddlExtensionForShadowTable",
      tableName, dropIfExists = false)
  }

  // TODO: Suranjan make sure that this table doesn't evict to disk by
  // setting some property, may be MemLRU?
  private def createActualTable(tableName: String,
      externalStore: ExternalStore): Unit = {
    // Create the table if the table didn't exist.
    var conn: Connection = null
    try {
      conn = connFactory()
      val tableExists = JdbcExtendedUtils.tableExists(tableName, conn,
        dialect, sqlContext)
      if (!tableExists) {
        val sql =
          s"CREATE TABLE $tableName $schemaExtensions DISABLE CONCURRENCY CHECKS"
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
    } catch {
      case sqle: java.sql.SQLException =>
        if (sqle.getMessage.contains("No suitable driver found")) {
          throw new java.sql.SQLException(s"${sqle.getMessage}\n" +
              "Ensure that the 'driver' option is set appropriately and " +
              "the driver jars available (--jars option in spark-submit).",
            sqle.getSQLState)
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
      val result = stmt.executeUpdate()
      stmt.close()
      result
    } finally {
      connection.close()
    }
  }

  override def flushRowBuffer(): Unit = {
    SnappyContext.getClusterMode(_context.sparkContext) match {
      case SnappyEmbeddedMode(_, _) | LocalMode(_, _) =>
        // force flush all the buckets into the column store
        Utils.mapExecutors(sqlContext, () => {
          ColumnFormatRelation.flushLocalBuckets(resolvedName)
          Iterator.empty
        }).count()
        ColumnFormatRelation.flushLocalBuckets(resolvedName)
      case _ =>
    }
  }
}

class ColumnFormatRelation(
    _table: String,
    _provider: String,
    _mode: SaveMode,
    _userSchema: StructType,
    schemaExtensions: String,
    ddlExtensionForShadowTable: String,
    _origOptions: Map[String, String],
    _externalStore: ExternalStore,
    partitioningColumns: Seq[String],
    _context: SQLContext)
  extends BaseColumnFormatRelation(
    _table,
    _provider,
    _mode,
    _userSchema,
    schemaExtensions,
    ddlExtensionForShadowTable,
    _origOptions,
    _externalStore,
    partitioningColumns,
    _context)
  with ParentRelation with DependentRelation {
  val tableOptions = new CaseInsensitiveMutableHashMap(_origOptions)

  override def addDependent(dependent: DependentRelation,
      catalog: SnappyStoreHiveCatalog): Boolean =
    DependencyCatalog.addDependent(table, dependent.name)

  override def removeDependent(dependent: DependentRelation,
      catalog: SnappyStoreHiveCatalog): Boolean =
    DependencyCatalog.removeDependent(table, dependent.name)

  override def dropIndex(indexIdent: QualifiedTableName,
      tableIdent: QualifiedTableName,
      ifExists: Boolean): Unit = {
    val snappySession = sqlContext.sparkSession.asInstanceOf[SnappySession]
    snappySession.sessionState.catalog.removeDependentRelation(tableIdent, indexIdent)
    // Remove the actual index
    snappySession.dropTable(indexIdent, ifExists)
  }

  override def getDependents(
      catalog: SnappyStoreHiveCatalog): Seq[String] =
    DependencyCatalog.getDependents(table)

  override def recoverDependentRelations(properties: Map[String, String]): Unit = {
    var dependentRelations: Array[String] = Array()
    if (properties.get(ExternalStoreUtils.DEPENDENT_RELATIONS).isDefined) {
      dependentRelations = properties(ExternalStoreUtils.DEPENDENT_RELATIONS).split(",")
    }

    val snappySession = sqlContext.sparkSession.asInstanceOf[SnappySession]
    val sncCatalog = snappySession.sessionState.catalog
    dependentRelations.foreach(rel => {
      val dr = sncCatalog.lookupRelation(sncCatalog.newQualifiedTableName(rel)) match {
        case LogicalRelation(r: DependentRelation, _, _) => r
      }
      addDependent(dr, sncCatalog)
    })

  }

  /**
   * Index table is same as the column table apart from how it is
   * partitioned and colocated. Add GEM_PARTITION_BY and GEM_COLOCATE_WITH
   * clause in its options. Also add GEM_INDEXED_TABLE parameter to
   * indicate that this is an index table.
   */
  private def createIndexTable(indexIdent: QualifiedTableName,
      tableIdent: QualifiedTableName,
      tableRelation: JDBCAppendableRelation,
      indexColumns: Map[String, Option[SortDirection]],
      options: Map[String, String]): DataFrame = {


    val parameters = new CaseInsensitiveMutableHashMap(options)
    val snappySession = sqlContext.sparkSession.asInstanceOf[SnappySession]
    val indexTblName = snappySession.getIndexTable(indexIdent).toString()
    val caseInsensitiveMap = new CaseInsensitiveMutableHashMap(tableRelation.origOptions)
    val tempOptions = caseInsensitiveMap.filterNot(pair => {
      pair._1.equalsIgnoreCase(StoreUtils.PARTITION_BY) ||
          pair._1.equalsIgnoreCase(StoreUtils.COLOCATE_WITH) ||
          pair._1.equalsIgnoreCase(JdbcExtendedUtils.DBTABLE_PROPERTY) ||
          pair._1.equalsIgnoreCase(ExternalStoreUtils.INDEX_NAME)
    }).toMap + (StoreUtils.PARTITION_BY -> indexColumns.keys.mkString(",")) +
        (StoreUtils.GEM_INDEXED_TABLE -> tableIdent.toString) +
        (JdbcExtendedUtils.DBTABLE_PROPERTY -> indexTblName)

    val indexOptions = parameters.get(StoreUtils.COLOCATE_WITH) match {
      case Some(value) =>
        val catalog = snappySession.sessionCatalog
        val colocateWith = {
          val colocationTable = catalog.newQualifiedTableName(value)
          if (catalog.tableExists(colocationTable)) {
            value
          } else {
            val idx = snappySession.getIndexTable(colocationTable)
            if (catalog.tableExists(idx)) {
              idx.toString
            } else {
              throw new AnalysisException(
                s"Could not find colocation table $colocationTable in catalog")
            }
          }
        }
        tempOptions + (StoreUtils.COLOCATE_WITH -> colocateWith)
      case _ => tempOptions
    }

    snappySession.createTable(
      indexTblName,
      "column",
      tableRelation.schema,
      indexOptions)
  }

  override def createIndex(indexIdent: QualifiedTableName,
      tableIdent: QualifiedTableName,
      indexColumns: Map[String, Option[SortDirection]],
      options: Map[String, String]): Unit = {

    val snappySession = sqlContext.sparkSession.asInstanceOf[SnappySession]
    createIndexTable(indexIdent, tableIdent, this, indexColumns, options)
    // Main table is updated to store the index information in it. We could have instead used
    // createIndex method of HiveClient to do this. But, there were two main issues:
    // a. Schema needs to be added to the HiveTable to supply indexedCols while creating
    // the index. But, when schema is added to the HiveTable object, it throws a foreign
    // key constraint failure on the serdes table. Need to look into this.
    // b. The bigger issue is that the createIndex needs an indexTable for creating this
    // index. Also, there are multiple things (like implementing HiveIndexHandler)
    // that are hive specific and can create issues for us from maintenance perspective
    try {
      snappySession.sessionState.catalog.addDependentRelation(
        tableIdent, snappySession.getIndexTable(indexIdent))

      val df = Dataset.ofRows(snappySession,
        snappySession.sessionCatalog.lookupRelation(tableIdent))

      // SB: Now populate the index table from base table.
      df.write.insertInto(snappySession.getIndexTable(indexIdent).toString())
    } catch {
      case e: Throwable =>
        snappySession.dropTable(indexIdent, ifExists = false)
        throw e
    }
  }

  /** Base table of this relation. */
  override def baseTable: Option[String] = tableOptions.get(StoreUtils.COLOCATE_WITH)

  /** Name of this relation in the catalog. */
  override def name: String = table
}

/**
 * Currently this is same as ColumnFormatRelation but has kept it as a separate class
 * to allow adding of any index specific functionality in future.
 */
class IndexColumnFormatRelation(
    _table: String,
    _provider: String,
    _mode: SaveMode,
    _userSchema: StructType,
    _schemaExtensions: String,
    _ddlExtensionForShadowTable: String,
    _origOptions: Map[String, String],
    _externalStore: ExternalStore,
    _partitioningColumns: Seq[String],
    _context: SQLContext,
    baseTableName: String)
    extends BaseColumnFormatRelation(
      _table,
      _provider,
      _mode,
      _userSchema,
      _schemaExtensions,
      _ddlExtensionForShadowTable,
      _origOptions,
      _externalStore,
      _partitioningColumns,
      _context)
        with DependentRelation {
  override def baseTable: Option[String] = Some(baseTableName)

  override def name: String = _table

  def getBaseTableRelation: ColumnFormatRelation = {
    val catalog = sqlContext.sparkSession.asInstanceOf[SnappySession].sessionCatalog
    catalog.lookupRelation(catalog.newQualifiedTableName(baseTableName)) match {
      case LogicalRelation(cr: ColumnFormatRelation, _, _) =>
        cr
      case _ =>
        throw new UnsupportedOperationException("Index scan other than Column table unsupported")
    }
  }

}

object ColumnFormatRelation extends Logging with StoreCallback {

  type IndexUpdateStruct = ((PreparedStatement, InternalRow) => Int, PreparedStatement)

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

  final def cachedBatchTableName(table: String): String =
    JDBCAppendableRelation.cachedBatchTableName(table)

  def getIndexUpdateStruct(indexEntry: ExternalTableMetaData,
      connectedExternalStore: ConnectedExternalStore):
  ColumnFormatRelation.IndexUpdateStruct = {
    assert(indexEntry.dml.nonEmpty)
    val rowInsertStr = indexEntry.dml
    (CodeGeneration.getGeneratedIndexStatement(indexEntry.entityName,
      indexEntry.schema.asInstanceOf[StructType],
      indexEntry.externalStore.asInstanceOf[JDBCSourceAsColumnarStore].connProperties.dialect),
        connectedExternalStore.conn.prepareStatement(rowInsertStr))
  }
}

final class DefaultSource extends ColumnarRelationProvider {

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schema: StructType): JDBCAppendableRelation = {

    val parameters = new CaseInsensitiveMutableHashMap(options)

    val table = ExternalStoreUtils.removeInternalProps(parameters)
    val sc = sqlContext.sparkContext
    val partitions = ExternalStoreUtils.getTotalPartitions(Some(sc), parameters,
      forManagedTable = true)
    val parametersForShadowTable = new CaseInsensitiveMutableHashMap(parameters)

    val partitioningColumn = StoreUtils.getPartitioningColumn(parameters)
    val primaryKeyClause = StoreUtils.getPrimaryKeyClause(parameters, schema, sqlContext)
    val ddlExtension = StoreUtils.ddlExtensionString(parameters,
      isRowTable = false, isShadowTable = false)

    val ddlExtensionForShadowTable = StoreUtils.ddlExtensionString(
      parametersForShadowTable, isRowTable = false, isShadowTable = true)

    // val dependentRelations = parameters.remove(ExternalStoreUtils.DEPENDENT_RELATIONS)
    val connProperties =
      ExternalStoreUtils.validateAndGetAllProps(Some(sc), parameters)

    StoreUtils.validateConnProps(parameters)

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
      partitions)

    var success = false
    val tableName = SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf)

    // create an index relation if it is an index table
    val baseTable = options.get(StoreUtils.GEM_INDEXED_TABLE)
    val relation = baseTable match {
      case Some(btable) => new IndexColumnFormatRelation(
          tableName,
        getClass.getCanonicalName,
        mode,
        schema,
        schemaExtension,
        ddlExtensionForShadowTable,
        options,
        externalStore,
        partitioningColumn,
        sqlContext,
        btable)
      case None => new ColumnFormatRelation(
          tableName,
        getClass.getCanonicalName,
        mode,
        schema,
        schemaExtension,
        ddlExtensionForShadowTable,
        options,
        externalStore,
        partitioningColumn,
        sqlContext)
    }
    val isRelationforSample = options.get(ExternalStoreUtils.RELATION_FOR_SAMPLE).
        map(_.toBoolean).getOrElse(false)

    try {
      relation.createTable(mode)
      if (!isRelationforSample) {
        val catalog = sqlContext.sparkSession.asInstanceOf[SnappySession].sessionCatalog
        catalog.registerDataSourceTable(
          catalog.newQualifiedTableName(tableName), Some(relation.schema),
          partitioningColumn.toArray, classOf[execution.columnar.DefaultSource].getCanonicalName,
          options, relation)
      }
      success = true
      relation
    } finally {
      if (!success && !relation.tableExists) {
        if (!isRelationforSample) {
          val catalog = sqlContext.sparkSession.asInstanceOf[SnappySession].sessionCatalog
          catalog.unregisterDataSourceTable(catalog.newQualifiedTableName(tableName),
            Some(relation))
        }
        // destroy the relation
        relation.destroy(ifExists = true)
      }
    }
  }
}
