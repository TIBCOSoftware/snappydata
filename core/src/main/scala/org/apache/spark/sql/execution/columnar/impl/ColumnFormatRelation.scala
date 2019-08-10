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
package org.apache.spark.sql.execution.columnar.impl

import java.sql.{Connection, PreparedStatement}

import scala.util.control.NonFatal

import com.gemstone.gemfire.internal.cache.{ExternalTableMetaData, LocalRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.sql.catalog.{RelationInfo, SnappyExternalCatalog}
import io.snappydata.{Constant, Property}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Descending, Expression, SortDirection}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier, analysis}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.execution.columnar._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.row.RowFormatScanRDD
import org.apache.spark.sql.execution.{ConnectionPool, PartitionedDataSourceScan, RefreshMetadata, SparkPlan}
import org.apache.spark.sql.internal.ColumnTableBulkOps
import org.apache.spark.sql.sources.JdbcExtendedUtils.quotedName
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
abstract class BaseColumnFormatRelation(
    _table: String,
    _provider: String,
    _mode: SaveMode,
    _userSchema: StructType,
    val schemaExtensions: String,
    val ddlExtensionForShadowTable: String,
    _origOptions: CaseInsensitiveMap,
    _externalStore: ExternalStore,
    val partitioningColumns: Seq[String],
    _context: SQLContext,
    _relationInfo: (RelationInfo, Option[LocalRegion]))
    extends JDBCAppendableRelation(_table, _provider, _mode, _userSchema,
      _origOptions, _externalStore, _context)
        with PartitionedDataSourceScan
        with RowInsertableRelation
        with MutableRelation {

  override def toString: String = s"${getClass.getSimpleName}[${Utils.toLowerCase(table)}]"

  override val connectionType: ConnectionType.Value =
    ExternalStoreUtils.getConnectionType(dialect)

  lazy val rowInsertStr: String = JdbcExtendedUtils.getInsertOrPutString(
    resolvedName, schema, putInto = false)

  override lazy val (schemaName: String, tableName: String) =
    JdbcExtendedUtils.getTableWithSchema(table, conn = null, Some(sqlContext.sparkSession))

  if (_relationInfo eq null) {
    refreshTableSchema(invalidateCached = false, fetchFromStore = false)
  } else {
    _schema = userSchema
    _relationInfoAndRegion = _relationInfo
  }

  override protected def withoutUserSchema: Boolean = userSchema.isEmpty

  override def numBuckets: Int = relationInfo.numBuckets

  override def isPartitioned: Boolean = relationInfo.isPartitioned

  def getColumnBatchStatistics(schema: Seq[AttributeReference]): PartitionStatistics = {
    new PartitionStatistics(schema)
  }

  override def partitionColumns: Seq[String] = {
    partitioningColumns
  }

  override private[sql] lazy val externalColumnTableName: String =
    ColumnFormatRelation.columnBatchTableName(table)

  override def scanTable(tableName: String, requiredColumns: Array[String],
      filters: Array[Expression], _ignore: () => Int): (RDD[Any], Array[Int]) = {

    // note: filters is expected to be already split by CNF.
    // see PhysicalScan#unapply
    super.scanTable(externalColumnTableName, requiredColumns, filters,
      () => Utils.getPrunedPartition(partitionColumns, filters, schema,
        numBuckets, partitioningColumns.length))
  }

  override def unhandledFilters(filters: Seq[Expression]): Seq[Expression] = filters

  override def buildUnsafeScan(requiredColumns: Array[String],
      filters: Array[Expression]): (RDD[Any], Seq[RDD[InternalRow]]) = {
    // Remove the update/delete key columns from RDD requiredColumns.
    // These will be handled by the ColumnTableScan directly.
    val columns = requiredColumns.filter(!_.startsWith(ColumnDelta.mutableKeyNamePrefix))
    val (rdd, projection) = scanTable(table, columns, filters, () => -1)
    val partitionEvaluator = rdd match {
      case c: ColumnarStorePartitionedRDD => c.getPartitionEvaluator
      case s => s.asInstanceOf[SmartConnectorColumnRDD].getPartitionEvaluator
    }
    // select the rowId from row buffer for update/delete keys
    val numColumns = columns.length
    val rowBufferColumns = if (numColumns < requiredColumns.length) {
      val newColumns = java.util.Arrays.copyOf(columns, numColumns + 1)
      newColumns(numColumns) = StoreUtils.ROWID_COLUMN_NAME
      newColumns
    } else columns
    val zipped = buildRowBufferRDD(partitionEvaluator, rowBufferColumns, filters,
      useResultSet = true, projection).zipPartitions(rdd) { (leftItr, rightItr) =>
      Iterator[Any](leftItr, rightItr)
    }
    (zipped, Nil)
  }


  def buildUnsafeScanForSampledRelation(requiredColumns: Array[String],
      filters: Array[Expression]): (RDD[Any], RDD[Any],
      Seq[RDD[InternalRow]]) = {
    val (rdd, projection) = scanTable(table, requiredColumns, filters, () => -1)
    val rowRDD = buildRowBufferRDD(() => rdd.partitions, requiredColumns, filters,
      useResultSet = true, projection)
    (rdd.asInstanceOf[RDD[Any]], rowRDD.asInstanceOf[RDD[Any]], Nil)
  }

  def buildRowBufferRDD(partitionEvaluator: () => Array[Partition],
      requiredColumns: Array[String], filters: Array[Expression],
      useResultSet: Boolean, projection: Array[Int]): RDD[Any] = {
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    connectionType match {
      case ConnectionType.Embedded =>
        val region = Misc.getRegionForTable(resolvedName, true).asInstanceOf[LocalRegion]
        new RowFormatScanRDD(
          session,
          resolvedName,
          isPartitioned,
          requiredColumns,
          pushProjections = false,
          useResultSet = useResultSet,
          connProperties,
          Array.empty[Expression],
          // use same partitions as the column store (SNAP-1083)
          partitionEvaluator = partitionEvaluator,
          partitionPruner = () => -1,
          commitTx = false, delayRollover = delayRollover,
          projection = projection, region = Some(region))
      case _ =>
        new SmartConnectorRowRDD(
          session,
          resolvedName,
          isPartitioned,
          requiredColumns,
          connProperties,
          filters,
          // use same partitions as the column store (SNAP-1083)
          partitionEvaluator,
          _partitionPruner = () => -1,
          relationInfo.catalogSchemaVersion,
          _commitTx = false, delayRollover)
    }
  }

  private def partitionExpressions(relation: LogicalRelation) = {
    // use case-insensitive resolution since partitioning columns during
    // creation could be using the same as opposed to during insert
    partitionColumns.map(colName =>
      relation.resolveQuoted(colName, analysis.caseInsensitiveResolution)
          .getOrElse(throw new AnalysisException(
            s"""Cannot resolve column "$colName" among (${relation.output})""")))
  }

  override def getInsertPlan(relation: LogicalRelation,
      child: SparkPlan): SparkPlan = {
    withTableWriteLock() { () =>
      new ColumnInsertExec(child, partitionColumns, partitionExpressions(relation),
        this, externalColumnTableName)
    }
  }

  override def getKeyColumns: Seq[String] = {
    // add partitioning columns for row buffer updates
    partitioningColumns ++ ColumnDelta.mutableKeyNames
  }

  /** Get key columns of the column table */
  override def getPrimaryKeyColumns(session: SnappySession): Seq[String] = {
    origOptions.get(ExternalStoreUtils.KEY_COLUMNS) match {
      case None => Nil
      case Some(keyCols) =>
        val parser = session.snappyParser
        parser.parseSQLOnly(keyCols, parser.parseIdentifiers.run())
    }
  }

  /**
   * Get a spark plan to update rows in the relation. The result of SparkPlan
   * execution should be a count of number of updated rows.
   */
  override def getUpdatePlan(relation: LogicalRelation, child: SparkPlan,
      updateColumns: Seq[Attribute], updateExpressions: Seq[Expression],
      keyColumns: Seq[Attribute]): SparkPlan = {
    withTableWriteLock() { () =>
      ColumnUpdateExec(child, externalColumnTableName, partitionColumns,
        partitionExpressions(relation), numBuckets, isPartitioned, schema, externalStore, this,
        updateColumns, updateExpressions, keyColumns, connProperties, onExecutor = false)
    }
  }

  /**
   * Get a spark plan to delete rows the relation. The result of SparkPlan
   * execution should be a count of number of updated rows.
   */
  override def getDeletePlan(relation: LogicalRelation, child: SparkPlan,
      keyColumns: Seq[Attribute]): SparkPlan = {
    withTableWriteLock() { () =>
      ColumnDeleteExec(child, externalColumnTableName, partitionColumns,
        partitionExpressions(relation), numBuckets, isPartitioned, schema, externalStore,
        this, keyColumns, connProperties, onExecutor = false)
    }
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
    // use bulk insert directly into column store for large number of rows

    val snc = sqlContext.sparkSession.asInstanceOf[SnappySession]
    val lock = snc.getContextObject[(Option[TableIdentifier], PartitionedRegion.RegionLock)](
      SnappySession.PUTINTO_LOCK) match {
      case None => snc.grabLock(table, schemaName, connProperties)
      case Some(_) => null // Do nothing as putInto will release lock
    }
    try {
      if (numRows > (batchSize * numBuckets)) {
        ColumnTableBulkOps.bulkInsertOrPut(rows, sqlContext.sparkSession, schema,
          resolvedName, putInto = false)
      } else {
        // insert into the row buffer
        val connection = ConnectionPool.getPoolConnection(table, dialect,
          connProperties.poolProps, connProps, connProperties.hikariCP)
        try {
          val stmt = connection.prepareStatement(rowInsertStr)

          val result = CodeGeneration.executeUpdate(table, stmt,
            rows, numRows > 1, batchSize, schema.fields, dialect)
          stmt.close()
          result
        } finally {
          connection.commit()
          connection.close()
        }
      }
    }
    finally {
      if (lock != null) {
        logDebug(s"Releasing the $lock object in InsertRows")
        snc.releaseLock(lock)
      }
    }
  }

  def withTableWriteLock()(f: () => SparkPlan): SparkPlan = {
    val snc = sqlContext.sparkSession.asInstanceOf[SnappySession]
    val lock = snc.getContextObject[(Option[TableIdentifier], PartitionedRegion.RegionLock)](
      SnappySession.PUTINTO_LOCK) match {
      case None => snc.grabLock(table, schemaName, connProperties)
      case Some(_) => null // Do nothing as putInto will release lock
    }
    try {
      f()
    }
    finally {
      if (lock != null) {
        logDebug(s"Added the $lock object to the context for $table")
        snc.addContextObject(
          SnappySession.BULKWRITE_LOCK, lock)
      }
    }
  }

  // truncate both actual and shadow table
  override def truncate(): Unit = writeLock {
    try {
      externalStore.tryExecute(externalColumnTableName) { conn =>
        JdbcExtendedUtils.truncateTable(conn, externalColumnTableName, dialect)
      }
    } finally {
      externalStore.tryExecute(table) { conn =>
        JdbcExtendedUtils.truncateTable(conn, table, dialect)
      }
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
          JdbcExtendedUtils.dropTable(conn, externalColumnTableName,
            dialect, sqlContext, ifExists)
        } finally {
          JdbcExtendedUtils.dropTable(conn, table, dialect, sqlContext, ifExists)
        }
      } finally {
        conn.commit()
        conn.close()
      }
    }
  }

  /**
   * Table definition: create table columnTable (
   * id varchar(36) not null, partitionId integer, numRows integer not null, data blob)
   * For a table with n columns, there will be n+1 region entries. A base entry and one entry
   * each for a column. The data column for the base entry will contain the stats.
   * id for the base entry would be the uuid while for column entries it would be uuid_colName.
   */
  private def createExternalTableForColumnBatches(tableName: String, conn: Connection): Unit = {
    require(tableName != null && tableName.length > 0,
      "createExternalTableForColumnBatches: expected non-empty table name")

    val (primaryKey, partitionStrategy, concurrency) = dialect match {
      // The driver if not a loner should be an accessor only
      case _: JdbcExtendedDialect =>
        (s"constraint ${quotedName(tableName + "_partitionCheck")} check (partitionId >= 0), " +
            "primary key (uuid, partitionId, columnIndex)",
            // d.getPartitionByClause("partitionId"),
            s"PARTITIONER '${classOf[ColumnPartitionResolver].getName}'",
            "  ENABLE CONCURRENCY CHECKS ")
      case _ => ("primary key (uuid)", "", "")
    }
    val colocationClause = s"COLOCATE WITH (${quotedName(table)})"
    val encoderClause = s"ENCODER '${classOf[ColumnFormatEncoder].getName}'"

    // if numRows or other columns are ever changed here, then change
    // the hardcoded positions in insert and PartitionedPhysicalRDD.CT_*
    createTable(conn,
      s"""create table ${quotedName(tableName)} (uuid bigint not null,
        partitionId integer, columnIndex integer, data blob, $primaryKey)
        $partitionStrategy $colocationClause $encoderClause
        $concurrency $ddlExtensionForShadowTable""", tableName)
  }

  override protected def createActualTables(conn: Connection): Unit = {
    val fullName = quotedName(resolvedName)
    val sql = s"CREATE TABLE $fullName $schemaExtensions ENABLE CONCURRENCY CHECKS"
    val pass = connProperties.connProps.remove(com.pivotal.gemfirexd.Attribute.PASSWORD_ATTR)
    if (isInfoEnabled) {
      val schemaString = JdbcExtendedUtils.schemaString(userSchema, connProperties.dialect)
      val optsString = if (origOptions.nonEmpty) {
        origOptions.filter(p => !p._1.startsWith(SnappyExternalCatalog.SCHEMADDL_PROPERTY)).map(
          p => s"${p._1} '${p._2}'").mkString(" OPTIONS (", ", ", ")")
      } else ""
      logInfo(s"Executing DDL: CREATE TABLE $fullName " +
          s"$schemaString USING $provider$optsString ; (url=${connProperties.url}; " +
          s"props=${connProperties.connProps})")
    }
    if (pass != null) {
      connProperties.connProps.setProperty(com.pivotal.gemfirexd.Attribute.PASSWORD_ATTR,
        pass.asInstanceOf[String])
    }
    JdbcExtendedUtils.executeUpdate(sql, conn)
    // setting table created to true here as cleanup
    // in case of failed creation does a exists check.
    tableCreated = true
    dialect match {
      case d: JdbcExtendedDialect => d.initializeTable(resolvedName,
        sqlContext.conf.caseSensitiveAnalysis, conn)
    }
    createExternalTableForColumnBatches(externalColumnTableName, conn)
    // store schema will miss complex types etc, so use the user-provided one
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    session.externalCatalog.invalidate(schemaName -> tableName)
    _schema = userSchema
    _relationInfoAndRegion = null
  }

  /**
   * Execute a DML SQL and return the number of rows affected.
   */
  override def executeUpdate(sql: String, defaultSchema: String): Int = {
    val connection = ConnectionPool.getPoolConnection(table, dialect,
      connProperties.poolProps, connProperties.connProps,
      connProperties.hikariCP)
    var currentSchema: String = null
    try {
      if (defaultSchema ne null) {
        currentSchema = connection.getSchema
        if (defaultSchema != currentSchema) {
          connection.setSchema(defaultSchema)
        }
      }
      val stmt = connection.prepareStatement(sql)
      val result = stmt.executeUpdate()
      stmt.close()
      result
    } finally {
      if (currentSchema ne null) connection.setSchema(currentSchema)
      connection.commit()
      connection.close()
    }
  }

  override def flushRowBuffer(): Unit = {
    val sc = sqlContext.sparkContext
    // force flush all the buckets into the column store
    RefreshMetadata.executeOnAll(sc, RefreshMetadata.FLUSH_ROW_BUFFER,
      resolvedName, executeInConnector = false)
  }
}

class ColumnFormatRelation(
    _table: String,
    _provider: String,
    _mode: SaveMode,
    _userSchema: StructType,
    _schemaExtensions: String,
    _ddlExtensionForShadowTable: String,
    _origOptions: CaseInsensitiveMap,
    _externalStore: ExternalStore,
    _partitioningColumns: Seq[String],
    _context: SQLContext,
    _relationInfo: (RelationInfo, Option[LocalRegion]) = null)
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
      _context,
      _relationInfo) with BulkPutRelation {

  val tableOptions = new CaseInsensitiveMutableHashMap(_origOptions)

  override def withKeyColumns(relation: LogicalRelation,
      keyColumns: Seq[String]): LogicalRelation = {
    // keyColumns should match the key fields required for update/delete
    if (keyColumns.takeRight(4) != ColumnDelta.mutableKeyNames) {
      throw new IllegalStateException(s"Unexpected keyColumns=$keyColumns, " +
          s"required=${ColumnDelta.mutableKeyNames}")
    }
    val cr = relation.relation.asInstanceOf[ColumnFormatRelation]
    if (cr.schema.exists(_.name.startsWith(ColumnDelta.mutableKeyNamePrefix))) return relation
    val schema = StructType(cr.schema ++ ColumnDelta.mutableKeyFields)
    val newRelation = new ColumnFormatRelation(cr.table, cr.provider,
      cr.mode, schema, cr.schemaExtensions, cr.ddlExtensionForShadowTable,
      cr.origOptions, cr.externalStore, cr.partitioningColumns, cr.sqlContext,
      _relationInfoAndRegion)
    newRelation.delayRollover = true
    relation.copy(relation = newRelation,
      expectedOutputAttributes = Some(relation.output ++ ColumnDelta.mutableKeyAttributes))
  }

  override def dropIndex(indexIdent: TableIdentifier,
      tableIdent: TableIdentifier,
      ifExists: Boolean): Unit = {
    val snappySession = sqlContext.sparkSession.asInstanceOf[SnappySession]
    // Remove the actual index
    snappySession.dropTable(indexIdent, ifExists, isView = false)
  }

  /**
   * Index table is same as the column table apart from how it is
   * partitioned and colocated. Add GEM_PARTITION_BY and GEM_COLOCATE_WITH
   * clause in its options. Also add GEM_INDEXED_TABLE parameter to
   * indicate that this is an index table.
   */
  private def createIndexTable(indexIdent: TableIdentifier,
      tableIdent: TableIdentifier,
      tableRelation: BaseColumnFormatRelation,
      indexColumns: Seq[(String, Option[SortDirection])],
      options: Map[String, String]): DataFrame = {

    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    // only allow if experimental-features are enabled
    if (!Property.EnableExperimentalFeatures.get(session.sessionState.conf)) {
      throw new UnsupportedOperationException(
        "CREATE INDEX on column tables is an experimental unsupported feature")
    }
    val parameters = new CaseInsensitiveMutableHashMap(options)
    // no index_type support for column indexes
    parameters.get(ExternalStoreUtils.INDEX_TYPE) match {
      case None =>
      case Some(t) => throw new UnsupportedOperationException(
        s"CREATE INDEX of type '$t' is not supported for column tables")
    }
    val parser = session.snappyParser
    val indexCols = indexColumns.map { p =>
      p._2 match {
        case Some(Descending) => throw new UnsupportedOperationException(s"Cannot create index " +
            s"'${indexIdent.unquotedString}' with DESC sort specification for column ${p._1}")
        case _ =>
      }
      parser.parseSQLOnly(p._1, parser.parseIdentifier.run())
    }
    val catalog = session.sessionCatalog
    val baseTable = catalog.resolveTableIdentifier(tableIdent).unquotedString
    val indexTblName = session.getIndexTable(indexIdent).unquotedString
    val tempOptions = tableRelation.origOptions.filterNot(pair => {
      pair._1.equalsIgnoreCase(StoreUtils.PARTITION_BY) ||
          pair._1.equalsIgnoreCase(StoreUtils.COLOCATE_WITH) ||
          pair._1.equalsIgnoreCase(SnappyExternalCatalog.DBTABLE_PROPERTY) ||
          pair._1.equalsIgnoreCase(ExternalStoreUtils.INDEX_NAME)
    }) + (StoreUtils.PARTITION_BY -> indexCols.mkString(",")) +
        (SnappyExternalCatalog.INDEXED_TABLE -> baseTable) +
        (SnappyExternalCatalog.BASETABLE_PROPERTY -> baseTable) +
        (SnappyExternalCatalog.DBTABLE_PROPERTY -> indexTblName)

    val indexOptions = parameters.get(StoreUtils.COLOCATE_WITH) match {
      case Some(value) =>
        val colocateWith = {
          val colocationTable = session.tableIdentifier(value)
          if (catalog.tableExists(colocationTable)) {
            value
          } else {
            val idx = session.getIndexTable(colocationTable)
            if (catalog.tableExists(idx)) {
              idx.unquotedString
            } else {
              throw new AnalysisException(
                s"Could not find colocation table ${colocationTable.unquotedString} in catalog")
            }
          }
        }
        tempOptions + (StoreUtils.COLOCATE_WITH -> colocateWith)
      case _ => tempOptions
    }

    session.createTable(
      indexTblName,
      "column",
      tableRelation.schema,
      indexOptions)
  }

  override def createIndex(indexIdent: TableIdentifier,
      tableIdent: TableIdentifier,
      indexColumns: Seq[(String, Option[SortDirection])],
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
      val df = Dataset.ofRows(snappySession,
        snappySession.sessionCatalog.lookupRelation(tableIdent))

      // SB: Now populate the index table from base table.
      df.write.insertInto(snappySession.getIndexTable(indexIdent).unquotedString)
    } catch {
      case NonFatal(e) =>
        snappySession.dropTable(indexIdent, ifExists = true, isView = false)
        throw e
    }
  }

  /**
   * Get a spark plan for puts. If the row is already present, it gets updated
   * otherwise it gets inserted into the table represented by this relation.
   * The result of SparkPlan execution should be a count of number of rows put.
   */
  override def getPutPlan(insertPlan: SparkPlan, updatePlan: SparkPlan): SparkPlan = {
    ColumnPutIntoExec(insertPlan, updatePlan)
  }

  override def getPutKeys(session: SnappySession): Option[Seq[String]] = {
    val keys = origOptions.get(ExternalStoreUtils.KEY_COLUMNS)
    keys match {
      case Some(x) =>
        val parser = session.snappyParser
        Some(parser.parseSQLOnly(x, parser.parseIdentifiers.run()))
      case None => None
    }
  }
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
    _origOptions: CaseInsensitiveMap,
    _externalStore: ExternalStore,
    _partitioningColumns: Seq[String],
    _context: SQLContext,
    baseTableName: String,
    _relationInfo: (RelationInfo, Option[LocalRegion]) = null)
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
      _context,
      _relationInfo) {

  def baseTable: Option[String] = Some(baseTableName)

  override def withKeyColumns(relation: LogicalRelation,
      keyColumns: Seq[String]): LogicalRelation = {
    val cr = relation.relation.asInstanceOf[IndexColumnFormatRelation]
    if (cr.schema.exists(_.name.startsWith(ColumnDelta.mutableKeyNamePrefix))) return relation
    val schema = StructType(cr.schema ++ ColumnDelta.mutableKeyFields)
    val newRelation = new IndexColumnFormatRelation(cr.table, cr.provider,
      cr.mode, schema, cr.schemaExtensions, cr.ddlExtensionForShadowTable, cr.origOptions,
      cr.externalStore, cr.partitioningColumns, cr.sqlContext, baseTableName,
      _relationInfoAndRegion)
    newRelation.delayRollover = true
    relation.copy(relation = newRelation,
      expectedOutputAttributes = Some(relation.output ++ ColumnDelta.mutableKeyAttributes))
  }

  def getBaseTableRelation: ColumnFormatRelation = {
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    val catalog = session.sessionState.catalog
    catalog.resolveRelation(session.tableIdentifier(baseTableName)) match {
      case LogicalRelation(cr: ColumnFormatRelation, _, _) => cr
      case _ =>
        throw new UnsupportedOperationException("Index scan other than Column table unsupported")
    }
  }
}

object ColumnFormatRelation extends Logging with StoreCallback {

  type IndexUpdateStruct = ((PreparedStatement, InternalRow) => Int, PreparedStatement)

  final def columnBatchTableName(table: String,
      session: Option[SparkSession] = None): String = {
    val (schema, tableName) = JdbcExtendedUtils.getTableWithSchema(
      table, null, session, forSpark = false)
    schema + '.' + Constant.SHADOW_SCHEMA_NAME_WITH_SEPARATOR +
        tableName + Constant.SHADOW_TABLE_SUFFIX
  }

  final def isColumnTable(tableName: String): Boolean = {
    tableName.contains(Constant.SHADOW_SCHEMA_NAME_WITH_PREFIX) &&
        tableName.endsWith(Constant.SHADOW_TABLE_SUFFIX)
  }

  def getIndexUpdateStruct(indexEntry: ExternalTableMetaData,
      connectedExternalStore: ConnectedExternalStore):
  ColumnFormatRelation.IndexUpdateStruct = {
    assert(indexEntry.dml.nonEmpty)
    val rowInsertStr = indexEntry.dml
    (CodeGeneration.getGeneratedIndexStatement(indexEntry.entityName,
      indexEntry.schema.asInstanceOf[StructType],
      indexEntry.externalStore.asInstanceOf[ExternalStore].connProperties.dialect),
        connectedExternalStore.conn.prepareStatement(rowInsertStr))
  }
}
