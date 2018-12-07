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
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer
import io.snappydata.Constant

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, EqualNullSafe, EqualTo, Expression, SortDirection, SpecificInternalRow, TokenLiteral, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.catalyst.{InternalRow, analysis}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.execution.columnar._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.row.RowFormatScanRDD
import org.apache.spark.sql.execution.{ConnectionPool, PartitionedDataSourceScan, RefreshMetadata, SparkPlan}
import org.apache.spark.sql.hive.{ConnectorCatalog, QualifiedTableName, RelationInfo, SnappyStoreHiveCatalog}
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
    _origOptions: Map[String, String],
    _externalStore: ExternalStore,
    val partitioningColumns: Seq[String],
    _context: SQLContext)
    extends JDBCAppendableRelation(_table, _provider, _mode, _userSchema,
      _origOptions, _externalStore, _context)
    with PartitionedDataSourceScan
    with RowInsertableRelation
    with MutableRelation {

  override def toString: String = s"${getClass.getSimpleName}[$table]"

  override val connectionType: ConnectionType.Value =
    ExternalStoreUtils.getConnectionType(dialect)

  lazy val rowInsertStr: String = JdbcExtendedUtils.getInsertOrPutString(
    resolvedName, schema, putInto = false)

  @transient override lazy val region: PartitionedRegion =
    Misc.getRegionForTable(resolvedName, true).asInstanceOf[PartitionedRegion]

  def getColumnBatchStatistics(schema: Seq[AttributeReference]): PartitionStatistics = {
    new PartitionStatistics(schema)
  }

  @transient
  lazy val clusterMode: ClusterMode = SnappyContext.getClusterMode(
    _context.sparkContext)

  @transient
  lazy val relInfo: RelationInfo = {
    clusterMode match {
      case ThinClientConnectorMode(_, _) =>
        val catalog = _context.sparkSession.sessionState.catalog.asInstanceOf[ConnectorCatalog]
        catalog.getCachedRelationInfo(catalog.newQualifiedTableName(table))
      case _ =>
        RelationInfo(numBuckets, isPartitioned, partitionColumns, Array.empty[String],
          Array.empty[String], Array.empty[Partition], -1)
    }
  }

  @transient
  override lazy val (numBuckets, isPartitioned) = {
    clusterMode match {
      case ThinClientConnectorMode(_, _) => (relInfo.numBuckets, relInfo.isPartitioned)
      case _ => (region.getTotalNumberOfBuckets, true)
    }
  }

  override def partitionColumns: Seq[String] = {
    partitioningColumns
  }

  override private[sql] lazy val externalColumnTableName: String =
    ColumnFormatRelation.columnBatchTableName(table,
      Some(() => sqlContext.sparkSession.asInstanceOf[SnappySession]))

  override def scanTable(tableName: String, requiredColumns: Array[String],
      filters: Array[Expression], _ignore: () => Int): (RDD[Any], Array[Int]) = {

    // this will yield partitioning column ordered Array of Expression (Literals/ParamLiterals).
    // RDDs needn't have to care for orderless hashing scheme at invocation point.
    val (pruningExpressions, fields) = partitionColumns.map { pc =>
      filters.collectFirst {
          case EqualTo(a: Attribute, v) if TokenLiteral.isConstant(v) &&
              pc.equalsIgnoreCase(a.name) => (v, schema(a.name))
          case EqualTo(v, a: Attribute) if TokenLiteral.isConstant(v) &&
              pc.equalsIgnoreCase(a.name) => (v, schema(a.name))
          case EqualNullSafe(a: Attribute, v) if TokenLiteral.isConstant(v) &&
              pc.equalsIgnoreCase(a.name) => (v, schema(a.name))
          case EqualNullSafe(v, a: Attribute) if TokenLiteral.isConstant(v) &&
              pc.equalsIgnoreCase(a.name) => (v, schema(a.name))
      }
    }.filter(_.nonEmpty).map(_.get).unzip

    def prunePartitions(): Int = {
      val pcFields = StructType(fields).toAttributes
      val mutableRow = new SpecificInternalRow(pcFields.map(_.dataType))
      val bucketIdGeneration = UnsafeProjection.create(
        HashPartitioning(pcFields, numBuckets)
            .partitionIdExpression :: Nil, pcFields)
      if (pruningExpressions.nonEmpty &&
          // verify all the partition columns are provided as filters
          pruningExpressions.length == partitioningColumns.length) {
        pruningExpressions.zipWithIndex.foreach { case (e, i) =>
          mutableRow(i) = e.eval(null)
        }
        bucketIdGeneration(mutableRow).getInt(0)
      } else {
        -1
      }
    }

    // note: filters is expected to be already split by CNF.
    // see PhysicalScan#unapply
    super.scanTable(externalColumnTableName, requiredColumns, filters, prunePartitions)
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
          partitionEvaluator,
          commitTx = false, delayRollover, projection, Some(region))
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
          relInfo.embedClusterRelDestroyVersion,
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
    new ColumnInsertExec(child, partitionColumns, partitionExpressions(relation),
      this, externalColumnTableName)
  }

  override def getKeyColumns: Seq[String] = {
    // add partitioning columns for row buffer updates

    // always use case-insensitive analysis for partitioning columns
    // since table creation can use case-insensitive in creation
    partitioningColumns.map(Utils.toUpperCase) ++ ColumnDelta.mutableKeyNames
  }

  /** Get key columns of the column table */
  override def getPrimaryKeyColumns: Seq[String] = {
    val keyColsOptions = _origOptions.get(ExternalStoreUtils.KEY_COLUMNS)
    if (keyColsOptions.isDefined) {
      keyColsOptions.get.split(",").map(_.trim)
    } else {
      Seq.empty[String]
    }
  }

  /**
   * Get a spark plan to update rows in the relation. The result of SparkPlan
   * execution should be a count of number of updated rows.
   */
  override def getUpdatePlan(relation: LogicalRelation, child: SparkPlan,
      updateColumns: Seq[Attribute], updateExpressions: Seq[Expression],
      keyColumns: Seq[Attribute]): SparkPlan = {
    ColumnUpdateExec(child, externalColumnTableName, partitionColumns,
      partitionExpressions(relation), numBuckets, isPartitioned, schema, externalStore, this,
      updateColumns, updateExpressions, keyColumns, connProperties, onExecutor = false)
  }

  /**
   * Get a spark plan to delete rows the relation. The result of SparkPlan
   * execution should be a count of number of updated rows.
   */
  override def getDeletePlan(relation: LogicalRelation, child: SparkPlan,
      keyColumns: Seq[Attribute]): SparkPlan = {
    ColumnDeleteExec(child, externalColumnTableName, partitionColumns,
      partitionExpressions(relation), numBuckets, isPartitioned, schema, externalStore,
      this, keyColumns, connProperties, onExecutor = false)
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
          JdbcExtendedUtils.dropTable(conn, table, dialect, sqlContext,
            ifExists)
        }
      } finally {
        conn.commit()
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
//            dialect match {
//              case SnappyStoreDialect =>
//                SnappyStoreDialect.initializeTable(table,
//                  sqlContext.conf.caseSensitiveAnalysis, conn)
//                SnappyStoreDialect.initializeTable(externalColumnTableName,
//                  sqlContext.conf.caseSensitiveAnalysis, conn)
//              case _ => // Do nothing
//            }
            return
          case SaveMode.ErrorIfExists =>
            // sys.error(s"Table $table already exists.") TODO: Why so?
            return
          case _ => // Ignore
        }
      }
    } finally {
      conn.commit()
      conn.close()
    }
    createActualTables(table, externalStore)
  }

  /**
   * Table definition: create table columnTable (
   *  id varchar(36) not null, partitionId integer, numRows integer not null, data blob)
   * For a table with n columns, there will be n+1 region entries. A base entry and one entry
   * each for a column. The data column for the base entry will contain the stats.
   * id for the base entry would be the uuid while for column entries it would be uuid_colName.
   */
  override def createExternalTableForColumnBatches(tableName: String,
      externalStore: ExternalStore): Unit = {
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

    // if the numRows or other columns are ever changed here, then change
    // the hardcoded positions in insert and PartitionedPhysicalRDD.CT_*
    createTable(externalStore,
      s"""create table ${quotedName(tableName)} (uuid bigint not null,
        partitionId integer, columnIndex integer, data blob, $primaryKey)
        $partitionStrategy $colocationClause $encoderClause
        $concurrency $ddlExtensionForShadowTable""",
      tableName, dropIfExists = false)
  }

  // TODO: Suranjan make sure that this table doesn't evict to disk by
  // setting some property, may be MemLRU?
  private def createActualTables(tableName: String,
      externalStore: ExternalStore): Unit = {
    // Create the table if the table didn't exist.
    var conn: Connection = null
    try {
      conn = connFactory()
      val tableExists = JdbcExtendedUtils.tableExists(tableName, conn,
        dialect, sqlContext)
      if (!tableExists) {
        val sql =
          s"CREATE TABLE ${quotedName(tableName)} $schemaExtensions ENABLE CONCURRENCY CHECKS"
        val pass = connProperties.connProps.remove(com.pivotal.gemfirexd.Attribute.PASSWORD_ATTR)
        if (isInfoEnabled) {
          val schemaString = JdbcExtendedUtils.schemaString(schema, connProperties.dialect)
          val optsString = if (origOptions.nonEmpty) {
            origOptions.map(p => s"${p._1} '${p._2}'").mkString(" OPTIONS (", ", ", ")")
          } else ""
          logInfo(s"Executing DDL (url=${connProperties.url}; " +
              s"props=${connProperties.connProps}): CREATE TABLE ${quotedName(tableName)} " +
              s"$schemaString USING $provider$optsString")
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
          case d: JdbcExtendedDialect => d.initializeTable(tableName,
            sqlContext.conf.caseSensitiveAnalysis, conn)
        }
        createExternalTableForColumnBatches(externalColumnTableName,
          externalStore)
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
        conn.commit()
        conn.close()
      }
    }
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
    _origOptions: Map[String, String],
    _externalStore: ExternalStore,
    _partitioningColumns: Seq[String],
    _context: SQLContext)
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
  with ParentRelation with DependentRelation with BulkPutRelation {
  val tableOptions = new CaseInsensitiveMutableHashMap(origOptions)

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
      cr.origOptions, cr.externalStore, cr.partitioningColumns, cr.sqlContext)
    newRelation.delayRollover = true
    relation.copy(relation = newRelation,
      expectedOutputAttributes = Some(relation.output ++ ColumnDelta.mutableKeyAttributes))
  }

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
    val tempOptions = tableRelation.origOptions.filterNot(pair => {
      pair._1.equalsIgnoreCase(StoreUtils.PARTITION_BY) ||
          pair._1.equalsIgnoreCase(StoreUtils.COLOCATE_WITH) ||
          pair._1.equalsIgnoreCase(JdbcExtendedUtils.DBTABLE_PROPERTY) ||
          pair._1.equalsIgnoreCase(ExternalStoreUtils.INDEX_NAME)
    }) + (StoreUtils.PARTITION_BY -> indexColumns.keys.mkString(",")) +
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
      case NonFatal(e) =>
        snappySession.dropTable(indexIdent, ifExists = true)
        throw e
    }
  }

  /** Base table of this relation. */
  override def baseTable: Option[String] = tableOptions.get(StoreUtils.COLOCATE_WITH)

  /** Name of this relation in the catalog. */
  override def name: String = table

  /**
    * Get a spark plan for puts. If the row is already present, it gets updated
    * otherwise it gets inserted into the table represented by this relation.
    * The result of SparkPlan execution should be a count of number of rows put.
    */
  override def getPutPlan(insertPlan: SparkPlan, updatePlan: SparkPlan): SparkPlan = {
    ColumnPutIntoExec(insertPlan, updatePlan)
  }

  override def getPutKeys: Option[Seq[String]] = {
    val keys = origOptions.get(ExternalStoreUtils.KEY_COLUMNS)
    keys match {
      case Some(x) => Some(x.split(",").map(s => s.trim).toSeq)
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

  override def withKeyColumns(relation: LogicalRelation,
      keyColumns: Seq[String]): LogicalRelation = {
    val cr = relation.relation.asInstanceOf[IndexColumnFormatRelation]
    if (cr.schema.exists(_.name.startsWith(ColumnDelta.mutableKeyNamePrefix))) return relation
    val schema = StructType(cr.schema ++ ColumnDelta.mutableKeyFields)
    val newRelation = new IndexColumnFormatRelation(cr.table, cr.provider,
      cr.mode, schema, cr.schemaExtensions, cr.ddlExtensionForShadowTable, cr.origOptions,
      cr.externalStore, cr.partitioningColumns, cr.sqlContext, baseTableName)
    newRelation.delayRollover = true
    relation.copy(relation = newRelation,
      expectedOutputAttributes = Some(relation.output ++ ColumnDelta.mutableKeyAttributes))
  }

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

  final def columnBatchTableName(table: String,
      session: Option[() => SnappySession] = None): String = {
    val (schema, tableName) = JdbcExtendedUtils.getTableWithSchema(table, null, session)
    schema + '.' + Constant.SHADOW_SCHEMA_NAME_WITH_SEPARATOR +
        tableName + Constant.SHADOW_TABLE_SUFFIX
  }

  final def getTableName(columnBatchTableName: String): String =
    GemFireContainer.getRowBufferTableName(columnBatchTableName)

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

final class DefaultSource extends SchemaRelationProvider
    with CreatableRelationProvider with DataSourceRegister {

  override def shortName(): String = SnappyParserConsts.COLUMN_SOURCE

  def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], specifiedSchema: StructType): JDBCAppendableRelation = {

    val parameters = new CaseInsensitiveMutableHashMap(options)

    // hive metastore is case-insensitive so table name is always upper-case
    val table = Utils.toUpperCase(ExternalStoreUtils.removeInternalProps(parameters))
    val partitions = ExternalStoreUtils.getAndSetTotalPartitions(
      Some(sqlContext.sparkContext), parameters, forManagedTable = true)
    val tableOptions = new CaseInsensitiveMap(parameters.toMap)
    val parametersForShadowTable = new CaseInsensitiveMutableHashMap(parameters)

    val partitioningColumns = StoreUtils.getPartitioningColumns(parameters)
    // change the schema to use VARCHAR for StringType for partitioning columns
    // so that the row buffer table can use it as part of primary key
    val (primaryKeyClause, stringPKCols) = StoreUtils.getPrimaryKeyClause(
      parameters, specifiedSchema, sqlContext)
    val schema = if (stringPKCols.isEmpty) specifiedSchema
    else {
      StructType(specifiedSchema.map { field =>
        if (stringPKCols.contains(field)) {
          field.copy(metadata = Utils.varcharMetadata(Constant.MAX_VARCHAR_SIZE,
            field.metadata))
        } else field
      })
    }

    val ddlExtension = StoreUtils.ddlExtensionString(parameters,
      isRowTable = false, isShadowTable = false)

    val ddlExtensionForShadowTable = StoreUtils.ddlExtensionString(
      parametersForShadowTable, isRowTable = false, isShadowTable = true)

    // val dependentRelations = parameters.remove(ExternalStoreUtils.DEPENDENT_RELATIONS)
    val connProperties = ExternalStoreUtils.validateAndGetAllProps(
      Some(sqlContext.sparkSession), parameters)

    StoreUtils.validateConnProps(parameters)

    val schemaString = JdbcExtendedUtils.schemaString(schema,
      connProperties.dialect)
    val schemaExtension = if (schemaString.length > 0) {
      val temp = schemaString.substring(0, schemaString.length - 1).
          concat(s", ${StoreUtils.ROWID_COLUMN_DEFINITION}, $primaryKeyClause )")
      s"$temp $ddlExtension"
    } else {
      s"$schemaString $ddlExtension"
    }

    var success = false
    val catalog = sqlContext.sparkSession.asInstanceOf[SnappySession].sessionCatalog
    val qualifiedTableName = catalog.newQualifiedTableName(table)
    val tableName = qualifiedTableName.toString
    val externalStore = new JDBCSourceAsColumnarStore(connProperties,
      partitions, tableName, schema)

    // create an index relation if it is an index table
    val baseTable = parameters.get(StoreUtils.GEM_INDEXED_TABLE)
    val relation = baseTable match {
      case Some(btable) => new IndexColumnFormatRelation(
        tableName,
        getClass.getCanonicalName,
        mode,
        schema,
        schemaExtension,
        ddlExtensionForShadowTable,
        tableOptions,
        externalStore,
        partitioningColumns,
        sqlContext,
        btable)
      case None => new ColumnFormatRelation(
        tableName,
        getClass.getCanonicalName,
        mode,
        schema,
        schemaExtension,
        ddlExtensionForShadowTable,
        tableOptions,
        externalStore,
        partitioningColumns,
        sqlContext)
    }
    val isRelationforSample = parameters.get(ExternalStoreUtils.RELATION_FOR_SAMPLE)
        .exists(_.toBoolean)

    try {
      relation.createTable(mode)
      if (!isRelationforSample) {
        catalog.registerDataSourceTable(qualifiedTableName, Some(relation.schema),
          partitioningColumns.toArray,
          classOf[execution.columnar.impl.DefaultSource].getCanonicalName,
          tableOptions, Some(relation))
      }
      success = true
      relation
    } finally {
      if (!success && relation.tableCreated) {
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

    createRelation(sqlContext, mode, options, schema)
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], data: DataFrame): JDBCAppendableRelation = {
    val catalog = sqlContext.sparkSession.asInstanceOf[SnappySession].sessionCatalog
    val relation = createRelation(sqlContext, mode, options,
      catalog.normalizeSchema(data.schema))
    var success = false
    try {
      relation.insert(data, mode == SaveMode.Overwrite)
      success = true
      relation
    } finally {
      if (!success && !relation.tableExists) {
        val catalog = sqlContext.sparkSession.asInstanceOf[SnappySession].sessionCatalog
        catalog.unregisterDataSourceTable(catalog.newQualifiedTableName(relation.table),
          Some(relation))
        // destroy the relation
        relation.destroy(ifExists = true)
      }
    }
  }
}
