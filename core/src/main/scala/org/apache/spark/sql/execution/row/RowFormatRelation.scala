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
package org.apache.spark.sql.execution.row

import scala.collection.mutable

import com.gemstone.gemfire.internal.cache.{CacheDistributionAdvisee, LocalRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.sql.catalog.SnappyExternalCatalog

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{And, Ascending, Attribute, Descending, EqualTo, Expression, In, SortDirection}
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.catalyst.{InternalRow, analysis}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.impl.SmartConnectorRowRDD
import org.apache.spark.sql.execution.columnar.{ConnectionType, ExternalStoreUtils}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.{ConnectionPool, PartitionedDataSourceScan, SparkPlan}
import org.apache.spark.sql.internal.ColumnTableBulkOps
import org.apache.spark.sql.row.JDBCMutableRelation
import org.apache.spark.sql.sources.JdbcExtendedUtils.quotedName
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.CodeGeneration

/**
 * A LogicalPlan implementation for an Snappy row table whose contents
 * are retrieved using a JDBC URL or DataSource.
 */
class RowFormatRelation(
    _connProperties: ConnectionProperties,
    _table: String,
    _mode: SaveMode,
    _userSpecifiedString: String,
    _parts: Array[Partition],
    _origOptions: CaseInsensitiveMap,
    _context: SQLContext)
    extends JDBCMutableRelation(_connProperties,
      _table,
      _mode,
      _userSpecifiedString,
      _parts,
      _origOptions,
      _context)
    with PartitionedDataSourceScan
    with RowPutRelation {

  override def toString: String = s"RowFormatRelation[${Utils.toLowerCase(table)}]"

  override val connectionType: ConnectionType.Value =
    ExternalStoreUtils.getConnectionType(dialect)

  private final lazy val putStr = JdbcExtendedUtils.getInsertOrPutString(
    table, schema, putInto = true)

  override protected def withoutUserSchema: Boolean = userSpecifiedString.isEmpty

  override def numBuckets: Int = relationInfo.numBuckets

  override def isPartitioned: Boolean = relationInfo.isPartitioned

  override def partitionColumns: Seq[String] = relationInfo.partitioningCols

  @transient private lazy val indexedColumns: Set[String] = relationInfo.indexCols.toSet

  override def sizeInBytes: Long = schemaName match {
    // fill in some small size for system tables/VTIs
    case SnappyExternalCatalog.SYS_SCHEMA =>
      math.max(102400, sqlContext.conf.autoBroadcastJoinThreshold / 10)
    case _ => super.sizeInBytes
  }

  private[this] def pushdownPKColumns(filters: Seq[Expression]): Seq[String] = {
    def getEqualToColumns(filters: Seq[Expression]): mutable.ArrayBuffer[String] = {
      val list = new mutable.ArrayBuffer[String](4)
      filters.foreach {
        case EqualTo(col: Attribute, _) => list += col.name
        case In(col: Attribute, _) => list += col.name
        case And(left, right) => list ++= getEqualToColumns(Array(left, right))
        case _ =>
      }
      list
    }

    // all columns of primary key have to be present in filter to be usable
    if (schemaName == SnappyExternalCatalog.SYS_SCHEMA) Nil
    else {
      val equalToColumns = getEqualToColumns(filters)
      val pkCols = relationInfo.pkCols.toSeq
      if (pkCols.forall(equalToColumns.contains)) pkCols else Nil
    }
  }

  override def unhandledFilters(filters: Seq[Expression]): Seq[Expression] = {
    filters.filter(ExternalStoreUtils.unhandledFilter(_,
      indexedColumns ++ pushdownPKColumns(filters)))
  }

  override def buildUnsafeScan(requiredColumns: Array[String],
      filters: Array[Expression]): (RDD[Any], Seq[RDD[InternalRow]]) = {
    val handledFilters = filters.flatMap(ExternalStoreUtils.handledFilter(_, indexedColumns
      ++ pushdownPKColumns(filters)) )
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]

    val rdd = connectionType match {
      case ConnectionType.Embedded =>
        val region = schemaName match {
          case SnappyExternalCatalog.SYS_SCHEMA => None
          case _ => Some(Misc.getRegionForTable(resolvedName, true).asInstanceOf[LocalRegion])
        }
        val pushProjections = !region.isInstanceOf[CacheDistributionAdvisee]
        new RowFormatScanRDD(
          session,
          resolvedName,
          isPartitioned,
          requiredColumns,
          pushProjections = pushProjections,
          useResultSet = pushProjections,
          connProperties,
          handledFilters,
          partitionPruner = () => Utils.getPrunedPartition(partitionColumns,
            filters, schema,
            numBuckets, relationInfo.partitioningCols.length),
          commitTx = true, delayRollover = false,
          projection = Array.emptyIntArray, region = region)

      case _ =>
        new SmartConnectorRowRDD(
          session,
          resolvedName,
          isPartitioned,
          requiredColumns,
          connProperties,
          handledFilters,
          _partEval = () => relationInfo.partitions,
          () => Utils.getPrunedPartition(partitionColumns,
          filters, schema,
          numBuckets, relationInfo.partitioningCols.length),
          relationInfo.catalogSchemaVersion,
          _commitTx = true, _delayRollover = false)
    }
    (rdd, Nil)
  }

  override def partitionExpressions(relation: LogicalRelation): Seq[Expression] = {
    // use case-insensitive resolution since partitioning columns during
    // creation could be using the same as opposed to during insert
    partitionColumns.map(colName =>
      relation.resolveQuoted(colName, analysis.caseInsensitiveResolution)
          .getOrElse(throw new AnalysisException(
            s"""Cannot resolve column "$colName" among (${relation.output})""")))
  }

  override def getPutPlan(relation: LogicalRelation,
      child: SparkPlan): SparkPlan = {
    RowInsertExec(child, putInto = true, partitionColumns,
      partitionExpressions(relation), numBuckets, isPartitioned, schema, Some(this),
      onExecutor = false, resolvedName, connProperties)
  }

  /**
   * If the row is already present, it gets updated otherwise it gets
   * inserted into the table represented by this relation
   *
   * @param rows the rows to be upserted
   * @return number of rows upserted
   */
  override def put(rows: Seq[Row]): Int = {
    val numRows = rows.length
    if (numRows == 0) {
      throw new IllegalArgumentException(
        "RowFormatRelation.put: no rows provided")
    }
    val connProps = connProperties.connProps
    val batchSize = connProps.getProperty("batchsize", "1000").toInt
    // use bulk insert using put plan for large number of rows
    if (numRows > (batchSize * 4)) {
      ColumnTableBulkOps.bulkInsertOrPut(rows, sqlContext.sparkSession, schema,
        table, putInto = true)
    } else {
      val connection = ConnectionPool.getPoolConnection(table, dialect,
        connProperties.poolProps, connProps, connProperties.hikariCP)
      try {
        val stmt = connection.prepareStatement(putStr)
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

  private def getColumnStr(colWithDirection: (String, Option[SortDirection])): String = {
    "\"" + Utils.toUpperCase(colWithDirection._1) + "\" " + (colWithDirection._2 match {
      case Some(Ascending) => "ASC"
      case Some(Descending) => "DESC"
      case None => ""
    })
  }

  override protected def constructSQL(indexName: String,
      baseTable: String,
      indexColumns: Map[String, Option[SortDirection]],
      options: Map[String, String]): String = {

    val columns = indexColumns.tail.foldLeft[String](
      getColumnStr(indexColumns.head))((cumulative, colsWithDirection) =>
      cumulative + "," + getColumnStr(colsWithDirection))

    val indexType = options.find(_._1.equalsIgnoreCase(ExternalStoreUtils.INDEX_TYPE)) match {
      case Some(x) => x._2
      case None => ""
    }
    s"CREATE $indexType INDEX ${quotedName(indexName)} ON ${quotedName(baseTable)} ($columns)"
  }
}
