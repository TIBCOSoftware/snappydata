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
package org.apache.spark.sql.execution.row

import scala.collection.mutable

import com.gemstone.gemfire.internal.cache.{LocalRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.access.index.GfxdIndexManager
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionByExpressionResolver

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Descending, SortDirection}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.execution.columnar.impl.SparkShellRowRDD
import org.apache.spark.sql.execution.columnar.{ConnectionType, ExternalStoreUtils}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCPartition
import org.apache.spark.sql.execution.{ConnectionPool, PartitionedDataSourceScan}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.row.{GemFireXDDialect, JDBCMutableRelation}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.{CodeGeneration, StoreUtils}

/**
 * A LogicalPlan implementation for an Snappy row table whose contents
 * are retrieved using a JDBC URL or DataSource.
 */
class RowFormatRelation(
    _connProperties: ConnectionProperties,
    _table: String,
    _provider: String,
    preservePartitions: Boolean,
    _mode: SaveMode,
    _userSpecifiedString: String,
    _parts: Array[Partition],
    _origOptions: Map[String, String],
    _context: SQLContext)
    extends JDBCMutableRelation(_connProperties,
      _table,
      _provider,
      _mode,
      _userSpecifiedString,
      _parts,
      _origOptions,
      _context)
    with PartitionedDataSourceScan
    with RowPutRelation {

  override def toString: String = s"RowFormatRelation[$table]"

  override val connectionType = ExternalStoreUtils.getConnectionType(dialect)

  final lazy val putStr = ExternalStoreUtils.getPutString(table, schema)

  private[this] lazy val resolvedName = ExternalStoreUtils.lookupName(table,
    tableSchema)

  @transient private[this] lazy val region: LocalRegion =
    Misc.getRegionForTable(resolvedName, true).asInstanceOf[LocalRegion]

  private[this] lazy val indexedColumns: mutable.HashSet[String] = {
    val cols = new mutable.HashSet[String]()
    val im = region.getIndexUpdater.asInstanceOf[GfxdIndexManager]
    if (im != null && im.getIndexConglomerateDescriptors != null) {
      val baseColumns = im.getContainer.getTableDescriptor.getColumnNamesArray
      val itr = im.getIndexConglomerateDescriptors.iterator()
      while (itr.hasNext) {
        // first column of index has to be present in filter to be usable
        val indexCols = itr.next().getIndexDescriptor.baseColumnPositions()
        cols += baseColumns(indexCols(0))
      }
    }
    cols
  }

  override def unhandledFilters(filters: Array[Filter]): Array[Filter] =
    filters.filter(ExternalStoreUtils.unhandledFilter(_, indexedColumns))

  override def buildUnsafeScan(requiredColumns: Array[String],
      filters: Array[Filter]): (RDD[Any], Seq[RDD[InternalRow]]) = {
    val handledFilters = filters.filter(ExternalStoreUtils
        .handledFilter(_, indexedColumns) eq ExternalStoreUtils.SOME_TRUE)
    val isPartitioned = region.getPartitionAttributes != null
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    val rdd = connectionType match {
      case ConnectionType.Embedded =>
        new RowFormatScanRDD(
          session,
          executorConnector,
          ExternalStoreUtils.pruneSchema(schemaFields, requiredColumns),
          resolvedName,
          isPartitioned,
          requiredColumns,
          pushProjections = false,
          useResultSet = false,
          connProperties,
          handledFilters
        )

      case _ =>
        new SparkShellRowRDD(
          session,
          executorConnector,
          ExternalStoreUtils.pruneSchema(schemaFields, requiredColumns),
          resolvedName,
          isPartitioned,
          requiredColumns,
          connProperties,
          handledFilters
        )
    }
    (rdd, Nil)
  }

  override lazy val numBuckets: Int = {
    region match {
      case pr: PartitionedRegion => pr.getTotalNumberOfBuckets
      case _ => 1
    }
  }

  override def partitionColumns: Seq[String] = {
    region match {
      case pr: PartitionedRegion =>
        val resolver = pr.getPartitionResolver
            .asInstanceOf[GfxdPartitionByExpressionResolver]
        val parColumn = resolver.getColumnNames
        parColumn.toSeq
      case _ => Seq.empty[String]
    }
  }

  /**
   * If the row is already present, it gets updated otherwise it gets
   * inserted into the table represented by this relation
   *
   * @param data the DataFrame to be upserted
   *
   * @return number of rows upserted
   */
  def put(data: DataFrame): Unit = {
    JdbcExtendedUtils.saveTable(data, table, schema,
      connProperties, upsert = true)
  }

  /**
   * If the row is already present, it gets updated otherwise it gets
   * inserted into the table represented by this relation
   *
   * @param rows the rows to be upserted
   *
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
    val connection = ConnectionPool.getPoolConnection(table, dialect,
      connProperties.poolProps, connProps, connProperties.hikariCP)
    try {
      val stmt = connection.prepareStatement(putStr)
      val result = CodeGeneration.executeUpdate(table, stmt,
        rows, numRows > 1, batchSize, schema.fields, dialect)
      stmt.close()
      result
    } finally {
      connection.close()
    }
  }

  private def getColumnStr(colWithDirection: (String, Option[SortDirection])): String = {
    colWithDirection._1 + " " + (colWithDirection._2 match
    {
      case Some(Ascending) => "ASC"
      case Some(Descending) => "DESC"
      case None => ""
    })

  }
  override protected def constructSQL(indexName: String,
      baseTable: String,
      indexColumns: Map[String, Option[SortDirection]],
      options: Map[String, String]): String = {

    val parameters = new CaseInsensitiveMutableHashMap(options)
    val columns = indexColumns.tail.foldLeft[String](
      getColumnStr(indexColumns.head))((cumulative, colsWithDirection) =>
      cumulative + "," + getColumnStr(colsWithDirection))


    val indexType = parameters.get(ExternalStoreUtils.INDEX_TYPE) match {
      case Some(x) => x
      case None => ""
    }
    s"CREATE $indexType INDEX $indexName ON $baseTable ($columns)"
  }
}

final class DefaultSource extends MutableRelationProvider {

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schema: String,
      data: Option[LogicalPlan]): RowFormatRelation = {

    val parameters = new CaseInsensitiveMutableHashMap(options)
    val table = ExternalStoreUtils.removeInternalProps(parameters)
    val partitions = ExternalStoreUtils.getTotalPartitions(
      sqlContext.sparkContext, parameters,
      forManagedTable = true, forColumnTable = false)
    val ddlExtension = StoreUtils.ddlExtensionString(parameters,
      isRowTable = true, isShadowTable = false)
    val schemaExtension = s"$schema $ddlExtension"
    val preservePartitions = parameters.remove("preservepartitions")
    val sc = sqlContext.sparkContext

    val connProperties =
      ExternalStoreUtils.validateAndGetAllProps(sc, parameters)

    StoreUtils.validateConnProps(parameters)

    var success = false
    val relation = new RowFormatRelation(connProperties,
      SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName,
      preservePartitions.exists(_.toBoolean),
      mode,
      schemaExtension,
      Array[Partition](JDBCPartition(null, 0)),
      options,
      sqlContext)
    try {
      relation.tableSchema = relation.createTable(mode)
      connProperties.dialect match {
        case GemFireXDDialect => StoreUtils.initStore(sqlContext, table,
          None, partitions, connProperties)
        case _ =>
      }
      data match {
        case Some(plan) =>
          relation.insert(Dataset.ofRows(sqlContext.sparkSession, plan))
        case None =>
      }
      success = true
      relation
    } finally {
      if (!success && !relation.tableExists) {
        // destroy the relation
        relation.destroy(ifExists = true)
      }
    }
  }
}
