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
package org.apache.spark.sql.rowtable

import java.util.Properties

import com.gemstone.gemfire.cache.Region
import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember
import com.gemstone.gemfire.internal.cache.PartitionedRegion
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionByExpressionResolver

import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils.CaseInsensitiveMutableHashMap
import org.apache.spark.sql.execution.columnar.{ConnectionProperties, ConnectionType, ExternalStoreUtils}
import org.apache.spark.sql.execution.datasources.jdbc.JDBCPartition
import org.apache.spark.sql.execution.{ConnectionPool, PartitionedDataSourceScan}
import org.apache.spark.sql.hive.SnappyStoreHiveCatalog
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.row.{GemFireXDDialect, JDBCMutableRelation}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.storage.BlockManagerId

/**
 * A LogicalPlan implementation for an Snappy row table whose contents
 * are retrieved using a JDBC URL or DataSource.
 */
class RowFormatRelation(
    _url: String,
    _table: String,
    _provider: String,
    preservepartitions: Boolean,
    _mode: SaveMode,
    _userSpecifiedString: String,
    _parts: Array[Partition],
    _poolProps: Map[String, String],
    _connProperties: Properties,
    _hikariCP: Boolean,
    _origOptions: Map[String, String],
    blockMap: Map[InternalDistributedMember, BlockManagerId],
    _context: SQLContext)
    extends JDBCMutableRelation(_url,
      _table,
      _provider,
      _mode,
      _userSpecifiedString,
      _parts,
      _poolProps,
      _connProperties,
      _hikariCP,
      _origOptions,
      _context)
    with PartitionedDataSourceScan
    with RowPutRelation {

  override def toString: String = s"RowFormatRelation[$table]"

  lazy val connectionType = ExternalStoreUtils.getConnectionType(url)

  final lazy val putStr = ExternalStoreUtils.getPutString(table, schema)

  override def buildScan(requiredColumns: Array[String],
      filters: Array[Filter]): RDD[Row] = {
    connectionType match {
      case ConnectionType.Embedded =>
        new RowFormatScanRDD(
          sqlContext.sparkContext,
          connector,
          ExternalStoreUtils.pruneSchema(schemaFields, requiredColumns),
          table,
          requiredColumns,
          ConnectionProperties(url, driver, _poolProps, connProperties, hikariCP),
          filters,
          parts,
          blockMap,
          connProperties
        ).asInstanceOf[RDD[Row]]

      case _ =>
        super.buildScan(requiredColumns, filters)
    }
  }

  /**
   * We need to set num partitions just to cheat Exchange of Spark.
   * This partition is not used for actual scan operator which depends on the
   * actual RDD.
   * Spark ClusteredDistribution is pretty simplistic to consider numShufflePartitions for
   * its partitioning scheme as Spark always uses shuffle.
   * Ideally it should consider child Spark plans partitioner.
   *
   */
  override def numPartitions: Int = {
    val resolvedName = StoreUtils.lookupName(table, tableSchema)
    val region: Region[_, _] = Misc.getRegionForTable(resolvedName, true)
    region match {
      case pr: PartitionedRegion => pr.getTotalNumberOfBuckets
      case _ => 1
    }
  }

  override def partitionColumns: Seq[String] = {
    val resolvedName = StoreUtils.lookupName(table, tableSchema)
    val region: Region[_, _] = Misc.getRegionForTable(resolvedName, true)
    val partitionColumn = region match {
      case pr: PartitionedRegion =>
        val resolver = pr.getPartitionResolver
            .asInstanceOf[GfxdPartitionByExpressionResolver]
        val parColumn = resolver.getColumnNames
        parColumn.toSeq
      case _ => Seq.empty[String]
    }
    partitionColumn
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
    StoreUtils.saveTable(data, url, table, connProperties, upsert = true)
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
    val connection = ConnectionPool.getPoolConnection(table, None, dialect,
      poolProperties, connProperties, hikariCP)
    try {
      val stmt = connection.prepareStatement(putStr)
      var result = 0
      if (numRows > 1) {
        for (row <- rows) {
          ExternalStoreUtils.setStatementParameters(stmt, schema.fields,
            row, dialect)
          stmt.addBatch()
        }
        val putCounts = stmt.executeBatch()
        result = putCounts.length
      } else {
        ExternalStoreUtils.setStatementParameters(stmt, schema.fields,
          rows.head, dialect)
        result = stmt.executeUpdate()
      }
      stmt.close()
      result
    } finally {
      connection.close()
    }
  }
}

final class DefaultSource extends MutableRelationProvider {

  override def createRelation(sqlContext: SQLContext, mode: SaveMode,
      options: Map[String, String], schema: String): RowFormatRelation = {

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

    val dialect = JdbcDialects.get(connProperties.url)
    val blockMap =
      dialect match {
        case GemFireXDDialect => StoreUtils.initStore(sqlContext, table,
          None, partitions, connProperties)
        case _ => Map.empty[InternalDistributedMember, BlockManagerId]
      }

    var success = false
    val relation = new RowFormatRelation(connProperties.url,
      SnappyStoreHiveCatalog.processTableIdentifier(table, sqlContext.conf),
      getClass.getCanonicalName,
      preservePartitions.exists(_.toBoolean),
      mode,
      schemaExtension,
      Array[Partition](JDBCPartition(null, 0)),
      connProperties.poolProps,
      connProperties.connProps,
      connProperties.hikariCP,
      options,
      blockMap,
      sqlContext)
    try {
      relation.tableSchema = relation.createTable(mode)
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
