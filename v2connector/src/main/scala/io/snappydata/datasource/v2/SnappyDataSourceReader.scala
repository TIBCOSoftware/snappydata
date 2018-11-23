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

package io.snappydata.datasource.v2

import java.util
import java.util.{List => JList}

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.partitioning.Partitioning
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, DataSourceReader, SupportsPushDownFilters, SupportsPushDownRequiredColumns, SupportsReportPartitioning}
import org.apache.spark.sql.types.StructType

// created on driver
class SnappyDataSourceReader(options: DataSourceOptions)
    extends DataSourceReader with
    SupportsReportPartitioning with
    SupportsPushDownRequiredColumns {
//    with SupportsPushDownFilters {


  lazy val tableMetaData: SnappyTableMetaData = getTableMetaData()
  // projected columns
  var projectedColumns: Option[StructType] = None

  private def getTableMetaData(): SnappyTableMetaData = {
    new SnappyTableMetaDataReader().getTableMetaData(options)
  }

  /**
   * Returns the actual schema of this data source reader, which may be different from the physical
   * schema of the underlying storage, as column pruning or other optimizations may happen.
   *
   * If this method fails (by throwing an exception), the action would fail and no Spark job was
   * submitted.
   */
  override def readSchema(): StructType = {
    projectedColumns.getOrElse(tableMetaData.schema)
  }

  /**
   * Returns a list of reader factories. Each factory is responsible for creating a data reader to
   * output data for one RDD partition. That means the number of factories returned here is same as
   * the number of RDD partitions this scan outputs.
   *
   * Note that, this may not be a full scan if the data source reader mixes in other optimization
   * interfaces like column pruning, filter push-down, etc. These optimizations are applied before
   * Spark issues the scan request.
   *
   * If this method fails (by throwing an exception), the action would fail and no Spark job was
   * submitted.
   */
  override def createDataReaderFactories(): JList[DataReaderFactory[Row]] = {
    // This will be called in the DataSourceV2ScanExec for creating
    // org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanExec.readerFactories.
    // Each factory object will correspond to one bucket/partition
    // This will be called on the driver
    // We will know the the name of the table from datasource options
    // We will fire a system procedure to get the bucket to host mapping
    // For each partition, we will create a factory with partition id and bucket id
    // PERF: To start with, with we will consider one partition per bucket, later we can
    // think batching multiple buckets for one partitions based on no of cores
    // Each factory object will be constructed with table name and bucket ids
    // call readSchema() and pushFilters() pass to the factory object so that
    // Will return all the filters as unhandled filters initially using pushedFilters()

    val factories =
      new util.ArrayList[DataReaderFactory[Row]](tableMetaData.bucketToServerMapping.get.length)
    var bucketId = 0
    val queryConstructs = QueryConstructs(readSchema())
    tableMetaData.bucketToServerMapping.foreach(b => b.foreach(_ => {
      factories.add(new SnappyDataReaderFactory(bucketId, tableMetaData, queryConstructs))
      bucketId = bucketId + 1
    } ))
    factories
  }

  /**
   * Applies column pruning w.r.t. the given requiredSchema.
   *
   * Implementation should try its best to prune the unnecessary columns or nested fields, but it's
   * also OK to do the pruning partially, e.g., a data source may not be able to prune nested
   * fields, and only prune top-level columns.
   *
   * Note that, data source readers should update {@link DataSourceReader#readSchema()} after
   * applying column pruning.
   */
  override def pruneColumns(requiredSchema: StructType): Unit = {
    // called by the engine to set projected columns so that our implementation can use those.
    // Implementation should return these in readSchema()
    if (requiredSchema.length > 0) projectedColumns = Option(requiredSchema)
  }

//  /**
//   * Pushes down filters, and returns filters that need to be evaluated after scanning.
//   */
//  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
//    // This method is passed all filters and is supposed to return unhandled filters
//    // We will return all the filters as unhandled filters initially using pushFilters()
//    null
//  }
//
//  /**
//   * Returns the filters that are pushed in {@link #pushFilters(Filter[])}.
//   * It's possible that there is no filters in the query and {@link #pushFilters(Filter[])}
//   * is never called, empty array should be returned for this case.
//   */
//  override def pushedFilters(): Array[Filter] = {
//    // looks like not much of use
//    null
//  }

  /**
   * Returns the output data partitioning that this reader guarantees.
   */
  override def outputPartitioning(): Partitioning = {
    new SnappyDataPartitioning(tableMetaData)
  }
}

case class QueryConstructs(projections: StructType, filters: Option[Array[Filter]] = None)