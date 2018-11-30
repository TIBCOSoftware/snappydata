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
package io.snappydata.datasource.v2.driver

import java.util
import java.util.{List => JList}

import io.snappydata.datasource.v2.partition.SnappyRowTableReaderFactory

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory

// created on driver
class RowTableDataSourceReader(options: DataSourceOptions, tableMetaData: SnappyTableMetaData)
    extends SnappyDataSourceReader(options, tableMetaData) {

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
    val queryConstructs = QueryConstructs(readSchema(),
      filtersPushedToSnappy, whereClause, whereClauseArgs)
    tableMetaData.bucketToServerMapping.foreach(b => b.foreach(_ => {
      factories.add(new SnappyRowTableReaderFactory(bucketId, tableMetaData, queryConstructs))
      bucketId = bucketId + 1
    } ))
    factories
  }
}
