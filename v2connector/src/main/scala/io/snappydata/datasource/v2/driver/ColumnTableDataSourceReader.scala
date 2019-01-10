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

import io.snappydata.datasource.v2.partition.SnappyColumnBatchReaderFactory

import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.{DataReaderFactory, SupportsScanColumnarBatch}
import org.apache.spark.sql.vectorized.ColumnarBatch

// created on driver
class ColumnTableDataSourceReader(options: DataSourceOptions, tableMetaData: SnappyTableMetaData)
    extends SnappyDataSourceReader(options, tableMetaData) with SupportsScanColumnarBatch {

  /**
   * Similar to {@link DataSourceReader#createDataReaderFactories()}, but returns columnar data
   * in batches.
   */
  override def createBatchDataReaderFactories: JList[DataReaderFactory[ColumnarBatch]] = {
    val factories = new util.ArrayList[DataReaderFactory[
        ColumnarBatch]](tableMetaData.bucketToServerMapping.get.length)
    var bucketId = 0
    val queryConstructs = QueryConstructs(readSchema(), filtersPushedToSnappy,
      whereClause, whereClauseArgs)
    tableMetaData.bucketToServerMapping.foreach(b => b.foreach(_ => {
      factories.add(new SnappyColumnBatchReaderFactory(bucketId, tableMetaData, queryConstructs))
      bucketId = bucketId + 1
    } ))
    factories
  }

}
