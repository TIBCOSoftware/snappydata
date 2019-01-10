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
package io.snappydata.datasource.v2.partition

import scala.collection.mutable.ArrayBuffer
import io.snappydata.datasource.v2.driver.{QueryConstructs, SnappyTableMetaData}
import org.apache.spark.sql.sources.v2.reader.DataReader
import org.apache.spark.sql.vectorized.ColumnarBatch

class SnappyColumnBatchReader (val bucketId: Int,
    tableMetaData: SnappyTableMetaData, queryConstructs: QueryConstructs)
    extends DataReader[ColumnarBatch] {

  val hostsAndURLs: ArrayBuffer[(String, String)] = tableMetaData.
      bucketToServerMapping.get(bucketId)

  val colBufferReader = new SnappyColumnTableReader(
    tableMetaData.tableName, queryConstructs.projections, tableMetaData.schema,
    queryConstructs.filters,
    bucketId, hostsAndURLs)
  colBufferReader.initialize

  val rowBufferReader = new SnappyRowTableReader(bucketId, tableMetaData, queryConstructs)
  var hasDataInRowBuffer = false

  override def next(): Boolean = {
    hasDataInRowBuffer = rowBufferReader.next()
    if (hasDataInRowBuffer) {
      hasDataInRowBuffer
    } else {
      colBufferReader.hasNext
    }
  }

  override def get(): ColumnarBatch = {
    if (hasDataInRowBuffer) {
      rowBufferReader.getAsColumnarBatch()
    } else {
      colBufferReader.next
    }
  }

  override def close(): Unit = {
    rowBufferReader.close()
    colBufferReader.close
  }
}
