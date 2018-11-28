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

import io.snappydata.datasource.v2.ConnectorUtils
import io.snappydata.datasource.v2.driver.{QueryConstructs, SnappyTableMetaData}

import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}
import org.apache.spark.sql.vectorized.ColumnarBatch

class SnappyColumnBatchReaderFactory(val bucketId: Int,
    tableMetaData: SnappyTableMetaData, queryConstructs: QueryConstructs)
    extends DataReaderFactory[ColumnarBatch] {

  /**
   * The preferred locations where the data reader returned by this reader factory can run faster,
   * but Spark does not guarantee to run the data reader on these locations.
   * The implementations should make sure that it can be run on any location.
   * The location is a string representing the host name.
   *
   * Note that if a host name cannot be recognized by Spark, it will be ignored as it was not in
   * the returned locations. By default this method returns empty string array, which means this
   * task has no location preference.
   *
   * If this method fails (by throwing an exception), the action would fail and no Spark job was
   * submitted.
   */
  override def preferredLocations(): Array[String] = {
    ConnectorUtils.preferredLocations(tableMetaData, bucketId)
  }

  /**
   * Returns a data reader to do the actual reading work.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  override def createDataReader(): DataReader[ColumnarBatch] = {
    new SnappyColumnBatchReader(bucketId, tableMetaData, queryConstructs)
  }

}
