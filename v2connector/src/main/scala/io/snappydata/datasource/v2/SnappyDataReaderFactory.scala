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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2.reader.{DataReader, DataReaderFactory}

/**
 * Creates {@link SnappyDataReader} that actually fetches data on executors
 * Also returns the preferred locations for the bucket id for which
 * {@link SnappyDataReader} is responsible for
 * @param bucketId          bucketId for which this factory is created
 * @param tableMetaData     metadata of the table being scanned
 */
class SnappyDataReaderFactory(val bucketId: Int, tableMetaData: SnappyTableMetaData)
    extends DataReaderFactory[Row] {

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
    if (tableMetaData.bucketToServerMapping.isEmpty) return new Array[String](0)

    // from bucketToServerMapping get the collection of hosts where the bucket exists
    // (each element in preferredServers ArrayBuffer is in the form of a tuple (host, jdbcURL))
    val preferredServers: ArrayBuffer[(String, String)] = tableMetaData.
        bucketToServerMapping.get(bucketId)
    if (preferredServers.isEmpty) return new Array[String](0)

    val locations = Array.ofDim[String](preferredServers.length)
    var index: Int = 0
    preferredServers.foreach(
      h => {
        locations(index) = h._1
        index = index + 1
      }
    )
    locations
  }

  /**
   * Returns a data reader to do the actual reading work.
   *
   * If this method fails (by throwing an exception), the corresponding Spark task would fail and
   * get retried until hitting the maximum retry times.
   */
  override def createDataReader(): DataReader[Row] = {
    new SnappyDataReader(bucketId, tableMetaData)
  }

}