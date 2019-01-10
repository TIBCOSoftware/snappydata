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

import io.snappydata.datasource.v2.driver.SnappyTableMetaData

/**
 * Contains utility methods required by connectors
 */
object ConnectorUtils {

  def preferredLocations(tableMetaData: SnappyTableMetaData, bucketId: Int): Array[String] = {
    if (tableMetaData.bucketToServerMapping.isEmpty) return new Array[String](0)

    val preferredServers: ArrayBuffer[(String, String)] = if (tableMetaData.bucketCount > 0) {
      // from bucketToServerMapping get the collection of hosts where the bucket exists
      // (each element in preferredServers ArrayBuffer is in the form of a tuple (host, jdbcURL))
      tableMetaData.bucketToServerMapping.get(bucketId)
    } else { // replicated tables
      tableMetaData.bucketToServerMapping.get(0)
    }

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

}
