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

import java.util.OptionalLong

import org.apache.spark.sql.sources.v2.reader.Statistics

class SnappyStatistics(val tableName: String) extends Statistics {

  // TODO: currently returning user provided stats,
  // fix this to return stats fetched from Snappy cluster
  private lazy val sizeBytes = UserProvidedStats.statsMap.get(tableName.toLowerCase() + "_size")

  override def sizeInBytes(): OptionalLong = {
    if (sizeBytes.isDefined) {
      OptionalLong.of(sizeBytes.get)
    } else {
      OptionalLong.empty()
    }

  }

  override def numRows(): OptionalLong = OptionalLong.empty()
}
