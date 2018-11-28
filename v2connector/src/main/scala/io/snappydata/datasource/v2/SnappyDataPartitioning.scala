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

import io.snappydata.datasource.v2.driver.SnappyTableMetaData

import org.apache.spark.sql.sources.v2.reader.partitioning.{ClusteredDistribution, Distribution, Partitioning}

class SnappyDataPartitioning(tableMetaData: SnappyTableMetaData) extends Partitioning {

  override def numPartitions(): Int = {
    if (tableMetaData.bucketCount > 0) {
      tableMetaData.bucketCount
    } else {
      1 // returning 1 for replicated table
    }
  }

  override def satisfy(distribution: Distribution): Boolean = {
    if (tableMetaData.bucketCount > 0) {
      distribution match {
        case c: ClusteredDistribution =>
          c.clusteredColumns.sameElements(tableMetaData.partitioningCols)
        case _ => false
      }
    } else { // replicated table
      false
    }
  }

}