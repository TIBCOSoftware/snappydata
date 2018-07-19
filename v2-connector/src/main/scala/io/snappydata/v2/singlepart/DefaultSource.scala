/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
package io.snappydata.v2.singlepart

// We need Unique package, where Spark looks for "DefaultSource"


import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources._
import scala.util.control.Breaks._

// DefaultSource checked by the Spark which implements the DataSourceV2
class DefaultSource extends DataSourceV2 with ReadSupport {

  def createReader(options: DataSourceOptions): DataSourceReader
  = new SimpleDataSourceReader()

}

class SimpleDataSourceReader extends DataSourceReader with SupportsPushDownFilters {

  // Schema : We have hardcoded the schema here with single column value.
  def readSchema(): StructType = StructType(Array(StructField("value", IntegerType)))


  // Single Factory assuming single Partition
  def createDataReaderFactories: java.util.List[DataReaderFactory[Row]] = {
    val factoryList = new java.util.ArrayList[DataReaderFactory[Row]]
    factoryList.add(new SimpleDataSourceReaderFactory(new Array[Filter](1))) // Single partition
    factoryList
  }

  var pushedFilters: Array[Filter] = Array[Filter]()

  def pushFilters(filters: Array[Filter]): Array[Filter] = {
    // scalastyle:off
    println(filters.toList)
    pushedFilters = filters
    pushedFilters
  }

}

class SimpleDataSourceReaderFactory(pushedFilters: Array[Filter])
  extends DataReaderFactory[Row]
    with DataReader[Row] {

  def createDataReader(): SimpleDataSourceReaderFactory =
    new SimpleDataSourceReaderFactory(pushedFilters)

  val values = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

  var index = 0

  def next: Boolean = {

    index = index + 1

    if (pushedFilters == null || pushedFilters.isEmpty) {

      index < values.length

    } else {

      breakable {
        while (index < values.length) {
          if (get().getInt(0) % 2 != 0) {
            index = index + 1
          } else {
            break
          }
        }
      }
    }
    index < values.length
  }

  def get: Row = {
    //print("\n Current Index "+index)
    val row = Row(values(index))
    row
  }

  def close(): Unit = Unit

}

