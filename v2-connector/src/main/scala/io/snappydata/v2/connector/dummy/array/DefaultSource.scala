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
package io.snappydata.v2.connector.dummy.array

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types._

// DefaultSource checked by the Spark which implements the DataSourceV2
class DefaultSource extends DataSourceV2 with ReadSupport {

    def createReader(options: DataSourceOptions): DataSourceReader
    = new StaticArrayDataSourceReader()

}

class StaticArrayDataSourceReader extends DataSourceReader {

    // Schema : We have hardcoded the schema here with single column value.
    def readSchema() : StructType = StructType(Array(StructField("value", StringType)))

    // Single Factory assuming single Partition
    def createDataReaderFactories : java.util.List[DataReaderFactory[Row]] = {
        val factoryList = new java.util.ArrayList[DataReaderFactory[Row]]
        factoryList.add(new StaticArrayDataSourceReaderFactory(0, 5))
        factoryList.add(new StaticArrayDataSourceReaderFactory(5, 9))
        factoryList
    }

}


class StaticArrayDataSourceReaderFactory(var start: Int, var end: Int)
  extends DataReaderFactory[Row] with DataReader[Row] {

  def createDataReader : StaticArrayDataSourceReaderFactory = new
      StaticArrayDataSourceReaderFactory(start, end)

  val values = Array("1", "2", "3", "4", "5", "6", "7", "8", "9", "10")
  var index = 0

  def next : Boolean = start <= end

  def get : Row = {
    val row = Row(values(start))
    start = start + 1
    row
  }

  def close() : Unit = Unit

}