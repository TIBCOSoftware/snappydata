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
package io.snappydata.v2.connector.dummy.snappystore

// We need Unique package, where Spark looks for "DefaultSource"


import java.lang.Exception
import java.sql.{Connection, DriverManager, ResultSet, Statement}

import org.apache.spark.sql.Row
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.types._


// DefaultSource checked by the Spark which implements the DataSourceV2
class DefaultSource extends DataSourceV2 with ReadSupport {

  def createReader(options: DataSourceOptions): DataSourceReader = {
    val url = options.get("url").orElse("")
    val driver = options.get("driver").orElse("")
    val size = options.get("size").orElse("")
    val tableName = options.get("tableName").orElse("")

    new SnappyDataSourceReader(tableName, url, driver, size)
  }

}

class SnappyDataSourceReader(tableName: String, url: String, driver: String,
                             size: String) extends DataSourceReader with SupportsPushDownFilters {

  // Schema : We have hardcoded the schema here with single column value.
  def readSchema(): StructType = StructType(Array(StructField("YEAR_", StringType)))

  // Single Factory assuming single Partition
  def createDataReaderFactories: java.util.List[DataReaderFactory[Row]] = {

    val factoryList = new java.util.ArrayList[DataReaderFactory[Row]]
    factoryList.add(
      new SnappyDataSourceReaderFactory(
        tableName, url, driver, size, pushedFilters))
    factoryList
  }

  var pushedFilters: Array[Filter] = Array[Filter]()

  def pushFilters(filters: Array[Filter]): Array[Filter] = {
    pushedFilters = filters
    pushedFilters
  }
}


class SnappyDataSourceReaderFactory(tableName: String, url: String,
                                    driver: String,
                                    size: String,
                                    pushedFilters: Array[Filter])
  extends DataReaderFactory[Row] with DataReader[Row] {

  var stmt: Statement = null
  var conn: Connection = null
  var resultSet: ResultSet = null

  override def createDataReader(): DataReader[Row]
  = new SnappyDataSourceReaderFactory(tableName, url, driver, size, pushedFilters)

  def next: Boolean = {
    if (resultSet == null) {
      print("\n Initializing the result set \n")
      // scalastyle:off
      Class.forName(driver)
      conn = DriverManager.getConnection(url)
      stmt = conn.createStatement()
      resultSet = stmt.executeQuery("SELECT * FROM " + tableName + " LIMIT 20")
    }
    resultSet.next()
  }

  def get: Row = {
    val year = resultSet.getString("YEAR_")
    println(year)
    val row = Row(year)
    row
  }

  def close(): Unit = {
    stmt.close()
    conn.close()
  }

}