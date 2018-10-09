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


import java.sql.{Connection, DriverManager, ResultSet, Statement}
import java.util.Optional

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2._
import org.apache.spark.sql.sources.v2.reader._
import org.apache.spark.sql.sources.v2.writer.{DataSourceWriter, DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}


// DefaultSource checked by the Spark which implements the DataSourceV2
class DefaultSource extends DataSourceV2 with ReadSupport with WriteSupport {

  def createReader(options: DataSourceOptions): DataSourceReader = {
    val url = options.get("url").orElse("")
    val driver = options.get("driver").orElse("")
    val size = options.get("size").orElse("")
    val tableName = options.get("tableName").orElse("")

    new SnappyDataSourceReader(tableName, url, driver, size)
  }

  override def createWriter(jobId: String, schema: StructType, mode: SaveMode,
      options: DataSourceOptions): Optional[DataSourceWriter] = {
    Optional.of(new SnappyDataSourceWriter(options, mode, schema))
  }
}

class SnappyDataSourceWriter(options: DataSourceOptions, mode: SaveMode, schema: StructType)
    extends DataSourceWriter {
  private val url = options.get("url").orElse("")
  private val tableName = options.get("tableName").orElse("")
  override def createWriterFactory(): DataWriterFactory[Row] =
    new SnappyDataWriterFactory(url, tableName, mode, schema)

  override def abort(messages: Array[WriterCommitMessage]): Unit = {
    // todo: handle failures here
  }

  override def commit(messages: Array[WriterCommitMessage]): Unit = {}
}

class SnappyDataWriterFactory(url: String, tableName: String, mode: SaveMode, schema: StructType)
    extends DataWriterFactory[Row] {
  override def createDataWriter(partitionId: Int, attemptNumber: Int): DataWriter[Row] = {
    new SnappyDataWriter(url, tableName, mode, schema)
  }
}

class SnappyDataWriter(url: String, tableName: String, mode: SaveMode, schema: StructType)
    extends DataWriter[Row] {

  private val conn = DriverManager.getConnection(url)
  private val stmt = conn.createStatement()

  override def write(record: Row): Unit = {
    // assuming hardcoded schema of single numeric column
    stmt.addBatch(s"INSERT INTO $tableName values(${record(0)})")
  }

  override def abort(): Unit = {
    // todo: handle failures here
  }

  override def commit(): WriterCommitMessage = {
    try {
      stmt.executeBatch()
    } finally {
      stmt.close()
      conn.close()
    }
    new SnappyWriteCommitMessage
  }
}

class SnappyWriteCommitMessage() extends WriterCommitMessage

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