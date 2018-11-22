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

import java.sql.{CallableStatement, DriverManager}

import scala.collection.mutable.ArrayBuffer

import io.snappydata.Constant

import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.types.{DataType, StructType}

final class SnappyTableMetaDataReader {

  var conn: java.sql.Connection = _
  private val getV2MetaDataStmtString = "call sys.V2_GET_TABLE_METADATA(?, ?, ?, ?, ?, ?)"
  private var getV2MetaDataStmt: CallableStatement = _

  // scalastyle:off classforname
  Class.forName("io.snappydata.jdbc.ClientPoolDriver").newInstance
  // scalastyle:on classforname


  def initializeConnection(hostPort: String): Unit = {
    // TODO: handle auth credentials
    val connectionURL = s"${Constant.POOLED_THIN_CLIENT_URL}$hostPort/route-query=false;"
    conn = DriverManager.getConnection(connectionURL)
    getV2MetaDataStmt = conn.prepareCall(getV2MetaDataStmtString)
  }


  def closeConnection(): Unit = {
    if (conn != null) {
      conn.close()
      conn = null
    }
  }

  def getTableMetaData(options: DataSourceOptions): SnappyTableMetaData = {
    try {
      val hostString = options.get(V2Constants.SnappyConnection).get()
      val tableName = options.get(V2Constants.TABLE_NAME).get()

      initializeConnection(hostString)

      getV2MetaDataStmt.setString(1, tableName)
      // Table schema
      getV2MetaDataStmt.registerOutParameter(2, java.sql.Types.CLOB)
      // storage type
      getV2MetaDataStmt.registerOutParameter(3, java.sql.Types.INTEGER)
      // bucket count
      getV2MetaDataStmt.registerOutParameter(4, java.sql.Types.INTEGER)
      // partitioning columns
      getV2MetaDataStmt.registerOutParameter(5, java.sql.Types.VARCHAR)
      // bucket to server or replica to server mapping
      getV2MetaDataStmt.registerOutParameter(6, java.sql.Types.CLOB)
      getV2MetaDataStmt.execute

      val schemaStr = getV2MetaDataStmt.getString(2)
      val schema = DataType.fromJson(schemaStr).asInstanceOf[StructType]

      val storageType = getV2MetaDataStmt.getString(3)
      val bucketCount = getV2MetaDataStmt.getInt(4)
      val partitioningCols = getV2MetaDataStmt.getString(5).split(":")

      val bucketToServerMappingString = getV2MetaDataStmt.getString(6)
      val bucketToServerMapping = if (bucketCount > 0) {
        Option(ConnectorUtils.setBucketToServerMappingInfo(bucketToServerMappingString))
      }
      else {
        None
      }

      SnappyTableMetaData(tableName, schema, storageType, bucketCount,
        partitioningCols, bucketToServerMapping)
    } finally {
      closeConnection()
    }
  }
}

case class SnappyTableMetaData(tableName: String,
    schema: StructType, tableStorageType: String,
    bucketCount: Int, partitioningCols: Seq[String] = Nil,
    bucketToServerMapping: Option[Array[ArrayBuffer[(String, String)]]] = None)
