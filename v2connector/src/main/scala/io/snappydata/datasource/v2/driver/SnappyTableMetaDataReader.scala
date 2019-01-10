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
package io.snappydata.datasource.v2.driver

import java.sql.{CallableStatement, DriverManager}
import java.util

import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.iapi.types.HarmonySerialBlob
import io.snappydata.Constant
import io.snappydata.datasource.v2.V2Constants
import io.snappydata.sql.catalog.SmartConnectorHelper
import io.snappydata.thrift.{CatalogMetadataDetails, CatalogMetadataRequest, snappydataConstants}
import org.apache.spark.sql.execution.columnar.TableNotFoundException
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.types.{DataType, StructType}

import scala.collection.mutable.ArrayBuffer

final class SnappyTableMetaDataReader {

  var conn: java.sql.Connection = _

  // scalastyle:off classforname
  Class.forName("io.snappydata.jdbc.ClientDriver").newInstance
  // scalastyle:on classforname

  private val getV2MetaDataStmtString = "call sys.GET_CATALOG_METADATA(?, ?, ?)"

  private var getCatalogMetaDataStmt: CallableStatement = _

  def initializeConnection(hostPort: String, user: String, password: String): Unit = {
    val connectionURL = s"${Constant.DEFAULT_THIN_CLIENT_URL}$hostPort/" +
      ";route-query=false;"
    conn = DriverManager.getConnection(connectionURL)
    getCatalogMetaDataStmt = conn.prepareCall(getV2MetaDataStmtString)
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
      val user = options.get(V2Constants.USER).get()
      val password = options.get(V2Constants.PASSWORD).get()
      val schemaAndTableName = tableName.split("\\.")

      initializeConnection(hostString, user, password)

      val request = new CatalogMetadataRequest()
      request.setSchemaName(schemaAndTableName(0)).setNameOrPattern(schemaAndTableName(1))
      val result = getCatalogInformation(request)

      if (result == null){
        throw new TableNotFoundException(schemaAndTableName(0), schemaAndTableName(1))
      }

      val tblSchema = result.catalogTable.tableSchema
      val tblType = result.catalogTable.provider
      val tblBucketCount = result.catalogTable.numBuckets
      val tblBucketOwner = result.catalogTable.bucketOwners

      val primaryKeyColumns = result.catalogTable.primaryKeyColumns
      val schema1 = DataType.fromJson(tblSchema).asInstanceOf[StructType]

      val partitioningCols1 = Option(primaryKeyColumns.toString) match {
        case Some(str) => str.split(":")
        case None => Array.empty[String]
      }

      // even though the name below is bucketToServerMapping; for replicated tables
      // this returns list of all servers on which replicated table exists
      val bucketToServerMapping = if (tblBucketCount > 0) {
        Option(SmartConnectorHelper.setBucketToServerMappingInfo(tblBucketCount, tblBucketOwner,
          true, true))
      } else {
        Option(SmartConnectorHelper.setReplicasToServerMappingInfo(
          tblBucketOwner.get(0).getSecondaries, true))
      }
      SnappyTableMetaData(tableName, schema1, tblType, tblBucketCount,
        partitioningCols1, bucketToServerMapping)
    } finally {
      closeConnection()
    }
  }

  def getCatalogInformation(request: CatalogMetadataRequest): CatalogMetadataDetails = {
    getCatalogMetaDataStmt.setInt(1, snappydataConstants.CATALOG_GET_TABLE)
    val requestBytes = GemFireXDUtils.writeThriftObject(request)
    getCatalogMetaDataStmt.setBlob(2, new HarmonySerialBlob(requestBytes))
    getCatalogMetaDataStmt.registerOutParameter(3, java.sql.Types.BLOB)
    assert(!getCatalogMetaDataStmt.execute())
    val resultBlob = getCatalogMetaDataStmt.getBlob(3)
    val resultLen = resultBlob.length().toInt
    val result = new CatalogMetadataDetails()
    assert(GemFireXDUtils.readThriftObject(result, resultBlob.getBytes(1, resultLen)) == 0)
    resultBlob.free()
    result
  }

  def getSecurePart(user: String, password: String): String = {
    var securePart = ""
    if (user != null && !user.isEmpty && password != null && !password.isEmpty) {
      securePart = s";user=$user;password=$password"
    }
    securePart
  }
}

/**
  * Metadata for tables
  *
  * @param tableName             table for which metadata is needed
  * @param schema                table schema (columns)
  * @param tableStorageType      table type that is ROW/COLUMN etc.
  * @param bucketCount           0 for replicated tables otherwise the actual count
  * @param partitioningCols      partitioning columns
  * @param bucketToServerMapping For a partitioned table, this is an array where each entry
  *                              is an ArrayBuffer of tuples and corresponds to a bucket(0th
  *                              entry for bucket#0 and so on).
  *                              Each entry in the ArrayBuffer is in the form of
  *                              (host, jdbcURL) for hosts where bucket exists
  *                              For replicated table the array contains a single ArrayBuffer
  *                              of tuples((host, jdbcURL)) for all hosts where the table exists
  */
case class SnappyTableMetaData(tableName: String,
                               schema: StructType, tableStorageType: String,
                               bucketCount: Int, partitioningCols: Seq[String] = Nil,
                               bucketToServerMapping: Option[Array[ArrayBuffer[(String, String)]]]
                               = None)
