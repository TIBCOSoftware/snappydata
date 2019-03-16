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
package io.snappydata.datasource.v2.partition

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.{Collections, Properties}

import com.pivotal.gemfirexd.internal.iapi.types.HarmonySerialBlob
import io.snappydata.Constant
import io.snappydata.thrift.internal.ClientPreparedStatement
import org.apache.spark.sql.execution.columnar.encoding.{ColumnEncoding, ColumnStatsSchema}
import org.apache.spark.sql.execution.columnar.{ColumnBatchIteratorOnRS, SharedExternalStoreUtils}
import org.apache.spark.sql.sources.{ConnectionProperties, Filter}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.sql.{SnappyColumnVector, SnappyStoreClientDialect}

import scala.collection.immutable.HashMap
import scala.collection.mutable.ArrayBuffer

/**
 *
 * @param tableName
 * @param projection
 * @param schema
 * @param filters
 * @param bucketId
 * @param hostList
 * @param relDestroyVersion
 */
class SnappyColumnTableReader(tableName: String, projection: StructType,
    schema: StructType, filters: Option[Array[Filter]], bucketId: Int,
    hostList: ArrayBuffer[(String, String)],
    relDestroyVersion: Int = -1) {

  private val identityLong: (AnyRef, Long) => Long = (_: AnyRef, l: Long) => l

  private var columnBatchIterator: ColumnBatchIteratorOnRS = null
  private var scan_batchNumRows = 0
  private var batchBuffer: ByteBuffer = null

  private val columnOrdinals: Array[Int] = new Array[Int](projection.length)

  private var conn: Connection = _

  /**
   *
   */
  def initialize: Unit = {
    setProjectedColumnOrdinals
    val connProperties = connectionProperties(hostList)
    conn = SharedExternalStoreUtils.getConnection(connProperties, hostList)
    val txId = null
    // fetch all the column blobs pushing down the filters
    val helper = new ColumnBatchScanHelper
    val (statement, rs) = helper.prepareScan(conn, txId,
      getTableName, columnOrdinals, serializeFilters, bucketId, relDestroyVersion, false)
    columnBatchIterator = new ColumnBatchIteratorOnRS(conn, columnOrdinals, statement, rs,
      null, bucketId)
  }

  /**
   *
   * @return
   */
  def next: ColumnarBatch = {

    // Initialize next columnBatch
    val scan_colNextBytes = columnBatchIterator.next()

    // Calculate the number of row in the current batch
    val numStatsColumns = ColumnStatsSchema.numStatsColumns(schema.length)
    val scan_statsRow = org.apache.spark.sql.collection.SharedUtils
        .toUnsafeRow(scan_colNextBytes, numStatsColumns)

    val deltaStatsDecoder = columnBatchIterator.getCurrentDeltaStats
    val scan_deltaStatsRow = org.apache.spark.sql.collection.SharedUtils.
        toUnsafeRow(deltaStatsDecoder, numStatsColumns)

    val scan_batchNumFullRows = scan_statsRow.getInt(0)
    val scan_batchNumDeltaRows = if (scan_deltaStatsRow != null) {
      scan_deltaStatsRow.getInt(0)
    } else 0
    scan_batchNumRows = scan_batchNumFullRows + scan_batchNumDeltaRows
    scan_batchNumRows = scan_batchNumRows - columnBatchIterator.getDeletedRowCount

    // Construct ColumnBatch and return
    val columnVectors = new Array[ColumnVector](projection.length)

    // scan_buffer_initialization
    var vectorIndex = 0
    for (columnOrdinal <- columnOrdinals) {
      batchBuffer = columnBatchIterator.getColumnLob(columnOrdinal - 1)
      val field = schema.fields(columnOrdinal - 1)

      val columnDecoder = ColumnEncoding.getColumnDecoder(batchBuffer, field,
        identityLong)

      val columnUpdatedDecoder = columnBatchIterator
          .getUpdatedColumnDecoder(columnDecoder, field, columnOrdinal - 1)

      val columnVector = new SnappyColumnVector(field.dataType, field,
        batchBuffer, scan_batchNumRows,
        columnOrdinal, columnDecoder,
        columnBatchIterator.getDeletedColumnDecoder, columnUpdatedDecoder)

      columnVectors(vectorIndex) = columnVector
      vectorIndex = vectorIndex + 1
    }

    val columBatch = new ColumnarBatch(columnVectors)
    columBatch.setNumRows(scan_batchNumRows)
    columBatch
  }

  /**
   *
   * @return
   */
  def hasNext: Boolean = {
    columnBatchIterator.hasNext
  }

  /**
   *
   */
  def close: Unit = {
    columnBatchIterator.close()
  }

  /**
   * Get the actual table name created inside the gemxd layer
   *
   * @return
   */
  private def getTableName: String = {
    val dotIndex = tableName.indexOf('.')
    val schema = tableName.substring(0, dotIndex)
    val table = if (dotIndex > 0) tableName.substring(dotIndex + 1) else tableName
    schema + '.' + Constant.SHADOW_SCHEMA_NAME_WITH_SEPARATOR +
        table + Constant.SHADOW_TABLE_SUFFIX
  }

  /**
   * Method takes in projection column schema and calculates ordinals
   * of the projected columns
   *
   * @return
   */
  private def setProjectedColumnOrdinals: Unit = {
    var ordinal = 0
    for (field <- projection.fields) {
      columnOrdinals(ordinal) = schema.fieldIndex(field.name) + 1
      ordinal = ordinal + 1
    }
  }

  def getBlob(value: Any, conn: Connection): java.sql.Blob = {
    val serializedValue: Array[Byte] = serialize(value)
    val blob = conn.createBlob()
    blob.setBytes(1, serializedValue)
    blob
  }

  def serialize(value: Any): Array[Byte] = {
    val baos: ByteArrayOutputStream = new ByteArrayOutputStream()
    val os: ObjectOutputStream = new ObjectOutputStream(baos)
    os.writeObject(value)
    os.close()
    baos.toByteArray
  }

  /**
   * Method serializes the passed filters from Spark format to snappy format.
   *
   * @return
   */
  private def serializeFilters: Array[Byte] = {
    if (filters.isDefined) {
      serialize(filters.get)
    } else {
      null
    }
  }

  /**
    * Connection Properties.
    * @param hostList
    * @return
    */
  private def connectionProperties(hostList: ArrayBuffer[(String, String)]):
        ConnectionProperties = {

    // TODO: Check how to make properties Dynamic [Pradeep]
    // Hard-coded properties should be made dynamic. It should be
    // passed as a property bag to this method which will be obtained
    // rom the original create statement options.
    val map: Map[String, String] = HashMap[String, String](("maxActive", "256"),
      ("testOnBorrow", "true"), ("maxIdle", "256"), ("validationInterval", "10000"),
      ("initialSize", "4"), ("driverClassName", "io.snappydata.jdbc.ClientDriver"))

    val poolProperties = new Properties
    poolProperties.setProperty("driver", "io.snappydata.jdbc.ClientDriver")
    poolProperties.setProperty("route-query", "false")

    val executorConnProps = new Properties
    executorConnProps.setProperty("lob-chunk-size", "33554432")
    executorConnProps.setProperty("driver", "io.snappydata.jdbc.ClientDriver")
    executorConnProps.setProperty("route-query", "false")
    executorConnProps.setProperty("lob-direct-buffers", "true")

    ConnectionProperties(hostList(0)._2,
      "io.snappydata.jdbc.ClientDriver", SnappyStoreClientDialect, map,
      poolProperties, executorConnProps, false)

  }
}

// TODO [Pradeep] possibly this code can be reused from the SmartConnectorRDDHelper.prepareScan()
final class ColumnBatchScanHelper {

  def prepareScan(conn: Connection, txId: String, columnTable: String, projection: Array[Int],
      serializedFilters: Array[Byte], bucketId: Int,
      catalogVersion: Int, useKryoSerializer: Boolean): (PreparedStatement, ResultSet) = {
    val pstmt = if (useKryoSerializer) {
      conn.prepareStatement("call sys.COLUMN_TABLE_SCAN(?, ?, ?, 1)")
    } else {
      conn.prepareStatement("call sys.COLUMN_TABLE_SCAN(?, ?, ?, 0)")
    }
    pstmt.setString(1, columnTable)
    pstmt.setString(2, projection.mkString(","))
    // serialize the filters
    if ((serializedFilters ne null) && serializedFilters.length > 0) {
      pstmt.setBlob(3, new HarmonySerialBlob(serializedFilters))
    } else {
      pstmt.setNull(3, java.sql.Types.BLOB)
    }
    pstmt match {
      case clientStmt: ClientPreparedStatement =>
        val bucketSet = Collections.singleton(Int.box(bucketId))
        clientStmt.setLocalExecutionBucketIds(bucketSet, columnTable, true)
        clientStmt.setCatalogVersion(catalogVersion)
        clientStmt.setSnapshotTransactionId(txId)
      case _ =>
        pstmt.execute("call sys.SET_BUCKETS_FOR_LOCAL_EXECUTION(" +
            s"'$columnTable', '${bucketId}', $catalogVersion)")
        if (txId ne null) {
          pstmt.execute(s"call sys.USE_SNAPSHOT_TXID('$txId')")
        }
    }

    val rs = pstmt.executeQuery()
    (pstmt, rs)
  }
}