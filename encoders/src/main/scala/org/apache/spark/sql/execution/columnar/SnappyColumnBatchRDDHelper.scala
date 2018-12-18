/*
 */
package org.apache.spark.sql.execution.columnar

import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.sql.Connection

import scala.collection.mutable.ArrayBuffer

import io.snappydata.Constant

import org.apache.spark.sql.execution.columnar.encoding.{ColumnEncoding, ColumnStatsSchema}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

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
class SnappyColumnBatchRDDHelper(tableName: String, projection: StructType,
    schema: StructType, filters: Option[Array[Filter]], bucketId: Int,
    hostList: ArrayBuffer[(String, String)],
    relDestroyVersion: Int = -1) {

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
    val connProperties = SharedExternalStoreUtils.connectionProperties(hostList)
    conn = SharedExternalStoreUtils.getConnection(connProperties, hostList)
    val txId = null
    // fetch all the column blobs pushing down the filters
    val helper = new SmartConnectorRDDHelper
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
        ColumnEncoding.identityLong)

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
}