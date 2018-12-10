/*
 *
 */
package org.apache.spark.sql.execution.columnar

import java.nio.ByteBuffer

import org.apache.spark.sql.execution.columnar.encoding.{ColumnDeleteDecoder, ColumnEncoding}
import org.apache.spark.sql.types.{DataType, Decimal, StructField}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarArray, ColumnarMap}
import org.apache.spark.unsafe.types.UTF8String

class SnappyColumnVector(dataType: DataType, structField: StructField,
    byteBuffer: ByteBuffer, numOfRow: Int, ordinal: Int,
    deletedColumnDecoder: ColumnDeleteDecoder)
    extends ColumnVector(dataType: DataType) {

  private val columnDecoder = ColumnEncoding.getColumnDecoder(byteBuffer, structField,
    ColumnEncoding.identityLong)

  var rowOrdinal = 0
  var currentNullCount = 0
  var nextNullPosition = columnDecoder.getNextNullPosition

  private val arrayOfBytes = if (byteBuffer == null || byteBuffer.isDirect) {
    null
  } else {
    byteBuffer.array
  }

  override def close(): Unit = {
    // TODO: look for the clean up part if any after the
    // columnVector read completion from upstream spark.
  }

  override def hasNull: Boolean = {
    columnDecoder.hasNulls
  }

  def skipDeletedRows(rowId: Int): Unit = {
    if (deletedColumnDecoder != null) {
      while (deletedColumnDecoder.deleted(rowId + rowOrdinal - currentNullCount)) {
        rowOrdinal = rowOrdinal + 1
      }
    }
  }

  @inline
  private def incrementAndGetNextNullPosition : Int = {
    currentNullCount = currentNullCount + 1
    nextNullPosition = columnDecoder.findNextNullPosition(
      arrayOfBytes, nextNullPosition, currentNullCount)
    nextNullPosition
  }

  @inline
  private def setAndGetCurrentNullCount: Int = {
    currentNullCount = columnDecoder.numNulls(arrayOfBytes, rowOrdinal, currentNullCount)
    currentNullCount
  }

  override def numNulls(): Int = {
    columnDecoder.numNulls(arrayOfBytes, rowOrdinal, currentNullCount)
  }

  override def isNullAt(rowId: Int): Boolean = {
    var hasNull = true
    if (rowOrdinal < nextNullPosition ||
        (rowOrdinal == nextNullPosition + 1 &&
            rowOrdinal < incrementAndGetNextNullPosition) ||
        (rowOrdinal != nextNullPosition && (setAndGetCurrentNullCount == 0 ||
            rowOrdinal != columnDecoder.getNextNullPosition))) {
      hasNull = false
    }
    rowOrdinal = rowOrdinal + 1
    hasNull
  }

  override def getInt(rowId: Int): Int = {
    skipDeletedRows(rowId)
    columnDecoder.readInt(arrayOfBytes, rowId + rowOrdinal - currentNullCount)
  }

  override def getBoolean(rowId: Int): Boolean = {
    skipDeletedRows(rowId)
    columnDecoder.readBoolean(arrayOfBytes, rowId + rowOrdinal - currentNullCount)
  }

  override def getByte(rowId: Int): Byte = {
    skipDeletedRows(rowId)
    columnDecoder.readByte(arrayOfBytes, rowId + rowOrdinal - currentNullCount)
  }

  override def getShort(rowId: Int): Short = {
    skipDeletedRows(rowId)
    columnDecoder.readShort(arrayOfBytes, rowId + rowOrdinal - currentNullCount)
  }

  override def getLong(rowId: Int): Long = {
    skipDeletedRows(rowId)
    columnDecoder.readLong(arrayOfBytes, rowId + rowOrdinal - currentNullCount)
  }

  override def getFloat(rowId: Int): Float = {
    skipDeletedRows(rowId)
    columnDecoder.readFloat(arrayOfBytes, rowId + rowOrdinal - currentNullCount)
  }

  override def getDouble(rowId: Int): Double = {
    skipDeletedRows(rowId)
    columnDecoder.readDouble(arrayOfBytes, rowId + rowOrdinal- currentNullCount)
  }

  override def getArray(rowId: Int): ColumnarArray = {
    // TODO Handling the Array conversion
    // columnDecoder.readArray(arrayOfBytes, rowId)
    /*
    Error:(65, 28) type mismatch;
      found   : org.apache.spark.sql.catalyst.util.ArrayData
      required: org.apache.spark.sql.vectorized.ColumnarArray
    columnDecoder.readArray(arrayOfBytes, rowId)
     */
    null
  }

  override def getMap(ordinal: Int): ColumnarMap = {
    // TODO Handling the Map conversion
    /*
    Error:(69, 26) type mismatch;
    found   : org.apache.spark.sql.catalyst.util.MapData
    required: org.apache.spark.sql.vectorized.ColumnarMap
    columnDecoder.readMap(arrayOfBytes, ordinal)
     */
    // columnDecoder.readMap(arrayOfBytes, ordinal)
    null
  }

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = {
    skipDeletedRows(rowId)
    columnDecoder.readDecimal(arrayOfBytes, rowId + rowOrdinal -
     currentNullCount, precision, scale)
  }

  override def getUTF8String(rowId: Int): UTF8String = {
    skipDeletedRows(rowId)
    columnDecoder.readUTF8String(arrayOfBytes, rowId + rowOrdinal - currentNullCount)
  }

  override def getBinary(rowId: Int): Array[Byte] = {
    skipDeletedRows(rowId)
    columnDecoder.readBinary(arrayOfBytes, rowId + rowOrdinal - currentNullCount)
  }

  override def getChild(ordinal: Int): ColumnVector = {
    // TODO : check for this later
    null
  }
}
