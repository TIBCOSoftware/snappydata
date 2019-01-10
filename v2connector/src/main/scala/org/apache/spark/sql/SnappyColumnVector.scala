/*
 *
 */
package org.apache.spark.sql

import java.nio.ByteBuffer

import org.apache.spark.sql.execution.columnar.encoding.{ColumnDecoder, ColumnDeleteDecoder, UpdatedColumnDecoderBase}
import org.apache.spark.sql.types.{DataType, Decimal, StructField}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarArray, ColumnarMap}
import org.apache.spark.unsafe.types.UTF8String

class SnappyColumnVector(dataType: DataType, structField: StructField,
    byteBuffer: ByteBuffer, numOfRow: Int, ordinal: Int,
    columnDecoder: ColumnDecoder,
    deletedColumnDecoder: ColumnDeleteDecoder,
    updatedColumnDecoder: UpdatedColumnDecoderBase)
    extends ColumnVector(dataType: DataType) {

  var currentNullCount = 0
  var currentDeletedCount = 0
  var nextNullPosition = 0

  private val arrayOfBytes = if (byteBuffer == null || byteBuffer.isDirect) {
    null
  } else {
    byteBuffer.array
  }

  override def close(): Unit = {
    // TODO Check for the close operation on the
    // ColumnVector, whenever the current columnVector
    // finished reading by the upstream spark.
  }

  override def hasNull: Boolean = {
    columnDecoder.hasNulls
  }

  @inline def skipDeletedRows(rowId: Int): Unit = {
    if (deletedColumnDecoder != null) {
      while (deletedColumnDecoder.deleted(rowId + currentDeletedCount - currentNullCount)) {
        currentDeletedCount = currentDeletedCount + 1
      }
    }
  }

  @inline private def incrementAndGetNextNullPosition: Int = {
    currentNullCount = currentNullCount + 1
    nextNullPosition = columnDecoder.findNextNullPosition(
      arrayOfBytes, nextNullPosition, currentNullCount)
    nextNullPosition
  }

  @inline private def setAndGetCurrentNullCount(rowId: Int): Int = {
    currentNullCount = columnDecoder.numNulls(arrayOfBytes,
      (rowId + currentDeletedCount),
      currentNullCount)
    currentNullCount
  }

  override def numNulls(): Int = {
    currentNullCount
  }

  override def isNullAt(rowId: Int): Boolean = {
    var hasNull = true
    if (updatedColumnDecoder == null ||
        updatedColumnDecoder.unchanged(rowId + currentDeletedCount - currentNullCount)){
      nextNullPosition = columnDecoder.getNextNullPosition
      if (rowId < nextNullPosition ||
          (rowId == nextNullPosition + 1 &&
              rowId < incrementAndGetNextNullPosition) ||
          (rowId != nextNullPosition && (setAndGetCurrentNullCount(rowId) == 0 ||
              rowId != columnDecoder.getNextNullPosition))) {
        hasNull = false
      }
    } else if (updatedColumnDecoder.readNotNull){
      hasNull = false
    }
    hasNull
  }

  override def getInt(rowId: Int): Int = {
    skipDeletedRows(rowId)
    if (updatedColumnDecoder != null &&
        !updatedColumnDecoder.unchanged(rowId + currentDeletedCount - currentNullCount)
        && updatedColumnDecoder.readNotNull){
      updatedColumnDecoder.getCurrentDeltaBuffer.readInt
    } else {
      columnDecoder.readInt(arrayOfBytes, rowId + currentDeletedCount - currentNullCount)
    }
  }

  override def getBoolean(rowId: Int): Boolean = {
    skipDeletedRows(rowId)
    if (updatedColumnDecoder != null &&
        !updatedColumnDecoder.unchanged(rowId + currentDeletedCount - currentNullCount)
        && updatedColumnDecoder.readNotNull){
      updatedColumnDecoder.getCurrentDeltaBuffer.readBoolean
    } else {
      columnDecoder.readBoolean(arrayOfBytes, rowId + currentDeletedCount - currentNullCount)
    }
  }

  override def getByte(rowId: Int): Byte = {
    skipDeletedRows(rowId)
    if (updatedColumnDecoder != null &&
        !updatedColumnDecoder.unchanged(rowId + currentDeletedCount - currentNullCount)
        && updatedColumnDecoder.readNotNull){
      updatedColumnDecoder.getCurrentDeltaBuffer.readByte
    } else {
      columnDecoder.readByte(arrayOfBytes, rowId + currentDeletedCount - currentNullCount)
    }
  }

  override def getShort(rowId: Int): Short = {
    skipDeletedRows(rowId)
    if (updatedColumnDecoder != null &&
        !updatedColumnDecoder.unchanged(rowId + currentDeletedCount - currentNullCount)
        && updatedColumnDecoder.readNotNull){
      updatedColumnDecoder.getCurrentDeltaBuffer.readShort
    } else {
      columnDecoder.readShort(arrayOfBytes, rowId + currentDeletedCount - currentNullCount)
    }
  }

  override def getLong(rowId: Int): Long = {
    skipDeletedRows(rowId)
    if (updatedColumnDecoder != null &&
        !updatedColumnDecoder.unchanged(rowId + currentDeletedCount - currentNullCount)
        && updatedColumnDecoder.readNotNull){
      updatedColumnDecoder.getCurrentDeltaBuffer.readLong
    } else {
      columnDecoder.readLong(arrayOfBytes, rowId + currentDeletedCount - currentNullCount)
    }
  }

  override def getFloat(rowId: Int): Float = {
    skipDeletedRows(rowId)
    if (updatedColumnDecoder != null &&
        !updatedColumnDecoder.unchanged(rowId + currentDeletedCount - currentNullCount)
        && updatedColumnDecoder.readNotNull){
      updatedColumnDecoder.getCurrentDeltaBuffer.readFloat
    } else {
      columnDecoder.readFloat(arrayOfBytes, rowId + currentDeletedCount - currentNullCount)
    }
  }

  override def getDouble(rowId: Int): Double = {
    skipDeletedRows(rowId)
    if (updatedColumnDecoder != null &&
        !updatedColumnDecoder.unchanged(rowId + currentDeletedCount - currentNullCount)
        && updatedColumnDecoder.readNotNull){
      updatedColumnDecoder.getCurrentDeltaBuffer.readDouble
    } else {
      columnDecoder.readDouble(arrayOfBytes, rowId + currentDeletedCount - currentNullCount)
    }
  }

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = {
    skipDeletedRows(rowId)
    if (updatedColumnDecoder != null &&
        !updatedColumnDecoder.unchanged(rowId + currentDeletedCount - currentNullCount)
        && updatedColumnDecoder.readNotNull){
      updatedColumnDecoder.getCurrentDeltaBuffer.readDecimal(precision, scale)
    } else {
      columnDecoder.readDecimal(arrayOfBytes, rowId + currentDeletedCount -
          currentNullCount, precision, scale)
    }
  }

  override def getUTF8String(rowId: Int): UTF8String = {
    skipDeletedRows(rowId)
    if (updatedColumnDecoder != null &&
        !updatedColumnDecoder.unchanged(rowId + currentDeletedCount - currentNullCount)
        && updatedColumnDecoder.readNotNull){
      updatedColumnDecoder.getCurrentDeltaBuffer.readUTF8String
    } else {
      columnDecoder.readUTF8String(arrayOfBytes, rowId + currentDeletedCount - currentNullCount)
    }
  }

  override def getBinary(rowId: Int): Array[Byte] = {
    skipDeletedRows(rowId)
    if (updatedColumnDecoder != null &&
        !updatedColumnDecoder.unchanged(rowId + currentDeletedCount - currentNullCount)
        && updatedColumnDecoder.readNotNull){
      updatedColumnDecoder.getCurrentDeltaBuffer.readBinary
    } else {
      columnDecoder.readBinary(arrayOfBytes, rowId + currentDeletedCount - currentNullCount)
    }
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

  override def getChild(ordinal: Int): ColumnVector = {
    // TODO : check for this later
    null
  }
}
