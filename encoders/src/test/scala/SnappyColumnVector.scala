/*
 *
 */
package org.apache.spark.sql.execution.columnar

import java.nio.ByteBuffer

import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoding
import org.apache.spark.sql.types.{DataType, Decimal, StructField}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarArray, ColumnarMap}
import org.apache.spark.unsafe.types.UTF8String

class SnappyColumnVector(dataType: DataType, structField: StructField,
    byteBuffer: ByteBuffer, numOfRow: Int, numOfNulls: Int)
    extends ColumnVector(dataType: DataType) {

  private val columnDecoder = ColumnEncoding.getColumnDecoder(byteBuffer, structField,
    ColumnEncoding.identityLong)
  private val arrayOfBytes = if (byteBuffer == null || byteBuffer.isDirect) {
    null
  } else {
    byteBuffer.array
  }

  override def close(): Unit = {}

  override def hasNull: Boolean = {
    columnDecoder.isNullAt(arrayOfBytes, 0)
  }

  override def numNulls(): Int = { numOfNulls }

  override def isNullAt(rowId: Int): Boolean = {
    columnDecoder.isNullAt(arrayOfBytes, rowId)
  }

  override def getBoolean(rowId: Int): Boolean = {
    columnDecoder.readBoolean(arrayOfBytes, rowId)
  }

  override def getByte(rowId: Int): Byte = {
    columnDecoder.readByte(arrayOfBytes, rowId)
  }

  override def getShort(rowId: Int): Short = {
    columnDecoder.readShort(arrayOfBytes, rowId)
  }

  override def getInt(rowId: Int): Int = {
    columnDecoder.readInt(arrayOfBytes, rowId)
  }

  override def getLong(rowId: Int): Long = {
    columnDecoder.readLong(arrayOfBytes, rowId)
  }

  override def getFloat(rowId: Int): Float = {
    columnDecoder.readFloat(arrayOfBytes, rowId)
  }

  override def getDouble(rowId: Int): Double = {
    columnDecoder.readDouble(arrayOfBytes, rowId)
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
    columnDecoder.readDecimal(arrayOfBytes, rowId, precision, scale)
  }

  override def getUTF8String(rowId: Int): UTF8String = {
    columnDecoder.readUTF8String(arrayOfBytes, rowId)
  }

  override def getBinary(rowId: Int): Array[Byte] = {
    columnDecoder.readBinary(arrayOfBytes, rowId)
  }

  override def getChild(ordinal: Int): ColumnVector = {
    // TODO : check for this later
    null
  }
}
