/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.execution.row

import java.sql.ResultSet
import java.util.GregorianCalendar

import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoding
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * An adapter for a ResultSet to pose as ColumnEncoding so that the same
 * generated code can be used for both row buffer and column data access.
 */
final class ResultSetEncodingAdapter(rs: ResultSet, columnPosition: Int)
    extends ColumnEncoding {

  private[this] val defaultCal = new GregorianCalendar()

  override def typeId: Int = -1

  override def supports(dataType: DataType): Boolean = true

  override def initializeDecoding(columnBytes: Array[Byte],
      field: Attribute): Unit = {}

  override def nextBoolean(columnBytes: Array[Byte]): Unit = {}

  override def nextByte(columnBytes: Array[Byte]): Unit = {}

  override def nextShort(columnBytes: Array[Byte]): Unit = {}

  override def nextInt(columnBytes: Array[Byte]): Unit = {}

  override def nextLong(columnBytes: Array[Byte]): Unit = {}

  override def nextFloat(columnBytes: Array[Byte]): Unit = {}

  override def nextDouble(columnBytes: Array[Byte]): Unit = {}

  override def nextDecimal(columnBytes: Array[Byte], precision: Int): Unit = {}

  override def nextUTF8String(columnBytes: Array[Byte]): Unit = {}

  override def nextInterval(columnBytes: Array[Byte]): Unit = {}

  override def nextBinary(columnBytes: Array[Byte]): Unit = {}

  override def notNull(columnBytes: Array[Byte], ordinal: Int): Byte = -1

  override def readBoolean(bytes: Array[Byte]): Boolean =
    rs.getBoolean(columnPosition)

  override def readByte(bytes: Array[Byte]): Byte =
    rs.getByte(columnPosition)

  override def readShort(bytes: Array[Byte]): Short =
    rs.getShort(columnPosition)

  override def readInt(bytes: Array[Byte]): Int =
    rs.getInt(columnPosition)

  override def readLong(bytes: Array[Byte]): Long =
    rs.getLong(columnPosition)

  override def readFloat(bytes: Array[Byte]): Float =
    rs.getFloat(columnPosition)

  override def readDouble(bytes: Array[Byte]): Double =
    rs.getDouble(columnPosition)

  override def readDecimal(bytes: Array[Byte], precision: Int,
      scale: Int): Decimal = {
    val dec = rs.getBigDecimal(columnPosition)
    if (dec != null) {
      Decimal.apply(dec, precision, scale)
    } else {
      null
    }
  }

  override def readUTF8String(columnBytes: Array[Byte]): UTF8String =
    UTF8String.fromString(rs.getString(columnPosition))

  override def readDate(columnBytes: Array[Byte]): Int = {
    defaultCal.clear()
    val date = rs.getDate(columnPosition, defaultCal)
    DateTimeUtils.fromJavaDate(date)
  }

  override def readTimestamp(columnBytes: Array[Byte]): Long = {
    defaultCal.clear()
    val timestamp = rs.getTimestamp(columnPosition, defaultCal)
    DateTimeUtils.fromJavaTimestamp(timestamp)
  }

  override def readBinary(bytes: Array[Byte]): Array[Byte] =
    rs.getBytes(columnPosition)

  override def readInterval(bytes: Array[Byte]): CalendarInterval =
    new CalendarInterval(0, rs.getLong(columnPosition))

  override def readArray(bytes: Array[Byte]): UnsafeArrayData = {
    val b = rs.getBytes(columnPosition)
    if (b != null) {
      val result = new UnsafeArrayData
      result.pointTo(b, Platform.BYTE_ARRAY_OFFSET, b.length)
      result
    } else null
  }

  override def readMap(bytes: Array[Byte]): UnsafeMapData = {
    val b = rs.getBytes(columnPosition)
    if (b != null) {
      val result = new UnsafeMapData
      result.pointTo(b, Platform.BYTE_ARRAY_OFFSET, b.length)
      result
    } else null
  }

  override def readStruct(bytes: Array[Byte], numFields: Int): UnsafeRow = {
    val b = rs.getBytes(columnPosition)
    if (b != null) {
      val result = new UnsafeRow(numFields)
      result.pointTo(b, Platform.BYTE_ARRAY_OFFSET, b.length)
      result
    } else null
  }

  override def wasNull(): Boolean = rs.wasNull()
}
