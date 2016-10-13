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

  override protected def initializeNulls(columnBytes: AnyRef,
      field: Attribute): Long = 0L

  override def initializeDecoding(columnBytes: AnyRef,
      field: Attribute): Long = 0L

  override def nextBoolean(columnBytes: AnyRef, cursor: Long): Long = 0L

  override def nextByte(columnBytes: AnyRef, cursor: Long): Long = 0L

  override def nextShort(columnBytes: AnyRef, cursor: Long): Long = 0L

  override def nextInt(columnBytes: AnyRef, cursor: Long): Long = 0L

  override def nextLong(columnBytes: AnyRef, cursor: Long): Long = 0L

  override def nextFloat(columnBytes: AnyRef, cursor: Long): Long = 0L

  override def nextDouble(columnBytes: AnyRef, cursor: Long): Long = 0L

  override def nextLongDecimal(columnBytes: AnyRef, cursor: Long): Long = 0L

  override def nextDecimal(columnBytes: AnyRef, cursor: Long): Long = 0L

  override def nextUTF8String(columnBytes: AnyRef, cursor: Long): Long = 0L

  override def nextInterval(columnBytes: AnyRef, cursor: Long): Long = 0L

  override def nextBinary(columnBytes: AnyRef, cursor: Long): Long = 0L

  override def notNull(columnBytes: AnyRef, ordinal: Int): Int = -1

  override def readBoolean(columnBytes: AnyRef, cursor: Long): Boolean =
    rs.getBoolean(columnPosition)

  override def readByte(columnBytes: AnyRef, cursor: Long): Byte =
    rs.getByte(columnPosition)

  override def readShort(columnBytes: AnyRef, cursor: Long): Short =
    rs.getShort(columnPosition)

  override def readInt(columnBytes: AnyRef, cursor: Long): Int =
    rs.getInt(columnPosition)

  override def readLong(columnBytes: AnyRef, cursor: Long): Long =
    rs.getLong(columnPosition)

  override def readFloat(columnBytes: AnyRef, cursor: Long): Float =
    rs.getFloat(columnPosition)

  override def readDouble(columnBytes: AnyRef, cursor: Long): Double =
    rs.getDouble(columnPosition)

  override def readLongDecimal(columnBytes: AnyRef, precision: Int,
      scale: Int, cursor: Long): Decimal = {
    val dec = rs.getBigDecimal(columnPosition)
    if (dec != null) {
      Decimal.apply(dec, precision, scale)
    } else {
      null
    }
  }

  override def readDecimal(columnBytes: AnyRef, precision: Int, scale: Int,
      cursor: Long): Decimal =
    readLongDecimal(columnBytes, precision, scale, cursor)

  override def readUTF8String(columnBytes: AnyRef, cursor: Long): UTF8String =
    UTF8String.fromString(rs.getString(columnPosition))

  override def readDate(columnBytes: AnyRef, cursor: Long): Int = {
    defaultCal.clear()
    val date = rs.getDate(columnPosition, defaultCal)
    DateTimeUtils.fromJavaDate(date)
  }

  override def readTimestamp(columnBytes: AnyRef, cursor: Long): Long = {
    defaultCal.clear()
    val timestamp = rs.getTimestamp(columnPosition, defaultCal)
    DateTimeUtils.fromJavaTimestamp(timestamp)
  }

  override def readBinary(columnBytes: AnyRef, cursor: Long): Array[Byte] =
    rs.getBytes(columnPosition)

  override def readInterval(columnBytes: AnyRef,
      cursor: Long): CalendarInterval =
    new CalendarInterval(0, rs.getLong(columnPosition))

  override def readArray(columnBytes: AnyRef, cursor: Long): UnsafeArrayData = {
    val b = rs.getBytes(columnPosition)
    if (b != null) {
      val result = new UnsafeArrayData
      result.pointTo(b, Platform.BYTE_ARRAY_OFFSET, b.length)
      result
    } else null
  }

  override def readMap(columnBytes: AnyRef, cursor: Long): UnsafeMapData = {
    val b = rs.getBytes(columnPosition)
    if (b != null) {
      val result = new UnsafeMapData
      result.pointTo(b, Platform.BYTE_ARRAY_OFFSET, b.length)
      result
    } else null
  }

  override def readStruct(columnBytes: AnyRef, numFields: Int,
      cursor: Long): UnsafeRow = {
    val b = rs.getBytes(columnPosition)
    if (b != null) {
      val result = new UnsafeRow(numFields)
      result.pointTo(b, Platform.BYTE_ARRAY_OFFSET, b.length)
      result
    } else null
  }

  override def wasNull(): Boolean = rs.wasNull()
}
