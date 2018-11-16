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
package org.apache.spark.sql.execution.row

import com.gemstone.gemfire.internal.shared.ClientSharedData
import io.snappydata.ResultSetWithNull

import org.apache.spark.sql.catalyst.util.{DateTimeUtils, SerializedArray, SerializedMap, SerializedRow}
import org.apache.spark.sql.execution.columnar.encoding.ColumnDecoder
import org.apache.spark.sql.types.{DataType, Decimal, StructField}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * An adapter for a ResultSet to pose as ColumnEncoding so that the same
 * generated code can be used for both row buffer and column data access.
 */
final class ResultSetDecoder(rs: ResultSetWithNull, columnPosition: Int)
    extends ColumnDecoder(null, 0L, null) {

  private[this] val defaultCal = ClientSharedData.getDefaultCleanCalendar

  override def typeId: Int = -1

  override def supports(dataType: DataType): Boolean = true

  // nulls can be present so always return true
  override protected[sql] def hasNulls: Boolean = true

  override protected[sql] def initializeNulls(columnBytes: AnyRef,
      startCursor: Long, field: StructField): Long = 0L

  override protected[sql] def initializeCursor(columnBytes: AnyRef, cursor: Long,
      dataType: DataType): Long = 0L

  override def getNextNullPosition: Int =
    if (rs.isNull(columnPosition)) 0 else 1 /* 1 will never match */

  override def findNextNullPosition(columnBytes: AnyRef, nextNullPosition: Int, num: Int): Int =
    1 /* batch size is always 1 */

  override def numNulls(columnBytes: AnyRef, ordinal: Int, num: Int): Int =
    if (rs.isNull(columnPosition)) 1 else 0

  override def isNullAt(columnBytes: AnyRef, position: Int): Boolean =
    rs.isNull(columnPosition)

  override def readBoolean(columnBytes: AnyRef, nonNullPosition: Int): Boolean =
    rs.getBoolean(columnPosition)

  override def readByte(columnBytes: AnyRef, nonNullPosition: Int): Byte =
    rs.getByte(columnPosition)

  override def readShort(columnBytes: AnyRef, nonNullPosition: Int): Short =
    rs.getShort(columnPosition)

  override def readInt(columnBytes: AnyRef, nonNullPosition: Int): Int =
    rs.getInt(columnPosition)

  override def readLong(columnBytes: AnyRef, nonNullPosition: Int): Long =
    rs.getLong(columnPosition)

  override def readFloat(columnBytes: AnyRef, nonNullPosition: Int): Float =
    rs.getFloat(columnPosition)

  override def readDouble(columnBytes: AnyRef, nonNullPosition: Int): Double =
    rs.getDouble(columnPosition)

  override def readLongDecimal(columnBytes: AnyRef, precision: Int,
      scale: Int, nonNullPosition: Int): Decimal = {
    val dec = rs.getBigDecimal(columnPosition)
    if (dec != null) {
      Decimal.apply(dec, precision, scale)
    } else {
      null
    }
  }

  override def readDecimal(columnBytes: AnyRef, precision: Int, scale: Int,
      nonNullPosition: Int): Decimal =
    readLongDecimal(columnBytes, precision, scale, nonNullPosition)

  override def readUTF8String(columnBytes: AnyRef, nonNullPosition: Int): UTF8String =
    UTF8String.fromString(rs.getString(columnPosition))

  override def readDate(columnBytes: AnyRef, nonNullPosition: Int): Int = {
    defaultCal.clear()
    val date = rs.getDate(columnPosition, defaultCal)
    if (date ne null) DateTimeUtils.fromJavaDate(date) else -1
  }

  override def readTimestamp(columnBytes: AnyRef, nonNullPosition: Int): Long = {
    defaultCal.clear()
    val timestamp = rs.getTimestamp(columnPosition, defaultCal)
    if (timestamp ne null) DateTimeUtils.fromJavaTimestamp(timestamp) else -1L
  }

  override def readBinary(columnBytes: AnyRef, nonNullPosition: Int): Array[Byte] =
    rs.getBytes(columnPosition)

  override def readInterval(columnBytes: AnyRef,
      nonNullPosition: Int): CalendarInterval = {
    val micros = rs.getLong(columnPosition)
    if (rs.wasNull()) null else new CalendarInterval(0, micros)
  }

  override def readArray(columnBytes: AnyRef, nonNullPosition: Int): SerializedArray = {
    val b = rs.getBytes(columnPosition)
    if (b != null) {
      val result = new SerializedArray(8) // includes size
      result.pointTo(b, Platform.BYTE_ARRAY_OFFSET, b.length)
      result
    } else null
  }

  override def readMap(columnBytes: AnyRef, nonNullPosition: Int): SerializedMap = {
    val b = rs.getBytes(columnPosition)
    if (b != null) {
      val result = new SerializedMap
      result.pointTo(b, Platform.BYTE_ARRAY_OFFSET)
      result
    } else null
  }

  override def readStruct(columnBytes: AnyRef, numFields: Int,
      nonNullPosition: Int): SerializedRow = {
    val b = rs.getBytes(columnPosition)
    if (b != null) {
      val result = new SerializedRow(4, numFields) // includes size
      result.pointTo(b, Platform.BYTE_ARRAY_OFFSET, b.length)
      result
    } else null
  }
}
