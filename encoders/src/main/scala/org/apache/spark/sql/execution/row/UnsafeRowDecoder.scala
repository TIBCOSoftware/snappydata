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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.execution.columnar.encoding.ColumnDecoder
import org.apache.spark.sql.types.{DataType, Decimal, StructField}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

// TODO: SW: change this to use SerializedRow/Array/Map (for sampler reservoir)
final class UnsafeRowDecoder(holder: UnsafeRowHolder, columnIndex: Int)
    extends ColumnDecoder(null, 0L, null) {

  override def typeId: Int = -2

  override def supports(dataType: DataType): Boolean = true

  // nulls can be present so always return true
  override protected[sql] def hasNulls: Boolean = true

  override protected[sql] def initializeNulls(columnBytes: AnyRef,
      startCursor: Long, field: StructField): Long = 0L

  override protected[sql] def initializeCursor(columnBytes: AnyRef, cursor: Long,
      dataType: DataType): Long = 0L

  override def getNextNullPosition: Int =
    if (holder.row.isNullAt(columnIndex)) 0 else 1 /* 1 will never match */

  override def findNextNullPosition(columnBytes: AnyRef, nextNullPosition: Int, num: Int): Int =
    1 /* batch size is always 1 */

  override def numNulls(columnBytes: AnyRef, ordinal: Int, num: Int): Int =
    if (holder.row.isNullAt(columnIndex)) 1 else 0

  override def isNullAt(columnBytes: AnyRef, position: Int): Boolean =
    holder.row.isNullAt(columnIndex)

  override def readBoolean(columnBytes: AnyRef, nonNullPosition: Int): Boolean =
    holder.row.getBoolean(columnIndex)

  override def readByte(columnBytes: AnyRef, nonNullPosition: Int): Byte =
    holder.row.getByte(columnIndex)

  override def readShort(columnBytes: AnyRef, nonNullPosition: Int): Short =
    holder.row.getShort(columnIndex)

  override def readInt(columnBytes: AnyRef, nonNullPosition: Int): Int =
    holder.row.getInt(columnIndex)

  override def readLong(columnBytes: AnyRef, nonNullPosition: Int): Long =
    holder.row.getLong(columnIndex)

  override def readFloat(columnBytes: AnyRef, nonNullPosition: Int): Float =
    holder.row.getFloat(columnIndex)

  override def readDouble(columnBytes: AnyRef, nonNullPosition: Int): Double =
    holder.row.getDouble(columnIndex)

  override def readLongDecimal(columnBytes: AnyRef, precision: Int, scale: Int,
      nonNullPosition: Int): Decimal =
    holder.row.getDecimal(columnIndex, precision, scale)

  override def readDecimal(columnBytes: AnyRef, precision: Int, scale: Int,
      nonNullPosition: Int): Decimal =
    holder.row.getDecimal(columnIndex, precision, scale)

  override def readUTF8String(columnBytes: AnyRef, nonNullPosition: Int): UTF8String =
    holder.row.getUTF8String(columnIndex)

  override def readBinary(columnBytes: AnyRef, nonNullPosition: Int): Array[Byte] =
    holder.row.getBinary(columnIndex)

  override def readInterval(columnBytes: AnyRef, nonNullPosition: Int): CalendarInterval =
    holder.row.getInterval(columnIndex)

  override def readArray(columnBytes: AnyRef, nonNullPosition: Int): ArrayData =
    holder.row.getArray(columnIndex)

  override def readMap(columnBytes: AnyRef, nonNullPosition: Int): MapData =
    holder.row.getMap(columnIndex)

  override def readStruct(columnBytes: AnyRef, numFields: Int,
      nonNullPosition: Int): InternalRow =
    holder.row.getStruct(columnIndex, numFields)
}

final class UnsafeRowHolder {
  private[row] var row: UnsafeRow = _

  def setRow(row: UnsafeRow): Unit = this.row = row
}
