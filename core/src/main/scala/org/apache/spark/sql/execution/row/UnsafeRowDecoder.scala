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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.execution.columnar.encoding.ColumnDecoder
import org.apache.spark.sql.types.{DataType, Decimal, StructField}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

// TODO: SW: change this to use SerializedRow/Array/Map (for sampler reservoir)
final class UnsafeRowDecoder(holder: UnsafeRowHolder, columnIndex: Int)
    extends ColumnDecoder {

  override def typeId: Int = -2

  override def supports(dataType: DataType): Boolean = true

  // nulls can be present so always return true
  override protected[sql] def hasNulls: Boolean = true

  override protected[sql] def initializeNulls(columnBytes: AnyRef,
      cursor: Long, field: StructField): Long = 0L

  override protected[sql] def initializeCursor(columnBytes: AnyRef, cursor: Long,
      field: StructField): Long = 0L

  override def nextBoolean(columnBytes: AnyRef, cursor: Long, mutated: Int): Long = 0L

  override def nextByte(columnBytes: AnyRef, cursor: Long, mutated: Int): Long = 0L

  override def nextShort(columnBytes: AnyRef, cursor: Long, mutated: Int): Long = 0L

  override def nextInt(columnBytes: AnyRef, cursor: Long, mutated: Int): Long = 0L

  override def nextLong(columnBytes: AnyRef, cursor: Long, mutated: Int): Long = 0L

  override def nextFloat(columnBytes: AnyRef, cursor: Long, mutated: Int): Long = 0L

  override def nextDouble(columnBytes: AnyRef, cursor: Long, mutated: Int): Long = 0L

  override def nextLongDecimal(columnBytes: AnyRef, cursor: Long, mutated: Int): Long = 0L

  override def nextDecimal(columnBytes: AnyRef, cursor: Long, mutated: Int): Long = 0L

  override def nextUTF8String(columnBytes: AnyRef, cursor: Long, mutated: Int): Long = 0L

  override def nextInterval(columnBytes: AnyRef, cursor: Long, mutated: Int): Long = 0L

  override def nextBinary(columnBytes: AnyRef, cursor: Long, mutated: Int): Long = 0L

  override def isNull(columnBytes: AnyRef, ordinal: Int, mutated: Int): Int =
    if (holder.row.isNullAt(columnIndex)) 1 else 0

  override def readBoolean(columnBytes: AnyRef, cursor: Long, mutated: Int): Boolean =
    holder.row.getBoolean(columnIndex)

  override def readByte(columnBytes: AnyRef, cursor: Long, mutated: Int): Byte =
    holder.row.getByte(columnIndex)

  override def readShort(columnBytes: AnyRef, cursor: Long, mutated: Int): Short =
    holder.row.getShort(columnIndex)

  override def readInt(columnBytes: AnyRef, cursor: Long, mutated: Int): Int =
    holder.row.getInt(columnIndex)

  override def readLong(columnBytes: AnyRef, cursor: Long, mutated: Int): Long =
    holder.row.getLong(columnIndex)

  override def readFloat(columnBytes: AnyRef, cursor: Long, mutated: Int): Float =
    holder.row.getFloat(columnIndex)

  override def readDouble(columnBytes: AnyRef, cursor: Long, mutated: Int): Double =
    holder.row.getDouble(columnIndex)

  override def readLongDecimal(columnBytes: AnyRef, precision: Int, scale: Int,
      cursor: Long, mutated: Int): Decimal =
    holder.row.getDecimal(columnIndex, precision, scale)

  override def readDecimal(columnBytes: AnyRef, precision: Int, scale: Int,
      cursor: Long, mutated: Int): Decimal =
    holder.row.getDecimal(columnIndex, precision, scale)

  override def readUTF8String(columnBytes: AnyRef, cursor: Long,
      mutated: Int): UTF8String = holder.row.getUTF8String(columnIndex)

  override def readBinary(columnBytes: AnyRef, cursor: Long, mutated: Int): Array[Byte] =
    holder.row.getBinary(columnIndex)

  override def readInterval(columnBytes: AnyRef, cursor: Long,
      mutated: Int): CalendarInterval = holder.row.getInterval(columnIndex)

  override def readArray(columnBytes: AnyRef, cursor: Long, mutated: Int): ArrayData =
    holder.row.getArray(columnIndex)

  override def readMap(columnBytes: AnyRef, cursor: Long, mutated: Int): MapData =
    holder.row.getMap(columnIndex)

  override def readStruct(columnBytes: AnyRef, numFields: Int,
      cursor: Long, mutated: Int): InternalRow =
    holder.row.getStruct(columnIndex, numFields)
}

final class UnsafeRowHolder {
  private[row] var row: UnsafeRow = _

  def setRow(row: UnsafeRow): Unit = this.row = row
}
