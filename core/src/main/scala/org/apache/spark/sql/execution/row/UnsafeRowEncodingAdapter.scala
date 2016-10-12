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

import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoding
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

final class UnsafeRowEncodingAdapter(holder: UnsafeRowHolder, columnIndex: Int)
    extends ColumnEncoding {

  override def typeId: Int = -2

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

  override def notNull(columnBytes: AnyRef, ordinal: Int): Int =
    if (holder.row.isNullAt(columnIndex)) 0 else 1

  override def readBoolean(columnBytes: AnyRef, cursor: Long): Boolean =
    holder.row.getBoolean(columnIndex)

  override def readByte(columnBytes: AnyRef, cursor: Long): Byte =
    holder.row.getByte(columnIndex)

  override def readShort(columnBytes: AnyRef, cursor: Long): Short =
    holder.row.getShort(columnIndex)

  override def readInt(columnBytes: AnyRef, cursor: Long): Int =
    holder.row.getInt(columnIndex)

  override def readLong(columnBytes: AnyRef, cursor: Long): Long =
    holder.row.getLong(columnIndex)

  override def readFloat(columnBytes: AnyRef, cursor: Long): Float =
    holder.row.getFloat(columnIndex)

  override def readDouble(columnBytes: AnyRef, cursor: Long): Double =
    holder.row.getDouble(columnIndex)

  override def readLongDecimal(columnBytes: AnyRef, precision: Int, scale: Int,
      cursor: Long): Decimal =
    holder.row.getDecimal(columnIndex, precision, scale)

  override def readDecimal(columnBytes: AnyRef, precision: Int, scale: Int,
      cursor: Long): Decimal =
    holder.row.getDecimal(columnIndex, precision, scale)

  override def readUTF8String(columnBytes: AnyRef, cursor: Long): UTF8String =
    holder.row.getUTF8String(columnIndex)

  override def readBinary(columnBytes: AnyRef, cursor: Long): Array[Byte] =
    holder.row.getBinary(columnIndex)

  override def readInterval(columnBytes: AnyRef, cursor: Long): CalendarInterval =
    holder.row.getInterval(columnIndex)

  override def readArray(columnBytes: AnyRef, cursor: Long): UnsafeArrayData =
    holder.row.getArray(columnIndex)

  override def readMap(columnBytes: AnyRef, cursor: Long): UnsafeMapData =
    holder.row.getMap(columnIndex)

  override def readStruct(columnBytes: AnyRef, numFields: Int,
      cursor: Long): UnsafeRow =
    holder.row.getStruct(columnIndex, numFields)
}

final class UnsafeRowHolder {
  private[row] var row: UnsafeRow = _

  def setRow(row: UnsafeRow): Unit = this.row = row
}
