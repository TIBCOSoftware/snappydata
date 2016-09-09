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

import org.apache.spark.sql.catalyst.expressions.{UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoding
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

final class UnsafeRowEncodingAdapter(holder: UnsafeRowHolder, columnIndex: Int)
    extends ColumnEncoding {

  override def typeId: Int = -2

  override def supports(dataType: DataType): Boolean = true

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

  override def notNull(columnBytes: Array[Byte], ordinal: Int): Byte =
    if (holder.row.isNullAt(columnIndex)) 0 else 1

  override def readBoolean(bytes: Array[Byte]): Boolean =
    holder.row.getBoolean(columnIndex)

  override def readByte(bytes: Array[Byte]): Byte =
    holder.row.getByte(columnIndex)

  override def readShort(bytes: Array[Byte]): Short =
    holder.row.getShort(columnIndex)

  override def readInt(bytes: Array[Byte]): Int =
    holder.row.getInt(columnIndex)

  override def readLong(bytes: Array[Byte]): Long =
    holder.row.getLong(columnIndex)

  override def readFloat(bytes: Array[Byte]): Float =
    holder.row.getFloat(columnIndex)

  override def readDouble(bytes: Array[Byte]): Double =
    holder.row.getDouble(columnIndex)

  override def readDecimal(bytes: Array[Byte], precision: Int,
      scale: Int): Decimal =
    holder.row.getDecimal(columnIndex, precision, scale)

  override def readUTF8String(columnBytes: Array[Byte]): UTF8String =
    holder.row.getUTF8String(columnIndex)

  override def readBinary(bytes: Array[Byte]): Array[Byte] =
    holder.row.getBinary(columnIndex)

  override def readInterval(bytes: Array[Byte]): CalendarInterval =
    holder.row.getInterval(columnIndex)

  override def readArray(bytes: Array[Byte]): UnsafeArrayData =
    holder.row.getArray(columnIndex)

  override def readMap(bytes: Array[Byte]): UnsafeMapData =
    holder.row.getMap(columnIndex)

  override def readStruct(bytes: Array[Byte], numFields: Int): UnsafeRow =
    holder.row.getStruct(columnIndex, numFields)

  override def wasNull(): Boolean = false
}

final class UnsafeRowHolder {
  private[row] var row: UnsafeRow = _

  def setRow(row: UnsafeRow): Unit = this.row = row
}
