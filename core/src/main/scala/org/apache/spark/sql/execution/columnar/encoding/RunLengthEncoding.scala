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
package org.apache.spark.sql.execution.columnar.encoding

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

trait RunLengthEncoding extends ColumnEncoding {

  override final def typeId: Int = 1

  override final def supports(dataType: DataType): Boolean = dataType match {
    case BooleanType | ByteType | ShortType |
         IntegerType | DateType | LongType | TimestampType | StringType => true
    case _ => false
  }
}

final class RunLengthDecoder
    extends RunLengthDecoderBase with NotNullDecoder

final class RunLengthDecoderNullable
    extends RunLengthDecoderBase with NullableDecoder

abstract class RunLengthDecoderBase
    extends ColumnDecoder with RunLengthEncoding {

  private[this] var runLength = 0
  private[this] var cursorPos = 0L
  private[this] var currentValueLong: Long = _
  private[this] var currentValueString: UTF8String = _

  override protected[sql] def initializeCursor(columnBytes: AnyRef, cursor: Long,
      field: StructField): Long = {
    cursorPos = cursor
    // use the current count + value for cursor since that will be read and
    // written most frequently while actual cursor will be less frequently used
    0L
  }

  override protected[sql] def realCursor(cursor: Long): Long = cursorPos

  override protected[sql] def setRealCursor(cursor: Long): Unit = cursorPos = cursor

  override final def nextByte(columnBytes: AnyRef, countValue: Long): Long = {
    val count = countValue.toInt
    if (count != runLength) {
      countValue + 1
    } else {
      val cursor = cursorPos
      val currentValue: Long = Platform.getByte(columnBytes, cursor)
      runLength = ColumnEncoding.readInt(columnBytes, cursor + 1)
      cursorPos = cursor + 5
      // reset count to 1
      (currentValue << 32) | 1L
    }
  }

  override final def readByte(columnBytes: AnyRef, countValue: Long): Byte =
    (countValue >> 32).toByte

  override final def nextBoolean(columnBytes: AnyRef, countValue: Long): Long =
    this.nextByte(columnBytes, countValue)

  override final def readBoolean(columnBytes: AnyRef,
      countValue: Long): Boolean =
    (countValue >> 32) == 1

  override final def nextShort(columnBytes: AnyRef, countValue: Long): Long = {
    val count = countValue.toInt
    if (count != runLength) {
      countValue + 1
    } else {
      val cursor = cursorPos
      val currentValue: Long = ColumnEncoding.readShort(columnBytes, cursor)
      runLength = ColumnEncoding.readInt(columnBytes, cursor + 2)
      cursorPos = cursor + 6
      // reset count to 1
      (currentValue << 32) | 1L
    }
  }

  override final def readShort(columnBytes: AnyRef, countValue: Long): Short =
    (countValue >> 32).toShort

  override final def nextInt(columnBytes: AnyRef, countValue: Long): Long = {
    val count = countValue.toInt
    if (count != runLength) {
      countValue + 1
    } else {
      val cursor = cursorPos
      val currentValue: Long = ColumnEncoding.readInt(columnBytes, cursor)
      runLength = ColumnEncoding.readInt(columnBytes, cursor + 4)
      cursorPos = cursor + 8
      // reset count to 1
      (currentValue << 32) | 1L
    }
  }

  override final def readInt(columnBytes: AnyRef, countValue: Long): Int =
    (countValue >> 32).toInt

  override final def readDate(columnBytes: AnyRef, countValue: Long): Int =
    (countValue >> 32).toInt

  override final def nextLong(columnBytes: AnyRef, count: Long): Long = {
    if (count != runLength) {
      count + 1
    } else {
      val cursor = cursorPos
      currentValueLong = ColumnEncoding.readLong(columnBytes, cursor)
      runLength = ColumnEncoding.readInt(columnBytes, cursor + 8)
      cursorPos = cursor + 12
      // reset count to 1
      1L
    }
  }

  override final def readLong(columnBytes: AnyRef, count: Long): Long =
    currentValueLong

  override final def readTimestamp(columnBytes: AnyRef, count: Long): Long =
    currentValueLong

  override final def nextUTF8String(columnBytes: AnyRef, count: Long): Long = {
    if (count != runLength) {
      count + 1
    } else {
      var cursor = cursorPos
      val currentValue = ColumnEncoding.readUTF8String(columnBytes, cursor)
      cursor += (4 + currentValue.numBytes())
      runLength = ColumnEncoding.readInt(columnBytes, cursor)
      cursorPos = cursor + 4
      currentValueString = currentValue
      // reset count to 1
      1L
    }
  }

  override final def readUTF8String(columnBytes: AnyRef,
      count: Long): UTF8String =
    currentValueString
}
