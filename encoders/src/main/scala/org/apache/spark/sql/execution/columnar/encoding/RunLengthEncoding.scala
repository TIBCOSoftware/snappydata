/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

final class RunLengthDecoder(columnBytes: AnyRef, startCursor: Long,
    field: StructField, initDelta: (AnyRef, Long) => Long = ColumnEncoding.identityLong)
    extends RunLengthDecoderBase(columnBytes, startCursor, field,
      initDelta) with NotNullDecoder

final class RunLengthDecoderNullable(columnBytes: AnyRef, startCursor: Long,
    field: StructField, initDelta: (AnyRef, Long) => Long = ColumnEncoding.identityLong)
    extends RunLengthDecoderBase(columnBytes, startCursor, field,
      initDelta) with NullableDecoder

trait RunLengthDecoding {

  protected[this] final var dataCursor: Long = _
  protected[this] final var endDataCursor: Long = _
  /**
   * The next position at which the value changes.
   * A value < Int.Max / 2 indicates that current run is of trues
   * while a value greater than that indicates that it is of false values
   * (the maximum number of values in a column batch cannot exceed Int.Max/2).
   */
  protected[this] final var nextPosition: Int = _

  /** any different handling when encoded positions end (e.g. for nulls, after last null) */
  protected def handleDataEnd(nextPosition: Int): Int = {
    throw new IllegalStateException(
      s"RunLengthEncoding: reading next run after data end (nextPosition=$nextPosition)")
  }

  /**
   * Read short run-length encoded as 1 or 2 bytes.
   */
  protected final def readRunLength(columnBytes: AnyRef, nextPosition: Int): Int = {
    if (dataCursor < endDataCursor) {
      var run: Int = Platform.getByte(columnBytes, dataCursor)
      dataCursor += 1
      if (run >= 0) run
      else if (dataCursor < endDataCursor) {
        // encoded as two bytes
        run = (run & 0x7f) | ((Platform.getByte(columnBytes, dataCursor) & 0xff) << 7)
        dataCursor += 1
        run
      } else {
        throw new IllegalStateException(
          s"RunLengthEncoding: read one byte when expected two (nextPosition=$nextPosition)")

      }
    } else handleDataEnd(nextPosition)
  }
}

abstract class RunLengthDecoderBase(columnBytes: AnyRef, startCursor: Long,
    field: StructField, initDelta: (AnyRef, Long) => Long)
    extends ColumnDecoder(columnBytes, startCursor, field,
      initDelta) with RunLengthEncoding {

  private[this] var runLengthEndPosition = -1
  private[this] var currentValueLong: Long = _
  private[this] var currentValueString: UTF8String = _

  override protected[sql] def initializeCursor(columnBytes: AnyRef, cursor: Long,
      dataType: DataType): Long = {
    currentCursor = cursor
    cursor
  }

  override final def readByte(columnBytes: AnyRef, nonNullPosition: Int): Byte = {
    if (runLengthEndPosition >= nonNullPosition) {
      currentValueLong.toByte
    } else {
      do {
        val cursor = currentCursor
        currentValueLong = Platform.getByte(columnBytes, cursor)
        runLengthEndPosition += ColumnEncoding.readInt(columnBytes, cursor + 1)
        currentCursor = cursor + 3
      } while (runLengthEndPosition < nonNullPosition)
      currentValueLong.toByte
    }
  }

  override final def readBoolean(columnBytes: AnyRef, nonNullPosition: Int): Boolean =
    readByte(columnBytes, nonNullPosition) == 1

  override final def readShort(columnBytes: AnyRef, nonNullPosition: Int): Short = {
    if (runLengthEndPosition >= nonNullPosition) {
      currentValueLong.toShort
    } else {
      do {
        val cursor = currentCursor
        currentValueLong = ColumnEncoding.readShort(columnBytes, cursor)
        runLengthEndPosition += ColumnEncoding.readInt(columnBytes, cursor + 2)
        currentCursor = cursor + 6
      } while (runLengthEndPosition < nonNullPosition)
      currentValueLong.toShort
    }
  }

  override final def readInt(columnBytes: AnyRef, nonNullPosition: Int): Int = {
    if (runLengthEndPosition >= nonNullPosition) {
      currentValueLong.toInt
    } else {
      do {
        val cursor = currentCursor
        currentValueLong = ColumnEncoding.readInt(columnBytes, cursor)
        runLengthEndPosition += ColumnEncoding.readInt(columnBytes, cursor + 4)
        currentCursor = cursor + 8
      } while (runLengthEndPosition < nonNullPosition)
      currentValueLong.toInt
    }
  }

  override final def readLong(columnBytes: AnyRef, nonNullPosition: Int): Long = {
    if (runLengthEndPosition >= nonNullPosition) {
      currentValueLong
    } else {
      do {
        val cursor = currentCursor
        currentValueLong = ColumnEncoding.readLong(columnBytes, cursor)
        runLengthEndPosition += ColumnEncoding.readInt(columnBytes, cursor + 8)
        currentCursor = cursor + 12
      } while (runLengthEndPosition < nonNullPosition)
      currentValueLong
    }
  }

  override final def readUTF8String(columnBytes: AnyRef, nonNullPosition: Int): UTF8String = {
    if (runLengthEndPosition >= nonNullPosition) {
      currentValueString
    } else {
      do {
        var cursor = currentCursor
        val currentValue = ColumnEncoding.readUTF8String(columnBytes, cursor)
        cursor += (4 + currentValue.numBytes())
        currentValueString = currentValue
        runLengthEndPosition += ColumnEncoding.readInt(columnBytes, cursor)
        currentCursor = cursor + 4
      } while (runLengthEndPosition < nonNullPosition)
      currentValueString
    }
  }
}
