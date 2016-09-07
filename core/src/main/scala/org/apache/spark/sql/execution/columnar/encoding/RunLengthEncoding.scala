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

private[columnar] final class RunLengthEncoding
    extends RunLengthEncodingBase with NotNullColumn

private[columnar] final class RunLengthEncodingNullable
    extends RunLengthEncodingBase with NullableColumn

private[columnar] abstract class RunLengthEncodingBase
    extends UncompressedBase {

  private[this] var valueCount = 0
  private[this] var run = 0
  private[this] var currentValueLong: Long = _
  private[this] var currentValueString: UTF8String = _

  override final def typeId: Int = 1

  override final def supports(dataType: DataType): Boolean = dataType match {
    case BooleanType | ByteType | ShortType |
         IntegerType | LongType | StringType => true
    case _ => false
  }

  override final def readByte(bytes: Array[Byte]): Byte =
    currentValueLong.asInstanceOf[Byte]

  override final def nextByte(bytes: Array[Byte]): Unit = {
    if (valueCount != run) {
      valueCount += 1
    } else {
      currentValueLong = Platform.getByte(bytes, cursor)
      cursor += 1
      run = super.readInt(bytes)
      cursor += 4
      valueCount = 1
    }
  }

  override final def readBoolean(bytes: Array[Byte]): Boolean =
    currentValueLong == 1

  override final def nextBoolean(bytes: Array[Byte]): Unit =
    this.nextByte(bytes)

  override final def readShort(bytes: Array[Byte]): Short =
    currentValueLong.asInstanceOf[Short]

  override final def nextShort(bytes: Array[Byte]): Unit = {
    if (valueCount != run) {
      valueCount += 1
    } else {
      currentValueLong = super.readShort(bytes)
      cursor += 2
      run = super.readInt(bytes)
      cursor += 4
      valueCount = 1
    }
  }

  override final def readInt(bytes: Array[Byte]): Int =
    currentValueLong.asInstanceOf[Int]

  override final def nextInt(bytes: Array[Byte]): Unit = {
    if (valueCount != run) {
      valueCount += 1
    } else {
      currentValueLong = super.readInt(bytes)
      cursor += 4
      run = super.readInt(bytes)
      cursor += 4
      valueCount = 1
    }
  }

  override final def readLong(bytes: Array[Byte]): Long =
    currentValueLong

  override final def nextLong(bytes: Array[Byte]): Unit = {
    if (valueCount != run) {
      valueCount += 1
    } else {
      currentValueLong = super.readLong(bytes)
      cursor += 8
      run = super.readInt(bytes)
      cursor += 4
      valueCount = 1
    }
  }

  override final def readUTF8String(bytes: Array[Byte]): UTF8String =
    currentValueString

  override final def nextUTF8String(bytes: Array[Byte]): Unit = {
    if (valueCount != run) {
      valueCount += 1
    } else {
      currentValueString = super.readUTF8String(bytes)
      cursor += (4 + currentValueString.numBytes())
      run = super.readInt(bytes)
      cursor += 4
      valueCount = 1
    }
  }
}
