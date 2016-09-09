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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.{DataType, LongType, TimestampType}
import org.apache.spark.unsafe.Platform

final class LongDeltaEncoding extends LongDeltaEncodingBase with NotNullColumn

final class LongDeltaEncodingNullable
    extends LongDeltaEncodingBase with NullableColumn

abstract class LongDeltaEncodingBase extends UncompressedBase {

  private[this] var prev = 0L

  override final def typeId: Int = 5

  override final def supports(dataType: DataType): Boolean = dataType match {
    case LongType | TimestampType => true
    case _ => false
  }

  override final def initializeDecoding(columnBytes: Array[Byte],
      field: Attribute): Unit = {
    // optimistically use the cursor as java index for single byte reads
    cursor -= Platform.BYTE_ARRAY_OFFSET
  }

  override final def nextLong(bytes: Array[Byte]): Unit = {
    val delta = bytes(cursor)
    cursor += 1
    if (delta > Byte.MinValue) {
      prev += delta
    } else {
      prev = ColumnEncoding.readLong(bytes, cursor + Platform.BYTE_ARRAY_OFFSET)
      cursor += 8
    }
  }

  override final def readLong(bytes: Array[Byte]): Long = prev

  override final def readTimestamp(columnBytes: Array[Byte]): Long = prev
}
