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
import org.apache.spark.sql.types.{BooleanType, DataType}

final class BooleanBitSetEncoding
    extends BooleanBitSetEncodingBase with NotNullColumn

final class BooleanBitSetEncodingNullable
    extends BooleanBitSetEncodingBase with NullableColumn

abstract class BooleanBitSetEncodingBase extends UncompressedBase {

  private[this] var currentBitIndex = 0
  private[this] var currentWord = 0L

  override final def typeId: Int = 3

  override final def supports(dataType: DataType): Boolean =
    dataType == BooleanType

  override def initializeDecoding(columnBytes: Array[Byte],
      field: Attribute): Unit = {
    // read the count but its not used since CachedBatch has numRows
    ColumnEncoding.readInt(columnBytes, cursor)
    cursor += 4
    // initialize to max to force reading word in first nextBoolean call
    currentBitIndex = ColumnEncoding.BITS_PER_LONG
  }

  override final def nextBoolean(bytes: Array[Byte]): Unit = {
    currentBitIndex += 1
    if (currentBitIndex >= ColumnEncoding.BITS_PER_LONG) {
      currentBitIndex = 0
      currentWord = ColumnEncoding.readLong(bytes, cursor)
      cursor += 8
    }
  }

  override final def readBoolean(bytes: Array[Byte]): Boolean =
    ((currentWord >> currentBitIndex) & 1) != 0
}
