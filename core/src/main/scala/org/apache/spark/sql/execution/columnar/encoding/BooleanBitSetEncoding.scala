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

import org.apache.spark.sql.types.{BooleanType, DataType, StructField}

trait BooleanBitSetEncoding extends ColumnEncoding {

  override final def typeId: Int = 4

  override final def supports(dataType: DataType): Boolean =
    dataType == BooleanType
}

final class BooleanBitSetDecoder
    extends BooleanBitSetDecoderBase with NotNullDecoder

final class BooleanBitSetDecoderNullable
    extends BooleanBitSetDecoderBase with NullableDecoder

abstract class BooleanBitSetDecoderBase
    extends ColumnDecoder with BooleanBitSetEncoding {

  private[this] var byteCursor = 0L
  private[this] var currentWord = 0L

  override protected def initializeCursor(columnBytes: AnyRef, cursor: Long,
      field: StructField): Long = {
    // read the count but its not used since CachedBatch has numRows
    ColumnEncoding.readInt(columnBytes, cursor)
    byteCursor = cursor + 4
    // return current bit index as the cursor so that is used and
    // incremented in the next call; the byte position will happen once
    // every 64 calls so that can be a member variable;
    // return max to force reading word in first nextBoolean call
    ColumnEncoding.BITS_PER_LONG
  }

  override final def nextBoolean(columnBytes: AnyRef, cursor: Long): Long = {
    var currentBitIndex = cursor
    currentBitIndex += 1
    if (currentBitIndex < ColumnEncoding.BITS_PER_LONG) currentBitIndex
    else {
      currentWord = ColumnEncoding.readLong(columnBytes, byteCursor)
      byteCursor += 8
      0L
    }
  }

  override final def readBoolean(columnBytes: AnyRef, cursor: Long): Boolean =
    ((currentWord >> cursor) & 1) != 0
}
