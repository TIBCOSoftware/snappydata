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

import org.apache.spark.sql.types.{DataType, DateType, IntegerType}
import org.apache.spark.unsafe.Platform

trait IntDeltaEncoding extends ColumnEncoding {

  override final def typeId: Int = 5

  override final def supports(dataType: DataType): Boolean = dataType match {
    case IntegerType | DateType => true
    case _ => false
  }
}

final class IntDeltaDecoder
    extends IntDeltaDecoderBase with NotNullDecoder

final class IntDeltaDecoderNullable
    extends IntDeltaDecoderBase with NullableDecoder

abstract class IntDeltaDecoderBase
    extends ColumnDecoder with IntDeltaEncoding {

  private[this] final var prev: Int = 0

  override protected[sql] def initializeCursor(columnBytes: AnyRef, cursor: Long,
      dataType: DataType): Long = cursor

  override final def nextInt(columnBytes: AnyRef, cursor: Long): Long = {
    val delta = Platform.getByte(columnBytes, cursor)
    if (delta > Byte.MinValue) {
      prev += delta
      cursor + 1
    } else {
      prev = ColumnEncoding.readInt(columnBytes, cursor + 1)
      cursor + 5
    }
  }

  override final def readInt(columnBytes: AnyRef, cursor: Long): Int = prev

  override final def readDate(columnBytes: AnyRef, cursor: Long): Int = prev
}
