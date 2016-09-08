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
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

final class DictionaryEncoding
    extends DictionaryEncodingBase with NotNullColumn {

  override def initializeDecoding(columnBytes: Array[Byte],
      field: Attribute): Unit = {
    super.initializeDecoding(columnBytes, field)
    initializeDecodingBase(columnBytes, field)
  }
}

final class DictionaryEncodingNullable
    extends DictionaryEncodingBase with NullableColumn {

  override def initializeDecoding(columnBytes: Array[Byte],
      field: Attribute): Unit = {
    super.initializeDecoding(columnBytes, field)
    initializeDecodingBase(columnBytes, field)
  }
}

abstract class DictionaryEncodingBase extends UncompressedBase {

  override final def typeId: Int = 2

  override final def supports(dataType: DataType): Boolean = dataType match {
    case StringType | IntegerType | LongType => true
    case _ => false
  }

  private[this] final var stringDictionary: Array[UTF8String] = _
  private[this] final var intDictionary: Array[Int] = _
  private[this] final var longDictionary: Array[Long] = _

  protected final def initializeDecodingBase(columnBytes: Array[Byte],
      field: Attribute): Unit = {
    val elementNum = super.readInt(columnBytes)
    cursor += 4
    field.dataType match {
      case StringType =>
        stringDictionary = new Array[UTF8String](elementNum)
        (0 until elementNum).foreach { index =>
          val s = super.readUTF8String(columnBytes)
          stringDictionary(index) = s
          cursor += (4 + s.numBytes())
        }
      case IntegerType =>
        intDictionary = new Array[Int](elementNum)
        (0 until elementNum).foreach { index =>
          intDictionary(index) = super.readInt(columnBytes)
          cursor += 4
        }
      case LongType =>
        longDictionary = new Array[Long](elementNum)
        (0 until elementNum).foreach { index =>
          longDictionary(index) = super.readLong(columnBytes)
          cursor += 8
        }
      case _ => throw new UnsupportedOperationException(
        s"DictionaryEncoding not supported for ${field.dataType}")
    }
    cursor -= 2 // move cursor back so that first next call increments it
  }

  override final def nextUTF8String(columnBytes: Array[Byte]): Unit =
    cursor += 2

  override final def readUTF8String(columnBytes: Array[Byte]): UTF8String =
    stringDictionary(super.readShort(columnBytes))

  override final def nextInt(columnBytes: Array[Byte]): Unit =
    cursor += 2

  override final def readInt(columnBytes: Array[Byte]): Int =
    intDictionary(super.readShort(columnBytes))

  override final def nextLong(columnBytes: Array[Byte]): Unit =
    cursor += 2

  override final def readLong(columnBytes: Array[Byte]): Long =
    longDictionary(super.readShort(columnBytes))
}
