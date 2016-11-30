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

import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.types.{DataType, DateType, IntegerType, LongType, StringType, StructField, TimestampType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

final class DictionaryEncoding
    extends DictionaryEncodingBase with NotNullColumn

final class DictionaryEncodingNullable
    extends DictionaryEncodingBase with NullableColumn

abstract class DictionaryEncodingBase extends ColumnEncoding {

  override final def typeId: Int = 2

  override final def supports(dataType: DataType): Boolean = dataType match {
    case StringType | IntegerType | DateType | LongType | TimestampType => true
    case _ => false
  }

  private[this] final var stringDictionary: Array[UTF8String] = _
  private[this] final var intDictionary: Array[Int] = _
  private[this] final var longDictionary: Array[Long] = _

  override def initializeDecoding(columnBytes: AnyRef,
      field: StructField): Long = {
    var cursor = super.initializeDecoding(columnBytes, field)
    val elementNum = ColumnEncoding.readInt(columnBytes, cursor)
    cursor += 4
    Utils.getSQLDataType(field.dataType) match {
      case StringType =>
        stringDictionary = new Array[UTF8String](elementNum)
        (0 until elementNum).foreach { index =>
          val s = ColumnEncoding.readUTF8String(columnBytes, cursor)
          stringDictionary(index) = s
          cursor += (4 + s.numBytes())
        }
      case IntegerType | DateType =>
        intDictionary = new Array[Int](elementNum)
        (0 until elementNum).foreach { index =>
          intDictionary(index) = ColumnEncoding.readInt(columnBytes, cursor)
          cursor += 4
        }
      case LongType | TimestampType =>
        longDictionary = new Array[Long](elementNum)
        (0 until elementNum).foreach { index =>
          longDictionary(index) = ColumnEncoding.readLong(columnBytes, cursor)
          cursor += 8
        }
      case _ => throw new UnsupportedOperationException(
        s"DictionaryEncoding not supported for ${field.dataType}")
    }
    cursor - 2 // move cursor back so that first next call increments it
  }

  override final def nextUTF8String(columnBytes: AnyRef, cursor: Long): Long =
    cursor + 2

  override final def readUTF8String(columnBytes: AnyRef,
      cursor: Long): UTF8String =
    stringDictionary(ColumnEncoding.readShort(columnBytes, cursor))

  override def getStringDictionary: Array[UTF8String] =
    stringDictionary

  override def readDictionaryIndex(columnBytes: AnyRef, cursor: Long): Int =
    if (ColumnEncoding.littleEndian) {
      Platform.getShort(columnBytes, cursor)
    } else {
      java.lang.Short.reverseBytes(Platform.getShort(columnBytes, cursor))
    }

  override final def nextInt(columnBytes: AnyRef, cursor: Long): Long =
    cursor + 2

  override final def readInt(columnBytes: AnyRef, cursor: Long): Int =
    intDictionary(ColumnEncoding.readShort(columnBytes, cursor))

  override final def nextLong(columnBytes: AnyRef, cursor: Long): Long =
    cursor + 2

  override final def readLong(columnBytes: AnyRef, cursor: Long): Long =
    longDictionary(ColumnEncoding.readShort(columnBytes, cursor))
}
