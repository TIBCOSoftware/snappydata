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

import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.{DataType, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

private[columnar] final class DictionaryEncoding[@specialized(Long,
  Int) T: ClassTag] extends DictionaryEncodingBase[T] with NotNullColumn {

  override def initializeDecoding(columnBytes: Array[Byte],
      field: Attribute): Unit = {
    super.initializeDecoding(columnBytes, field)
    initializeDecodingBase(columnBytes, field)
  }
}

private[columnar] final class DictionaryEncodingNullable[@specialized(Long,
  Int) T: ClassTag] extends DictionaryEncodingBase[T] with NullableColumn {

  override def initializeDecoding(columnBytes: Array[Byte],
      field: Attribute): Unit = {
    super.initializeDecoding(columnBytes, field)
    initializeDecodingBase(columnBytes, field)
  }
}

private[columnar] abstract class DictionaryEncodingBase[@specialized(Long,
  Int) T: ClassTag] extends UncompressedBase {

  override final def typeId: Int = 2

  override final def supports(dataType: DataType): Boolean = dataType match {
    case StringType | IntegerType | LongType => true
    case _ => false
  }

  private var dictionary: Array[T] = _

  protected final def initializeDecodingBase(columnBytes: Array[Byte],
      field: Attribute): Unit = {
    val elementNum = super.readInt(columnBytes)
    dictionary = new Array[T](elementNum)
    field.dataType match {
      case StringType =>
        val stringDictionary = dictionary.asInstanceOf[Array[UTF8String]]
        (0 until elementNum).foreach { index =>
          stringDictionary(index) = super.readUTF8String(columnBytes)
        }
      case IntegerType =>
        val intDictionary = dictionary.asInstanceOf[Array[Int]]
        (0 until elementNum).foreach { index =>
          intDictionary(index) = super.readInt(columnBytes)
        }
      case LongType =>
        val longDictionary = dictionary.asInstanceOf[Array[Long]]
        (0 until elementNum).foreach { index =>
          longDictionary(index) = super.readLong(columnBytes)
        }
      case _ => throw new UnsupportedOperationException(
        s"DictionaryEncoding not supported for ${field.dataType}")
    }
    cursor -= 2 // move cursor back so that first next call increments it
  }

  override final def readUTF8String(columnBytes: Array[Byte]): UTF8String = {
    val index = super.readShort(columnBytes)
    dictionary.asInstanceOf[Array[UTF8String]].apply(index)
  }

  override final def nextUTF8String(columnBytes: Array[Byte]): Unit =
    cursor += 2

  override final def readInt(columnBytes: Array[Byte]): Int = {
    val index = super.readShort(columnBytes)
    dictionary.asInstanceOf[Array[Int]].apply(index)
  }

  override final def nextInt(columnBytes: Array[Byte]): Unit =
    cursor += 2

  override final def readLong(columnBytes: Array[Byte]): Long = {
    val index = super.readShort(columnBytes)
    dictionary.asInstanceOf[Array[Long]].apply(index)
  }

  override final def nextLong(columnBytes: Array[Byte]): Unit =
    cursor += 2
}
