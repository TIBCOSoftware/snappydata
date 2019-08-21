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

package org.apache.spark.sql.collection

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/**
 * A `[[Row]]` implementation that can be reused
 * (much like `SpecificMutableRow` for `InternalRow`)
 */
class ReusableRow(val values: Array[MutableValue])
    extends Row with Serializable {
  def this(dataTypes: Seq[DataType]) =
    this(
      dataTypes.map {
        case BooleanType => new MutableBoolean
        case ByteType => new MutableByte
        case ShortType => new MutableShort
        // We use INT for DATE internally
        case IntegerType | DateType => new MutableInt
        // We use Long for Timestamp internally
        case LongType | TimestampType => new MutableLong
        case FloatType => new MutableFloat
        case DoubleType => new MutableDouble
        case _ => new MutableAny
      }.toArray)

  def this() = this(Nil)

  override final def length: Int = values.length

  final def setNullAt(ordinal: Int): Unit = {
    values(ordinal).isNull = true
  }

  override final def isNullAt(ordinal: Int): Boolean = values(ordinal).isNull

  override final def copy(): Row = {
    val newValues = new Array[Any](values.length)
    var i = 0
    while (i < values.length) {
      newValues(i) = values(i).boxed
      i += 1
    }

    new GenericRow(newValues)
  }

  final def update(ordinal: Int, value: Any) {
    if (value == null) {
      setNullAt(ordinal)
    } else {
      values(ordinal).update(value)
    }
  }

  final def setInt(ordinal: Int, value: Int): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableInt]
    currentValue.isNull = false
    currentValue.value = value
  }

  override final def getInt(ordinal: Int): Int = {
    values(ordinal).asInstanceOf[MutableInt].value
  }

  final def setFloat(ordinal: Int, value: Float): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableFloat]
    currentValue.isNull = false
    currentValue.value = value
  }

  override final def getFloat(ordinal: Int): Float = {
    values(ordinal).asInstanceOf[MutableFloat].value
  }

  final def setBoolean(ordinal: Int, value: Boolean): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableBoolean]
    currentValue.isNull = false
    currentValue.value = value
  }

  override final def getBoolean(ordinal: Int): Boolean = {
    values(ordinal).asInstanceOf[MutableBoolean].value
  }

  final def setDouble(ordinal: Int, value: Double): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableDouble]
    currentValue.isNull = false
    currentValue.value = value
  }

  override final def getDouble(ordinal: Int): Double = {
    values(ordinal).asInstanceOf[MutableDouble].value
  }

  final def setShort(ordinal: Int, value: Short): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableShort]
    currentValue.isNull = false
    currentValue.value = value
  }

  override final def getShort(ordinal: Int): Short = {
    values(ordinal).asInstanceOf[MutableShort].value
  }

  final def setLong(ordinal: Int, value: Long): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableLong]
    currentValue.isNull = false
    currentValue.value = value
  }

  override final def getLong(ordinal: Int): Long = {
    values(ordinal).asInstanceOf[MutableLong].value
  }

  final def setByte(ordinal: Int, value: Byte): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableByte]
    currentValue.isNull = false
    currentValue.value = value
  }

  override final def getByte(ordinal: Int): Byte = {
    values(ordinal).asInstanceOf[MutableByte].value
  }

  override final def get(ordinal: Int): Any = values(ordinal).boxed
}
