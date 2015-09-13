/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.collection

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/**
 * A `[[Row]]` implementation that can be reused
 * (much like `SpecificMutableRow` for `InternalRow`)
 */
final class ReusableRow(val values: Array[MutableValue]) extends Row {
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

  def this() = this(Seq.empty)

  override def length: Int = values.length

  def setNullAt(ordinal: Int): Unit = {
    values(ordinal).isNull = true
  }

  override def isNullAt(ordinal: Int): Boolean = values(ordinal).isNull

  override def copy(): Row = {
    val newValues = new Array[Any](values.length)
    var i = 0
    while (i < values.length) {
      newValues(i) = values(i).boxed
      i += 1
    }

    new GenericRow(newValues)
  }

  def update(ordinal: Int, value: Any) {
    if (value == null) {
      setNullAt(ordinal)
    } else {
      values(ordinal).update(value)
    }
  }

  def setInt(ordinal: Int, value: Int): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableInt]
    currentValue.isNull = false
    currentValue.value = value
  }

  override def getInt(ordinal: Int): Int = {
    values(ordinal).asInstanceOf[MutableInt].value
  }

  def setFloat(ordinal: Int, value: Float): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableFloat]
    currentValue.isNull = false
    currentValue.value = value
  }

  override def getFloat(ordinal: Int): Float = {
    values(ordinal).asInstanceOf[MutableFloat].value
  }

  def setBoolean(ordinal: Int, value: Boolean): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableBoolean]
    currentValue.isNull = false
    currentValue.value = value
  }

  override def getBoolean(ordinal: Int): Boolean = {
    values(ordinal).asInstanceOf[MutableBoolean].value
  }

  def setDouble(ordinal: Int, value: Double): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableDouble]
    currentValue.isNull = false
    currentValue.value = value
  }

  override def getDouble(ordinal: Int): Double = {
    values(ordinal).asInstanceOf[MutableDouble].value
  }

  def setShort(ordinal: Int, value: Short): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableShort]
    currentValue.isNull = false
    currentValue.value = value
  }

  override def getShort(ordinal: Int): Short = {
    values(ordinal).asInstanceOf[MutableShort].value
  }

  def setLong(ordinal: Int, value: Long): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableLong]
    currentValue.isNull = false
    currentValue.value = value
  }

  override def getLong(ordinal: Int): Long = {
    values(ordinal).asInstanceOf[MutableLong].value
  }

  def setByte(ordinal: Int, value: Byte): Unit = {
    val currentValue = values(ordinal).asInstanceOf[MutableByte]
    currentValue.isNull = false
    currentValue.value = value
  }

  override def getByte(ordinal: Int): Byte = {
    values(ordinal).asInstanceOf[MutableByte].value
  }

  override def get(ordinal: Int): Any = values(ordinal).boxed
}
