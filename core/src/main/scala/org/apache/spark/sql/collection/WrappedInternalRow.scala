/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

/**
 * Wraps an `InternalRow` to expose a `Row`
 */
final class WrappedInternalRow(override val schema: StructType,
    columnConverters: Option[Array[Any => Any]] = None) extends Row {

  @transient private lazy val converters = columnConverters.getOrElse(
    schema.fields.map(f => Utils.createScalaConverter(f.dataType)))

  private var _internalRow: InternalRow = _
  @transient private lazy val cache = new Array[Any](schema.length)

  def this(internalRow: InternalRow, schema: StructType,
      columnConverters: Option[Array[Any => Any]]) = {
    this(schema, columnConverters)
    _internalRow = internalRow
  }

  def internalRow: InternalRow = _internalRow

  def internalRow_=(row: InternalRow): WrappedInternalRow = {
    if (_internalRow ne null) {
      val cache = this.cache
      val len = cache.length
      var i = 0
      while (i < len) {
        if (cache(i) != null) {
          cache(i) = null
        }
        i += 1
      }
    }
    _internalRow = row
    this
  }

  override def length: Int = schema.length

  override def isNullAt(ordinal: Int): Boolean = _internalRow.isNullAt(ordinal)

  override def getBoolean(ordinal: Int): Boolean =
    _internalRow.getBoolean(ordinal)

  override def getByte(ordinal: Int): Byte = _internalRow.getByte(ordinal)

  override def getShort(ordinal: Int): Short = _internalRow.getShort(ordinal)

  override def getInt(ordinal: Int): Int = _internalRow.getInt(ordinal)

  override def getLong(ordinal: Int): Long = _internalRow.getLong(ordinal)

  override def getFloat(ordinal: Int): Float = _internalRow.getFloat(ordinal)

  override def getDouble(ordinal: Int): Double = _internalRow.getDouble(ordinal)

  override def getString(ordinal: Int): String = {
    val v = cache(ordinal)
    if (v == null) {
      val s = _internalRow.getUTF8String(ordinal).toString
      cache(ordinal) = s
      s
    } else {
      v.asInstanceOf[String]
    }
  }

  def getUTF8String(ordinal: Int): UTF8String = {
    val v = cache(ordinal)
    if (v == null) {
      val s = _internalRow.getUTF8String(ordinal)
      cache(ordinal) = s
      s
    } else {
      v.asInstanceOf[UTF8String]
    }
  }

  override def get(ordinal: Int): Any = {
    val v = cache(ordinal)
    if (v == null) {
      val s = converters(ordinal)(_internalRow.get(ordinal,
        schema(ordinal).dataType))
      cache(ordinal) = s
      s
    } else {
      v
    }
  }

  override def copy(): WrappedInternalRow = {
    val row = new WrappedInternalRow(schema, Some(converters))
    row._internalRow = _internalRow
    row
  }
}
