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
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

/**
 * Wraps an `InternalRow` to expose a `Row`
 */
final class WrappedInternalRow(override val schema: StructType,
    val converters: Array[(InternalRow, Int) => Any]) extends Row {

  private var _internalRow: InternalRow = _
  private val cache = new Array[Any](schema.length)

  private[sql] def internalRow = _internalRow

  private[sql] def internalRow_=(row: InternalRow): Unit = {
    _internalRow = row
    val len = cache.length
    var i = 0
    while (i < len) {
      if (cache(i) != null) {
        cache(i) = null
      }
      i += 1
    }
  }

  override def length: Int = schema.length

  override def isNullAt(ordinal: Int): Boolean = _internalRow.isNullAt(ordinal)

  override def getBoolean(ordinal: Int) = _internalRow.getBoolean(ordinal)

  override def getByte(ordinal: Int) = _internalRow.getByte(ordinal)

  override def getShort(ordinal: Int) = _internalRow.getShort(ordinal)

  override def getInt(ordinal: Int) = _internalRow.getInt(ordinal)

  override def getLong(ordinal: Int) = _internalRow.getLong(ordinal)

  override def getFloat(ordinal: Int) = _internalRow.getFloat(ordinal)

  override def getDouble(ordinal: Int) = _internalRow.getDouble(ordinal)

  override def getString(ordinal: Int) = {
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

  override def get(ordinal: Int) = {
    val v = cache(ordinal)
    if (v == null) {
      cache(ordinal) = converters(ordinal)(_internalRow, ordinal)
    } else {
      v
    }
  }

  override def copy() = {
    val row = new WrappedInternalRow(schema, converters)
    row._internalRow = _internalRow
    row
  }
}

object WrappedInternalRow {

  def createConverters(schema: StructType) = schema.fields.map { f =>
    CatalystTypeConverters.createRowToScalaConverter(f.dataType)
  }
}
