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

package org.apache.spark.sql.collection

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.BaseGenericInternalRow
import org.apache.spark.sql.types.{DataType, StructField}

/**
 * Wraps a `Row` to expose an `InternalRow`
 */
final class WrappedRow(override val numFields: Int,
    val converters: Array[Any => Any]) extends BaseGenericInternalRow {

  private var _row: Row = _
  private val cache = new Array[Any](numFields)

  def this(schema: Array[StructField]) = this(schema.length, schema.map { f =>
    Utils.createCatalystConverter(f.dataType)
  })

  def row = _row

  def row_=(r: Row): Unit = {
    if (_row ne null) {
      val len = cache.length
      var i = 0
      while (i < len) {
        if (cache(i) != null) {
          cache(i) = null
        }
        i += 1
      }
    }
    _row = r
  }

  protected def genericGet(ordinal: Int): Any = {
    val v = cache(ordinal)
    if (v == null) {
      val s = converters(ordinal)(_row.get(ordinal))
      cache(ordinal) = s
      s
    } else {
      v
    }
  }

  override def isNullAt(ordinal: Int): Boolean = _row.isNullAt(ordinal)

  override def getBoolean(ordinal: Int) = _row.getBoolean(ordinal)

  override def getByte(ordinal: Int) = _row.getByte(ordinal)

  override def getShort(ordinal: Int) = _row.getShort(ordinal)

  override def getInt(ordinal: Int) = _row.getInt(ordinal)

  override def getLong(ordinal: Int) = _row.getLong(ordinal)

  override def getFloat(ordinal: Int) = _row.getFloat(ordinal)

  override def getDouble(ordinal: Int) = _row.getDouble(ordinal)

  override def get(ordinal: Int, dataType: DataType): AnyRef =
    genericGet(ordinal).asInstanceOf[AnyRef]

  /**
   * Make a copy of the current [[InternalRow]] object.
   */
  override def copy(): InternalRow = {
    val r = new WrappedRow(numFields, converters)
    r._row = _row
    r
  }
}
