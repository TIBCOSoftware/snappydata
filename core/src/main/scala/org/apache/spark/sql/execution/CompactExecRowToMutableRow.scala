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

package org.apache.spark.sql.execution

import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.internal.shared.ClientSharedData
import com.pivotal.gemfirexd.internal.engine.store.{AbstractCompactExecRow, ResultWasNull}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types.{DataType, Decimal, DecimalType, StructType}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String

abstract class CompactExecRowToMutableRow extends ResultNullHolder {

  val schema: StructType

  protected final val dataTypes: ArrayBuffer[DataType] =
    new ArrayBuffer[DataType](schema.length)

  protected final val fieldTypes = StoreUtils.mapCatalystTypes(
    schema, dataTypes)

  final lazy val defaultCal = ClientSharedData.getDefaultCalendar

  final lazy val defaultTZ = defaultCal.getTimeZone

  protected final def createInternalRow(execRow: AbstractCompactExecRow,
      mutableRow: SpecificInternalRow): InternalRow = {
    var i = 0
    while (i < schema.length) {
      val pos = i + 1
      fieldTypes(i) match {
        case StoreUtils.STRING_TYPE =>
          // TODO: SW: change format in SQLChar to be full UTF8
          // and then use getAsBytes+UTF8String.fromBytes here
          val v = execRow.getAsString(pos, this)
          if (v != null) {
            mutableRow.update(i, UTF8String.fromString(v))
          } else {
            mutableRow.setNullAt(i)
            wasNull = false
          }
        case StoreUtils.INT_TYPE =>
          val v = execRow.getAsInt(pos, this)
          if (v != 0 || !wasNull) {
            mutableRow.setInt(i, v)
          } else {
            mutableRow.setNullAt(i)
            wasNull = false
          }
        case StoreUtils.LONG_TYPE =>
          val v = execRow.getAsLong(pos, this)
          if (v != 0L || !wasNull) {
            mutableRow.setLong(i, v)
          } else {
            mutableRow.setNullAt(i)
            wasNull = false
          }
        case StoreUtils.BINARY_LONG_TYPE =>
          val bytes = execRow.getAsBytes(pos, this)
          if (bytes != null) {
            var v = 0L
            var j = 0
            val numBytes = bytes.size
            while (j < numBytes) {
              v = 256 * v + (255 & bytes(j))
              j = j + 1
            }
            mutableRow.setLong(i, v)
          } else {
            mutableRow.setNullAt(i)
            wasNull = false
          }
        case StoreUtils.SHORT_TYPE =>
          val v = execRow.getAsShort(pos, this)
          if (v != 0 || !wasNull) {
            mutableRow.setShort(i, v)
          } else {
            mutableRow.setNullAt(i)
            wasNull = false
          }
        case StoreUtils.BYTE_TYPE =>
          val v = execRow.getAsByte(pos, this)
          if (v != 0 || !wasNull) {
            mutableRow.setByte(i, v)
          } else {
            mutableRow.setNullAt(i)
            wasNull = false
          }
        case StoreUtils.BOOLEAN_TYPE =>
          val v = execRow.getAsBoolean(pos, this)
          if (!wasNull) {
            mutableRow.setBoolean(i, v)
          } else {
            mutableRow.setNullAt(i)
            wasNull = false
          }
        case StoreUtils.DECIMAL_TYPE =>
          // When connecting with Oracle DB through JDBC, the precision and
          // scale of BigDecimal object returned by ResultSet.getBigDecimal
          // is not correctly matched to the table schema reported by
          // ResultSetMetaData.getPrecision and ResultSetMetaData.getScale.
          // If inserting values like 19999 into a column with NUMBER(12, 2)
          // type, you get through a BigDecimal object with scale as 0. But the
          // dataframe schema has correct type as DecimalType(12, 2).
          // Thus, after saving the dataframe into parquet file and then
          // retrieve it, you will get wrong result 199.99. So it is needed
          // to set precision and scale for Decimal based on JDBC metadata.
          val v = execRow.getAsBigDecimal(pos, this)
          if (v != null) {
            val d = schema.fields(i).dataType.asInstanceOf[DecimalType]
            mutableRow.update(i, Decimal(v, d.precision, d.scale))
          } else {
            mutableRow.setNullAt(i)
            wasNull = false
          }
        case StoreUtils.DOUBLE_TYPE =>
          val v = execRow.getAsDouble(pos, this)
          if (!wasNull) {
            mutableRow.setDouble(i, v)
          } else {
            mutableRow.setNullAt(i)
            wasNull = false
          }
        case StoreUtils.FLOAT_TYPE =>
          val v = execRow.getAsFloat(pos, this)
          if (!wasNull) {
            mutableRow.setFloat(i, v)
          } else {
            mutableRow.setNullAt(i)
            wasNull = false
          }
        case StoreUtils.DATE_TYPE =>
          val cal = this.defaultCal
          cal.clear()
          val millis = execRow.getAsDateMillis(i, cal, this)
          if (!wasNull) {
            mutableRow.setInt(i, Utils.millisToDays(millis, defaultTZ))
          } else {
            mutableRow.setNullAt(i)
            wasNull = false
          }
        case StoreUtils.TIMESTAMP_TYPE =>
          val cal = this.defaultCal
          cal.clear()
          val micros = execRow.getAsTimestampMicros(i, cal, this)
          if (!wasNull) {
            mutableRow.setLong(i, micros)
          } else {
            mutableRow.setNullAt(i)
            wasNull = false
          }
        case StoreUtils.BINARY_TYPE =>
          val v = execRow.getAsBytes(pos, this)
          if (v != null) {
            mutableRow.update(i, v)
          } else {
            mutableRow.setNullAt(i)
            wasNull = false
          }
        case StoreUtils.ARRAY_TYPE =>
          val v = execRow.getAsBytes(pos, this)
          if (v != null) {
            val array = new UnsafeArrayData
            array.pointTo(v, Platform.BYTE_ARRAY_OFFSET, v.length)
            mutableRow.update(i, array)
          } else {
            mutableRow.setNullAt(i)
            wasNull = false
          }
        case StoreUtils.MAP_TYPE =>
          val v = execRow.getAsBytes(pos, this)
          if (v != null) {
            val map = new UnsafeMapData
            map.pointTo(v, Platform.BYTE_ARRAY_OFFSET, v.length)
            mutableRow.update(i, map)
          } else {
            mutableRow.setNullAt(i)
            wasNull = false
          }
        case StoreUtils.STRUCT_TYPE =>
          val v = execRow.getAsBytes(pos, this)
          if (v != null) {
            val s = schema.fields(i).dataType.asInstanceOf[StructType]
            val row = new UnsafeRow(s.fields.length)
            row.pointTo(v, Platform.BYTE_ARRAY_OFFSET, v.length)
            mutableRow.update(i, row)
          } else {
            mutableRow.setNullAt(i)
            wasNull = false
          }
        case _ => throw new IllegalArgumentException(
          s"Unsupported field ${schema.fields(i)}")
      }
      i = i + 1
    }
    mutableRow
  }
}

class ResultNullHolder extends ResultWasNull {

  final var wasNull: Boolean = _

  override final def setWasNull(): Unit = {
    wasNull = true
  }

  final def wasNullAndClear(): Boolean = {
    val result = wasNull
    wasNull = false
    result
  }
}

final class ResultSetNullHolder extends ResultNullHolder {

  lazy val defaultCal = ClientSharedData.getDefaultCleanCalendar

  lazy val defaultTZ = defaultCal.getTimeZone
}
