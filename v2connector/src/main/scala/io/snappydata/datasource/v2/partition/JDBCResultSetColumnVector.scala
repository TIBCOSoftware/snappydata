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
package io.snappydata.datasource.v2.partition

import java.sql.ResultSet

import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarArray, ColumnarMap}
import org.apache.spark.unsafe.types.UTF8String


class JDBCResultSetColumnVector(dataType: DataType, rs: ResultSet,
    columnIndex: Int) extends ColumnVector(dataType) {

  override def close(): Unit = {}

  override def hasNull: Boolean = { rs.getObject(columnIndex) == null }

  override def numNulls(): Int = {
    if (rs.getObject(columnIndex) == null) {
      1
    } else {
      0
    }
  }

  override def isNullAt(rowId: Int): Boolean = {
    rs.getObject(columnIndex) == null
  }

  override def getBoolean(rowId: Int): Boolean = {
    rs.getBoolean(columnIndex)
  }

  override def getByte(rowId: Int): Byte = rs.getByte(columnIndex)


  override def getShort(rowId: Int): Short = rs.getByte(columnIndex)


  override def getInt(rowId: Int): Int = rs.getInt(columnIndex)

  override def getLong(rowId: Int): Long = rs.getLong(columnIndex)

  override def getFloat(rowId: Int): Float = rs.getFloat(columnIndex)

  override def getDouble(rowId: Int): Double = rs.getDouble(columnIndex)

  override def getArray(rowId: Int): ColumnarArray = {
    throw new IllegalStateException("Not implemented")
  }

  override def getMap(ordinal: Int): ColumnarMap =
    throw new IllegalStateException("Not implemented")

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = {
    val dec = rs.getBigDecimal(columnIndex)
    if (dec != null) {
      Decimal.apply(dec, precision, scale)
    } else {
      null
    }
  }

  override def getUTF8String(rowId: Int): UTF8String = {
    UTF8String.fromString(rs.getString(columnIndex))
  }

  override def getBinary(rowId: Int): Array[Byte] =
    throw new IllegalStateException("Not implemented")

  override def getChild(ordinal: Int): ColumnVector =
    throw new IllegalStateException("Not implemented")
}
