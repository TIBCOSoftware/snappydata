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
package org.apache.spark.sql.sources

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

/**
 * Optimized cast for a column in a row to double.
 */
// TODO: also add code generation for this conversion
trait CastDouble {

  val doubleColumnType: DataType

  protected var castType: Int = _
  protected var numericCast: Numeric[Any] = _

  protected final def init(): Unit = {
    val columnType = doubleColumnType
    castType = columnType match {
      case IntegerType => 0
      case DoubleType => 1
      case LongType => 2
      case FloatType => 3
      case ShortType => 4
      case ByteType => 5
      case d: DecimalType => 6
      case x: NumericType => 7
      case other => sys.error(s"Type $other does not support cast to double")
    }
    numericCast = doubleColumnType match {
      case x: NumericType => x.numeric.asInstanceOf[Numeric[Any]]
      case _ => null
    }
  }

  final def toDouble(row: Row, ordinal: Int, default: Double): Double = {
    castType match {
      case 0 =>
        if (!row.isNullAt(ordinal)) row.getInt(ordinal) else default
      case 1 =>
        if (!row.isNullAt(ordinal)) row.getDouble(ordinal) else default
      case 2 =>
        if (!row.isNullAt(ordinal)) row.getLong(ordinal) else default
      case 3 =>
        if (!row.isNullAt(ordinal)) row.getFloat(ordinal) else default
      case 4 =>
        if (!row.isNullAt(ordinal)) row.getShort(ordinal) else default
      case 5 =>
        if (!row.isNullAt(ordinal)) row.getByte(ordinal) else default
      case 6 =>
        val v = row.get(ordinal)
        if (v != null) v.asInstanceOf[Decimal].toDouble else default
      case 7 =>
        val v = row.get(ordinal)
        if (v != null) numericCast.toDouble(v) else default
    }
  }

  final def toDouble(row: InternalRow, ordinal: Int,
      default: Double): Double = {
    castType match {
      case 0 =>
        if (!row.isNullAt(ordinal)) row.getInt(ordinal) else default
      case 1 =>
        if (!row.isNullAt(ordinal)) row.getDouble(ordinal) else default
      case 2 =>
        if (!row.isNullAt(ordinal)) row.getLong(ordinal) else default
      case 3 =>
        if (!row.isNullAt(ordinal)) row.getFloat(ordinal) else default
      case 4 =>
        if (!row.isNullAt(ordinal)) row.getShort(ordinal) else default
      case 5 =>
        if (!row.isNullAt(ordinal)) row.getByte(ordinal) else default
      case 6 =>
        val v = row.get(ordinal, doubleColumnType)
        if (v != null) v.asInstanceOf[Decimal].toDouble else default
      case 7 =>
        val v = row.get(ordinal, doubleColumnType)
        if (v != null) numericCast.toDouble(v) else default
    }
  }

  /** cast a non-null value to double */
  final def toDouble(v: Any): Double = {
    castType match {
      case 0 => v.asInstanceOf[Int]
      case 1 => v.asInstanceOf[Double]
      case 2 => v.asInstanceOf[Long]
      case 3 => v.asInstanceOf[Float]
      case 4 => v.asInstanceOf[Short]
      case 5 => v.asInstanceOf[Byte]
      case 6 => v.asInstanceOf[Decimal].toDouble
      case 7 => numericCast.toDouble(v)
    }
  }
}
