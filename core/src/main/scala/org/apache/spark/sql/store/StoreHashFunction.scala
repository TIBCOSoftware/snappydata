
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

package org.apache.spark.sql.store

import com.pivotal.gemfirexd.internal.iapi.types._

import org.apache.spark.sql.CatalystHashFunction
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.Decimal
import scala.util.control.Breaks._

import org.apache.spark.unsafe.hash.Murmur3_x86_32

/**
 * A partitioner that helps store to collocate data with Spark's
 * partitions. Each store layer hash computation invokes this to get their bucket information.
 */
class StoreHashFunction extends CatalystHashFunction {

  val seed = 42
  val hasher = new Murmur3_x86_32(seed)

  override def computeHash(key: Any,  numPartitions : Int): Int = {
    val update: Int =
      if (key == null) {
        0
      } else {
        key match {
          case b: Boolean => if (b) hasher.hashInt(1) else hasher.hashInt(0)
          case b: Byte => b.toInt
          case s: Short => s.toInt
          case i: Int => new Murmur3_x86_32(42).hashInt(i)
          case l: Long => (l ^ (l >>> 32)).toInt
          case f: Float => java.lang.Float.floatToIntBits(f)
          case d: Double =>
            val b = java.lang.Double.doubleToLongBits(d)
            (b ^ (b >>> 32)).toInt
          case a: Array[Byte] => java.util.Arrays.hashCode(a)
          case string: java.lang.String => computeHashCode(string)
          case timeStamp : java.sql.Timestamp => computeHashCode(timeStamp, numPartitions)
          case date : java.util.Date => computeHashCode(date, numPartitions)
          //Custom type checks for Store
          case boolean: SQLBoolean => computeHashCode(boolean, numPartitions)
          case sqlDate: SQLDate => computeHashCode(sqlDate, numPartitions)
          case sqlBit: SQLBit => {
            val bytes = sqlBit.getBytes()
            if(bytes == null ) 0 else java.util.Arrays.hashCode(bytes)
          }
          case sqlLong : SQLLongint => computeHashCode(sqlLong, numPartitions)
          case sqlReal: SQLReal => computeHashCode(sqlReal, numPartitions)
          case clob: SQLClob => computeHashCode(clob.getCharArray(true))
          case varchar: SQLVarchar => computeHashCode(varchar.getCharArray(true))
          case time: SQLTimestamp => computeHashCode(time, numPartitions)
          case decimal: SQLDecimal => computeHashCode(decimal, numPartitions)
          case sqlInteger :SQLInteger => computeHashCode(sqlInteger, numPartitions)
          case other => other.hashCode()
        }
      }
    update
  }

  private def pmod(a: Int, n: Int): Int = { //We should push this logic to store layer
    val r = a % n
    if (r < 0) r + n else a
  }


  private def computeHashCode(sqlInteger: SQLInteger, numPartitions : Int): Int = {
    pmod(hasher.hashInt(sqlInteger.getInt), numPartitions)
  }

  private def computeHashCode(sd: SQLDate, numPartitions : Int): Int = {
    pmod(hasher.hashInt(DateTimeUtils.millisToDays(sd.getTimeInMillis(null))), numPartitions)
  }

  private def computeHashCode(boolean: SQLBoolean, numPartitions : Int): Int = {
    val hasIntVal = if (boolean.getBoolean) hasher.hashInt(1) else hasher.hashInt(0)
    pmod(hasIntVal, numPartitions)
  }

  private def computeHashCode(sqlReal: SQLReal, numPartitions : Int): Int = {
    pmod(hasher.hashInt(java.lang.Float.floatToIntBits(sqlReal.getFloat)), numPartitions)
  }

  private def computeHashCode(longInt: SQLLongint, numPartitions : Int): Int = {
    pmod(hasher.hashLong(longInt.getLong), numPartitions)
  }

/*  private def computeHashCode(sqlBit: SQLBit, numPartitions : Int): Int = {
    val bytes = sqlBit.getBytes()
    var result = seed
    bytes.map( b => )
    if(bytes == null ) 0 else java.util.Arrays.hashCode(bytes)
  }*/

  // This is a suboptimal code and neew to create an Object for hash computation. TODO SNAP-711
  private def computeHashCode(decimal: SQLDecimal, numPartitions : Int): Int = {
    val javaBigDecimal = decimal.getObject()
    if (javaBigDecimal == null) 0 else computeHash(Decimal(javaBigDecimal.asInstanceOf[java.math
    .BigDecimal]), numPartitions)
  }

  /**
   * Returns the number of micros since epoch from java.sql.Timestamp.
   * Same code as CatalystConverter for timestamp .
   */
  private def computeHashCode(time: SQLTimestamp, numPartitions : Int): Int = {
    val ht = time.getEpochTime(null) * 1000L + (time.getNanos().toLong / 1000)
    computeHash(ht, numPartitions)
  }

  private def computeHashCode(data: Array[Char]): Int = {
    var result = 1
    val end = data.length
    def addToHash(value: Int) {
      result = 31 * result + value.toByte
    }

    var index = 0
    while (index <= end - 1) {
      {
        val c: Char = data(index)
        if (c < 0x80) {
          addToHash(c)
        } else if (c < 0x800) {
          addToHash((c >> 6) | 0xc0)
          addToHash((c & 0x3f) | 0x80)
        } else if (Character.isSurrogate(c)) {
          val high: Char = c
          val low: Char = if (index + 1 != end) data(index + 1) else 0
          if (!Character.isSurrogatePair(high, low)) {
            throw new RuntimeException("The supplementary unicode is not in proper format")
          }
          // A valid surrogate pair. Get the supplementary code

          index = index + 1

          val sch = Character.toCodePoint(high, low)
          //4 Byte Int
          addToHash((sch >> 18) | 0xf0)
          addToHash(((sch >> 12) & 0x3f) | 0x80)
          addToHash(((sch >> 6) & 0x3f) | 0x80)
          addToHash((sch & 0x3f) | 0x80)
        }
        else {
          //3 Byte Int
          addToHash((c >> 12) | 0xe0)
          addToHash(((c >> 6) & 0x3f) | 0x80)
          addToHash((c & 0x3f) | 0x80)
        }
        index = index + 1
      }
    }

    return result
  }
}
