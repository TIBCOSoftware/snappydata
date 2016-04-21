
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

/**
 * A partitioner that helps store to collocate data with Spark's
 * partitions. Each store layer hash computation invokes this to get their bucket information.
 */
class StoreHashFunction extends CatalystHashFunction {

  override def computeHash(key: Any): Int = {
    val update: Int =
      if (key == null) {
        0
      } else {
        key match {
          case b: Boolean => if (b) 0 else 1
          case b: Byte => b.toInt
          case s: Short => s.toInt
          case i: Int => i
          case l: Long => (l ^ (l >>> 32)).toInt
          case f: Float => java.lang.Float.floatToIntBits(f)
          case d: Double =>
            val b = java.lang.Double.doubleToLongBits(d)
            (b ^ (b >>> 32)).toInt
          case a: Array[Byte] => java.util.Arrays.hashCode(a)
          case str: java.lang.String => utfStringHashCode(str.getBytes("utf-8"))
          case timeStamp : java.sql.Timestamp => hashJavaSqlTimestamp(timeStamp)
          case date : java.util.Date => hashJavaDate(date)
          //Custom type checks for Store
          case sb: SQLBoolean => if (sb.getBoolean) 0 else 1
          case sd: SQLDate => hashSQLDate(sd)
          case sd: SQLBit => {
            val bytes = sd.getBytes()
            if(bytes == null ) 0 else java.util.Arrays.hashCode(bytes)
          }
          case sf: SQLReal => java.lang.Float.floatToIntBits(sf.getFloat)
          case clob: SQLClob => hashClob(clob.getCharArray(true))
          case varchar: SQLVarchar => hashClob(varchar.getCharArray(true))
          case time: SQLTimestamp => hashSQLTimestamp(time)
          case decimal: SQLDecimal => hashSQLDecimal(decimal)
          case other => other.hashCode()
        }
      }
    update
  }


  private def hashSQLDate(sd: SQLDate): Int = {
    computeHash(DateTimeUtils.millisToDays(sd.getTimeInMillis(null)))
  }

  // This is a suboptimal code and neew to create an Object for hash computation. TODO SNAP-711
  private def hashSQLDecimal(decimal: SQLDecimal): Int = {
    val javaBigDecimal = decimal.getObject()
    if (javaBigDecimal == null) 0 else computeHash(Decimal(javaBigDecimal.asInstanceOf[java.math
    .BigDecimal]))
  }

  /**
   * Returns the number of micros since epoch from java.sql.Timestamp.
   * Same code as CatalystConverter for timestamp .
   */
  private def hashSQLTimestamp(time: SQLTimestamp): Int = {
    val ht = time.getEpochTime(null) * 1000L + (time.getNanos().toLong / 1000)
    computeHash(ht)
  }

  private def hashClob(data: Array[Char]): Int = {
    var result = 1
    var b = 0.toByte
    for (index <- 0 to data.length - 1) {
      {
        val c: Int = data(index)
        if ((c >= 0x0001) && (c <= 0x007F)) {
          b = (c & 0xFF).toByte
          result = 31 * result + b
        }
        else if (c > 0x07FF) {
          b = (0xE0 | ((c >> 12) & 0x0F)).toByte
          result = 31 * result + b
          b = (0x80 | ((c >> 6) & 0x3F)).toByte
          result = 31 * result + b
          b = (0x80 | ((c >> 0) & 0x3F)).toByte
          result = 31 * result + b
        }
        else {
          b = (0xC0 | ((c >> 6) & 0x1F)).toByte
          result = 31 * result + b
          b = (0x80 | ((c >> 0) & 0x3F)).toByte
          result = 31 * result + b
        }
      }
    }
    return result
  }

}
