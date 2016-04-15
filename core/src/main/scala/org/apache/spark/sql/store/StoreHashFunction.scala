
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

import java.util.Calendar

import com.pivotal.gemfirexd.internal.iapi.types._

import org.apache.spark.sql.CatalystHashFunction


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
          //Custom type checks for Store
          case sb: SQLBoolean => if (sb.getBoolean) 0 else 1
          case sd: SQLDate => sd.getDate(Calendar.getInstance()).hashCode()
          case sd: SQLBit => java.util.Arrays.hashCode(sd.getBytes())
          case sf: SQLReal => java.lang.Float.floatToIntBits(sf.getFloat)
          case clob: SQLClob => hashClob(clob.getCharArray(true))
          case varchar: SQLVarchar => hashClob(varchar.getCharArray(true))
          case time: SQLTimestamp => time.getTimestamp(Calendar.getInstance()).hashCode()
          case decimal: SQLDecimal => decimal.getObject().hashCode()
          case other => other.hashCode()
        }
      }
    update
  }

  def hashClob(data: Array[Char]): Int = {
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
