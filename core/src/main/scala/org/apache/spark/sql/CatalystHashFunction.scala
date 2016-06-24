
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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.util.DateTimeUtils

/**
 * This class acts as a public interface to the hashcode logic implemented at catalyst layer.
 * It ensures Spark's partitioning and store's partitioning follows the same logic.
 * This helps in reducing shuffle operations when Spark's DataFrame is joined with store data.
 * PairRDD's can also use this partitioner to colocate their data with Store tables/DataFrames.
 */
class CatalystHashFunction {

  def computeHash(key: Any, numPartitions : Int): Int = {
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
          case str: java.lang.String => computeHashCode(str)
          case timeStamp : java.sql.Timestamp => computeHashCode(timeStamp, numPartitions)
          case date : java.util.Date => computeHashCode(date, numPartitions)
          case other => other.hashCode()
        }
      }
    update
  }


  def computeHashCode(sd: java.util.Date, numPartitions : Int): Int = {
    computeHash(DateTimeUtils.millisToDays(sd.getTime), numPartitions)
  }

  def computeHashCode(time: java.sql.Timestamp, numPartitions : Int): Int = {
    val ht = DateTimeUtils.fromJavaTimestamp(time)
    computeHash(ht, numPartitions)
  }


  def computeHashCode(str: String): Int = {
    var result = 1
    val end = str.length

    def addToHash(value: Int) {
      result = 31 * result + value.toByte
    }

    var index = 0
    while (index <= end - 1) {
      val c: Char = str.charAt(index)
      if (c < 0x80) {
        addToHash(c)
      } else if (c < 0x800) {
        addToHash((c >> 6) | 0xc0)
        addToHash((c & 0x3f) | 0x80)
      } else if (Character.isSurrogate(c)) {
        val high: Char = c
        val low: Char = if (index + 1 != end) str.charAt(index + 1) else 0
        if (!Character.isSurrogatePair(high, low)) {
          throw new RuntimeException("The supplementary unicode is not in proper format")
        }
        // A valid surrogate pair. Get the supplementary code

        index = index + 1

        val sch = Character.toCodePoint(high, low)

        addToHash((sch >> 18) | 0xf0)
        addToHash(((sch >> 12) & 0x3f) | 0x80)
        addToHash(((sch >> 6) & 0x3f) | 0x80)
        addToHash((sch & 0x3f) | 0x80)
      }
      else {
        addToHash((c >> 12) | 0xe0)
        addToHash(((c >> 6) & 0x3f) | 0x80)
        addToHash((c & 0x3f) | 0x80)
      }
      index = index + 1
    }

    result
  }


  /**
   * This hashcode implementation matches that of Spark's hashcode implementation for rows.
   */
  def hashValue(key: Any, numPartitions: Int): Int = {
    //var result: Int = 37
    //val update = computeHash(key, numPartitions)
    //result = 37 * result + update
    //result
    computeHash(key, numPartitions)
  }

  /**
   * This hashcode implementation matches that of Spark's hashcode implementation for rows.
   */
  def hashValue(objs: scala.Array[Object], numPartitions: Int): Int = {
    var result: Int = 37
    var i = 0
    val len = objs.length
    while (i < len) {
      val update = computeHash(objs(i), numPartitions)
      result = 37 * result + update
      i += 1
    }
    result
  }

}
