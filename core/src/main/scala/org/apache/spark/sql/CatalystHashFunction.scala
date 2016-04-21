
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

  def computeHash(key: Any): Int = {
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
          case other => other.hashCode()
        }
      }
    update
  }


  def hashJavaDate(sd: java.util.Date): Int = {
    computeHash(DateTimeUtils.millisToDays(sd.getTime))
  }

  def hashJavaSqlTimestamp(time: java.sql.Timestamp): Int = {
    val ht = DateTimeUtils.fromJavaTimestamp(time)
    computeHash(ht)
  }

  // This is a suboptimal code and neew to create an Object for hash computation. TODO SNAP-710
  def utfStringHashCode(a: Array[Byte]): Int = {
    var result = 1
    val numBytes = a.length

    var i: Int = 0
    while (i < numBytes) {
      {
        result = 31 * result + a(i)
      }

      i += 1;
    }
    result
  }


  /**
   * This hashcode implementation matches that of Spark's hashcode implementation for rows.
   */
  def hashValue(key: Any): Int = {
    var result: Int = 37
    val update = computeHash(key)
    result = 37 * result + update
    result
  }

  /**
   * This hashcode implementation matches that of Spark's hashcode implementation for rows.
   */
  def hashValue(objs: scala.Array[Object]): Int = {
    var result: Int = 37
    var i = 0
    val len = objs.length
    while (i < len) {
      val update = computeHash(objs(i))
      result = 37 * result + update
      i += 1
    }
    result
  }

}
