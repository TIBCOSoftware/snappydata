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

package org.apache.spark.sql.store

import com.pivotal.gemfirexd.internal.iapi.types._
import com.pivotal.gemfirexd.internal.shared.common.ResolverUtils

import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types.Decimal
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.hash.Murmur3_x86_32
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

/**
 * A partitioner that helps store to collocate data with Spark's
 * partitions. Each store layer hash computation invokes this to
 * get their bucket information.
 */
final class StoreHashFunction {

  def computeColumnHash(key: Any, seed: Int): Int = {
    key match {
      case null => seed
      // Custom type checks for Store
      case char: SQLChar =>
        computeStringHash(char.getCharArray(true), seed)
      case i: SQLInteger => Murmur3_x86_32.hashInt(i.getInt, seed)
      case l: SQLLongint => Murmur3_x86_32.hashLong(l.getLong, seed)
      case d: SQLDouble => Murmur3_x86_32.hashLong(
        java.lang.Double.doubleToLongBits(d.getDouble), seed)
      case b: SQLBoolean => Murmur3_x86_32.hashInt(
        if (b.getBoolean) 1 else 0, seed)
      case b: SQLTinyint => Murmur3_x86_32.hashInt(b.getByte, seed)
      case s: SQLSmallint => Murmur3_x86_32.hashInt(s.getShort, seed)
      case f: SQLReal => Murmur3_x86_32.hashInt(
        java.lang.Float.floatToIntBits(f.getFloat), seed)
      case d: SQLDecimal =>
        // This is a suboptimal code and new to create an Object for
        // hash computation. TODO SNAP-711
        val bytes = d.getBigDecimal.unscaledValue().toByteArray
        Murmur3_x86_32.hashUnsafeBytes(bytes, Platform.BYTE_ARRAY_OFFSET,
          bytes.length, seed)
      case ts: SQLTimestamp =>
        val tsVal = ts.getEpochTime(null) * 1000L +
            (ts.getNanos.toLong / 1000L)
        Murmur3_x86_32.hashLong(tsVal, seed)
      case date: SQLDate =>
        val days = DateTimeUtils.millisToDays(date.getTimeInMillis(null))
        Murmur3_x86_32.hashInt(days, seed)
      case b: SQLBit =>
        val a = b.getBytes
        Murmur3_x86_32.hashUnsafeBytes(a, Platform.BYTE_ARRAY_OFFSET,
          a.length, seed)

      // raw object types
      case s: String =>
        val len = s.length
        val internalChars = ResolverUtils.getInternalCharsOnly(s, len)
        if (internalChars != null) computeStringHash(internalChars, seed)
        else computeStringHash(ResolverUtils.getChars(s, len), seed)
      case i: Int => Murmur3_x86_32.hashInt(i, seed)
      case l: Long => Murmur3_x86_32.hashLong(l, seed)
      case d: Double =>
        Murmur3_x86_32.hashLong(java.lang.Double.doubleToLongBits(d), seed)
      case b: Boolean => Murmur3_x86_32.hashInt(if (b) 1 else 0, seed)
      case b: Byte => Murmur3_x86_32.hashInt(b, seed)
      case s: Short => Murmur3_x86_32.hashInt(s, seed)
      case f: Float =>
        Murmur3_x86_32.hashInt(java.lang.Float.floatToIntBits(f), seed)
      case d: Decimal =>
        // This is a suboptimal code and new to create an Object for
        // hash computation. TODO SNAP-711
        val bytes = d.toJavaBigDecimal.unscaledValue().toByteArray
        Murmur3_x86_32.hashUnsafeBytes(bytes, Platform.BYTE_ARRAY_OFFSET,
          bytes.length, seed)
      case ts: java.sql.Timestamp =>
        val tsVal = DateTimeUtils.fromJavaTimestamp(ts)
        Murmur3_x86_32.hashLong(tsVal, seed)
      case date: java.util.Date =>
        val days = DateTimeUtils.millisToDays(date.getTime)
        Murmur3_x86_32.hashInt(days, seed)
      case a: Array[Byte] => Murmur3_x86_32.hashUnsafeBytes(a,
        Platform.BYTE_ARRAY_OFFSET, a.length, seed)
      case c: CalendarInterval => Murmur3_x86_32.hashInt(c.months,
        Murmur3_x86_32.hashLong(c.microseconds, seed))
      case s: UTF8String => Murmur3_x86_32.hashUnsafeBytes(s.getBaseObject,
        s.getBaseOffset, s.numBytes(), seed)

      case _ => throw new IllegalStateException("Unknown object of class " +
          s"${key.getClass.getCanonicalName}: $key")
    }
  }

  def computeHash(key: Any, numPartitions: Int): Int = {
    val hash = computeColumnHash(key, 42) % numPartitions
    if (hash >= 0) hash else hash + numPartitions
  }

  /**
   * This hashcode implementation matches that of Spark's hashcode
   * implementation for rows (HashExpression.eval).
   */
  def computeHash(objs: scala.Array[Object], numPartitions: Int): Int = {
    var result: Int = 42
    var i = 0
    val len = objs.length
    while (i < len) {
      result = computeColumnHash(objs(i), result)
      i += 1
    }
    val hash = result % numPartitions
    if (hash >= 0) hash else hash + numPartitions
  }

  private def computeStringHash(data: Array[Char], seed: Int): Int = {
    if (data == null) {
      return seed
    }

    var currentInt = 0
    var shift = 0
    var result = seed

    def addToHash(c: Int) {
      if (shift < 32) {
        currentInt |= (c << shift)
        shift += 8
      } else {
        result = mixH1(result, mixK1(currentInt))
        currentInt = c
        shift = 8
      }
    }

    val end = data.length
    var bytesLen = 0
    var index = 0
    while (index < end) {
      val c = data(index)
      if (c < 0x80) {
        if (shift < 32) {
          currentInt |= (c << shift)
          shift += 8
        } else {
          result = mixH1(result, mixK1(currentInt))
          currentInt = c
          shift = 8
        }
        bytesLen += 1
      } else if (c < 0x800) {
        if (shift < 24) {
          currentInt |= (((c >> 6) | 0xc0) << shift)
          shift += 8
          currentInt |= (((c & 0x3f) | 0x80) << shift)
          shift += 8
        } else {
          addToHash((c >> 6) | 0xc0)
          addToHash((c & 0x3f) | 0x80)
        }
        bytesLen += 2
      } else if (Character.isSurrogate(c)) {
        index += 1
        val low: Char = if (index != end) data(index) else 0
        if (!Character.isSurrogatePair(c, low)) {
          throw new IllegalStateException(
            "The supplementary unicode is not in proper format")
        }
        // A valid surrogate pair. Get the supplementary code

        val sch = Character.toCodePoint(c, low)
        // 4 Byte Int
        if (shift == 32) {
          result = mixH1(result, mixK1(currentInt))
          currentInt = (sch >> 18) | 0xf0
          currentInt |= ((((sch >> 12) & 0x3f) | 0x80) << 8)
          currentInt |= ((((sch >> 6) & 0x3f) | 0x80) << 16)
          currentInt |= (((sch & 0x3f) | 0x80) << 24)
        } else {
          addToHash((sch >> 18) | 0xf0)
          addToHash(((sch >> 12) & 0x3f) | 0x80)
          addToHash(((sch >> 6) & 0x3f) | 0x80)
          addToHash((sch & 0x3f) | 0x80)
        }
        bytesLen += 4
      } else {
        // 3 Byte Int
        addToHash((c >> 12) | 0xe0)
        addToHash(((c >> 6) & 0x3f) | 0x80)
        addToHash((c & 0x3f) | 0x80)
        bytesLen += 3
      }
      index += 1
    }

    if (shift == 32) {
      result = mixH1(result, mixK1(currentInt))
    } else {
      // mix each of the bytes
      var shift2 = 0
      while (shift2 < shift) {
        val k1 = (currentInt >>> shift2).toByte
        result = mixH1(result, mixK1(k1))
        shift2 += 8
      }
    }

    fmix(result, bytesLen)
  }

  private def mixK1(v: Int): Int = {
    var k1 = v
    k1 *= 0xcc9e2d51
    k1 = Integer.rotateLeft(k1, 15)
    k1 *= 0x1b873593
    k1
  }

  private def mixH1(v1: Int, v2: Int): Int = {
    var h1 = v1
    var k1 = v2
    h1 ^= k1
    h1 = Integer.rotateLeft(h1, 13)
    h1 = h1 * 5 + 0xe6546b64
    h1
  }

  // Finalization mix - force all bits of a hash block to avalanche
  private def fmix(v: Int, length: Int): Int = {
    var h1 = v
    h1 ^= length
    h1 ^= h1 >>> 16
    h1 *= 0x85ebca6b
    h1 ^= h1 >>> 13
    h1 *= 0xc2b2ae35
    h1 ^= h1 >>> 16
    h1
  }
}
