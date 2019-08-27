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
package io.snappydata.util

object NumberUtils {
  /**
   * Returns the floor of the log base 2 of i
   * @param i The number to take log of
   * @return floor of log base 2 of i
   */
  def ilog2(i: Int): Int = {
    if (i <= 0) return 0
    var r = 0
    var vv = i >> 1
    while (vv != 0) {
      r += 1
      vv = vv >> 1
    }
    return r
  }

  /**
   * Tests if i is positive and a power of two
   * @param i
   * @return true iff i is a power of two
   */
  def isPowerOfTwo(i: Int): Boolean = {
    if (i < 0) return false
    return (i & (i - 1)) == 0
  }

  /*
   * If the number is a power of 2, return the power or else return -1 if it is not a power of 2
   */
  def isPowerOf2(num: Long): Int = {
    var numPower: Int = 0
    var x = num
    while (((x & 1) == 0) && x > 1) { /* While x is even and > 1 */
      x >>= 1;
      numPower += 1
    }
    if (x == 1) {
      numPower
    } else {
      -1
    }

  }

  def nearestPowerOf2LE(num: Int): Long =
    if (isPowerOfTwo(num)) {
      num
    } else {
      0x80000000 >>> Integer.numberOfLeadingZeros(num - 1)
    }

  def nearestPowerOf2GE(num: Int): Int =
    if (isPowerOfTwo(num)) {
      num
    } else {
      var v = num;
      v -= 1
      v |= v >> 1
      v |= v >> 2
      v |= v >> 4
      v |= v >> 8
      v |= v >> 16
      v += 1
      v
    }
}
