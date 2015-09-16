package io.snappydata.util

/**
 * Created by pbm on 2015-04-24.
 */
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
