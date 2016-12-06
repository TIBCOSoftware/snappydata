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
package io.snappydata.util

/**
 * Singleton update/access into an array of longs treated as a bit set
 * (much like Spark's `BitSet` class but statically operated).
 */
object BitSet {

  /** Return the number of longs required to hold numBits. */
  def numWords(numBits: Int): Int = ((numBits - 1) >> 6) + 1

  /** Return true if no bit is set else false. */
  def isEmpty(words: Array[Long]): Boolean = {
    val numWords = words.length
    var i = 0
    while (i < numWords) {
      if (words(i) != 0L) return false
      i += 1
    }
    true
  }

  /** Return the number of bits set to true in this BitSet. */
  def cardinality(words: Array[Long]): Int = {
    var sum = 0
    var i = 0
    val numWords = words.length
    while (i < numWords) {
      sum += java.lang.Long.bitCount(words(i))
      i += 1
    }
    sum
  }

  /**
   * Sets the bit at the specified index to true.
   *
   * @param words the bitset array
   * @param index the bit index
   */
  def set(words: Array[Long], index: Int): Unit = {
    val bitmask = 1L << (index & 0x3f) // mod 64 and shift
    words(index >> 6) |= bitmask // div by 64 and mask
  }

  /**
   * Return the value of the bit with the specified index. The value is true
   * if the bit with the index is currently set in this BitSet;
   * otherwise, the result is false.
   *
   * @param words the bitset array
   * @param index the bit index
   * @return the value of the bit with the specified index
   */
  def get(words: Array[Long], index: Int): Boolean = {
    val bitmask = 1L << (index & 0x3f) // mod 64 and shift
    (words(index >> 6) & bitmask) != 0 // div by 64 and mask
  }

  /**
   * Returns the index of the first bit that is set to true that occurs on or
   * after the specified starting index.
   * If no such bit exists then -1 is returned.
   *
   * To iterate over the true bits in a BitSet, use the following loop:
   *
   * for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
   * // operate on index i here
   * }
   *
   * @param words     the bitset array
   * @param fromIndex the index to start checking from (inclusive)
   * @return the index of the next set bit, or -1 if there is no such bit
   */
  def nextSetBit(words: Array[Long], fromIndex: Int): Int = {
    val numWords = words.length
    var wordIndex = fromIndex >> 6
    if (wordIndex >= numWords) {
      return -1
    }

    // Try to find the next set bit in the current word
    val subIndex = fromIndex & 0x3f
    var word = words(wordIndex) >> subIndex
    if (word != 0) {
      return (wordIndex << 6) + subIndex +
          java.lang.Long.numberOfTrailingZeros(word)
    }

    // Find the next set bit in the rest of the words
    wordIndex += 1
    while (wordIndex < numWords) {
      word = words(wordIndex)
      if (word != 0) {
        return (wordIndex << 6) + java.lang.Long.numberOfTrailingZeros(word)
      }
      wordIndex += 1
    }

    -1
  }
}
