/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
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
 *
 * Some of the code taken from Spark's BitSetSuite having the below license.
 */
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.store

import io.snappydata.SnappyFunSuite

import org.apache.spark.sql.execution.columnar.encoding.BitSet
import org.apache.spark.unsafe.Platform

/**
 * Tests for the static methods of [[BitSet]].
 *
 * Some parts taken from Spark's BitSetSuite.
 */
class BitSetTest extends SnappyFunSuite {

  private val baseAddress = Platform.LONG_ARRAY_OFFSET

  private var bitsetSize = 0

  private def get(bitset: Array[Long], index: Int): Boolean =
    BitSet.isSet(bitset, baseAddress, index)

  private def set(bitset: Array[Long], index: Int): Unit =
    BitSet.set(bitset, baseAddress, index)

  private def clear(bitset: Array[Long], index: Int): Unit =
    BitSet.clear(bitset, baseAddress, index)

  private def anySet(bitset: Array[Long], index: Int): Boolean =
    BitSet.anySet(bitset, baseAddress + ((index + 7) >> 3),
      (((bitsetSize << 6) - index) + 63) >> 6)

  private def nextSetBit(bitset: Array[Long], index: Int): Int =
    BitSet.nextSetBit(bitset, baseAddress, index, bitsetSize)

  private def cardinality(bitset: Array[Long], index: Int): Int =
    BitSet.cardinality(bitset, baseAddress, index, bitsetSize)

  test("basic set, get and clear") {
    val maxSetBit = 96
    val setBits = Seq(0, 9, 1, 10, 90, maxSetBit)
    val bitset = new Array[Long](4)
    bitsetSize = 2

    for (i <- 0 until 100) {
      assert(!get(bitset, i))
    }

    setBits.foreach(i => set(bitset, i))

    for (i <- 0 until 100) {
      assert(get(bitset, i) === setBits.contains(i))
    }
    for (i <- 0 until 100) {
      assert(anySet(bitset, i) === (i <= maxSetBit), "failed for " + i)
    }

    // clear the bits and check after each clear
    for (i <- 0 until 100) {
      if (setBits.contains(i)) {
        clear(bitset, i)
      }
      for (j <- 0 until 100) {
        assert(get(bitset, j) === (j > i && setBits.contains(j)))
        assert(anySet(bitset, j) === (j <= maxSetBit && i < maxSetBit))
      }
    }

    for (i <- 0 until 100) {
      assert(!get(bitset, i))
      assert(!anySet(bitset, i))
    }

    setBits.foreach(i => clear(bitset, i))

    for (i <- 0 until 100) {
      assert(!get(bitset, i))
      assert(!anySet(bitset, i))
    }
  }

  test("100% full bit set then clear all") {
    val bitset = new Array[Long](200)
    bitsetSize = 157

    for (i <- 0 until 10000) {
      assert(!get(bitset, i))
      set(bitset, i)
    }
    for (i <- 0 until 10000) {
      assert(get(bitset, i))
    }
    // clear the bits and check after each clear
    for (i <- 0 until 10000) {
      clear(bitset, i)
      for (j <- 0 until 10000) {
        assert(get(bitset, j) === (j > i))
      }
    }
    for (i <- 0 until 10000) {
      assert(!get(bitset, i))
      assert(!anySet(bitset, i))
    }
  }

  test("nextSetBit") {
    val setBits = Seq(0, 9, 1, 10, 90, 96)
    val bitset = new Array[Long](4)
    bitsetSize = 2

    setBits.foreach(i => set(bitset, i))

    assert(nextSetBit(bitset, 0) === 0)
    assert(nextSetBit(bitset, 1) === 1)
    assert(nextSetBit(bitset, 2) === 9)
    assert(nextSetBit(bitset, 9) === 9)
    assert(nextSetBit(bitset, 10) === 10)
    assert(nextSetBit(bitset, 11) === 90)
    assert(nextSetBit(bitset, 80) === 90)
    assert(nextSetBit(bitset, 91) === 96)
    assert(nextSetBit(bitset, 96) === 96)
    assert(nextSetBit(bitset, 97) === Int.MaxValue)
  }

  test("cardinality") {
    val setBits = Seq(0, 9, 1, 10, 100, 90, 34, 108, 130, 127, 128, 96, 123, 180, 191)
    val bitset = new Array[Long](3)
    bitsetSize = 2

    setBits.foreach(set(bitset, _))

    assert(cardinality(bitset, 0) === 0)
    assert(cardinality(bitset, 1) === 1)
    assert(cardinality(bitset, 2) === 2)
    assert(cardinality(bitset, 9) === 2)
    assert(cardinality(bitset, 10) === 3)
    assert(cardinality(bitset, 11) === 4)
    assert(cardinality(bitset, 80) === 5)
    assert(cardinality(bitset, 91) === 6)
    assert(cardinality(bitset, 96) === 6)
    assert(cardinality(bitset, 97) === 7)
    assert(cardinality(bitset, 100) === 7)
    assert(cardinality(bitset, 101) === 8)
    assert(cardinality(bitset, 107) === 8)
    assert(cardinality(bitset, 108) === 8)
    assert(cardinality(bitset, 109) === 9)
    assert(cardinality(bitset, 123) === 9)
    assert(cardinality(bitset, 124) === 10)
    assert(cardinality(bitset, 127) === 10)
    assert(cardinality(bitset, 128) === 11)
    assert(cardinality(bitset, 130) === 11)
    assert(cardinality(bitset, 131) === 11)
    assert(cardinality(bitset, 150) === 11)
    assert(cardinality(bitset, 180) === 11)
    assert(cardinality(bitset, 181) === 11)
    assert(cardinality(bitset, 190) === 11)
    assert(cardinality(bitset, 191) === 11)
    assert(cardinality(bitset, 192) === 11)
    assert(cardinality(bitset, 193) === 11)
    assert(cardinality(bitset, 298989839) === 11)
  }
}
