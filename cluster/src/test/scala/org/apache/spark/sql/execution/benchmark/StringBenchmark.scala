/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

package org.apache.spark.sql.execution.benchmark

import java.util.UUID

import io.snappydata.SnappyFunSuite

import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Benchmark
import org.apache.spark.util.random.XORShiftRandom

/**
 * Comparisons for UTF8String optimizations.
 */
class StringBenchmark extends SnappyFunSuite {

  private def doGC(): Unit = {
    System.gc()
    System.runFinalization()
    System.gc()
    System.runFinalization()
  }

  private def runUTF8StringCompareTo(numElements: Int, numDistinct: Int,
      numIters: Int = 10, preSorted: Boolean = false): Unit = {
    val rnd = new XORShiftRandom

    def randomSuffix: String = {
      (1 to rnd.nextInt(6)).map(_ => rnd.nextInt(10)).mkString("")
    }

    val randData = Array.fill(numDistinct)(s"${UUID.randomUUID().toString}-$randomSuffix")
    val sdata = Array.fill(numElements)(randData(rnd.nextInt(numDistinct)))
    val data = sdata.map(UTF8String.fromString)

    if (preSorted) java.util.Arrays.sort(data, null)
    var cdata: Array[UTF8String] = null
    var cdata2: Array[UTF8String] = null

    def displayNumber(num: Int): String = {
      if (num % 1000000 == 0) s"${num / 1000000}M"
      else if (num % 1000 == 0) s"${num / 1000}K"
      else num.toString
    }

    val benchmark = new Benchmark(s"Sort${if (preSorted) "(pre-sorted)" else ""} " +
        s"num=${displayNumber(numElements)} distinct=${displayNumber(numDistinct)}", numElements)

    ColumnCacheBenchmark.addCaseWithCleanup(benchmark, "Spark", numIters, () => Unit,
      doGC, () => Unit, () => cdata = data.clone()) { _ =>
      java.util.Arrays.sort(cdata, new java.util.Comparator[UTF8String] {
        override def compare(o1: UTF8String, o2: UTF8String): Int = {
          StringBenchmark.sparkCompare(o1, o2)
        }
      })
    }
    ColumnCacheBenchmark.addCaseWithCleanup(benchmark, "Snappy", numIters, () => Unit,
      doGC, () => Unit, () => cdata2 = data.clone()) { _ =>
      java.util.Arrays.sort(cdata2, null)
    }

    benchmark.run()

    // compare the results
    assert(cdata.toSeq === cdata2.toSeq)
  }

  ignore("UTF8String optimized compareTo") {
    runUTF8StringCompareTo(1000000, 1000)
    runUTF8StringCompareTo(1000000, 1000000)
    runUTF8StringCompareTo(1000000, 1000, preSorted = true)
    runUTF8StringCompareTo(1000000, 1000000, preSorted = true)
  }

  ignore("UTF8String optimized contains") {
    val numElements = 1000000
    val numIters = 10
    val rnd = new XORShiftRandom

    def randomSuffix: String = {
      (1 to rnd.nextInt(30)).map(_ => rnd.nextInt(10)).mkString("")
    }

    val randData = Array.fill(numElements)(
      s"${UUID.randomUUID().toString}-${UUID.randomUUID().toString}-$randomSuffix")
    val data = randData.map(UTF8String.fromString)
    val search = "0123456789"
    val searchStr = UTF8String.fromString(search)
    val pattern = java.util.regex.Pattern.compile(search)

    val benchmark = new Benchmark("compare contains", numElements)

    benchmark.addCase("UTF8String (orig)", numIters) { _ =>
      var i = 0
      var matched = 0
      while (i < numElements) {
        if (StringBenchmark.sparkContains(data(i), searchStr)) {
          matched += 1
        }
        i += 1
      }
    }
    benchmark.addCase("UTF8String (opt)", numIters) { _ =>
      var i = 0
      var matched = 0
      while (i < numElements) {
        if (data(i).contains(searchStr)) {
          matched += 1
        }
        i += 1
      }
    }
    benchmark.addCase("String", numIters) { _ =>
      var i = 0
      var matched = 0
      while (i < numElements) {
        if (randData(i).contains(search)) {
          matched += 1
        }
        i += 1
      }
    }
    benchmark.addCase("Regex", numIters) { _ =>
      var i = 0
      var matched = 0
      while (i < numElements) {
        if (pattern.matcher(randData(i)).find(0)) {
          matched += 1
        }
        i += 1
      }
    }

    benchmark.run()
  }
}

object StringBenchmark {

  /**
   * This is the equivalent of original upstream Apache Spark UTF8String.compare
   * having the exact same performance profile (and byte code).
   */
  def sparkCompare(o1: UTF8String, o2: UTF8String): Int = {
    val len = Math.min(o1.numBytes(), o2.numBytes())
    var i = 0
    while (i < len) {
      val res = (Platform.getByte(o1.getBaseObject, o1.getBaseOffset + i) & 0xFF) -
          (Platform.getByte(o2.getBaseObject, o2.getBaseOffset + i) & 0xFF)
      if (res != 0) return res
      i += 1
    }
    o1.numBytes() - o2.numBytes()
  }

  /**
   * This is the equivalent of original upstream Apache Spark UTF8String.contains
   * having the exact same performance profile (and byte code).
   */
  def sparkContains(source: UTF8String, target: UTF8String): Boolean = {
    if (target.numBytes == 0) return true
    val first = target.getByte(0)
    var i = 0
    while (i <= source.numBytes - target.numBytes) {
      if (source.getByte(i) == first && matchAt(source, target, i)) return true
      i += 1
    }
    false
  }

  private def matchAt(source: UTF8String, target: UTF8String, pos: Int): Boolean = {
    if (target.numBytes + pos > source.numBytes || pos < 0) return false
    ByteArrayMethods.arrayEquals(source.getBaseObject, source.getBaseOffset + pos,
      target.getBaseObject, target.getBaseOffset, target.numBytes)
  }
}
