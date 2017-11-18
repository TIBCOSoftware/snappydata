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

import scala.collection.mutable

import com.gemstone.gemfire.internal.shared.OpenHashSet
import com.gemstone.gnu.trove.THashSet
import io.snappydata.SnappyFunSuite
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet

import org.apache.spark.sql.execution.benchmark.ColumnCacheBenchmark.addCaseWithCleanup
import org.apache.spark.util.Benchmark
import org.apache.spark.util.random.XORShiftRandom

/**
 * Some tests for basic map/set structures used by SnappyData and store.
 */
class MapTest extends SnappyFunSuite {
  private val GET = 1
  private val INSERT = 2
  private val DELETE = 3

  test("open hash set comparison") {
    val numEntries = 1000000
    val numOperations = 1000000
    val numIterations = 10

    val oset1 = new THashSet(numEntries, 0.6f)
    val oset2 = new ObjectOpenHashSet[Item](numEntries, 0.6f)
    val oset3 = new OpenHashSet[Item](numEntries, 0.6f)

    val hashingStrategy = oset3.getHashingStrategy

    val rnd = new XORShiftRandom()
    val data = Array.fill(numEntries)(Item(rnd.nextLong(),
      s"str${rnd.nextInt(100)}", rnd.nextDouble()))
    val dataI = Array.fill(numEntries)(Item(rnd.nextLong(),
      s"str${rnd.nextInt(100)}", rnd.nextDouble()))
    val operations = Array.fill(numOperations)(rnd.nextInt(10) match {
      case 0 | 1 | 2 | 3 | 4 | 5 => // get
        val index = rnd.nextInt(data.length + dataI.length)
        val item = if (index >= data.length) dataI(index - data.length)
        else data(index)
        item.operation = GET
        item
      case 6 | 7 | 8 => // insert
        val item = dataI(rnd.nextInt(dataI.length))
        item.operation = INSERT
        item
      case 9 => // delete
        val index = rnd.nextInt(data.length + dataI.length)
        val item = if (index >= data.length) dataI(index - data.length)
        else data(index)
        item.operation = DELETE
        item
    })

    var benchmark = new Benchmark("open hashing (mixed ops)", numOperations)

    val results = new mutable.ArrayBuffer[Long]()

    addCaseWithCleanup(benchmark, "THashSet", numIterations,
      () => Unit, () => Unit, () => Unit, () => {
        oset1.clear()
        data.foreach(oset1.add)
      })(_ => {
      var sum = 0L
      for (op <- operations) {
        val item = op.operation match {
          case GET =>
            oset1.get(op).asInstanceOf[Item]
          case INSERT =>
            oset1.add(op)
            op
          case DELETE =>
            if (oset1.remove(op)) op else null
        }
        if (item ne null) {
          sum += item.l + item.s.length + item.d.toLong
        }
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "FastUtil", numIterations,
      () => Unit, () => Unit, () => Unit, () => {
        oset2.clear()
        data.foreach(oset2.add)
      })(_ => {
      var sum = 0L
      for (op <- operations) {
        val item = op.operation match {
          case GET =>
            oset2.get(op)
          case INSERT =>
            oset2.add(op)
            op
          case DELETE =>
            if (oset2.remove(op)) op else null
        }
        if (item ne null) {
          sum += item.l + item.s.length + item.d.toLong
        }
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "Snappy OHS", numIterations,
      () => Unit, () => Unit, () => Unit, () => {
        oset3.clear()
        data.foreach(oset3.add)
      })(_ => {
      var sum = 0L
      for (op <- operations) {
        val item = op.operation match {
          case GET =>
            oset3.getKey(op, OpenHashSet.keyHash(op, hashingStrategy),
              hashingStrategy).asInstanceOf[Item]
          case INSERT =>
            oset3.add(op)
            op
          case DELETE =>
            oset3.removeKey(op, OpenHashSet.keyHash(op, hashingStrategy),
              hashingStrategy).asInstanceOf[Item]
        }
        if (item ne null) {
          sum += item.l + item.s.length + item.d.toLong
        }
      }
      results += sum
    })

    benchmark.run()

    var expected = results.head
    results.indices.foreach { index =>
      val r = results(index)
      assert(r === expected, s"Mismatch at index = $index")
    }

    benchmark = new Benchmark("open hashing (iteration)", numEntries)
    results.clear()

    addCaseWithCleanup(benchmark, "THashSet", numIterations,
      () => Unit, () => Unit, () => Unit)(_ => {
      var sum = 0L
      val iter = oset1.iterator()
      while (iter.hasNext) {
        val item = iter.next().asInstanceOf[Item]
        sum += item.l + item.s.length + item.d.toLong
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "FastUtil", numIterations,
      () => Unit, () => Unit, () => Unit)(_ => {
      var sum = 0L
      val iter = oset2.iterator()
      while (iter.hasNext) {
        val item = iter.next()
        sum += item.l + item.s.length + item.d.toLong
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "Snappy OHS", numIterations,
      () => Unit, () => Unit, () => Unit)(_ => {
      var sum = 0L
      val iter = oset3.iterator()
      while (iter.hasNext) {
        val item = iter.next()
        sum += item.l + item.s.length + item.d.toLong
      }
      results += sum
    })

    benchmark.run()

    expected = results.head
    results.indices.foreach { index =>
      val r = results(index)
      assert(r === expected, s"Mismatch at index = $index")
    }

    benchmark = new Benchmark("open hashing (insert)", numEntries)
    results.clear()

    addCaseWithCleanup(benchmark, "THashSet", numIterations,
      () => oset1.clear(), () => Unit, () => oset1.clear())(
      _ => data.foreach(oset1.add))
    addCaseWithCleanup(benchmark, "FastUtil", numIterations,
      () => oset2.clear(), () => Unit, () => oset2.clear())(
      _ => data.foreach(oset2.add))
    addCaseWithCleanup(benchmark, "Snappy OHS", numIterations,
      () => oset3.clear(), () => Unit, () => oset3.clear())(
      _ => data.foreach(oset3.add))

    benchmark.run()
  }
}

final case class Item(l: Long, s: String, d: Double) {
  var operation: Int = _

  override def hashCode(): Int = (l ^ (l >>> 32)).toInt

  override def toString: String = s"Item($l, $s, $d, op=$operation)"
}
