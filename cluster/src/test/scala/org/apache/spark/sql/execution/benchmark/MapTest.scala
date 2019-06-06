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
 */
package org.apache.spark.sql.execution.benchmark

import scala.collection.mutable

import com.gemstone.gnu.trove.{THashMap, THashSet}
import io.snappydata.SnappyFunSuite
import it.unimi.dsi.fastutil.objects.{Object2ObjectOpenHashMap, ObjectOpenHashSet}
import org.eclipse.collections.impl.map.mutable.UnifiedMap
import org.eclipse.collections.impl.set.mutable.UnifiedSet

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

  ignore("hash set comparison") {
    val numEntries = 1000000
    val numOperations = 1000000
    val numIterations = 10

    val oset1 = new THashSet(numEntries, 0.7f)
    val omap2 = new java.util.HashMap[Item, Item](numEntries)
    var imap3: Map[Item, Item] = null
    val omap3 = new mutable.HashMap[Item, Item]
    val oset4 = new ObjectOpenHashSet[Item](numEntries)
    val oset5 = new UnifiedSet[Item](numEntries)

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

    var benchmark = new Benchmark("hashing mixed ops", numOperations)

    val results = new mutable.ArrayBuffer[Long]()

    addCaseWithCleanup(benchmark, "THashSet", numIterations,
      () => Unit, () => Unit, () => Unit, () => {
        oset1.clear()
        data.foreach(oset1.add)
      })(_ => {
      var sum = 0L
      for (op <- operations) {
        val item = op.operation match {
          case GET => oset1.get(op).asInstanceOf[Item]
          case INSERT =>
            oset1.add(op)
            op
          case DELETE => if (oset1.remove(op)) op else null
        }
        if (item ne null) {
          sum += item.l + item.s.length + item.d.toLong
        }
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "Java HashMap", numIterations,
      () => Unit, () => Unit, () => Unit, () => {
        omap2.clear()
        data.foreach(d => omap2.put(d, d))
      })(_ => {
      var sum = 0L
      for (op <- operations) {
        val item = op.operation match {
          case GET => omap2.get(op)
          case INSERT =>
            omap2.put(op, op)
            op
          case DELETE => omap2.remove(op)
        }
        if (item ne null) {
          sum += item.l + item.s.length + item.d.toLong
        }
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "FastUtil", numIterations,
      () => Unit, () => Unit, () => Unit, () => {
        oset4.clear()
        data.foreach(oset4.add)
      })(_ => {
      var sum = 0L
      for (op <- operations) {
        val item = op.operation match {
          case GET => oset4.get(op)
          case INSERT =>
            oset4.add(op)
            op
          case DELETE => if (oset4.remove(op)) op else null
        }
        if (item ne null) {
          sum += item.l + item.s.length + item.d.toLong
        }
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "Eclipse Collections", numIterations,
      () => Unit, () => Unit, () => Unit, () => {
        oset5.clear()
        data.foreach(oset5.add)
      })(_ => {
      var sum = 0L
      for (op <- operations) {
        val item = op.operation match {
          case GET => oset5.get(op)
          case INSERT =>
            oset5.add(op)
            op
          case DELETE => if (oset5.remove(op)) op else null
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

    benchmark = new Benchmark("hashing iteration", numEntries)
    results.clear()

    addCaseWithCleanup(benchmark, "THashSet", numIterations,
      () => data.foreach(oset1.add), () => Unit, () => Unit)(_ => {
      var sum = 0L
      val iter = oset1.iterator()
      while (iter.hasNext) {
        val item = iter.next().asInstanceOf[Item]
        sum += item.l + item.s.length + item.d.toLong
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "Java HashMap", numIterations,
      () => data.foreach(d => omap2.put(d, d)), omap2.clear, () => Unit)(_ => {
      var sum = 0L
      val iter = omap2.keySet().iterator()
      while (iter.hasNext) {
        val item = iter.next()
        sum += item.l + item.s.length + item.d.toLong
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "FastUtil", numIterations,
      () => data.foreach(oset4.add), oset4.clear, () => Unit)(_ => {
      var sum = 0L
      val iter = oset4.iterator()
      while (iter.hasNext) {
        val item = iter.next()
        sum += item.l + item.s.length + item.d.toLong
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "Eclipse Collections", numIterations,
      () => data.foreach(oset5.add), oset5.clear, () => Unit)(_ => {
      var sum = 0L
      val iter = oset5.iterator()
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

    benchmark = new Benchmark("hashing gets", numEntries)
    results.clear()

    addCaseWithCleanup(benchmark, "Scala Immutable HashMap", numIterations,
      () => {
        data.foreach(d => omap3.put(d, d))
        imap3 = omap3.toMap
        omap3.clear
      }, () => imap3 = null, () => Unit)(_ => {
      var sum = 0L
      var i = 0
      while (i < numEntries) {
        val item = imap3(data(i))
        sum += item.l + item.s.length + item.d.toLong
        i += 1
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "Scala HashMap", numIterations,
      () => data.foreach(d => omap3.put(d, d)), omap3.clear, () => Unit)(_ => {
      var sum = 0L
      var i = 0
      while (i < numEntries) {
        val item = omap3(data(i))
        sum += item.l + item.s.length + item.d.toLong
        i += 1
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "THashSet", numIterations,
      () => data.foreach(oset1.add), oset1.clear, () => Unit)(_ => {
      var sum = 0L
      var i = 0
      while (i < numEntries) {
        val item = oset1.get(data(i)).asInstanceOf[Item]
        sum += item.l + item.s.length + item.d.toLong
        i += 1
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "Java HashMap", numIterations,
      () => data.foreach(d => omap2.put(d, d)), omap2.clear, () => Unit)(_ => {
      var sum = 0L
      var i = 0
      while (i < numEntries) {
        val item = omap2.get(data(i))
        sum += item.l + item.s.length + item.d.toLong
        i += 1
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "FastUtil", numIterations,
      () => data.foreach(oset4.add), oset4.clear, () => Unit)(_ => {
      var sum = 0L
      var i = 0
      while (i < numEntries) {
        val item = oset4.get(data(i))
        sum += item.l + item.s.length + item.d.toLong
        i += 1
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "Eclipse Collections", numIterations,
      () => data.foreach(oset5.add), oset5.clear, () => Unit)(_ => {
      var sum = 0L
      var i = 0
      while (i < numEntries) {
        val item = oset5.get(data(i))
        sum += item.l + item.s.length + item.d.toLong
        i += 1
      }
      results += sum
    })

    benchmark.run()

    expected = results.head
    results.indices.foreach { index =>
      val r = results(index)
      assert(r === expected, s"Mismatch at index = $index")
    }

    benchmark = new Benchmark("hashing inserts", numEntries)
    results.clear()

    addCaseWithCleanup(benchmark, "THashSet", numIterations,
      oset1.clear, () => Unit, oset1.clear)(
      _ => data.foreach(oset1.add))
    addCaseWithCleanup(benchmark, "Java HashMap", numIterations,
      () => omap2.clear(), () => Unit, () => omap2.clear())(
      _ => data.foreach(d => omap2.put(d, d)))
    addCaseWithCleanup(benchmark, "FastUtil", numIterations,
      oset4.clear, () => Unit, oset4.clear)(
      _ => data.foreach(oset4.add))
    addCaseWithCleanup(benchmark, "Eclipse Collections", numIterations,
      oset5.clear, () => Unit, oset5.clear)(
      _ => data.foreach(oset5.add))

    benchmark.run()
  }

  ignore("hash map comparison") {
    val numEntries = 1000000
    val numOperations = 1000000
    val numIterations = 10

    val omap1 = new THashMap(numEntries, 0.7f)
    val omap2 = new java.util.HashMap[Item, Item](numEntries)
    var imap3: Map[Item, Item] = null
    val omap3 = new scala.collection.mutable.HashMap[Item, Item]()
    val omap4 = new Object2ObjectOpenHashMap[Item, Item](numEntries)
    val omap5 = new UnifiedMap[Item, Item](numEntries)

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

    var benchmark = new Benchmark("hashing mixed ops", numOperations)

    val results = new mutable.ArrayBuffer[Long]()

    addCaseWithCleanup(benchmark, "THashMap", numIterations,
      () => Unit, () => Unit, () => Unit, () => {
        omap1.clear()
        data.foreach(d => omap1.put(d, d))
      })(_ => {
      var sum = 0L
      for (op <- operations) {
        val item = op.operation match {
          case GET => omap1.get(op).asInstanceOf[Item]
          case INSERT =>
            omap1.put(op, op)
            op
          case DELETE => omap1.remove(op).asInstanceOf[Item]
        }
        if (item ne null) {
          sum += item.l + item.s.length + item.d.toLong
        }
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "Java HashMap", numIterations,
      () => Unit, () => Unit, () => Unit, () => {
        omap2.clear()
        data.foreach(d => omap2.put(d, d))
      })(_ => {
      var sum = 0L
      for (op <- operations) {
        val item = op.operation match {
          case GET => omap2.get(op)
          case INSERT =>
            omap2.put(op, op)
            op
          case DELETE => omap2.remove(op)
        }
        if (item ne null) {
          sum += item.l + item.s.length + item.d.toLong
        }
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "Scala HashMap", numIterations,
      () => Unit, () => Unit, () => Unit, () => {
        omap3.clear()
        data.foreach(d => omap3.put(d, d))
      })(_ => {
      var sum = 0L
      for (op <- operations) {
        val item = op.operation match {
          case GET => omap3.get(op).orNull
          case INSERT =>
            omap3.put(op, op)
            op
          case DELETE => omap3.remove(op).orNull
        }
        if (item ne null) {
          sum += item.l + item.s.length + item.d.toLong
        }
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "FastUtil", numIterations,
      () => Unit, () => Unit, () => Unit, () => {
        omap4.clear()
        data.foreach(d => omap4.put(d, d))
      })(_ => {
      var sum = 0L
      for (op <- operations) {
        val item = op.operation match {
          case GET => omap4.get(op)
          case INSERT =>
            omap4.put(op, op)
            op
          case DELETE => omap4.remove(op)
        }
        if (item ne null) {
          sum += item.l + item.s.length + item.d.toLong
        }
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "Eclipse Collections Map", numIterations,
      () => Unit, () => Unit, () => Unit, () => {
        omap5.clear()
        data.foreach(d => omap5.put(d, d))
      })(_ => {
      var sum = 0L
      for (op <- operations) {
        val item = op.operation match {
          case GET => omap5.get(op)
          case INSERT =>
            omap5.put(op, op)
            op
          case DELETE => omap5.remove(op)
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

    benchmark = new Benchmark("hashing iteration", numEntries)
    results.clear()

    addCaseWithCleanup(benchmark, "THashMap", numIterations,
      () => data.foreach(d => omap1.put(d, d)), omap1.clear, () => Unit)(_ => {
      var sum = 0L
      val iter = omap1.keySet().iterator()
      while (iter.hasNext) {
        val item = iter.next().asInstanceOf[Item]
        sum += item.l + item.s.length + item.d.toLong
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "Java HashMap", numIterations,
      () => data.foreach(d => omap2.put(d, d)), omap2.clear, () => Unit)(_ => {
      var sum = 0L
      val iter = omap2.keySet().iterator()
      while (iter.hasNext) {
        val item = iter.next()
        sum += item.l + item.s.length + item.d.toLong
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "Scala HashMap", numIterations,
      () => data.foreach(d => omap3.put(d, d)), omap3.clear, () => Unit)(_ => {
      results += omap3.keysIterator.foldLeft(0L)((sum, item) =>
        sum + item.l + item.s.length + item.d.toLong)
    })
    addCaseWithCleanup(benchmark, "FastUtil", numIterations,
      () => data.foreach(d => omap4.put(d, d)), omap4.clear, () => Unit)(_ => {
      var sum = 0L
      val iter = omap4.keySet().iterator()
      while (iter.hasNext) {
        val item = iter.next()
        sum += item.l + item.s.length + item.d.toLong
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "Eclipse Collections Map", numIterations,
      () => data.foreach(d => omap5.put(d, d)), omap5.clear, () => Unit)(_ => {
      var sum = 0L
      val iter = omap5.keySet().iterator()
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

    benchmark = new Benchmark("hashing gets", numEntries)
    results.clear()

    addCaseWithCleanup(benchmark, "Scala Immutable HashMap", numIterations,
      () => {
        data.foreach(d => omap3.put(d, d))
        imap3 = omap3.toMap
        omap3.clear
      }, () => imap3 = null, () => Unit)(_ => {
      var sum = 0L
      var i = 0
      while (i < numEntries) {
        val item = imap3(data(i))
        sum += item.l + item.s.length + item.d.toLong
        i += 1
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "Scala HashMap", numIterations,
      () => data.foreach(d => omap3.put(d, d)), omap3.clear, () => Unit)(_ => {
      var sum = 0L
      var i = 0
      while (i < numEntries) {
        val item = omap3(data(i))
        sum += item.l + item.s.length + item.d.toLong
        i += 1
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "THashMap", numIterations,
      () => data.foreach(d => omap1.put(d, d)), omap1.clear, () => Unit)(_ => {
      var sum = 0L
      var i = 0
      while (i < numEntries) {
        val item = omap1.get(data(i)).asInstanceOf[Item]
        sum += item.l + item.s.length + item.d.toLong
        i += 1
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "Java HashMap", numIterations,
      () => data.foreach(d => omap2.put(d, d)), omap2.clear, () => Unit)(_ => {
      var sum = 0L
      var i = 0
      while (i < numEntries) {
        val item = omap2.get(data(i))
        sum += item.l + item.s.length + item.d.toLong
        i += 1
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "FastUtil", numIterations,
      () => data.foreach(d => omap4.put(d, d)), omap4.clear, () => Unit)(_ => {
      var sum = 0L
      var i = 0
      while (i < numEntries) {
        val item = omap4.get(data(i))
        sum += item.l + item.s.length + item.d.toLong
        i += 1
      }
      results += sum
    })
    addCaseWithCleanup(benchmark, "Eclipse Collections Map", numIterations,
      () => data.foreach(d => omap5.put(d, d)), omap5.clear, () => Unit)(_ => {
      var sum = 0L
      var i = 0
      while (i < numEntries) {
        val item = omap5.get(data(i))
        sum += item.l + item.s.length + item.d.toLong
        i += 1
      }
      results += sum
    })

    benchmark.run()

    expected = results.head
    results.indices.foreach { index =>
      val r = results(index)
      assert(r === expected, s"Mismatch at index = $index")
    }

    benchmark = new Benchmark("hashing inserts", numEntries)
    results.clear()

    addCaseWithCleanup(benchmark, "THashMap", numIterations,
      omap1.clear, () => Unit, omap1.clear)(
      _ => data.foreach(d => omap1.put(d, d)))
    addCaseWithCleanup(benchmark, "Java HashMap", numIterations,
      omap2.clear, () => Unit, omap2.clear)(
      _ => data.foreach(d => omap2.put(d, d)))
    addCaseWithCleanup(benchmark, "Scala HashMap", numIterations,
      omap3.clear, () => Unit, omap3.clear)(
      _ => data.foreach(d => omap3.put(d, d)))
    addCaseWithCleanup(benchmark, "FastUtil", numIterations,
      omap4.clear, () => Unit, omap4.clear)(
      _ => data.foreach(d => omap4.put(d, d)))
    addCaseWithCleanup(benchmark, "Eclipse Collections Map", numIterations,
      omap5.clear, () => Unit, omap5.clear)(
      _ => data.foreach(d => omap5.put(d, d)))

    benchmark.run()
  }

  ignore("compare small map gets") {
    val numEntries = 20
    val numLoops = 1000000
    val numIterations = 10

    val omap1 = new THashMap(numEntries, 0.7f)
    val omap2 = new java.util.HashMap[String, String](numEntries)
    var imap3: Map[String, String] = null
    val omap3 = new scala.collection.mutable.HashMap[String, String]()
    val omap4 = new java.util.concurrent.ConcurrentHashMap[String, String](32, 0.7f, 1)
    val omap5 = new scala.collection.concurrent.TrieMap[String, String]
    val omap6 = new Object2ObjectOpenHashMap[String, String](numEntries)
    val omap7 = new UnifiedMap[String, String](numEntries)

    val rnd = new XORShiftRandom()
    val data = Array.fill(numEntries)(s"str${rnd.nextInt(100)}")

    val benchmark = new Benchmark("hashing gets", numEntries * numLoops)

    benchmark.addCase("Scala Immutable HashMap", numIterations,
      () => {
        data.foreach(d => omap3.put(d, d))
        imap3 = omap3.toMap
        omap3.clear
      }, () => imap3 = null)(_ => {
      var loop = 0
      while (loop < numLoops) {
        var i = 0
        while (i < numEntries) {
          assert(data(i) == imap3(data(i)))
          i += 1
        }
        loop += 1
      }
    })
    benchmark.addCase("Scala HashMap", numIterations,
      () => data.foreach(d => omap3.put(d, d)), omap3.clear)(_ => {
      var loop = 0
      while (loop < numLoops) {
        var i = 0
        while (i < numEntries) {
          assert(data(i) == omap3(data(i)))
          i += 1
        }
        loop += 1
      }
    })
    benchmark.addCase("THashMap", numIterations,
      () => data.foreach(d => omap1.put(d, d)), omap1.clear)(_ => {
      var loop = 0
      while (loop < numLoops) {
        var i = 0
        while (i < numEntries) {
          assert(data(i) == omap1.get(data(i)))
          i += 1
        }
        loop += 1
      }
    })
    benchmark.addCase("Java HashMap", numIterations,
      () => data.foreach(d => omap2.put(d, d)), omap2.clear)(_ => {
      var loop = 0
      while (loop < numLoops) {
        var i = 0
        while (i < numEntries) {
          assert(data(i) == omap2.get(data(i)))
          i += 1
        }
        loop += 1
      }
    })
    benchmark.addCase("Java ConcurrentHashMap", numIterations,
      () => data.foreach(d => omap4.put(d, d)), omap4.clear)(_ => {
      var loop = 0
      while (loop < numLoops) {
        var i = 0
        while (i < numEntries) {
          assert(data(i) == omap4.get(data(i)))
          i += 1
        }
        loop += 1
      }
    })
    benchmark.addCase("Scala TrieMap", numIterations,
      () => data.foreach(d => omap5.put(d, d)), omap5.clear)(_ => {
      var loop = 0
      while (loop < numLoops) {
        var i = 0
        while (i < numEntries) {
          assert(data(i) == omap5(data(i)))
          i += 1
        }
        loop += 1
      }
    })
    benchmark.addCase("FastUtil Map", numIterations,
      () => data.foreach(d => omap6.put(d, d)), omap6.clear)(_ => {
      var loop = 0
      while (loop < numLoops) {
        var i = 0
        while (i < numEntries) {
          assert(data(i) == omap6.get(data(i)))
          i += 1
        }
        loop += 1
      }
    })
    benchmark.addCase("Eclipse Collections Map", numIterations,
      () => data.foreach(d => omap7.put(d, d)), omap7.clear)(_ => {
      var loop = 0
      while (loop < numLoops) {
        var i = 0
        while (i < numEntries) {
          assert(data(i) == omap7.get(data(i)))
          i += 1
        }
        loop += 1
      }
    })

    benchmark.run()
  }
}

final case class Item(l: Long, s: String, d: Double) {
  var operation: Int = _

  override def hashCode(): Int = (l ^ (l >>> 32)).toInt

  override def toString: String = s"Item($l, $s, $d, op=$operation)"
}
