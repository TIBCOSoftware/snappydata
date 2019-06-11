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

package org.apache.spark.sql.collection

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.{Lock, ReentrantReadWriteLock}

import scala.collection.{GenTraversableOnce, mutable}
import scala.reflect.ClassTag
import scala.util.Random

private[sql] class ConcurrentSegmentedHashMap[K, V, M <: SegmentMap[K, V] : ClassTag](
    private val initialSize: Int,
    val loadFactor: Double,
    val concurrency: Int,
    val segmentCreator: (Int, Double, Int, Int) => M,
    val hasher: K => Int) extends Serializable {

  /** maximum size of batches in bulk insert API */
  private[this] final val MAX_BULK_INSERT_SIZE = 256

  /**
   * A default constructor creates a concurrent hash map with initial size `32`
   * and concurrency `16`.
   */
  def this(concurrency: Int, segmentCreator: (Int, Double, Int, Int) => M,
      hasher: K => Int) =
    this(32, SegmentMap.DEFAULT_LOAD_FACTOR, concurrency,
      segmentCreator, hasher)

  require(initialSize > 0,
    s"ConcurrentSegmentedHashMap: unexpected initialSize=$initialSize")
  require(loadFactor > 0.0 && loadFactor < 1.0,
    s"ConcurrentSegmentedHashMap: unexpected loadFactor=$loadFactor")
  require(concurrency > 0,
    s"ConcurrentSegmentedHashMap: unexpected concurrency=$concurrency")
  require(segmentCreator != null,
    "ConcurrentSegmentedHashMap: null segmentCreator")

  private def initSegmentCapacity(nsegs: Int) =
    math.max(2, SegmentMap.nextPowerOf2(initialSize / nsegs))

  private val _segments: Array[M] = {
    val nsegs = math.min(concurrency, 1 << 16)
    val segs = new Array[M](nsegs)
    // calculate the initial capacity of each segment
    segs.indices.foreach(i => {
      segs(i) = segmentCreator(initSegmentCapacity(nsegs),
        loadFactor, i, nsegs)
    })
    segs
  }
  private val _size = new AtomicLong(0)

  private val (_segmentShift, _segmentMask) = {
    var sshift = 0
    var ssize = 1
    val concurrency = _segments.length
    if (concurrency > 1) {
      while (ssize < concurrency) {
        sshift += 1
        ssize <<= 1
      }
    }
    (32 - sshift, ssize - 1)
  }

  private final def segmentFor(hash: Int): M = {
    _segments((hash >>> _segmentShift) & _segmentMask)
  }

  final def contains(k: K): Boolean = {
    val hasher = this.hasher
    val hash = if (hasher != null) hasher(k) else k.##
    val seg = segmentFor(hash)

    val lock = seg.readLock
    lock.lock()
    try {
      seg.contains(k, hash)
    } finally {
      lock.unlock()
    }
  }

  final def apply(k: K): V = {
    val hasher = this.hasher
    val hash = if (hasher != null) hasher(k) else k.##
    val seg = segmentFor(hash)

    val lock = seg.readLock
    lock.lock()
    try {
      seg(k, hash)
    } finally {
      lock.unlock()
    }
  }

  final def get(k: K): Option[V] = {
    val hasher = this.hasher
    val hash = if (hasher != null) hasher(k) else k.##
    val seg = segmentFor(hash)

    val lock = seg.readLock
    lock.lock()
    try {
      Option(seg(k, hash))
    } finally {
      lock.unlock()
    }
  }

  final def update(k: K, v: V): Boolean = {
    val hasher = this.hasher
    val hash = if (hasher != null) hasher(k) else k.##
    val seg = segmentFor(hash)

    val lock = seg.writeLock
    var added = false
    lock.lock()
    try {
      added = seg.update(k, hash, v)
    } finally {
      lock.unlock()
    }
    if (added) {
      _size.incrementAndGet()
      true
    } else false
  }

  final def changeValue(k: K, change: ChangeValue[K, V]): java.lang.Boolean = {
    val hasher = this.hasher
    val hash = if (hasher != null) hasher(k) else k.##
    val seg = segmentFor(hash)

    val lock = seg.writeLock
    var added: java.lang.Boolean = null
    lock.lock()
    try {
      added = seg.changeValue(k, hash, change, isLocal = true)
    } finally {
      var beforeResult: AnyRef = null
      try {
        beforeResult = seg.beforeSegmentEnd()
      } finally {
        lock.unlock()
        seg.segmentEnd(beforeResult)
      }
    }
    if (added != null && added.booleanValue()) _size.incrementAndGet()

    added
  }

  final def bulkChangeValues(ks: Iterator[K], change: ChangeValue[K, V], bucketId: (Int) => Int,
      isLocal: Boolean) : Long = {
    val segs = this._segments
    val segShift = _segmentShift
    val segMask = _segmentMask
    val hasher = this.hasher

    // first group keys by segments
    val nsegs = segs.length
    val nsegsRange = 0 until nsegs
    val groupedKeys = new Array[mutable.ArrayBuffer[K]](nsegs)
    val groupedHashes = new Array[mutable.ArrayBuilder.ofInt](nsegs)
    var numAdded = 0

    def getLockedValidSegmentAndLock(i: Int): (M, ReentrantReadWriteLock.WriteLock) = {
      var seg = segs(i)
      var lock = seg.writeLock
      lock.lock()
      while (!seg.valid) {
        lock.unlock()
        seg = segs(i)
        lock = seg.writeLock
        lock.lock()
      }
      (seg, lock)
    }


    def addNumToSize(): Unit = {
      if (numAdded > 0) {
        _size.addAndGet(numAdded)
        numAdded = 0
      }
    }

    var rowCount = 0
    // split into max batch sizes to avoid buffering up too much
    val iter = new SlicedIterator[K](ks, 0, MAX_BULK_INSERT_SIZE)
    while (iter.hasNext) {
      iter.foreach { k =>
        val hash = if (hasher != null) hasher(k) else k.##
        val segIndex = (hash >>> segShift) & segMask
        val buffer = groupedKeys(segIndex)
        if (buffer != null) {
          buffer += change.keyCopy(k)
          groupedHashes(segIndex) += hash
        } else {
          val newBuffer = new mutable.ArrayBuffer[K](4)
          val newHashBuffer = new mutable.ArrayBuilder.ofInt()
          newHashBuffer.sizeHint(4)
          newBuffer += change.keyCopy(k)
          newHashBuffer += hash
          groupedKeys(segIndex) = newBuffer
          groupedHashes(segIndex) = newHashBuffer
        }
        rowCount += 1
      }

      var lockedState = false
      // now lock segments one by one and then apply changes for all keys
      // of the locked segment
      // shuffle the indexes to minimize segment thread contention
      Random.shuffle[Int, IndexedSeq](nsegsRange).foreach { i =>
        val keys = groupedKeys(i)
        if (keys != null) {
          val hashes = groupedHashes(i).result()
          val nhashes = hashes.length
          var (seg, lock) = getLockedValidSegmentAndLock(i)
          lockedState = true
          try {
            var added: java.lang.Boolean = null
            var idx = 0
            while (idx < nhashes) {
              added = seg.changeValue(keys(idx), bucketId(hashes(idx)), change, isLocal)
              if (added != null) {
                if (added.booleanValue()) {
                  numAdded += 1
                }
                idx += 1
              } else {
                // indicates that loop must be broken immediately
                // need to take the latest reference of segmnet
                // after segmnetAbort is successful
                addNumToSize()
                lock.unlock()
                lockedState = false
                // Because two threads can concurrently call segmentAbort
                // & is since locks are released, there is no guarantee that
                // one thread would correctly identify if the other has cleared
                // the segments. So after the changeSegment, it should unconditionally
                // refresh the segments
                change.segmentAbort(seg)
                val segmentAndLock = getLockedValidSegmentAndLock(i)
                lockedState = true
                seg = segmentAndLock._1
                lock = segmentAndLock._2
                idx += 1
              }
            }
          } finally {
            var beforeResult: AnyRef = null
            try {
              beforeResult = seg.beforeSegmentEnd()
            } finally {
              if (lockedState) {
                addNumToSize()
                lock.unlock()
              }
              // invoke the segmentEnd method outside of the segment lock
              seg.segmentEnd(beforeResult)
            }
          }
        }
      }
      // pick up another set of keys+values
      iter.setSlice(0, MAX_BULK_INSERT_SIZE)
      for (b <- groupedKeys) if (b != null) b.clear()
      for (b <- groupedHashes) if (b != null) b.clear()
    }

    rowCount
  }

  def foldSegments[U](init: U)(f: (U, M) => U): U = _segments.foldLeft(init)(f)

  def foldSegments[U](start: Int, end: Int, init: U)(f: (U, M) => U): U = {
    val segments = _segments
    (start until end).foldLeft(init)((itr, i) => f(itr, segments(i)))
  }

  /**
   * No synchronization in this method so use with care.
   * Use it only if you know what you are doing.
   */
  def flatMap[U](f: M => GenTraversableOnce[U]): Iterator[U] =
    _segments.iterator.flatMap(f)

  def foldValuesRead[U](init: U, f: (Int, V, U) => U): U = {
    _segments.foldLeft(init) { (v, seg) =>
      SegmentMap.lock(seg.readLock()) {
        seg.foldValues(v, f)
      }
    }
  }

  def foldEntriesRead[U](init: U, copyIfRequired: Boolean,
      f: (K, V, U) => U): U = {
    _segments.foldLeft(init) { (v, seg) =>
      SegmentMap.lock(seg.readLock()) {
        seg.foldEntries(v, copyIfRequired, f)
      }
    }
  }

  def writeLockAllSegments[U](f: Array[M] => U): U = {
    val segments = _segments
    val locksObtained = new mutable.ArrayBuffer[Lock](segments.length)
    try {
      for (seg <- segments) {
        val lock = seg.writeLock()
        lock.lock()
        locksObtained += lock
      }
      f(segments)
    } finally {
      for (lock <- locksObtained) {
        lock.unlock()
      }
    }
  }

  def clear(): Unit = writeLockAllSegments { segments =>
    val nsegments = segments.length
    segments.indices.foreach(i => {
      segments(i).valid_=(false)
      segments(i).clearBucket()
      segments(i) = segmentCreator(initSegmentCapacity(segments.length), loadFactor, i, nsegments)
    })
    _size.set(0)
  }

  final def size: Long = _size.get

  final def isEmpty: Boolean = _size.get == 0

  def toSeq: Seq[(K, V)] = {
    val size = this.size
    if (size <= Int.MaxValue) {
      val buffer = new mutable.ArrayBuffer[(K, V)](size.toInt)
      foldEntriesRead[Unit]((), true, { (k, v, _) => buffer += ((k, v)) })
      buffer
    } else {
      throw new IllegalStateException(s"ConcurrentSegmentedHashMap: size=$size" +
          " is greater than maximum integer so cannot be converted to a flat Seq")
    }
  }

  def toValues: Seq[V] = {
    val size = this.size
    if (size <= Int.MaxValue) {
      val buffer = new mutable.ArrayBuffer[V](size.toInt)
      foldValuesRead[Unit]((), { (_, v, _) => buffer += v })
      buffer
    } else {
      throw new IllegalStateException(s"ConcurrentSegmentedHashMap: size=$size" +
          " is greater than maximum integer so cannot be converted to a flat Seq")
    }
  }

  def toKeys: Seq[K] = {
    val size = this.size
    if (size <= Int.MaxValue) {
      val buffer = new mutable.ArrayBuffer[K](size.toInt)
      foldEntriesRead[Unit]((), true, { (k, _, _) => buffer += k })
      buffer
    } else {
      throw new IllegalStateException(s"ConcurrentSegmentedHashMap: size=$size" +
          " is greater than maximum integer so cannot be converted to a flat Seq")
    }
  }
}
