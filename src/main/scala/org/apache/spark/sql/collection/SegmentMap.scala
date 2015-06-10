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

package org.apache.spark.sql.collection

import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantReadWriteLock

trait SegmentMap[K, V] extends ReentrantReadWriteLock {

  def fold[U](init: U)(f: (K, V, U) => U): U

  def iterator: Iterator[(K, V)]

  def size: Int

  def isEmpty: Boolean

  def contains(k: K, hash: Int): Boolean

  def apply(k: K, hash: Int): V

  def update(k: K, hash: Int, v: V): Boolean

  def changeValue(k: K, hash: Int, change: ChangeValue[K, V]): Option[Boolean]
}

trait ChangeValue[K, V] {

  def defaultValue(k: K): V

  def mergeValue(k: K, v: V): V

  def segmentEnd(segment: SegmentMap[K, V]): Unit

  def segmentAbort(segment: SegmentMap[K, V]): Boolean
}

object SegmentMap {

  val DEFAULT_LOAD_FACTOR = 0.7

  val TRUE_OPTION = Some(true)
  val FALSE_OPTION = Some(false)

  def nextPowerOf2(n: Int): Int = {
    val highBit = Integer.highestOneBit(n)
    if (highBit == n) n else highBit << 1
  }

  def lock[R](lock: Lock)(f: => R): R = {
    lock.lock()
    try {
      f
    } finally {
      lock.unlock()
    }
  }
}
