/*
 * Copyright (c) 2017-2022 TIBCO Software Inc. All rights reserved.
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

import scala.collection.Iterator

/**
 * An iterator that will keep generating iterators by invoking a given function
 * and return a flattened iterator. Typically useful to generate an
 * iterator on top of an arbitrary source where source size or structure is
 * not pre-determined (e.g. source may not have flatMap/fold or such methods).
 *
 * Use the GenerateFlatIterator.TERMINATE token to indicate end of iteration.
 */
final class GenerateFlatIterator[A, U](val genFunc: U => (Iterator[A], U),
    val init: U) extends Iterator[A] with AutoCloseable {

  private[this] var currentPair = genFunc(init)
  private[this] var currentIter = currentPair._1
  private[this] var lastIter = currentIter

  override def hasNext: Boolean = {
    if (currentIter.hasNext) true
    else if (currentIter eq GenerateFlatIterator.TERMINATE) false
    else {
      do {
        currentPair = genFunc(currentPair._2)
        currentIter = currentPair._1
        if (currentIter eq GenerateFlatIterator.TERMINATE) return false
        lastIter = currentIter
      } while (!currentIter.hasNext)
      true
    }
  }

  override def next(): A = currentIter.next()

  override def close(): Unit = this.lastIter match {
    case a: AutoCloseable => a.close()
    case _ =>
  }
}

final class SlicedIterator[A](val iter: Iterator[A]) extends Iterator[A] {

  def this(itr: Iterator[A], from: Int, until: Int) = {
    this(itr)
    setSlice(from, until)
  }

  private var remaining = 0

  def setSlice(from: Int, until: Int): Unit = {
    if (from > 0) {
      val lo = from max 0
      var toDrop = lo
      while (toDrop > 0 && iter.hasNext) {
        iter.next()
        toDrop -= 1
      }
      remaining = until - lo
    }
    else {
      remaining = until
    }
  }

  override def hasNext: Boolean = remaining > 0 && iter.hasNext

  override def next(): A =
    if (remaining > 0) {
      remaining -= 1
      iter.next()
    }
    else Iterator.empty.next()
}

object GenerateFlatIterator {
  val TERMINATE: Iterator[Nothing] = Iterator()
}
