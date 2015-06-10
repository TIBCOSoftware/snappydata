package org.apache.spark.sql.collection

import scala.collection.Iterator

/**
 * An iterator that will keep generating iterators by invoking a given function
 * and return a flattened iterator. Typically useful to generate an
 * iterator on top of an arbitrary source where source size or structure is
 * not pre-determined (e.g. source may not have flatMap/fold or such methods)
 */
final class GenerateFlatIterator[A, U](val genFunc: U => (Iterator[A], U),
                                       val init: U) extends Iterator[A] {

  private var currentPair = genFunc(init)
  private var currentIter = currentPair._1

  override def hasNext: Boolean = {
    if (currentIter.hasNext) true
    else if (currentIter eq GenerateFlatIterator.TERMINATE) false
    else {
      do {
        currentPair = genFunc(currentPair._2)
        currentIter = currentPair._1
        if (currentIter eq GenerateFlatIterator.TERMINATE) return false
      } while (!currentIter.hasNext)
      true
    }
  }

  override def next(): A = currentIter.next()
}

final class SlicedIterator[A](val iter: Iterator[A]) extends Iterator[A] {

  private var remaining = 0

  def setSlice(from: Int, until: Int): Unit = {
    val lo = from max 0
    var toDrop = lo
    while (toDrop > 0 && iter.hasNext) {
      iter.next()
      toDrop -= 1
    }
    remaining = until - lo
  }

  override def hasNext = remaining > 0 && iter.hasNext

  override def next(): A =
    if (remaining > 0) {
      remaining -= 1
      iter.next()
    }
    else Iterator.empty.next()
}

object GenerateFlatIterator {
  val TERMINATE = Iterator()
}
