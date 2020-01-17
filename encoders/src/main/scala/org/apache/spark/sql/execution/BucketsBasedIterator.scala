/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package org.apache.spark.sql.execution

import java.util

import scala.collection.AbstractIterator

trait BucketsBasedIterator {
  def getBucketSet(): java.util.Set[Integer]
}

object BucketsBasedIterator {
  def apply[T](iterators : Iterator[T]*): Iterator[Any] = {
    val bucketsOpt = iterators.collectFirst {
      case bucketBased: BucketsBasedIterator => bucketBased.getBucketSet()
    }
    val combinedIter = Iterator[Iterator[T]](iterators: _*)
    bucketsOpt.map(bukets =>
      new AbstractIterator[Iterator[T]] with BucketsBasedIterator {
        val buckets = bukets
        val iter = combinedIter
        override def getBucketSet(): util.Set[Integer] = buckets
        override def hasNext: Boolean = iter.hasNext
        override def next(): Iterator[T] = iter.next()
      }
    ).getOrElse(combinedIter)
  }
}