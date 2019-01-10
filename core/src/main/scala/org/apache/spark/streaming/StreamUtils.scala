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
package org.apache.spark.streaming

import java.io.OutputStream

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.util.RateLimitedOutputStream

/**
 * Some utility methods for streaming operations.
 */
object StreamUtils {

  private[this] val generatedRDDsMethod = classOf[DStream[_]]
      .getMethod("generatedRDDs")

  /** Invoke <code>DStream.getOrCompute</code> */
  def getOrCompute[T: ClassTag](dStream: DStream[T],
      time: Time): Option[RDD[T]] = dStream.getOrCompute(time)

  def getGeneratedRDDs[T: ClassTag](dStream: DStream[T]): mutable.Map[Time,
      RDD[T]] = {
    // using reflection here since Spark's object is a HashMap while it is
    // a ConcurrentHashMap in snappydata's version of Spark
    // [TODO SPARK PR] it should be a concurrent map in Apache Spark too
    generatedRDDsMethod.invoke(dStream).asInstanceOf[mutable.Map[Time, RDD[T]]]
  }

  def getRateLimitedOutputStream(out: OutputStream,
      desiredBytesPerSec: Int): RateLimitedOutputStream = {
    new RateLimitedOutputStream(out, desiredBytesPerSec)
  }
}
