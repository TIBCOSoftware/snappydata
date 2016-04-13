/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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

import scala.collection.mutable
import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.DStream

/**
 * Some utility methods for streaming operations.
 */
object StreamUtils {

  /** Invoke <code>DStream.getOrCompute</code> */
  def getOrCompute[T: ClassTag](dStream: DStream[T],
      time: Time): Option[RDD[T]] = dStream.getOrCompute(time)

  def getGeneratedRDDs[T: ClassTag](dStream: DStream[T]): mutable.Map[Time,
      RDD[T]] = {
    // using reflection here since Spark's object is a HashMap while it is
    // a ConcurrentHashMap in snappydata's version of Spark
    // [TODO PR] it should be a concurrent map in Apache Spark too
    dStream.getClass.getMethod("generatedRDDs").invoke(dStream)
        .asInstanceOf[mutable.Map[Time, RDD[T]]]
  }

  def isStreamInitialized(stream: DStream[_]): Boolean = stream.isInitialized

  def setStorageLevel(level: StorageLevel, stream: DStream[_]): Unit =
    stream.storageLevel = level

  def setCheckpointDuration(duration: Duration, stream: DStream[_]): Unit =
    stream.checkpointDuration = duration
}
