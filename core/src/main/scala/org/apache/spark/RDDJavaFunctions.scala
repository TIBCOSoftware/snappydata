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
package org.apache.spark


import scala.collection.JavaConverters._

import org.apache.spark.api.java.JavaPairRDD._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.api.java.function.{FlatMapFunction, Function => JFunction}
import org.apache.spark.sql.snappy._


class RDDJavaFunctions[U](val javaRDD: JavaRDD[U]) {

  /**
   * Return a new RDD by applying a function to all elements of this RDD.
   *
   * This variant also preserves the preferred locations of parent RDD.
   */
  def mapPreserve[R](f: JFunction[U, R]): JavaRDD[R] = {
    JavaRDD.fromRDD(
      new RDDExtensions(javaRDD.rdd)(fakeClassTag[U])
          .mapPreserve(f)(fakeClassTag[R])
    )(fakeClassTag[R])
  }

  /**
   * Return a new JavaRDD by applying a function to each partition of given RDD.
   *
   * This variant also preserves the preferred locations of parent RDD.
   *
   * `preservesPartitioning` indicates whether the input function preserves
   * the partitioner, which should be `false` unless this is a pair RDD and
   * the input function doesn't modify the keys.
   */

  def mapPartitionsPreserve[R](f: FlatMapFunction[java.util.Iterator[U], R],
      preservesPartitioning: Boolean): JavaRDD[R] = {
    def fn: (Iterator[U]) => Iterator[R] = {
      (x: Iterator[U]) => f.call(x.asJava).asScala
    }
    JavaRDD.fromRDD(
      new RDDExtensions(javaRDD.rdd)(fakeClassTag[U])
          .mapPartitionsPreserve(fn, preservesPartitioning)(fakeClassTag[R])
    )(fakeClassTag[R])
  }

  /**
   * Return a new JavaRDD by applying a function to each partition of given RDD,
   * while tracking the index of the original partition.
   *
   * This variant also preserves the preferred locations of parent RDD.
   *
   * `preservesPartitioning` indicates whether the input function preserves
   * the partitioner, which should be `false` unless this is a pair RDD and
   * the input function doesn't modify the keys.
   */
  def mapPartitionsPreserveWithIndex[R](
      f: FlatMapFunction[(Integer, java.util.Iterator[U]), R],
      preservesPartitioning: Boolean = false): JavaRDD[R] = {

    def fn: (Int, Iterator[U]) => Iterator[R] = {
      (x: Int, y: Iterator[U]) => f.call(x, y.asJava).asScala
    }
    JavaRDD.fromRDD(
      new RDDExtensions(javaRDD.rdd)(fakeClassTag[U])
          .mapPartitionsPreserveWithIndex(fn, preservesPartitioning)(fakeClassTag[R])
    )(fakeClassTag[R])
  }

}
