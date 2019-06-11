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
package org.apache.spark.sql

import scala.reflect.ClassTag

import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.util.Utils
import org.apache.spark.{Dependency, Partition, Partitioner, SparkContext, TaskContext}


private[sql] final class MapPartitionsPreserveRDD[U: ClassTag, T: ClassTag](
    prev: RDD[T], f: (TaskContext, Partition, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false)
    extends MapPartitionsRDD[U, T](prev, null, preservesPartitioning) {

  // TODO [sumedh] why doesn't the standard MapPartitionsRDD do this???
  override def getPreferredLocations(
      split: Partition): Seq[String] = firstParent[T].preferredLocations(split)

  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    f(context, split, firstParent[T].iterator(split, context))

  override def count(): Long = sparkContext.runJob(this, RDDs.getIteratorSize _).sum
}

private[sql] final class PreserveLocationsRDD[U: ClassTag, T: ClassTag](
    prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false, p: Int => Seq[String])
    extends MapPartitionsRDD[U, T](prev, f, preservesPartitioning) {

  override def getPreferredLocations(split: Partition): Seq[String] = p(split.index)

  override def count(): Long = sparkContext.runJob(this, RDDs.getIteratorSize _).sum
}

/**
 * RDD that delegates calls to the base RDD. However the dependencies
 * and preferred locations of this RDD can be altered.
 */
class DelegateRDD[T: ClassTag](
    sc: SparkContext,
    val baseRdd: RDD[T],
    val otherRDDs : Seq[RDD[T]],
    preferredLocations: Array[Seq[String]] = null,
    allDependencies: Seq[Dependency[_]] = null)
    extends RDD[T](sc,
      if (allDependencies == null) baseRdd.dependencies
      else allDependencies)
        with Serializable {

  @transient override val partitioner: Option[Partitioner] = baseRdd.partitioner

  override def getPreferredLocations(
      split: Partition): Seq[String] = {
    if (preferredLocations eq null) baseRdd.preferredLocations(split)
    else preferredLocations(split.index)
  }

  override protected def getPartitions: Array[Partition] = baseRdd.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[T] =
    baseRdd.compute(split, context)
}

case class EmptyIteratorWithRowCount[U](rowCount : Long) extends Iterator[U] {
  def hasNext: Boolean = false
  def next(): Nothing = throw new NoSuchElementException("next on empty iterator")
}

object RDDs {
  def getIteratorSize[T](iterator: Iterator[T]): Long = iterator match {
    case EmptyIteratorWithRowCount(rowCount) => rowCount
    case _ => Utils.getIteratorSize[T](iterator)
  }
}
