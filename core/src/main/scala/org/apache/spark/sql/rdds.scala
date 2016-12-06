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
package org.apache.spark.sql

import scala.reflect.ClassTag

import com.gemstone.gemfire.internal.cache.PartitionedRegion

import org.apache.spark.rdd.{MapPartitionsRDD, RDD}
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.{Partition, Partitioner, TaskContext}


private[sql] final class MapPartitionsPreserveRDD[U: ClassTag, T: ClassTag](
    prev: RDD[T], f: (TaskContext, Partition, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false)
    extends MapPartitionsRDD[U, T](prev, null, preservesPartitioning) {

  // TODO [sumedh] why doesn't the standard MapPartitionsRDD do this???
  override def getPreferredLocations(
      split: Partition): Seq[String] = firstParent[T].preferredLocations(split)

  override def compute(split: Partition, context: TaskContext): Iterator[U] =
    f(context, split, firstParent[T].iterator(split, context))
}

private[spark] final class PreserveLocationsRDD[U: ClassTag, T: ClassTag](
    prev: RDD[T],
    f: (TaskContext, Int, Iterator[T]) => Iterator[U],
    preservesPartitioning: Boolean = false, p: (Int) => Seq[String])
    extends MapPartitionsRDD[U, T](prev, f, preservesPartitioning) {

  override def getPreferredLocations(split: Partition): Seq[String] = p(split.index)
}

private[sql] final class PartitionedPreferredLocationsRDD[T: ClassTag](
    var child: RDD[T], @transient val region: PartitionedRegion)
    extends RDD[T](child) {

  @transient override val partitioner: Option[Partitioner] = child.partitioner

  assert(region.getTotalNumberOfBuckets == child.getNumPartitions)

  override def getPreferredLocations(split: Partition): Seq[String] = {
    StoreUtils.getBucketPreferredLocations(region, split.index)
  }

  override protected def getPartitions: Array[Partition] = child.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[T] =
    child.compute(split, context)

  override def clearDependencies() {
    super.clearDependencies()
    child = null
  }
}
