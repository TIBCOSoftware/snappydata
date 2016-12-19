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
package org.apache.spark.sql.execution.joins

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, Partition, Partitioner, SparkContext, TaskContext}

/**
 * RDD that delegates all the calls to the base RDD. However the dependencies of
 * this RDD can be altered.
 */
class DelegateRDD[T: ClassTag](
    sc: SparkContext,
    baseRdd: RDD[T],
    implicit val alldependencies: Seq[Dependency[_]] = null)
    extends RDD[T](sc,
      if (alldependencies == null) baseRdd.dependencies
      else alldependencies)
        with Serializable {

  @transient override val partitioner: Option[Partitioner] = baseRdd.partitioner

  override def getPreferredLocations(
      split: Partition): Seq[String] = baseRdd.preferredLocations(split)

  override protected def getPartitions: Array[Partition] = baseRdd.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[T] =
    baseRdd.compute(split, context)
}
