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

import scala.collection.{Map}
import scala.io.Codec
import scala.reflect.{ClassTag, _}

import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.spark.partial.{BoundedDouble, PartialResult}
import org.apache.spark.rdd._
import org.apache.spark.storage.{StorageLevel}
import org.apache.spark._
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.util.{Utils}

/**
 * RDD that delegates all the calls to the base RDD. However the dependencies of
 * this RDD can be altered.
 */
class DelegateRDD[T: ClassTag](
    sc: SparkContext,
    baseRdd: RDD[T],
    implicit val alldependencies: Seq[Dependency[_]] = null)
    extends RDD[T](sc,
      if (alldependencies == null) { baseRdd.dependencies }
      else { alldependencies} )
        with Serializable {

  override protected def getPartitions: Array[Partition] = baseRdd.partitions

  override def compute(split: Partition, context: TaskContext): Iterator[T] =
    baseRdd.compute(split, context)

  @transient override val partitioner: Option[Partitioner] = baseRdd.partitioner

  override def persist(newLevel: StorageLevel): this.type = {
    baseRdd.persist(newLevel);
    this
  }

  override def persist(): this.type = {
    baseRdd.persist()
    this
  }

  override def cache(): this.type = {
    baseRdd.cache()
    this
  }

  override def unpersist(blocking: Boolean = true): this.type = {
    baseRdd.unpersist(blocking)
    this
  }

  override def getStorageLevel: StorageLevel = baseRdd.getStorageLevel


  override def map[U: ClassTag](f: T => U): RDD[U] = baseRdd.map(f)
  override def flatMap[U: ClassTag](f: T => TraversableOnce[U]): RDD[U] = baseRdd.flatMap(f)
  override  def filter(f: T => Boolean): RDD[T] = baseRdd.filter(f)
  override def distinct(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] =
    baseRdd.distinct(numPartitions)(ord)

  override def distinct(): RDD[T] = baseRdd.distinct

  override def repartition(numPartitions: Int)(implicit ord: Ordering[T] = null): RDD[T] =
    baseRdd.repartition(numPartitions)(ord)

  override def coalesce(numPartitions: Int, shuffle: Boolean = false,
      partitionCoalescer: Option[PartitionCoalescer] = Option.empty)
      (implicit ord: Ordering[T] = null)
  : RDD[T] = baseRdd.coalesce(numPartitions, shuffle, partitionCoalescer)(ord)

  override def sample(
      withReplacement: Boolean,
      fraction: Double,
      seed: Long = Utils.random.nextLong): RDD[T] = baseRdd.sample(withReplacement, fraction, seed)

  override def randomSplit(
      weights: Array[Double],
      seed: Long = Utils.random.nextLong): Array[RDD[T]] = baseRdd.randomSplit(weights, seed)

  override def takeSample(
      withReplacement: Boolean,
      num: Int,
      seed: Long = Utils.random.nextLong): Array[T] = baseRdd.takeSample(withReplacement, num, seed)

  override def union(other: RDD[T]): RDD[T] = baseRdd.union(other)

  override def ++(other: RDD[T]): RDD[T] = baseRdd.++(other)
  override def sortBy[K](
      f: (T) => K,
      ascending: Boolean = true,
      numPartitions: Int = this.partitions.length)
      (implicit ord: Ordering[K], ctag: ClassTag[K]): RDD[T] =
    baseRdd.sortBy(f, ascending, numPartitions)(ord, ctag)

  override def intersection(other: RDD[T]): RDD[T] = baseRdd.intersection(other)

  override def intersection(
      other: RDD[T],
      partitioner: Partitioner)(implicit ord: Ordering[T] = null): RDD[T] =
    baseRdd.intersection(other, partitioner)(ord)

  override def intersection(other: RDD[T], numPartitions: Int): RDD[T] =
    baseRdd.intersection(other, numPartitions)

  override def glom(): RDD[Array[T]] = baseRdd.glom

  override def cartesian[U: ClassTag](other: RDD[U]): RDD[(T, U)] = baseRdd.cartesian(other)

  override def groupBy[K](f: T => K)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] =
    baseRdd.groupBy(f)(kt)

  override def groupBy[K](
      f: T => K,
      numPartitions: Int)(implicit kt: ClassTag[K]): RDD[(K, Iterable[T])] =
    baseRdd.groupBy(f, numPartitions)

  override def groupBy[K](f: T => K, p: Partitioner)
      (implicit kt: ClassTag[K], ord: Ordering[K] = null)
  : RDD[(K, Iterable[T])] = baseRdd.groupBy(f, p)(kt, ord)

  override def pipe(command: String): RDD[String] = baseRdd.pipe(command)

  override def pipe(command: String, env: Map[String, String]): RDD[String] =
    baseRdd.pipe(command, env)

  override def pipe(
      command: Seq[String],
      env: Map[String, String] = Map(),
      printPipeContext: (String => Unit) => Unit = null,
      printRDDElement: (T, String => Unit) => Unit = null,
      separateWorkingDir: Boolean = false,
      bufferSize: Int = 8192,
      encoding: String = Codec.defaultCharsetCodec.name): RDD[String] =
    baseRdd.pipe(command, env, printPipeContext,
      printRDDElement, separateWorkingDir, bufferSize, encoding)

  override def mapPartitions[U: ClassTag](
      f: Iterator[T] => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] =
    baseRdd.mapPartitions(f, preservesPartitioning)

  override def mapPartitionsWithIndex[U: ClassTag](
      f: (Int, Iterator[T]) => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] =
    baseRdd.mapPartitionsWithIndex(f, preservesPartitioning)

  override def zip[U: ClassTag](other: RDD[U]): RDD[(T, U)] = baseRdd.zip(other)

  override def zipPartitions[B: ClassTag, V: ClassTag]
  (rdd2: RDD[B], preservesPartitioning: Boolean)
      (f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] =
    baseRdd.zipPartitions(rdd2, preservesPartitioning)(f)

  override def zipPartitions[B: ClassTag, V: ClassTag]
  (rdd2: RDD[B])
      (f: (Iterator[T], Iterator[B]) => Iterator[V]): RDD[V] = baseRdd.zipPartitions(rdd2)(f)

  override def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag]
  (rdd2: RDD[B], rdd3: RDD[C], preservesPartitioning: Boolean)
      (f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] =
    baseRdd.zipPartitions(rdd2, rdd3, preservesPartitioning) (f)

  override def zipPartitions[B: ClassTag, C: ClassTag, V: ClassTag]
  (rdd2: RDD[B], rdd3: RDD[C])
      (f: (Iterator[T], Iterator[B], Iterator[C]) => Iterator[V]): RDD[V] =
    baseRdd.zipPartitions(rdd2, rdd3)(f)

  override def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag]
  (rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D], preservesPartitioning: Boolean)
      (f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] =
    baseRdd.zipPartitions(rdd2, rdd3, rdd4, preservesPartitioning)(f)

  override def zipPartitions[B: ClassTag, C: ClassTag, D: ClassTag, V: ClassTag]
  (rdd2: RDD[B], rdd3: RDD[C], rdd4: RDD[D])
      (f: (Iterator[T], Iterator[B], Iterator[C], Iterator[D]) => Iterator[V]): RDD[V] =
    baseRdd.zipPartitions(rdd2, rdd3, rdd4)(f)


  override def foreach(f: T => Unit): Unit =
    baseRdd.foreach(f)

  override def foreachPartition(f: Iterator[T] => Unit): Unit =
    baseRdd.foreachPartition(f)

  override def collect(): Array[T] =
    baseRdd.collect()

  override def toLocalIterator: Iterator[T] =
    baseRdd.toLocalIterator

  override def collect[U: ClassTag](f: PartialFunction[T, U]): RDD[U] =
    baseRdd.collect(f)

  override def subtract(other: RDD[T]): RDD[T] =
    baseRdd.subtract(other)

  override def subtract(other: RDD[T], numPartitions: Int): RDD[T] =
    baseRdd.subtract(other, numPartitions)

  override def subtract(
      other: RDD[T],
      p: Partitioner)(implicit ord: Ordering[T] = null): RDD[T] =
    baseRdd.subtract(other, p)(ord)

  override def reduce(f: (T, T) => T): T = baseRdd.reduce(f)

  override def treeReduce(f: (T, T) => T, depth: Int = 2): T = baseRdd.treeReduce(f, depth)

  override def fold(zeroValue: T)(op: (T, T) => T): T = baseRdd.fold(zeroValue)(op)

  override def aggregate[U: ClassTag](zeroValue: U)(seqOp: (U, T) => U, combOp: (U, U) => U): U =
    baseRdd.aggregate(zeroValue)(seqOp, combOp)


  override def treeAggregate[U: ClassTag](zeroValue: U)(
      seqOp: (U, T) => U,
      combOp: (U, U) => U,
      depth: Int = 2): U = baseRdd.treeAggregate(zeroValue)(seqOp, combOp, depth)

  override def count(): Long = baseRdd.count

  override def countApprox(
      timeout: Long,
      confidence: Double = 0.95): PartialResult[BoundedDouble] =
    baseRdd.countApprox(timeout, confidence)

  override def countByValue()(implicit ord: Ordering[T] = null): Map[T, Long] =
    baseRdd.countByValue()(ord)

  override def countByValueApprox(timeout: Long, confidence: Double = 0.95)
      (implicit ord: Ordering[T] = null)
  : PartialResult[Map[T, BoundedDouble]] =
    baseRdd.countByValueApprox(timeout, confidence)(ord)

  override def countApproxDistinct(p: Int, sp: Int): Long = baseRdd.countApproxDistinct(p, sp)

  override def countApproxDistinct(relativeSD: Double = 0.05): Long =
    baseRdd.countApproxDistinct(relativeSD)

  override def zipWithIndex(): RDD[(T, Long)] = baseRdd.zipWithIndex()

  override def zipWithUniqueId(): RDD[(T, Long)] = baseRdd.zipWithUniqueId()

  override def take(num: Int): Array[T] = baseRdd.take(num)

  override def first(): T = baseRdd.first

  override def top(num: Int)(implicit ord: Ordering[T]): Array[T] = baseRdd.top(num)

  override def takeOrdered(num: Int)(implicit ord: Ordering[T]): Array[T] =
    baseRdd.takeOrdered(num)(ord)

  override def max()(implicit ord: Ordering[T]): T = baseRdd.max()(ord)

  override def min()(implicit ord: Ordering[T]): T = baseRdd.min()(ord)

  override def isEmpty(): Boolean = baseRdd.isEmpty

  override def saveAsTextFile(path: String): Unit = baseRdd.saveAsTextFile(path)

  override def saveAsTextFile(path: String, codec: Class[_ <: CompressionCodec]): Unit =
    baseRdd.saveAsTextFile(path, codec)

  override def saveAsObjectFile(path: String): Unit = baseRdd.saveAsObjectFile(path)

  override def keyBy[K](f: T => K): RDD[(K, T)] = baseRdd.keyBy(f)

  override def checkpoint(): Unit = baseRdd.checkpoint()

  override def localCheckpoint(): this.type = {
    baseRdd.localCheckpoint()
    this
  }

  override def isCheckpointed: Boolean = baseRdd.isCheckpointed

  override def getCheckpointFile: Option[String] = baseRdd.getCheckpointFile

  /** A description of this RDD and its recursive dependencies for debugging. */
  override def toDebugString: String = baseRdd.toDebugString

  override def toString: String = baseRdd.toString()

  override def toJavaRDD() : JavaRDD[T] = baseRdd.toJavaRDD
}

