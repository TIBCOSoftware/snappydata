/*
 * Copyright (c) 2010-2016 SnappyData, Inc. All rights reserved.
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

package org.apache.spark.sql.columnar

/**
 * A version of Spark's InMemoryRelation where new rows can be appended.
 * Append creates new CachedBatches like a normal buildBuffers as required,
 * all of which are tracked in driver as separate RDD[CachedBatch] and a
 * union over all existing is used for a query execution.
 */
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.rdd.{RDD, UnionRDD}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.execution.{ConvertToUnsafe, SparkPlan}
import org.apache.spark.sql.snappy._
import org.apache.spark.storage.StorageLevel

private[sql] class InMemoryAppendableRelation(
    override val output: Seq[Attribute],
    override val useCompression: Boolean,
    override val batchSize: Int,
    override val storageLevel: StorageLevel,
    override val child: SparkPlan,
    override val tableName: Option[String])(
    private var _ccb: RDD[CachedBatch] = null,
    private var _stats: Statistics = null,
    private var _bstats: Accumulable[ArrayBuffer[InternalRow], InternalRow] = null,
    private[columnar] var _cachedBufferList: ArrayBuffer[RDD[CachedBatch]] =
    new ArrayBuffer[RDD[CachedBatch]]())
    extends InMemoryRelation(output, useCompression, batchSize,
      storageLevel, child, tableName)(_ccb: RDD[CachedBatch],
      _stats: Statistics,
      _bstats: Accumulable[ArrayBuffer[InternalRow], InternalRow])
    with MultiInstanceRelation with InMemoryAppendableRelationTrait {

  override private[sql] val reservoirRDD: Option[RDD[InternalRow]] = None


  private val bufferLock = new ReentrantReadWriteLock()

  /** Acquires a read lock on the cache for the duration of `f`. */
  private[sql] def readLock[A](f: => A): A = {
    val lock = bufferLock.readLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  /** Acquires a write lock on the cache for the duration of `f`. */
  private[sql] def writeLock[A](f: => A): A = {
    val lock = bufferLock.writeLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  // If the cached column buffers were not passed in, we calculate them
  // in the constructor. As in Spark, the actual work of caching is lazy.
  if (super.cachedColumnBuffers != null) writeLock {
    if (_cachedBufferList.isEmpty) _cachedBufferList += super.cachedColumnBuffers
  }

  def appendBatch(batch: RDD[CachedBatch]) = writeLock {
    _cachedBufferList += batch
  }

  def truncate() = writeLock {
    for (batch <- _cachedBufferList) {
      batch.unpersist(true)
    }
    _cachedBufferList.clear()
  }

  def batchAggregate(accumulated: ArrayBuffer[CachedBatch],
      batch: CachedBatch): ArrayBuffer[CachedBatch] = {
    accumulated += batch
  }

  override def recache(): Unit = {
    sys.error(
      s"InMemoryAppendableRelation: unexpected call to recache for $tableName")
  }

  override def withOutput(newOutput: Seq[Attribute]): InMemoryRelation = {
    new InMemoryAppendableRelation(newOutput, useCompression, batchSize,
      storageLevel, child, tableName)(super.cachedColumnBuffers,
      statisticsToBePropagated, batchStats, _cachedBufferList)
  }

  override def children: Seq[LogicalPlan] = Seq.empty

  override def newInstance(): this.type = {
    new InMemoryAppendableRelation(
      output.map(_.newInstance()),
      useCompression,
      batchSize,
      storageLevel,
      child,
      tableName)(super.cachedColumnBuffers,
      statisticsToBePropagated,
      batchStats, _cachedBufferList).asInstanceOf[this.type]
  }

  def getInMemoryRelationCachedColumnBuffers: RDD[CachedBatch] = super.cachedColumnBuffers

  override def cachedColumnBuffers: RDD[CachedBatch] = readLock {
    // toArray call below is required to take a snapshot of buffer
    new UnionRDD[CachedBatch](child.sqlContext.sparkContext,
      _cachedBufferList.toArray[RDD[CachedBatch]])
  }

  override protected def otherCopyArgs: Seq[AnyRef] =
    Seq(super.cachedColumnBuffers, statisticsToBePropagated,
      batchStats, _cachedBufferList)

  override private[sql] def uncache(blocking: Boolean): Unit = {
    super.uncache(blocking)
    writeLock {
      _cachedBufferList.foreach(_.unpersist(blocking))
      _cachedBufferList.clear()
    }
  }
}

private[sql] object InMemoryAppendableRelation {
  def apply(useCompression: Boolean,
      batchSize: Int,
      storageLevel: StorageLevel,
      child: SparkPlan,
      tableName: Option[String]): InMemoryAppendableRelation =
    new InMemoryAppendableRelation(child.output, useCompression, batchSize,
      storageLevel, if (child.outputsUnsafeRows) child else ConvertToUnsafe(child),
      tableName)()

}

private[sql] class InMemoryAppendableColumnarTableScan(
    override val attributes: Seq[Attribute],
    override val predicates: Seq[Expression],
    override val relation: InMemoryAppendableRelation)
    extends InMemoryColumnarTableScan(attributes, predicates, relation) {

  protected override def doExecute(): RDD[InternalRow] = {

    val rdd = relation.reservoirRDD
    val rel = relation.output
    if (rdd.isEmpty) {
      return super.doExecute()
    }
    val rel_out = relation.output
    val reservoirRows: RDD[InternalRow] = rdd.get.mapPartitionsPreserve { rows =>

      // Find the ordinals and data types of the requested columns.
      // If none are requested, use the narrowest (the field with
      // minimum default element size).
      val (requestedColumnIndices, requestedColumnDataTypes) = if (attributes.isEmpty) {
        val (narrowestOrdinal, narrowestDataType) =

          rel.zipWithIndex.map { case (a, ordinal) =>

            ordinal -> a.dataType
          } minBy { case (_, dataType) =>
            ColumnType(dataType).defaultSize
          }
        Seq(narrowestOrdinal) -> Seq(narrowestDataType)
      } else {
        attributes.map { a =>

          rel.indexWhere(_.exprId == a.exprId) -> a.dataType

        }.unzip
      }

      val nextRow = new SpecificMutableRow(requestedColumnDataTypes)

      new Iterator[InternalRow] {

        override def hasNext: Boolean = rows.hasNext

        override def next() = {
          val row = rows.next()

          requestedColumnIndices.indices.foreach { i =>
            nextRow(i) = row.get(requestedColumnIndices(i),
              null /* returned GenericMutableRow does not use DataType */)
          }

          nextRow
        }
      }
    }

    new UnionRDD[InternalRow](this.sparkContext, Seq(super.doExecute(),
      reservoirRows))
  }
}

private[sql] object InMemoryAppendableColumnarTableScan {
  def apply(attributes: Seq[Attribute], predicates: Seq[Expression],
      relation: InMemoryAppendableRelation): SparkPlan = {
    new InMemoryAppendableColumnarTableScan(attributes, predicates, relation)
  }
}
