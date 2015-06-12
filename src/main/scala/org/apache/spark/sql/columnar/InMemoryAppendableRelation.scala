package org.apache.spark.sql.columnar

/**
 * A version of Spark's InMemoryRelation where new rows can be appended.
 * Append creates new CachedBatches like a normal buildBuffers as required,
 * all of which are tracked in driver as separate RDD[CachedBatch] and a
 * union over all existing is used for a query execution.
 *
 * Created by Soubhik on 5/22/15.
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.spark._
import org.apache.spark.rdd.{RDD, UnionRDD}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{CachedRDD, Row}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

private[sql] class InMemoryAppendableRelation
(override val output: Seq[Attribute],
 override val useCompression: Boolean,
 override val batchSize: Int,
 override val storageLevel: StorageLevel,
 override val child: SparkPlan,
 override val tableName: Option[String])(
  private var _ccb: RDD[CachedBatch] = null,
  private var _stats: Statistics = null,
  private var _bstats: Accumulable[ArrayBuffer[Row], Row] = null,
  private var _cachedBufferList: ArrayBuffer[RDD[CachedBatch]] =
  new ArrayBuffer[RDD[CachedBatch]]())
  extends InMemoryRelation(output, useCompression, batchSize,
    storageLevel, child, tableName)(
      _ccb: RDD[CachedBatch],
      _stats: Statistics,
      _bstats: Accumulable[ArrayBuffer[Row], Row])
  with MultiInstanceRelation {

  private[sql] val reservoirRDD = new CachedRDD(tableName.get, schema)(
    child.sqlContext)
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

  // If the cached column buffers were not passed in, we calculate them in the constructor.
  // As in Spark, the actual work of caching is lazy.
  if (super.cachedColumnBuffers != null) writeLock {
    if (_cachedBufferList.isEmpty) _cachedBufferList += super.cachedColumnBuffers
  }

  def appendBatch(batch: RDD[CachedBatch]) = writeLock {
    _cachedBufferList += batch
  }

  override def recache(): Unit = {
    sys.error(
      s"InMemoryAppendableRelation: unexpected call to recache for $tableName")
  }

  override def withOutput(newOutput: Seq[Attribute]): InMemoryAppendableRelation = {
    new InMemoryAppendableRelation(
      newOutput, useCompression, batchSize, storageLevel, child, tableName)(
        super.cachedColumnBuffers, super.statisticsToBePropagated, batchStats,
        _cachedBufferList)
  }

  override def children: Seq[LogicalPlan] = Seq.empty

  override def newInstance(): this.type = {
    new InMemoryAppendableRelation(
      output.map(_.newInstance()),
      useCompression,
      batchSize,
      storageLevel,
      child,
      tableName)(
        super.cachedColumnBuffers,
        super.statisticsToBePropagated,
        batchStats, _cachedBufferList).asInstanceOf[this.type]
  }

  override def cachedColumnBuffers: RDD[CachedBatch] = readLock {
    // toArray call below is required to take a snapshot of buffer
    new UnionRDD[CachedBatch](child.sqlContext.sparkContext,
      _cachedBufferList.toArray[RDD[CachedBatch]])
  }

  override protected def otherCopyArgs: Seq[AnyRef] =
    Seq(super.cachedColumnBuffers, super.statisticsToBePropagated,
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

  final class CachedBatchHolder(getColumnBuilders: => Array[ColumnBuilder],
                                var rowCount: Int, val batchSize: Int,
                                val batches: ArrayBuffer[CachedBatch]) {

    var columnBuilders = getColumnBuilders

    /**
     * Append a single row to the current CachedBatch (creating a new one
     * if not present or has exceeded its capacity)
     */
    private def appendRow_(newBuilders: Boolean, row: Row): Unit = {
      val rowLength = row.length
      if (rowLength > 0) {
        // Added for SPARK-6082. This assertion can be useful for scenarios when
        // something like Hive TRANSFORM is used. The external data generation
        // script used in TRANSFORM may result malformed rows, causing
        // ArrayIndexOutOfBoundsException, which is somewhat hard to decipher.
        assert(rowLength == columnBuilders.length, s"Row column number " +
          s"mismatch, expected ${columnBuilders.length} columns, " +
          s"but got $rowLength. Row content: $row")

        var i = 0
        while (i < rowLength) {
          columnBuilders(i).appendFrom(row, i)
          i += 1
        }
        rowCount += 1
      }
      if (rowCount >= batchSize) {
        // create a new CachedBatch and push into the array of
        // CachedBatches so far in this iteration
        val stats = Row.merge(columnBuilders.map(
          _.columnStats.collectedStatistics): _*)
        // TODO: somehow push into global batchStats
        batches += CachedBatch(columnBuilders.map(_.build().array()), stats)
        if (newBuilders) columnBuilders = getColumnBuilders
      }
    }

    def appendRow(u: Unit, row: Row): Unit = appendRow_(newBuilders = true, row)

    // empty for now
    def endRows(u: Unit): Unit = {}

    def forceEndOfBatch(): ArrayBuffer[CachedBatch] = {
      if (rowCount > 0) {
        // setting rowCount to batchSize temporarily will automatically
        // force creation of a new batch in appendRow
        rowCount = batchSize
        appendRow_(newBuilders = false, EmptyRow)
      }
      this.batches
    }
  }

  def apply(useCompression: Boolean,
            batchSize: Int,
            storageLevel: StorageLevel,
            child: SparkPlan,
            tableName: Option[String]): InMemoryAppendableRelation =
    new InMemoryAppendableRelation(child.output, useCompression, batchSize,
      storageLevel, child, tableName)()

  def apply(useCompression: Boolean,
            batchSize: Int,
            tableName: String,
            schema: StructType,
            output: Seq[Attribute]): CachedBatchHolder = {
    def columnBuilders = output.map { attribute =>
      val columnType = ColumnType(attribute.dataType)
      val initialBufferSize = columnType.defaultSize * batchSize
      ColumnBuilder(attribute.dataType, initialBufferSize,
        attribute.name, useCompression)
    }.toArray

    new CachedBatchHolder(columnBuilders, 0, batchSize,
      new ArrayBuffer[CachedBatch](1))
  }
}

private[sql] class InMemoryAppendableColumnarTableScan
(
  override val attributes: Seq[Attribute],
  override val predicates: Seq[Expression],
  override val relation: InMemoryAppendableRelation)
  extends InMemoryColumnarTableScan(attributes, predicates, relation) {

  protected override def doExecute(): RDD[Row] = {

    val reservoirRows: RDD[Row] = relation.asInstanceOf[InMemoryAppendableRelation].reservoirRDD.mapPartitions { rowIterator =>

      // Find the ordinals and data types of the requested columns.  If none are requested, use the
      // narrowest (the field with minimum default element size).
      val (requestedColumnIndices, requestedColumnDataTypes) = if (attributes.isEmpty) {
        val (narrowestOrdinal, narrowestDataType) =
          relation.output.zipWithIndex.map { case (a, ordinal) =>
            ordinal -> a.dataType
          } minBy { case (_, dataType) =>
            ColumnType(dataType).defaultSize
          }
        Seq(narrowestOrdinal) -> Seq(narrowestDataType)
      } else {
        attributes.map { a =>
          relation.output.indexWhere(_.exprId == a.exprId) -> a.dataType
        }.unzip
      }

      val nextRow = new SpecificMutableRow(requestedColumnDataTypes)

      new Iterator[Row] {

        override def hasNext: Boolean = rowIterator.hasNext

        override def next() = {
          val row = rowIterator.next()

          requestedColumnIndices.indices.foreach { i =>
            nextRow(i) = row(requestedColumnIndices(i))
          }

          nextRow
        }
      }
    }

    new UnionRDD[Row](this.sparkContext, Seq(super.doExecute(), reservoirRows))
  }
}

private[sql] object InMemoryAppendableColumnarTableScan {
  def apply(attributes: Seq[Attribute], predicates: Seq[Expression],
            relation: InMemoryAppendableRelation): SparkPlan = {
    new InMemoryAppendableColumnarTableScan(attributes, predicates, relation)
  }
}
