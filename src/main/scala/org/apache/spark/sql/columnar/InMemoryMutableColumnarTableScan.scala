package org.apache.spark.sql.columnar

/**
 * Created by soubhikc on 5/22/15.
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
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.CatalystTypeConverters
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

private[sql] object InMemoryAppendableRelation {
  def apply(useCompression: Boolean,
            batchSize: Int,
            storageLevel: StorageLevel,
            child: SparkPlan,
            tableName: Option[String]): InMemoryAppendableRelation =
    new InMemoryAppendableRelation(child.output, useCompression, batchSize, storageLevel, child, tableName)()

  /**
   * Append given rows to local data. Later we will explicitly
   * persist sampled RDDs and then establish their relation here.
   */
  def appendRows(rows: Iterator[Row],
                 tableName: String,
                 schema: StructType,
                 output: Seq[Attribute],
                 useCompression: Boolean,
                 batchSize: Int): CachedBatch = {
    val columnBuilders = output.map { attribute =>
      val columnType = ColumnType(attribute.dataType)
      val initialBufferSize = columnType.defaultSize * batchSize
      ColumnBuilder(attribute.dataType, initialBufferSize, attribute.name, useCompression)
    }.toArray

    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    var rowCount = 0
    while (rows.hasNext && rowCount < batchSize) {
      val row = rows.next()

      if (!(row equals EmptyRow)) {
        // Added for SPARK-6082. This assertion can be useful for scenarios when something
        // like Hive TRANSFORM is used. The external data generation script used in TRANSFORM
        // may result malformed rows, causing ArrayIndexOutOfBoundsException, which is somewhat
        // hard to decipher.
        assert(
          row.size == columnBuilders.size,
          s"""Row column number mismatch, expected ${output.size} columns, but got ${row.size}.
                                                                                                |Row content: $row
               """.stripMargin)

        val catalystRow = {
          converter(row).asInstanceOf[Row]
        }

        var i = 0
        while (i < catalystRow.length) {
          columnBuilders(i).appendFrom(catalystRow, i)
          i += 1
        }
        rowCount += 1
      }
    }

    // TODO: add proper local stats, or communicate with master
    val newBatch = CachedBatch(columnBuilders.map(_.build().array()), null)
    //putBlockData(newBatch, batchId, rddId, counter, tableName)
    newBatch
  }
}

/*
private[sql] case class CachedBatch(buffers: Array[Array[Byte]], stats: Row)

private[sql] object InMemoryLocalBlockHolder {

  private val globalBlockIdMap = new OpenHashMap[String, Seq[BlockId]]

  private def putBlockData[T](data: T, batchId: Int, rddId: Int,
                              counter: Int, tableName: String): BlockId = {
    val blockManager = SparkEnv.get.blockManager
    val blockId = new LocalBlockId( s"""${tableName}_inmemory_append_${batchId}_""",
      rddId, s"_${counter}")
    blockManager.putSingle(blockId, data, StorageLevel.MEMORY_AND_DISK, false)
    globalBlockIdMap.changeValue(tableName, Seq(blockId), {
      _ :+ blockId
    })
    blockId
  }

  private def getBlockDataIterator[T](blockId: BlockId): Iterator[T] = {
    val blockManager = SparkEnv.get.blockManager
    blockManager.get(blockId) match {
      case Some(blockResult) =>
        val context = TaskContext.get()
        if (context != null) {
          // Partition is already materialized, so just return its values
          val existingMetrics = context.taskMetrics
            .getInputMetricsForReadMethod(blockResult.readMethod)
          existingMetrics.incBytesRead(blockResult.bytes)

          val iter = blockResult.data.asInstanceOf[Iterator[T]]
          new InterruptibleIterator[T](context, iter) {
            override def next(): T = {
              existingMetrics.incRecordsRead(1)
              delegate.next()
            }
          }
        }
        else {
          blockResult.data.asInstanceOf[Iterator[T]]
        }
    }
  }

  def getTableData[T](tableName: String): Seq[T] = {
    // TODO: log a warning if no block found for table in getOrElse
    globalBlockIdMap.get(tableName).getOrElse(Seq.empty[BlockId]) flatMap {
      getBlockDataIterator(_)
    }
  }
}
*/

private[sql] class InMemoryAppendableRelation(override val output: Seq[Attribute],
                                              override val useCompression: Boolean,
                                              override val batchSize: Int,
                                              override val storageLevel: StorageLevel,
                                              var childP: SparkPlan,
                                              override val tableName: Option[String])(
                                               private var _ccb: RDD[CachedBatch] = null,
                                               private var _stats: Statistics = null,
                                               private var _bstats: Accumulable[ArrayBuffer[Row], Row] = null,
                                               private var _cachedBufferList: ArrayBuffer[RDD[CachedBatch]] =
                                               new ArrayBuffer[RDD[CachedBatch]]())
  extends InMemoryRelation(output, useCompression, batchSize, storageLevel, childP, tableName)(
    _ccb: RDD[CachedBatch],
    _stats: Statistics,
    _bstats: Accumulable[ArrayBuffer[Row], Row])
  with MultiInstanceRelation {

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
    sys.error(s"InMemoryAppendableRelation: unexpected call to recache for ${tableName}")
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

/*
private[sql] case class InMemoryMutableColumnarTableScan(
                                                          attributes: Seq[Attribute],
                                                          predicates: Seq[Expression],
                                                          relation: InMemoryAppendableRelation)
  extends LeafNode {

  override def output: Seq[Attribute] = attributes

  private def statsFor(a: Attribute) = relation.partitionStatistics.forAttribute(a)

  // Returned filter predicate should return false iff it is impossible for the input expression
  // to evaluate to `true' based on statistics collected about this partition batch.
  val buildFilter: PartialFunction[Expression, Expression] = {
    case And(lhs: Expression, rhs: Expression)
      if buildFilter.isDefinedAt(lhs) || buildFilter.isDefinedAt(rhs) =>
      (buildFilter.lift(lhs) ++ buildFilter.lift(rhs)).reduce(_ && _)

    case Or(lhs: Expression, rhs: Expression)
      if buildFilter.isDefinedAt(lhs) && buildFilter.isDefinedAt(rhs) =>
      buildFilter(lhs) || buildFilter(rhs)

    case EqualTo(a: AttributeReference, l: Literal) =>
      statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound
    case EqualTo(l: Literal, a: AttributeReference) =>
      statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound

    case LessThan(a: AttributeReference, l: Literal) => statsFor(a).lowerBound < l
    case LessThan(l: Literal, a: AttributeReference) => l < statsFor(a).upperBound

    case LessThanOrEqual(a: AttributeReference, l: Literal) => statsFor(a).lowerBound <= l
    case LessThanOrEqual(l: Literal, a: AttributeReference) => l <= statsFor(a).upperBound

    case GreaterThan(a: AttributeReference, l: Literal) => l < statsFor(a).upperBound
    case GreaterThan(l: Literal, a: AttributeReference) => statsFor(a).lowerBound < l

    case GreaterThanOrEqual(a: AttributeReference, l: Literal) => l <= statsFor(a).upperBound
    case GreaterThanOrEqual(l: Literal, a: AttributeReference) => statsFor(a).lowerBound <= l

    case IsNull(a: Attribute) => statsFor(a).nullCount > 0
    case IsNotNull(a: Attribute) => statsFor(a).count - statsFor(a).nullCount > 0
  }

  val partitionFilters: Seq[Expression] = {
    predicates.flatMap { p =>
      val filter = buildFilter.lift(p)
      val boundFilter =
        filter.map(
          BindReferences.bindReference(
            _,
            relation.partitionStatistics.schema,
            allowFailures = true))

      boundFilter.foreach(_ =>
        filter.foreach(f => logInfo(s"Predicate $p generates partition filter: $f")))

      // If the filter can't be resolved then we are missing required statistics.
      boundFilter.filter(_.resolved)
    }
  }

  lazy val enableAccumulators: Boolean =
    sqlContext.getConf("spark.sql.inMemoryTableScanStatistics.enable", "false").toBoolean

  // Accumulators used for testing purposes
  lazy val readPartitions: Accumulator[Int] = sparkContext.accumulator(0)
  lazy val readBatches: Accumulator[Int] = sparkContext.accumulator(0)

  private val inMemoryPartitionPruningEnabled = sqlContext.conf.inMemoryPartitionPruning

  protected override def doExecute(): RDD[Row] = {
    if (enableAccumulators) {
      readPartitions.setValue(0)
      readBatches.setValue(0)
    }

    val tableName = relation.tableName.getOrElse(throw sys.error(
      "InMemoryMutableColumnarScan: tableName not found"))
    //new CachedRDD()(sqlContext).
    relation.cachedColumnBuffers.mapPartitions { emptyRowIterator =>
      val partitionFilter = newPredicate(
        partitionFilters.reduceOption(And).getOrElse(Literal(true)),
        relation.partitionStatistics.schema)

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

      def cachedBatchesToRows(cacheBatches: Iterator[CachedBatch]): Iterator[Row] = {
        val rows = cacheBatches.flatMap { cachedBatch =>
          //cachedBatch.buffers.flatMap(buf => {
          // Build column accessors
          val columnAccessors = requestedColumnIndices.map { batchColumnIndex =>
            ColumnAccessor(
              relation.output(batchColumnIndex).dataType,
              ByteBuffer.wrap(cachedBatch.buffers(batchColumnIndex)))
          }

          // Extract rows via column accessors
          new Iterator[Row] {
            private[this] val rowLen = nextRow.length

            override def next(): Row = {
              var i = 0
              while (i < rowLen) {
                columnAccessors(i).extractTo(nextRow, i)
                i += 1
              }
              nextRow
            }

            override def hasNext: Boolean = columnAccessors(0).hasNext
          }
          //})
        }

        if (rows.hasNext && enableAccumulators) {
          readPartitions += 1
        }

        rows
      }

      // obtain the CachedBatches locally
      cachedBatchesToRows(InMemoryLocalBlockHolder.getTableData[CachedBatch](tableName).iterator)
    }
  }
}
*/
