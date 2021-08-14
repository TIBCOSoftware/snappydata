/*
 * Copyright (c) 2017-2021 TIBCO Software Inc. All rights reserved.
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

package org.apache.spark.sql.execution.columnar.impl

import java.nio.ByteBuffer
import java.util.function.BiFunction
import java.util.{Collections, NoSuchElementException}

import com.gemstone.gemfire.internal.cache.{BucketRegion, ExternalTableMetaData, LocalRegion, RegionEntry, TXManagerImpl, TXStateInterface}
import com.gemstone.gemfire.internal.concurrent.ConcurrentHashSet
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer
import io.snappydata.Property.{ColumnCompactionRatio, ColumnUpdateCompactionRatio, SerializeWrites}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.collection.{LazyIterator, Utils}
import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoding
import org.apache.spark.sql.execution.columnar.{ColumnBatchIterator, ColumnInsertExec, ColumnTableScan, ExternalStore, ExternalStoreUtils}
import org.apache.spark.sql.execution.row.ResultSetTraversal
import org.apache.spark.sql.execution.{BufferedRowIterator, WholeStageCodegenExec}
import org.apache.spark.sql.store.CodeGeneration
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Logging, SparkEnv}

/**
 * Compact column batches, if required, and insert new compacted column batches,
 * or if they are too small then push into row delta buffer.
 */
object ColumnCompactor extends Logging {

  /**
   * Track the keys being currently compacted to check for concurrent compactions.
   * While the default as true for "snappydata.sql.serializeWrites" will prevent multiple
   * concurrent operations, a single operation can have multiple tasks on the same bucket
   * but should never have overlapping keys.
   */
  private[this] val compactionsInProgress = new ConcurrentHashSet[ColumnFormatKey]()

  private[this] lazy val compactionRatio: Double = try {
    SparkEnv.get match {
      case null => System.getProperty(ColumnCompactionRatio.name,
        ColumnCompactionRatio.defaultValue.get.toString).toDouble
      case env => ColumnCompactionRatio.get(env.conf)
    }
  } catch {
    case e: Exception =>
      val sysProp = System.getProperty(ColumnCompactionRatio.name)
      val msgPart =
        if (sysProp ne null) s"as system property = $sysProp"
        else "in spark configuration"
      val defaultValue = ColumnCompactionRatio.defaultValue.get
      logError(s"Ignoring invalid value of ${ColumnCompactionRatio.name} $msgPart" +
          s"(reverting to default of $defaultValue): $e")
      defaultValue
  }

  private[this] lazy val updateCompactionRatio: Double = try {
    SparkEnv.get match {
      case null => System.getProperty(ColumnUpdateCompactionRatio.name,
        ColumnUpdateCompactionRatio.defaultValue.get.toString).toDouble
      case env => ColumnUpdateCompactionRatio.get(env.conf)
    }
  } catch {
    case e: Exception =>
      val sysProp = System.getProperty(ColumnUpdateCompactionRatio.name)
      val msgPart =
        if (sysProp ne null) s"as system property = $sysProp"
        else "in spark configuration"
      val defaultValue = ColumnUpdateCompactionRatio.defaultValue.get
      logError(s"Ignoring invalid value of ${ColumnUpdateCompactionRatio.name} $msgPart" +
          s"(reverting to default of $defaultValue): $e")
      defaultValue
  }

  private def getSchema(metadata: ExternalTableMetaData): StructType = {
    metadata.schema.asInstanceOf[StructType]
  }

  /**
   * Check if compaction of a batch with given updated/deleted rows and total is required.
   */
  private[columnar] def isCompactionRequired(numDeltaRows: Int, numBatchRows: Int,
      forDelete: Boolean): Boolean = {
    assert(numDeltaRows <= numBatchRows)
    if (forDelete) {
      compactionRatio < 1.0 && compactionRatio * numBatchRows.toDouble <= numDeltaRows.toDouble
    } else {
      updateCompactionRatio < 1.0 &&
          updateCompactionRatio * numBatchRows.toDouble <= numDeltaRows.toDouble
    }
  }

  /**
   * Returns Some(true) if all the rows in a batch have been deleted, Some(false) if the
   * batch needs to be compacted, and None if neither.
   */
  private[columnar] def batchDeleteOrCompact(deleteBuffer: ByteBuffer): Option[Boolean] = {
    val allocator = ColumnEncoding.getAllocator(deleteBuffer)
    val bufferBytes = allocator.baseObject(deleteBuffer)
    val bufferCursor = allocator.baseOffset(deleteBuffer) + 4
    val numBaseRows = ColumnEncoding.readInt(bufferBytes, bufferCursor)
    val numDeletes = ColumnEncoding.readInt(bufferBytes, bufferCursor + 4)
    if (numDeletes >= numBaseRows) Some(true)
    else if (isCompactionRequired(numDeletes, numBaseRows, forDelete = true)) Some(false)
    else None
  }

  /**
   * Check if all the rows in a batch have been deleted.
   */
  private[columnar] def checkBatchDeleted(deleteBuffer: ByteBuffer): Boolean = {
    batchDeleteOrCompact(deleteBuffer).contains(true)
  }

  def getValidTransaction(expectedRolloverDisabled: Boolean): Option[TXStateInterface] = {
    // should only be invoked in the context of an active snapshot transaction,
    // and rollover should be disabled else the reader might see the newly inserted batch too
    val tx = TXManagerImpl.getCurrentTXState
    if ((tx eq null) || !tx.isSnapshot || !tx.isInProgress) {
      logError(s"ColumnCompactor: should only be invoked in the context of an " +
          s"active SNAPSHOT transaction (current is $tx)")
      None
    } else if (expectedRolloverDisabled != tx.getProxy.isColumnRolloverDisabled) {
      logError(s"ColumnCompactor: expected column rollover to be $expectedRolloverDisabled")
      None
    } else Option(tx)
  }

  /**
   * Perform compaction of the batch with given key.
   *
   * TODO: PERF: for delta compaction, merge and replace required columns only using
   * ColumnDeltaEncoder.merge rather than creating an entire new batch for best performance.
   * Also update the stats row for those columns.
   */
  def compact(key: ColumnFormatKey, bucket: BucketRegion): Boolean = {
    if (!bucket.getBucketAdvisor.isPrimary) return false

    // check that cache should be open (assert will never fail rather the getter itself will)
    assert(!Misc.getGemFireCache.isClosed)

    // rollover would have been enabled when compaction is actually executed
    val tx = getValidTransaction(expectedRolloverDisabled = false)
    if (tx.isEmpty) return false

    val statsKey = key.toStatsRowKey
    // check that no other compaction on the key should be going on in parallel
    if (!compactionsInProgress.add(statsKey)) {
      logError(s"A concurrent compaction is running on bucket ${bucket.getFullPath} for batch " +
          s"with $statsKey which can lead to loss of data. Is $SerializeWrites set to false?")
      return false
    }
    try {
      // compaction is just using ColumnTableScan to read and rows that are fed to ColumnInsertExec
      // which will do the requited inserts then delete the old batch
      val pr = bucket.getPartitionedRegion
      val container = pr.getUserAttribute.asInstanceOf[GemFireContainer]
      val metadata = container.fetchHiveMetaData(false)
      val schema = getSchema(metadata)
      val columnTableName = container.getQualifiedTableName
      val tableName = Misc.getFullTableNameFromRegionPath(pr.getColocatedWithRegion.getFullPath)
      logDebug(s"ColumnCompactor: compacting batch in bucket ${bucket.getFullPath} " +
          s"having $statsKey in $tableName, transaction: ${tx.get}")
      // we enable splitting into delta row buffer only for partitioned tables because the
      // delta buffer puts can go to remote nodes for non-partitioned tables breaking
      // per-partition snapshot isolation that can cause scans to temporarily see duplicates
      val maxDeltaRows = {
        if (metadata.partitioningColumns.length == 0) -1 else metadata.columnMaxDeltaRows
      }
      Utils.withThreadLocalTransactionForBucket(bucket.getId, pr, { _ =>
        Utils.withTempTaskContextIfAbsent {
          val gen = CodeGeneration.compileCode(
            CodeGeneration.compactKey(tableName), schema.fields, () => {
              val schemaAttrs = schema.toAttributes
              val tableScan = ColumnTableScan(schemaAttrs, dataRDD = null,
                otherRDDs = Nil, numBuckets = -1,
                partitionColumns = Nil, partitionColumnAliases = Nil,
                baseRelation = null, schema, allFilters = Nil, schemaAttrs,
                caseSensitive = true)
              // using isPartitioned=true so that the bucketId set in iter.init() takes effect
              val insertPlan = ColumnInsertExec(tableScan, Nil, Nil,
                numBuckets = -1, isPartitioned = true, None,
                (-metadata.columnBatchSize, maxDeltaRows, metadata.compressionCodec),
                columnTableName, onExecutor = true, schema,
                metadata.externalStore.asInstanceOf[ExternalStore], useMemberVariables = false)
              // now generate the code with the help of WholeStageCodegenExec
              // this is only used for local code generation while its RDD
              // semantics and related methods are all ignored
              val (ctx, code) = ExternalStoreUtils.codeGenOnExecutor(
                WholeStageCodegenExec(insertPlan), insertPlan)
              (code, ctx.references.toArray)
            })
          val iter = gen._1.generate(gen._2).asInstanceOf[BufferedRowIterator]

          // create an iterator for the single batch and pass to generated code
          iter.init(bucket.getId, Array(Iterator[Any](new LazyIterator(() =>
            new ResultSetTraversal(conn = null, stmt = null, rs = null, context = null,
              java.util.Collections.emptySet[Integer]())), new SingleColumnBatchIterator(
            statsKey, bucket)).asInstanceOf[Iterator[InternalRow]]))
          // if the insert count is zero (e.g. due to bucket just moved away) then don't delete
          // ignore the result which is the insert count
          var count = 0L
          while (iter.hasNext) {
            count += iter.next().getLong(0)
          }
          if (count > 0) {
            ColumnDelta.deleteBatch(statsKey, pr, schema.length)
            logDebug(s"ColumnCompactor: successfully compacted in bucket ${bucket.getFullPath} " +
                s"and deleted batch with $statsKey in $tableName")
          } else if (!bucket.getBucketAdvisor.isPrimary) {
            logWarning(s"ColumnCompactor: primary bucket ${bucket.getFullPath} moved " +
                s"for batch with $statsKey in $tableName")
          } else {
            logError(s"ColumnCompactor: missing batch in bucket ${bucket.getFullPath} " +
                s"for compaction of batch with $statsKey in $tableName")
          }
          count > 0
        }
      })
    } finally {
      compactionsInProgress.remove(statsKey)
    }
  }
}

/**
 * Provides a ColumnBatchIterator over a single column batch for [[ColumnTableScan]].
 */
final class SingleColumnBatchIterator(statsKey: ColumnFormatKey, bucket: BucketRegion)
    extends ColumnBatchIterator(bucket, batch = null, Collections.singleton(bucket.getId),
      Array.emptyIntArray, fullScan = true, context = null) {

  override protected def createIterator(container: GemFireContainer, region: LocalRegion,
      tx: TXStateInterface): PRIterator = {
    val txState = if (tx ne null) tx.getLocalTXState else null
    val createIterator = new BiFunction[BucketRegion, java.lang.Long,
        java.util.Iterator[RegionEntry]] {
      override def apply(br: BucketRegion,
          numEntries: java.lang.Long): java.util.Iterator[RegionEntry] = {
        new ClusteredColumnIterator {
          private[this] var _hasNext = true

          override def hasNext: Boolean = _hasNext

          override def next(): RegionEntry = {
            if (_hasNext) {
              _hasNext = false
              br.getRegionEntry(statsKey)
            } else throw new NoSuchElementException
          }

          override def getColumnValue(column: Int): AnyRef =
            br.get(statsKey.withColumnIndex(column), null)

          override def close(): Unit = {}
        }
      }
    }
    val createRemoteIterator = new BiFunction[java.lang.Integer, PRIterator,
        java.util.Iterator[RegionEntry]] {
      override def apply(bucketId: Integer,
          iter: PRIterator): java.util.Iterator[RegionEntry] = {
        Collections.emptyIterator[RegionEntry]()
      }
    }
    val pr = bucket.getPartitionedRegion
    new pr.PRLocalScanIterator(getBucketSet, txState, createIterator, createRemoteIterator,
      false, true, false)
  }
}

/**
 * Result of compaction of a column batch added to transaction pre-commit results.
 *
 * NOTE: if the layout of this class or ColumnFormatKey changes, then update the regex pattern in
 * SnapshotConnectionListener.parseCompactionResult that parses the toString() of this class
 */
case class CompactionResult(batchKey: ColumnFormatKey, bucketId: Int, success: Boolean)
