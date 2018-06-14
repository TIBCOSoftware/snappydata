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
package org.apache.spark.sql.execution.columnar.impl

import java.util.function.LongFunction

import scala.collection.mutable.ArrayBuffer

import com.gemstone.gemfire.cache.RegionDestroyedException
import com.gemstone.gemfire.internal.cache.DiskBlockSortManager.DiskBlockSorter
import com.gemstone.gemfire.internal.cache.DistributedRegion.{DiskEntryPage, DiskPosition}
import com.gemstone.gemfire.internal.cache._
import com.gemstone.gemfire.internal.cache.store.SerializedDiskBuffer
import com.gemstone.gemfire.internal.concurrent.CustomEntryConcurrentHashMap
import com.gemstone.gemfire.internal.shared.FetchRequest
import com.google.common.primitives.Ints
import com.koloboke.function.LongObjPredicate
import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.ddl.resolver.GfxdPartitionByExpressionResolver
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.internal.engine.store.GemFireContainer
import com.pivotal.gemfirexd.internal.iapi.util.ReuseFactory
import io.snappydata.collection.LongObjectHashMap

import org.apache.spark.sql.catalyst
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.expressions.{Ascending, BindReferences, BoundReference, Descending, Expression, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.columnar.encoding.{BitSet, ColumnStatsSchema}
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatEntry._
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.Platform

/**
 * A customized iterator for column store tables that projects out the required
 * columns and returns those column batches first that have all their columns
 * in the memory. Further this will make use of [[DiskBlockSortManager]] to
 * allow for concurrent partition iterators to do cross-partition disk block
 * sorting and fault-in for best disk read performance (SNAP-2012).
 *
 * @param baseRegion usually the first bucket region being iterated
 * @param projection array of projected columns (1-based, excluding delta or meta-columns)
 */
final class ColumnFormatIterator(baseRegion: LocalRegion, projection: Array[Int],
    fullScan: Boolean, txState: TXState)
    extends ClusteredColumnIterator with DiskRegionIterator {

  type MapValueIterator =
    CustomEntryConcurrentHashMap[AnyRef, AbstractRegionEntry]#ValueIterator

  private val distributedRegion = if (baseRegion.isUsedForPartitionedRegionBucket) {
    baseRegion.getPartitionedRegion
  } else baseRegion
  private var currentRegion: LocalRegion = _
  private var entryIterator: MapValueIterator = _
  private var currentDiskSorter: DiskBlockSorter = _
  private var diskEnumerator: DiskBlockSorter#ReaderIdEnumerator = _
  private var currentDiskBatch: DiskMultiColumnBatch = _
  private var nextDiskBatch: DiskMultiColumnBatch = _

  /**
   * The current set of in-memory batches being iterated.
   */
  private val inMemoryBatches = new java.util.ArrayList[LongObjectHashMap[AnyRef]](4)
  private var inMemoryBatchIndex: Int = _
  private var inMemorySortedBatches: Array[(InternalRow, LongObjectHashMap[AnyRef])] = _

  private val container = distributedRegion.getUserAttribute
      .asInstanceOf[GemFireContainer]
  /**
   * TODO VB: restrict its usage in only two cases :
   * 1. Case of Delta Insert
   * 2. case of Colocated join
   */
  val columnTableSorting = container.fetchHiveMetaData(false).columnTableSortOrder
  private val isColumnBatchSorted = StoreUtils.isColumnBatchSortedAscending(columnTableSorting) ||
      StoreUtils.isColumnBatchSortedDescending(columnTableSorting)

  private val canOverflow = !isColumnBatchSorted &&
    distributedRegion.isOverflowEnabled && distributedRegion.getDataPolicy.withPersistence()

  private val (partitioningProjection, statsLen, partitioningOrdering) = if (isColumnBatchSorted) {
    val rowBufferTable = GemFireContainer.getRowBufferTableName(container.getQualifiedTableName)
    val rowBufferRegion = Misc.getRegionForTable(rowBufferTable, true).asInstanceOf[LocalRegion]
    val paritioningPositions = GemFireXDUtils.getResolver(rowBufferRegion)
        .asInstanceOf[GfxdPartitionByExpressionResolver].getColumnPositions
    val tableSchema = container.fetchHiveMetaData(false).schema.asInstanceOf[StructType]
    val statsSchema = tableSchema.map(f =>
      ColumnStatsSchema(f.name, f.dataType, nullCountNullable = true))
    val fullStatsSchema = ColumnStatsSchema.COUNT_ATTRIBUTE +: statsSchema.flatMap(_.schema)
    val partitioningExprs = paritioningPositions.map(pos => statsSchema(pos - 1).lowerBound).
        map(ae => {
          BindReferences.bindReference(ae.asInstanceOf[Expression], fullStatsSchema).
            asInstanceOf[BoundReference]
        })
    val ordering = if (StoreUtils.isColumnBatchSortedAscending(columnTableSorting)) {
      GenerateOrdering.generate(partitioningExprs.map(SortOrder(_, Ascending)))
    } else GenerateOrdering.generate(partitioningExprs.map(SortOrder(_, Descending)))
    (UnsafeProjection.create(partitioningExprs), fullStatsSchema.length, ordering)
  } else (null, 0, null)

  private val projectionBitSet = {
    if (projection.length > 0) {
      val maxProjection = Ints.max(projection: _*)
      val bitset = new Array[Long](UnsafeRow.calculateBitSetWidthInBytes(maxProjection) >>> 3)
      for (p <- projection) {
        BitSet.set(bitset, Platform.LONG_ARRAY_OFFSET, p)
      }
      bitset
    } else ReuseFactory.getZeroLenLongArray
  }

  private val newMapCreator = new LongFunction[LongObjectHashMap[AnyRef]] {
    override def apply(uuid: Long): LongObjectHashMap[AnyRef] =
      LongObjectHashMap.withExpectedSize[AnyRef](projection.length *
          // + 2 due to RegionEntry also being put
          (ColumnDelta.USED_MAX_DEPTH + 2) - DELETE_MASK_COL_INDEX)
  }

  private lazy val (readerId, diskPosition, diskEntries) = {
    if (canOverflow) {
      val sortManager = distributedRegion.getDiskStore.getSortManager
      currentDiskSorter = sortManager.getSorter(distributedRegion, fullScan, null)
      (sortManager.newReaderId(), new DiskPosition, new java.util.ArrayList[DiskEntryPage]())
    } else (0, null, null)
  }

  // start iteration with the first provided region
  setRegion(baseRegion)

  private def switchDiskBlockSorter(): Unit = {
    currentDiskSorter = distributedRegion.getDiskStore.getSortManager.getSorter(
      distributedRegion, fullScan, diskEntries)
  }

  private def checkRegion(region: LocalRegion): Unit = {
    try {
      region.checkReadiness()
    } catch {
      case e: RegionDestroyedException => if (region.isUsedForPartitionedRegionBucket) {
        region.getPartitionedRegion.checkReadiness()
        throw new BucketNotFoundException(e.getMessage)
      } else throw e
    }
  }

  override def setRegion(region: LocalRegion): Unit = {
    // check if the region is available till the end of its iteration
    if (currentRegion ne null) {
      checkRegion(currentRegion)
    }
    checkRegion(region)
    currentRegion = region
    entryIterator = region.entries.regionEntries().iterator().asInstanceOf[MapValueIterator]
    if (isColumnBatchSorted) {
      inMemorySortedBatches = initSortedBatchSets()
    } else advanceToNextBatchSet()
  }

  override def initDiskIterator(): Boolean = {
    entryIterator = null
    if (canOverflow) {
      val numDiskEntries = diskEntries.size()
      if (numDiskEntries == 0) false
      else {
        // wait as per number of entries but subject to a max limit
        val maxWaitMillis = Math.min(500L, numDiskEntries >>> 2)
        // if not a full scan then force fault-ins
        diskEnumerator = currentDiskSorter.enumerate(readerId, !fullScan, maxWaitMillis)
        nextDiskBatch = diskEnumerator.nextElement().asInstanceOf[DiskMultiColumnBatch]
        diskEntries.clear()
        true
      }
    } else false
  }

  override def hasNext: Boolean = {
    if (isColumnBatchSorted) {
      inMemoryBatchIndex + 1 < inMemorySortedBatches.length
    } else if (entryIterator ne null) {
      if (inMemoryBatchIndex + 1 < inMemoryBatches.size()) true else advanceToNextBatchSet()
    } else nextDiskBatch ne null
  }

  override def next(): RegionEntry = {
    if (isColumnBatchSorted) {
      inMemoryBatchIndex += 1
      if (inMemoryBatchIndex >= inMemorySortedBatches.length) {
        if (!advanceToNextBatchSet()) throw new NoSuchElementException
      }
      val map = inMemorySortedBatches(inMemoryBatchIndex)
      map._2.getGlobalState.asInstanceOf[RegionEntry]
    } else if (entryIterator ne null) {
      inMemoryBatchIndex += 1
      if (inMemoryBatchIndex >= inMemoryBatches.size()) {
        if (!advanceToNextBatchSet()) throw new NoSuchElementException
      }
      val map = inMemoryBatches.get(inMemoryBatchIndex)
      map.getGlobalState.asInstanceOf[RegionEntry]
    } else if (nextDiskBatch ne null) {
      if (currentDiskBatch ne null) currentDiskBatch.release()
      currentDiskBatch = nextDiskBatch
      nextDiskBatch = diskEnumerator.nextElement().asInstanceOf[DiskMultiColumnBatch]
      currentDiskBatch.getEntry
    } else {
      close()
      throw new NoSuchElementException
    }
  }

  override def getColumnValue(columnIndex: Int): AnyRef = {
    val column = columnIndex & 0xffffffffL
    if (isColumnBatchSorted) {
      inMemorySortedBatches(inMemoryBatchIndex)._2.get(column)
    } else if (entryIterator ne null) inMemoryBatches.get(inMemoryBatchIndex).get(column)
    else if (columnIndex == DELTA_STATROW_COL_INDEX) currentDiskBatch.getDeltaStatsValue
    else currentDiskBatch.entryMap.get(column)
  }

  override def close(): Unit = {
    if (currentDiskBatch ne null) {
      currentDiskBatch.release()
      currentDiskBatch = null
    }
  }

  private def setValue(entry: RegionEntry, columnIndex: Int,
      uuidMap: LongObjectHashMap[AnyRef]): Unit = {
    var v = entry.getValue(currentRegion)
    if (v eq null) {
      checkRegion(currentRegion)
      // try once more
      v = entry.getValue(currentRegion)
    }
    if (v ne null) uuidMap.justPut(columnIndex & 0xffffffffL, v)
  }

  def advanceToNextBatchSet(): Boolean = {
    inMemoryBatches.clear()
    inMemoryBatchIndex = -1
    while (entryIterator.hasNext) {
      /**
       * Maintains the current set of batches that are being iterated.
       * When all columns provided in the projectionBitSet have been marked as [[inMemoryBatches]]
       * or sent to [[currentDiskSorter]], then the batch is cleared from the map.
       */
      val activeBatches = LongObjectHashMap.withExpectedSize[LongObjectHashMap[AnyRef]](4)

      // iterate till next map index since all columns of the same batch
      // are guaranteed to be in the same index
      val mapIndex = entryIterator.getMapTableIndex
      while (entryIterator.hasNext && mapIndex == entryIterator.getMapTableIndex) {
        val aEntry = entryIterator.next()
        var entry: RegionEntry = aEntry
        val key = aEntry.getRawKey.asInstanceOf[ColumnFormatKey]
        // check if it is one of required projection columns, their deltas or meta-columns
        val columnIndex = key.columnIndex
        if ((columnIndex < 0 && columnIndex >= DELETE_MASK_COL_INDEX) || {
          val tableColumn = ColumnDelta.tableColumnIndex(columnIndex)
          tableColumn > 0 &&
              BitSet.isSet(projectionBitSet, Platform.LONG_ARRAY_OFFSET,
                tableColumn, projectionBitSet.length)
        }) {
          // note that the map used below uses value==0 to indicate free, so the
          // column indexes have to be 1-based (and negative for deltas/meta-data)
          // and so the same values as that stored in ColumnFormatKey are used
          val uuidMap = activeBatches.computeIfAbsent(key.uuid, newMapCreator)
          // set the stats entry in the state
          if (columnIndex == STATROW_COL_INDEX) {
            if (uuidMap.getGlobalState eq null) uuidMap.setGlobalState(entry)
            // put the stats entry in the map in any case for possible use by disk iterator
            if (canOverflow) uuidMap.justPut((1L << 32) | (columnIndex & 0xffffffffL), entry)
          } else {
            // fetch the TX snapshot entry; the stats row entry is skipped here
            // since that will be done by higher-level PR iterator that returns
            // the stats row entry
            if (txState ne null) {
              entry = txState.getLocalEntry(distributedRegion, currentRegion,
                -1 /* not used */ , aEntry, false).asInstanceOf[RegionEntry]
            }
            if (canOverflow) {
              // for in-memory entries, optimistically get the value for the entry and
              // put in the map but for the case if there are overflowed entries for
              // this batch then need to replace the values with RegionEntries, so put
              // those too in the map but with a different key by setting its MSB as 1
              uuidMap.justPut((1L << 32) | (columnIndex & 0xffffffffL), entry)
              // check and mark if any entry is overflowed to disk
              if (uuidMap.getGlobalState ne None) {
                if (entry.isValueNull) {
                  // indicate overflowed entries with globalState as None in the map
                  uuidMap.setGlobalState(None)
                } else setValue(entry, columnIndex, uuidMap)
              }
            } else setValue(entry, columnIndex, uuidMap)
          }
        }
      }

      // if there are entries that are overflowed, then pass them to the disk sorter
      // while entries that are fully in memory are stored and returned
      if (activeBatches.size() > 0) {
        if (canOverflow) {
          activeBatches.forEachWhile(new LongObjPredicate[LongObjectHashMap[AnyRef]] {
            override def test(uuid: Long, map: LongObjectHashMap[AnyRef]): Boolean = {
              // check if map has overflowed entries
              if (map.getGlobalState eq None) {
                val diskBatch = new DiskMultiColumnBatch(currentRegion, readerId,
                  new Array[AnyRef](map.size()))
                // collect all the overflowed entries and push those into diskBatch
                // which will sort them to find the minimum oplog+offset and also
                // use the same for iteration order for best performance
                map.forEachWhile(new LongObjPredicate[AnyRef] {
                  override def test(columnIndex: Long, entry: AnyRef): Boolean = {
                    // skip ColumnValues
                    if ((columnIndex & 0xffffffff00000000L) != 0) {
                      diskBatch.addEntry(diskPosition, entry.asInstanceOf[RegionEntry])
                    }
                    true
                  }
                })
                diskBatch.finish()
                // add the new multi-column disk batch to sorter
                diskEntries.add(diskBatch)
                if (!currentDiskSorter.addEntry(diskBatch)) {
                  switchDiskBlockSorter()
                }
              } else if (map.getGlobalState ne null) {
                inMemoryBatches.add(map)
              }
              true
            }
          })
        } else {
          activeBatches.forEachWhile(new LongObjPredicate[LongObjectHashMap[AnyRef]] {
            override def test(uuid: Long, map: LongObjectHashMap[AnyRef]): Boolean = {
              if (map.getGlobalState ne null) inMemoryBatches.add(map)
              true
            }
          })
        }
        if (!inMemoryBatches.isEmpty) return true
      }
    }
    false
  }

  def initSortedBatchSets(): Array[(InternalRow, LongObjectHashMap[AnyRef])] = {
    val inMemorySortedBatchBuffer = new ArrayBuffer[(InternalRow, LongObjectHashMap[AnyRef])]()
    inMemoryBatchIndex = -1
    while (entryIterator.hasNext) {
      /**
       * Maintains the current set of batches that are being iterated.
       * When all columns provided in the projectionBitSet have been marked as
       * [[inMemorySortedBatchBuffer]] then the batch is cleared from the map.
       */
      val activeBatches = LongObjectHashMap.withExpectedSize[LongObjectHashMap[AnyRef]](4)
      val partitionRows = LongObjectHashMap.withExpectedSize[InternalRow](4)

      // iterate till next map index since all columns of the same batch
      // are guaranteed to be in the same index
      val mapIndex = entryIterator.getMapTableIndex
      while (entryIterator.hasNext && mapIndex == entryIterator.getMapTableIndex) {
        val aEntry = entryIterator.next()
        var entry: RegionEntry = aEntry
        val key = aEntry.getRawKey.asInstanceOf[ColumnFormatKey]
        // check if it is one of required projection columns, their deltas or meta-columns
        val columnIndex = key.columnIndex
        if ((columnIndex < 0 && columnIndex >= DELETE_MASK_COL_INDEX) || {
          val tableColumn = ColumnDelta.tableColumnIndex(columnIndex)
          tableColumn > 0 &&
              BitSet.isSet(projectionBitSet, Platform.LONG_ARRAY_OFFSET,
                tableColumn, projectionBitSet.length)
        }) {
          // note that the map used below uses value==0 to indicate free, so the
          // column indexes have to be 1-based (and negative for deltas/meta-data)
          // and so the same values as that stored in ColumnFormatKey are used
          val uuidMap = activeBatches.computeIfAbsent(key.uuid, newMapCreator)
          // set the stats entry in the state
          if (columnIndex == STATROW_COL_INDEX) {
            if (uuidMap.getGlobalState eq null) uuidMap.setGlobalState(entry)
            val statsValue = entry.getValue(currentRegion).asInstanceOf[ColumnFormatValue]
            val statsVal = statsValue.getValueRetain(FetchRequest.DECOMPRESS)
            try {
              val statsRow = Utils.toUnsafeRow(statsVal.getBuffer, statsLen)
              partitionRows.justPut(key.uuid, partitioningProjection(statsRow).copy())
            } finally {
              statsValue.release()
            }
          } else {
            // fetch the TX snapshot entry; the stats row entry is skipped here
            // since that will be done by higher-level PR iterator that returns
            // the stats row entry
            if (txState ne null) {
              entry = txState.getLocalEntry(distributedRegion, currentRegion,
                -1 /* not used */ , aEntry, false).asInstanceOf[RegionEntry]
            }
            setValue(entry, columnIndex, uuidMap)
          }
        }
      }

      // if there are entries that are overflowed, then pass them to the disk sorter
      // while entries that are fully in memory are stored and returned
      if (activeBatches.size() > 0) {
        activeBatches.forEachWhile(new LongObjPredicate[LongObjectHashMap[AnyRef]] {
          override def test(uuid: Long, map: LongObjectHashMap[AnyRef]): Boolean = {
            if (map.getGlobalState ne null) {
              inMemorySortedBatchBuffer += partitionRows.get(uuid) -> map
            }
            true
          }
        })
      }
    }
    val unsorted = inMemorySortedBatchBuffer.toArray
    // TODO VB: Discuss with Sumedh for using partitioningOrdering
    // val sorted = unsorted.sortBy(_._1)(partitioningOrdering)
    val sorted = unsorted.sortBy(_._1)(new TemporaryRowComparator)
    sorted
  }
}

private final class TemporaryRowComparator extends Ordering[InternalRow] {

  @Override
  def compare(r1: catalyst.InternalRow, r2: catalyst.InternalRow): Int = {
    val a = r1.getInt(0)
    val b = r2.getInt(0)
    r1.getInt(0).compareTo(r2.getInt(0))
  }
}

/**
 * This class is to enable clustering of same column batch entries together with
 * the minimum oplog+offset for a column being returned in sorter. Usually one
 * expects the other column blocks to be near contiguous but in some extreme cases
 * they may not be in which case there may be some jumping around while reading
 * which is unavoidable in current scheme because scan has to read all required
 * columns together.
 */
private final class DiskMultiColumnBatch(_region: LocalRegion, _readerId: Int,
    private var diskEntries: Array[AnyRef]) extends DiskEntryPage(null, _region, _readerId) {

  private var arrayIndex: Int = _
  private var faultIn: Boolean = _
  private var closing: Boolean = _
  // track delta stats separately since it is required for stats filtering
  // and should not lead to other columns getting read from disk (or worse faulted in)
  private var deltaStatsEntry: RegionEntry = _

  private[impl] lazy val entryMap: LongObjectHashMap[AnyRef] = {
    if (closing) null
    else {
      // read all the entries in this column batch to fault them in or read without
      // fault-in at this point to build the temporary column to value map for this batch
      val map = LongObjectHashMap.withExpectedSize[AnyRef](arrayIndex)
      var i = 0
      while (i < arrayIndex) {
        val entry = diskEntries(i)
        val re = entry.asInstanceOf[RegionEntry]
        val v = if (faultIn) {
          val v = re.getValue(region)
          if (GemFireCacheImpl.hasNewOffHeap) v match {
            // do an explicit retain to match the behaviour of getValueInVMOrDiskWithoutFaultIn
            case s: SerializedDiskBuffer => s.retain(); s
            case _ => v
          } else v
        } else re.getValueInVMOrDiskWithoutFaultIn(region)
        map.justPut(getKey(re).columnIndex & 0xffffffffL, v)
        i += 1
      }
      diskEntries = null
      map
    }
  }

  private def getKey(entry: RegionEntry): ColumnFormatKey =
    entry.getRawKey.asInstanceOf[ColumnFormatKey]

  def getDeltaStatsValue: AnyRef =
    if (deltaStatsEntry ne null) deltaStatsEntry.getValue(region) else null

  def addEntry(diskPosition: DiskPosition, entry: RegionEntry): Unit = {
    // store the stats entry separately to provide to top-level iterator
    val key = getKey(entry)
    if (key.columnIndex == STATROW_COL_INDEX) {
      this.entry = entry
    } else if (key.columnIndex == DELTA_STATROW_COL_INDEX) {
      this.deltaStatsEntry = entry
    } else {
      // fetch disk position even for in-memory entries because they are likely to
      // be overflowed by the time iterator gets to them (and if not then memory
      //   read will be fast in any case)
      entry.isOverflowedToDisk(region, diskPosition, true)
      diskEntries(arrayIndex) = new DiskEntryPage(diskPosition, entry, region)
      arrayIndex += 1
    }
  }

  def finish(): Unit = {
    if (arrayIndex > 0) {
      // generally small size to sort so will be done efficiently in-place by the normal
      // sorter and hence not using the GemXD TimSort that reuses potentially large arrays
      java.util.Arrays.sort(diskEntries, 0, arrayIndex, DiskEntryPage.DEPComparator.instance)
      // replace the DiskEntryPage objects with RegionEntry to release the extra memory
      var i = 0
      while (i < arrayIndex) {
        val diskEntry = diskEntries(i).asInstanceOf[DiskEntryPage]
        // set the minimum position as the one to be used for this multi-column batch
        if (i == 0) setPosition(diskEntry.getOplogId, diskEntry.getOffset)
        diskEntries(i) = diskEntry.getEntry
        i += 1
      }
    }
  }

  override protected def readEntryValue(): AnyRef = {
    // mark the entryMap for fault-in
    faultIn = true
    closing = false
    super.readEntryValue()
  }

  private[impl] def release(): Unit = {
    closing = true
    val entryMap = this.entryMap
    if ((entryMap ne null) && entryMap.size() > 0) {
      if (GemFireCacheImpl.hasNewOffHeap) entryMap.forEachWhile(new LongObjPredicate[AnyRef] {
        override def test(i: Long, v: AnyRef): Boolean = {
          v match {
            case s: SerializedDiskBuffer => s.release()
            case _ =>
          }
          true
        }
      })
      entryMap.clear()
    }
  }
}
