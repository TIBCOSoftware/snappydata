package org.apache.spark.sql.execution

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.mutable
import scala.language.reflectiveCalls

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{MutableRow, Row}
import org.apache.spark.sql.collection.{ChangeValue, GenerateFlatIterator, SegmentMap, Utils}
import org.apache.spark.sql.execution.StratifiedSampler._
import org.apache.spark.sql.types.StructType

/**
 * A stratified sampling implementation that uses a fraction and initial
 * cache size. Latter is used as the initial reservoir size per stratum
 * for reservoir sampling. It primarily tries to satisfy the fraction of
 * the total data repeatedly filling up the cache as required (and expanding
 * the cache size for bigger reservoir if required in next rounds).
 * The fraction is attempted to be satisfied while ensuring that the selected
 * rows are equally divided among the current stratum (for those that received
 * any rows, that is).
 */
final class StratifiedSamplerCached(override val qcs: Array[Int],
    override val name: String,
    override val schema: StructType,
    private val cacheSize: AtomicInteger,
    val fraction: Double,
    val cacheBatchSize: Int,
    val timeSeriesColumn: Int,
    val timeInterval: Int)
    extends StratifiedSampler(qcs, name, schema) {

  private val batchSamples, slotSize = new AtomicInteger
  /** Keeps track of the maximum number of samples in a strata seen so far */
  private val maxSamples = new AtomicLong
  // initialize timeSlotStart to MAX so that it will always be set first
  // time around for the slot (since every valid time will be less)
  private val timeSlotStart = new AtomicLong(Long.MaxValue)
  private val timeSlotEnd = new AtomicLong
  /**
   * Total number of samples collected in a timeInterval. Not atomic since
   * it is always updated and read under global lock.
   */
  private var timeSlotSamples = 0

  private def setTimeSlot(timeSlot: Long) {
    compareOrderAndSet(timeSlotStart, timeSlot, getMax = false)
    compareOrderAndSet(timeSlotEnd, timeSlot, getMax = true)
  }

  private def updateTimeSlot(row: Row, useCurrentTimeIfNoColumn: Boolean) {
    if (timeSeriesColumn >= 0) {
      val ts = row.get(timeSeriesColumn)
      val timeSlot =
        ts match {
          case tl: Long => tl
          case ti: Int => ti.toLong
          case td: java.util.Date => td.getTime
          case _ => throw new AnalysisException(
            s"StratifiedSampler: Cannot parse 'timeSeriesColumn'=$ts")
        }

      setTimeSlot(timeSlot)
    }
    // update the timeSlot if a) explicitly requested, or
    // b) in case if it has not been initialized at all
    else if (useCurrentTimeIfNoColumn || timeSlotEnd.get == 0L) {
      setTimeSlot(System.currentTimeMillis)
    }
  }

  private final class ProcessRows[U](val processSelected: Any => Any,
      val processFlush: (U, Row) => U,
      val endBatch: U => U, var result: U)
      extends ChangeValue[Row, StrataReservoir] {

    override def keyCopy(row: Row) = row.copy()

    override def defaultValue(row: Row): StrataReservoir = {
      val capacity = cacheSize.get
      val reservoir = new Array[MutableRow](capacity)
      reservoir(0) = newMutableRow(row, processSelected)
      Utils.fillArray(reservoir, EMPTY_ROW, 1, capacity)
      // for time-series data don't start with shortfall since this indicates
      // that a new stratum has appeared which can remain under-sampled for
      // a while till it doesn't get enough rows
      val initShortFall = if (timeInterval > 0) {
        // update timeSlot start and end
        updateTimeSlot(row, useCurrentTimeIfNoColumn = true)
        0
      } else math.max(0, maxSamples.get - capacity).toInt
      val sr = new StrataReservoir(1, 1, reservoir, 1, initShortFall)
      maxSamples.compareAndSet(0, 1)
      batchSamples.incrementAndGet()
      slotSize.incrementAndGet()
      sr
    }

    override def mergeValue(row: Row, sr: StrataReservoir): StrataReservoir = {
      // else update meta information in current strata
      sr.batchTotalSize += 1
      val reservoirCapacity = cacheSize.get + sr.prevShortFall
      if (sr.reservoirSize >= reservoirCapacity) {
        val rnd = rng.nextInt(sr.batchTotalSize)
        // pick up this row with probability of reservoirCapacity/totalSize
        if (rnd < reservoirCapacity) {
          // replace a random row in reservoir
          sr.reservoir(rng.nextInt(reservoirCapacity)) =
              newMutableRow(row, processSelected)
          // update timeSlot start and end
          if (timeInterval > 0) {
            updateTimeSlot(row, useCurrentTimeIfNoColumn = false)
          }
        }
        if (batchSamples.get >= (fraction * slotSize.incrementAndGet())) sr
        else null
      } else {
        // if reservoir has empty slots then fill them up first
        val reservoirLen = sr.reservoir.length
        if (reservoirLen <= sr.reservoirSize) {
          // new size of reservoir will be > reservoirSize given that it
          // increases only in steps of 1 and the expression
          // reservoirLen + (reservoirLen >>> 1) + 1 will certainly be
          // greater than reservoirLen
          val newReservoir = new Array[MutableRow](math.max(math.min(
            reservoirCapacity, reservoirLen + (reservoirLen >>> 1) + 1),
            cacheSize.get))
          Utils.fillArray(newReservoir, EMPTY_ROW, reservoirLen,
            newReservoir.length)
          System.arraycopy(sr.reservoir, 0, newReservoir,
            0, reservoirLen)
          sr.reservoir = newReservoir
        }
        sr.reservoir(sr.reservoirSize) = newMutableRow(row, processSelected)
        sr.reservoirSize += 1
        sr.totalSamples += 1

        // update timeSlot start and end
        if (timeInterval > 0) {
          updateTimeSlot(row, useCurrentTimeIfNoColumn = false)
        }

        compareOrderAndSet(maxSamples, sr.totalSamples, getMax = true)
        batchSamples.incrementAndGet()
        slotSize.incrementAndGet()
        sr
      }
    }

    override def segmentAbort(seg: SegmentMap[Row, StrataReservoir]) = {
      stratas.synchronized {
        // top-level synchronized above to avoid possible deadlocks with
        // segment locks if two threads are trying to drain cache concurrently

        // reset batch counters
        val nsamples = batchSamples.get
        if (nsamples > 0 && nsamples < (fraction * slotSize.get)) {
          result = flushCache(result, processFlush, endBatch)
        }
      }
      false
    }
  }

  private def flushCache[U](init: U, process: (U, Row) => U,
      // first acquire all the segment write locks
      // so no concurrent processors are in progress
      endBatch: U => U): U = stratas.writeLock { segs =>

    val nsamples = batchSamples.get
    val numStratas = stratas.size

    // if more than 50% of keys are empty, then clear the whole map
    val emptyStrata = segs.foldLeft(0)(
      _ + _.valuesIterator.count(_.reservoir.isEmpty))

    val prevCacheSize = this.cacheSize.get
    val timeInterval = this.timeInterval
    var fullReset = false
    if (timeInterval > 0) {
      timeSlotSamples += nsamples
      // update the timeSlot with current time if no timeSeries column
      // has been provided
      if (timeSeriesColumn < 0) {
        updateTimeSlot(null, useCurrentTimeIfNoColumn = true)
      }
      // in case the current timeSlot is over, reset maxSamples
      // (thus causing shortFall to clear up)
      val tsEnd = timeSlotEnd.get
      fullReset = (tsEnd != 0) && ((tsEnd - timeSlotStart.get) >=
          (timeInterval.toLong * 1000L))
      if (fullReset) {
        maxSamples.set(0)
        // reset timeSlot start and end
        timeSlotStart.set(Long.MaxValue)
        timeSlotEnd.set(0)
        // recalculate the reservoir size for next round as per the average
        // of total number of non-empty reservoirs
        val nonEmptyStrata = numStratas.toInt - emptyStrata
        if (nonEmptyStrata > 0) {
          val newCacheSize = timeSlotSamples / nonEmptyStrata
          // no need to change if it is close enough already
          if (math.abs(newCacheSize - prevCacheSize) > (prevCacheSize / 10)) {
            // try to be somewhat gentle in changing to new value
            this.cacheSize.set(math.max(4,
              (newCacheSize * 2 + prevCacheSize) / 3))
          }
        }
        timeSlotSamples = 0
      }
    }
    else {
      // increase the cacheSize for future data but not much beyond
      // cacheBatchSize
      val newCacheSize = prevCacheSize + (prevCacheSize >>> 1)
      if ((newCacheSize * numStratas) <= math.abs(this.cacheBatchSize) * 3) {
        this.cacheSize.set(newCacheSize)
      }
    }
    batchSamples.set(0)
    slotSize.set(0)

    // in case rows to be flushed do not fall into >50% of cacheBatchSize
    // then flush into pending list instead
    val batchSize = this.cacheBatchSize
    val pendingBatch = this.pendingBatch

    def processSegment(cacheSize: Int, fullReset: Boolean) =
      foldDrainSegment(cacheSize, fullReset, process) _

    val result = pendingBatch.writeLock {
      if (batchSize > 0) {
        // flush the pendingBatch at this point if possible
        val pendingLen = pendingBatch.length
        val totalSamples = pendingLen + nsamples
        if (((totalSamples % batchSize) << 1) > batchSize) {
          // full flush of samples and pendingBatch
          val u = pendingBatch.foldLeft(init)(process)
          pendingBatch.clear()
          endBatch(segs.foldLeft(u)(processSegment(prevCacheSize, fullReset)))
        }
        else if ((totalSamples << 1) > batchSize) {
          // some batches can be flushed but rest to be moved to pendingBatch
          val u = pendingBatch.foldLeft(init)(process)
          pendingBatch.clear()
          // now apply process on the reservoir cache as much as required
          // (as per the maximum multiple of batchSize) and copy remaining
          // into pendingBatch
          def processAndCopyToBuffer(cacheSize: Int, fullReset: Boolean,
              numToFlush: Int,
              buffer: mutable.ArrayBuffer[Row]) = {
            var remaining = numToFlush
            foldDrainSegment(cacheSize, fullReset, { (u: U, row) =>
              if (remaining == 0) {
                buffer += row
                u
              } else {
                remaining -= 1
                process(u, row)
              }
            }) _
          }
          val numToFlush = (totalSamples / batchSize) * batchSize -
              pendingLen
          assert(numToFlush >= 0,
            s"StratifiedSampler: unexpected numToFlush=$numToFlush")
          endBatch(segs.foldLeft(u)(processAndCopyToBuffer(prevCacheSize,
            fullReset, numToFlush, pendingBatch)))
        }
        else {
          // move collected samples into pendingBatch
          segs.foldLeft(pendingBatch)(foldDrainSegment(prevCacheSize,
            fullReset, _ += _))
          init
        }
      }
      else {
        endBatch(segs.foldLeft(init)(processSegment(prevCacheSize, fullReset)))
      }
    }
    if (numStratas < (emptyStrata << 1)) {
      stratas.clear()
    }
    result
  }

  override protected def strataReservoirSize: Int = cacheSize.get

  override def append[U](rows: Iterator[Row], processSelected: Any => Any,
      init: U, processFlush: (U, Row) => U,
      endBatch: U => U): U = {
    if (rows.hasNext) {
      val processedResult = new ProcessRows(processSelected, processFlush,
        endBatch, init)
      stratas.bulkChangeValues(rows, processedResult)
      processedResult.result
    } else init
  }

  override def sample(items: Iterator[Row], flush: Boolean): Iterator[Row] = {
    // use "batchSize" to determine the sample buffer size
    val batchSize = BUFSIZE
    val sampleBuffer = new mutable.ArrayBuffer[Row](math.min(batchSize,
      (batchSize * fraction * 10).toInt))

    new GenerateFlatIterator[Row, Boolean](finished => {
      val sbuffer = sampleBuffer
      if (sbuffer.nonEmpty) sbuffer.clear()
      val processRows = new ProcessRows[Unit](null, (_, sampledRow) => {
        sbuffer += sampledRow
      }, identity, ())

      var flushed = false
      while (!flushed && items.hasNext) {
        if (stratas.changeValue(items.next(), processRows) == null) {
          processRows.segmentAbort(null)
          flushed = sbuffer.nonEmpty
        }
      }

      if (sbuffer.nonEmpty) {
        (sbuffer.iterator, false)
      }
      else if (finished || !flush) {
        // if required notify any other waiting samplers that iteration is done
        if (numSamplers.decrementAndGet() == 1) numSamplers.synchronized {
          numSamplers.notifyAll()
        }
        (GenerateFlatIterator.TERMINATE, true)
      }
      // flush == true
      else {
        // wait for all other partitions to flush the cache
        waitForSamplers(1, 5000)
        setFlushStatus(true)
        // remove sampler used only for DataFrame => DataFrame transformation
        removeSampler(name, markFlushed = true)
        flushCache[Unit]((), (_, sampledRow) => {
          sbuffer += sampledRow
        }, identity)
        (sbuffer.iterator, true)
      }
    }, false)
  }

  override def clone: StratifiedSamplerCached = new StratifiedSamplerCached(
    qcs, name, schema, cacheSize, fraction, cacheBatchSize,
    timeSeriesColumn, timeInterval)
}
