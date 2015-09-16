package org.apache.spark.sql.execution

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.mutable
import scala.language.reflectiveCalls

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.collection._
import org.apache.spark.sql.execution.StratifiedSampler._
import org.apache.spark.sql.sources.CastLongTime
import org.apache.spark.sql.types.{DataType, StructType}

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
final class StratifiedSamplerCached(_qcs: Array[Int],
    _name: String,
    _schema: StructType,
    private val cacheSize: AtomicInteger,
    val fraction: Double,
    val cacheBatchSize: Int,
    val timeSeriesColumn: Int,
    val timeInterval: Long)
    extends StratifiedSampler(_qcs, _name, _schema) with CastLongTime {

  private val batchSamples, slotSize = new AtomicInteger
  /** Keeps track of the maximum number of samples in a stratum seen so far */
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

  override def timeColumnType: Option[DataType] = {
    if (timeSeriesColumn >= 0) {
      Some(schema(timeSeriesColumn).dataType)
    } else {
      None
    }
  }

  private def setTimeSlot(timeSlot: Long) {
    compareOrderAndSet(timeSlotStart, timeSlot, getMax = false)
    compareOrderAndSet(timeSlotEnd, timeSlot, getMax = true)
  }

  private def updateTimeSlot(row: Row,
      useCurrentTimeIfNoColumn: Boolean): Unit = {
    val tsCol = timeSeriesColumn
    if (tsCol >= 0) {
      val timeVal = parseMillis(row, tsCol)
      if (timeVal > 0) setTimeSlot(timeVal)
    }
    // update the timeSlot if a) explicitly requested, or
    // b) in case if it has not been initialized at all
    else if (useCurrentTimeIfNoColumn || timeSlotEnd.get == 0L) {
      setTimeSlot(System.currentTimeMillis)
    }
  }

  private final class ProcessRows[U](val processFlush: (U, InternalRow) => U,
      val endBatch: U => U, var result: U)
      extends ChangeValue[Row, StratumReservoir] {

    override def keyCopy(row: Row) = row.copy()

    override def defaultValue(row: Row): StratumCache = {
      val capacity = cacheSize.get
      val reservoir = new Array[GenericMutableRow](capacity)
      reservoir(0) = newMutableRow(row)
      Utils.fillArray(reservoir, EMPTY_ROW, 1, capacity)
      // for time-series data don't start with shortfall since this indicates
      // that a new stratum has appeared which can remain under-sampled for
      // a while till it doesn't get enough rows
      val initShortFall = if (timeInterval > 0) {
        // update timeSlot start and end
        updateTimeSlot(row, useCurrentTimeIfNoColumn = true)
        0
      } else math.max(0, maxSamples.get - capacity).toInt
      val sc = new StratumCache(reservoir, 1, 1, 1, initShortFall)
      maxSamples.compareAndSet(0, 1)
      batchSamples.incrementAndGet()
      slotSize.incrementAndGet()
      sc
    }

    override def mergeValue(row: Row, sr: StratumReservoir): StratumCache = {
      val sc = sr.asInstanceOf[StratumCache]
      // else update meta information in current stratum
      sc.batchTotalSize += 1
      val reservoirCapacity = cacheSize.get + sc.prevShortFall
      if (sc.reservoirSize >= reservoirCapacity) {
        val rnd = rng.nextInt(sc.batchTotalSize)
        // pick up this row with probability of reservoirCapacity/totalSize
        if (rnd < reservoirCapacity) {
          // replace a random row in reservoir
          sc.reservoir(rng.nextInt(reservoirCapacity)) = newMutableRow(row)
          // update timeSlot start and end
          if (timeInterval > 0) {
            updateTimeSlot(row, useCurrentTimeIfNoColumn = false)
          }
        }
        if (batchSamples.get >= (fraction * slotSize.incrementAndGet())) sc
        else null
      } else {
        // if reservoir has empty slots then fill them up first
        val reservoirLen = sc.reservoir.length
        if (reservoirLen <= sc.reservoirSize) {
          // new size of reservoir will be > reservoirSize given that it
          // increases only in steps of 1 and the expression
          // reservoirLen + (reservoirLen >>> 1) + 1 will certainly be
          // greater than reservoirLen
          val newReservoir = new Array[GenericMutableRow](math.max(math.min(
            reservoirCapacity, reservoirLen + (reservoirLen >>> 1) + 1),
            cacheSize.get))
          Utils.fillArray(newReservoir, EMPTY_ROW, reservoirLen,
            newReservoir.length)
          System.arraycopy(sc.reservoir, 0, newReservoir,
            0, reservoirLen)
          sc.reservoir = newReservoir
        }
        sc.reservoir(sc.reservoirSize) = newMutableRow(row)
        sc.reservoirSize += 1
        sc.totalSamples += 1

        // update timeSlot start and end
        if (timeInterval > 0) {
          updateTimeSlot(row, useCurrentTimeIfNoColumn = false)
        }

        compareOrderAndSet(maxSamples, sc.totalSamples, getMax = true)
        batchSamples.incrementAndGet()
        slotSize.incrementAndGet()
        sc
      }
    }

    override def segmentAbort(seg: SegmentMap[Row, StratumReservoir]) = {
      strata.synchronized {
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

  private def flushCache[U](init: U, process: (U, InternalRow) => U,
      // first acquire all the segment write locks
      // so no concurrent processors are in progress
      endBatch: U => U): U = strata.writeLock { segs =>

    val nsamples = batchSamples.get
    val numStrata = strata.size

    // if more than 50% of keys are empty, then clear the whole map
    val emptyStrata = segs.foldLeft(0) {
      _ + _.valuesIterator.count(_.reservoir.length == 0)
    }

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
      fullReset = (tsEnd != 0) && ((tsEnd - timeSlotStart.get) >= timeInterval)
      if (fullReset) {
        maxSamples.set(0)
        // reset timeSlot start and end
        timeSlotStart.set(Long.MaxValue)
        timeSlotEnd.set(0)
        // recalculate the reservoir size for next round as per the average
        // of total number of non-empty reservoirs
        val nonEmptyStrata = numStrata.toInt - emptyStrata
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
    } else {
      // increase the cacheSize for future data but not much beyond
      // cacheBatchSize
      val newCacheSize = prevCacheSize + (prevCacheSize >>> 1)
      if ((newCacheSize * numStrata) <= math.abs(this.cacheBatchSize) * 3) {
        this.cacheSize.set(newCacheSize)
      }
    }
    batchSamples.set(0)
    slotSize.set(0)

    // in case rows to be flushed do not fall into >50% of cacheBatchSize
    // then flush into pending list instead
    val batchSize = this.cacheBatchSize

    def processSegment(cacheSize: Int, fullReset: Boolean) =
      foldDrainSegment(cacheSize, fullReset, process) _

    val result = pendingBatch.synchronized {
      if (batchSize > 0) {
        val pbatch = pendingBatch.get()
        // flush the pendingBatch at this point if possible
        val pendingLen = pbatch.length
        val totalSamples = pendingLen + nsamples
        if (((totalSamples % batchSize) << 1) > batchSize) {
          // full flush of samples and pendingBatch
          val u = pbatch.foldLeft(init)(process)
          val newPBatch = new mutable.ArrayBuffer[InternalRow](pendingLen)
          pendingBatch.set(newPBatch)
          endBatch(segs.foldLeft(u)(processSegment(prevCacheSize, fullReset)))
        } else if ((totalSamples << 1) > batchSize) {
          // some batches can be flushed but rest to be moved to pendingBatch
          val u = pbatch.foldLeft(init)(process)
          val newPBatch = new mutable.ArrayBuffer[InternalRow](pendingLen)
          // now apply process on the reservoir cache as much as required
          // (as per the maximum multiple of batchSize) and copy remaining
          // into pendingBatch
          def processAndCopyToBuffer(cacheSize: Int, fullReset: Boolean,
              numToFlush: Int, buffer: mutable.ArrayBuffer[InternalRow]) = {
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
            s"$module: unexpected numToFlush=$numToFlush")
          val res = endBatch(segs.foldLeft(u)(processAndCopyToBuffer(
            prevCacheSize, fullReset, numToFlush, newPBatch)))
          // pendingBatch is COW so need to replace in final shape
          pendingBatch.set(newPBatch)
          res
        } else {
          // copy existing to newBatch
          val newPBatch = new mutable.ArrayBuffer[InternalRow](pendingLen)
          pbatch.copyToBuffer(newPBatch)
          // move collected samples into new pendingBatch
          segs.foldLeft(newPBatch)(foldDrainSegment(prevCacheSize,
            fullReset, _ += _))
          // pendingBatch is COW so need to replace in final shape
          pendingBatch.set(newPBatch)
          init
        }
      } else {
        endBatch(segs.foldLeft(init)(processSegment(prevCacheSize, fullReset)))
      }
    }
    if (numStrata < (emptyStrata << 1)) {
      strata.clear()
    }
    result
  }

  override protected def strataReservoirSize: Int = cacheSize.get

  override def append[U](rows: Iterator[Row],
      init: U, processFlush: (U, InternalRow) => U, endBatch: U => U): U = {
    if (rows.hasNext) {
      val processedResult = new ProcessRows(processFlush, endBatch, init)
      strata.bulkChangeValues(rows, processedResult)
      processedResult.result
    } else init
  }

  override def sample(items: Iterator[InternalRow],
      flush: Boolean): Iterator[InternalRow] = {
    // use "batchSize" to determine the sample buffer size
    val batchSize = BUFSIZE
    val sampleBuffer = new mutable.ArrayBuffer[InternalRow](math.min(batchSize,
      (batchSize * fraction * 10).toInt))
    val wrappedRow = new WrappedInternalRow(schema,
      WrappedInternalRow.createConverters(schema))

    new GenerateFlatIterator[InternalRow, Boolean](finished => {
      val sbuffer = sampleBuffer
      if (sbuffer.nonEmpty) sbuffer.clear()
      val processRows = new ProcessRows[Unit]((_, sampledRow) => {
        sbuffer += sampledRow
      }, identity, ())

      var flushed = false
      while (!flushed && items.hasNext) {
        wrappedRow.internalRow = items.next()
        if (strata.changeValue(wrappedRow, processRows) == null) {
          processRows.segmentAbort(null)
          flushed = sbuffer.nonEmpty
        }
      }

      if (sbuffer.nonEmpty) {
        (sbuffer.iterator, false)
      } else if (finished || !flush) {
        // if required notify any other waiting samplers that iteration is done
        if (numSamplers.decrementAndGet() == 1) numSamplers.synchronized {
          numSamplers.notifyAll()
        }
        (GenerateFlatIterator.TERMINATE, true)
      } else {
        // flush == true
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

/**
 * An extension to StratumReservoir to also track total samples seen since
 * last time slot and short fall from previous rounds.
 */
private final class StratumCache(_reservoir: Array[GenericMutableRow],
    _reservoirSize: Int, _batchTotalSize: Int, var totalSamples: Int,
    var prevShortFall: Int) extends StratumReservoir(_reservoir,
  _reservoirSize, _batchTotalSize) {

  override def reset(prevReservoirSize: Int, newReservoirSize: Int,
      fullReset: Boolean) {

    if (newReservoirSize > 0) {
      val numSamples = reservoirSize

      // reset transient data
      super.reset(prevReservoirSize, newReservoirSize, fullReset)

      // check for the end of current time-slot; if it has ended, then
      // also reset the "shortFall" and other such history
      if (fullReset) {
        totalSamples = 0
        prevShortFall = 0
      } else {
        // First update the shortfall for next round.
        // If it has not seen much data for sometime and has fallen behind
        // a lot, then it is likely gone and there is no point in increasing
        // shortfall indefinitely (else it will over sample if seen in future)
        // [sumedh] Above observation does not actually hold. Two cases:
        // 1) timeInterval is defined: in this case we better keep the shortFall
        //    till the end of timeInterval when it shall be reset in any case
        // 2) timeInterval is not defined: this is a case of non-time series
        //    data where we should keep shortFall till the end of data
        /*
        if (prevReservoirSize <= reservoirSize ||
          prevShortFall <= (prevReservoirSize + numSamples)) {
          prevShortFall += (prevReservoirSize - numSamples)
        }
        */
        prevShortFall += (prevReservoirSize - numSamples)
      }
    }
  }
}
