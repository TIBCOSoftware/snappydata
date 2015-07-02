package org.apache.spark.sql.execution

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.mutable
import scala.language.reflectiveCalls
import scala.util.control.NonFatal

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{MutableRow, Row}
import org.apache.spark.sql.collection.{ChangeValue, GenerateFlatIterator, SegmentMap, Utils}
import org.apache.spark.sql.execution.StratifiedSampler._
import org.apache.spark.sql.sources.{CastDouble, StatVarianceCounter}
import org.apache.spark.sql.types._

/**
 * A stratified sampling implementation that uses an error limit with confidence
 * on a numerical column to sample as much as required to maintaining the
 * expected error within the limit. An optional initial cache size can
 * be specified that is used as the initial reservoir size per stratum
 * for reservoir sampling. The error limit is attempted to be honoured for
 * each of the stratum independently and the sampling rate increased or
 * decreased accordingly. It uses standard closed form estimation of the
 * sampling error increasing or decreasing the sampling as required (and
 * expanding the cache size for bigger reservoir if required in next rounds).
 */
final class StratifiedSamplerErrorLimit(override val qcs: Array[Int],
    override val name: String,
    override val schema: StructType,
    private val cacheSize: AtomicInteger,
    val errorLimit: Double,
    val confidence: Double,
    val errorLimitColumn: Int,
    val cacheBatchSize: Int,
    val timeSeriesColumn: Int,
    val timeInterval: Int)
    extends StratifiedSampler(qcs, name, schema) {

  val fraction = 0.01 // SW:
  private val batchSamples, slotSize = new AtomicInteger
  /** Keeps track of the maximum number of samples in a stratum seen so far */
  private val maxSamples = new AtomicLong
  // initialize timeSlotStart to MAX so that it will always be set first
  // time around for the slot (since every valid time will be less)
  private val timeSlotStart = new AtomicLong(Long.MaxValue)
  private val timeSlotEnd = new AtomicLong

  /** Store type of column once to avoid checking for every row at runtime */
  private val timeSeriesColumnType: Int = {
    if (timeSeriesColumn >= 0) {
      val ts = schema(timeSeriesColumn)
      ts.dataType match {
        case LongType => 0
        case IntegerType => 1
        case TimestampType | DateType => 2
        case _ => throw new AnalysisException(
          s"StratifiedSampler: Cannot parse 'timeSeriesColumn'=$ts")
      }
    } else -1
  }

  /**
   * Total number of samples collected in a timeInterval. Not atomic since
   * it is always updated and read under global lock.
   */
  private var timeSlotSamples = 0

  private def setTimeSlot(timeSlot: Long) {
    compareOrderAndSet(timeSlotStart, timeSlot, getMax = false)
    compareOrderAndSet(timeSlotEnd, timeSlot, getMax = true)
  }

  private def parseMillisFromAny(ts: Any): Long = {
    ts match {
      case tts: java.sql.Timestamp =>
        val time = tts.getTime
        if (tts.getNanos >= 500000) time + 1 else time
      case td: java.util.Date => td.getTime
      case tl: Long => tl
      case ti: Int => ti.toLong
      case _ => throw new AnalysisException(
        s"StratifiedSampler: Cannot parse 'timeSeriesColumn'=$ts")
    }
  }

  private def updateTimeSlot(row: Row, useCurrentTimeIfNoColumn: Boolean) {
    try {
      val tsType = timeSeriesColumnType
      if (tsType >= 0) {
        tsType match {
          case 0 =>
            val ts = row.getLong(timeSeriesColumn)
            if (ts != 0 || !row.isNullAt(timeSeriesColumn)) {
              setTimeSlot(ts)
            }
          case 1 =>
            val ts = row.getInt(timeSeriesColumn)
            if (ts != 0 || !row.isNullAt(timeSeriesColumn)) {
              setTimeSlot(ts)
            }
          case 2 =>
            val ts = row(timeSeriesColumn)
            if (ts != null) {
              setTimeSlot(parseMillisFromAny(ts))
            }
        }
      }
      // update the timeSlot if a) explicitly requested, or
      // b) in case if it has not been initialized at all
      else if (useCurrentTimeIfNoColumn || timeSlotEnd.get == 0L) {
        setTimeSlot(System.currentTimeMillis)
      }
    } catch {
      case NonFatal(e) => if (timeSeriesColumn < 0 ||
          !row.isNullAt(timeSeriesColumn)) throw e
      case t => throw t
    }
  }

  private final class ProcessRows[U](val processSelected: Any => Any,
      val processFlush: (U, Row) => U,
      val endBatch: U => U, var result: U)
      extends ChangeValue[Row, StratumReservoir] with CastDouble {

    override def keyCopy(row: Row) = row.copy()

    override def doubleColumnType: DataType = schema(errorLimitColumn).dataType

    /** Update the stats required to honour error limit for a new base row */
    def updateStratumStats(sr: StratumCache, row: Row,
        updateSampleStats: Boolean, replacedRow: Row): Unit = {
      val statVal = toDouble(row, errorLimitColumn, Double.NaN)
      if (statVal != Double.NaN) {
        sr.merge(statVal)
        if (updateSampleStats) {
          var prevVal: Double = 0
          if (replacedRow != null && {
            prevVal = toDouble(replacedRow, errorLimitColumn, Double.NaN)
            prevVal
          } != Double.NaN) {
            sr.stratumMean += (statVal - prevVal) / sr.batchSize
          } else {
            sr.batchSize += 1
            sr.stratumMean += (statVal - sr.stratumMean) / sr.batchSize
          }
        }
      }
    }

    /** Check whether the cache needs to be flushed.
      *
      * @return java.lang.Boolean.TRUE if cache needs to be flushed and fully
      *         reset, java.lang.Boolean.FALSE if cache needs to be flushed but
      *         no full reset, and null if cache does not need to be flushed
      */
    def checkCacheFlush(slotSize: Int): java.lang.Boolean = {
      // now that there is no "fraction" as reference, this will flush
      // the cache based on following criteria (cannot grow indefinitely)
      // 1) if time-slot is defined and is over then flush in any case
      // 2) if cache has grown beyond 10*cacheBatchSize then flush (regardless
      //    of time-slot defined or not)
      // 3) subject 2) to minimum 10 rows per stratum on an average
      // 4) do the above three checks every 10 rows to avoid too much
      //    processing on every row
      // The above constant 10 is hardcoded and making them configurable
      // will be too confusing to the users

      if ((slotSize % 10) == 1) {
        if (timeInterval >= 0) {
          // in case the current timeSlot is over, reset maxSamples
          // (thus causing shortFall to clear up)
          val tsEnd = timeSlotEnd.get
          if ((tsEnd != 0) && ((tsEnd - timeSlotStart.get) >=
              (timeInterval.toLong * 1000L))) {
            return java.lang.Boolean.TRUE
          }
        }
        if (slotSize > (cacheBatchSize * 10) && slotSize > (strata.size * 10)) {
          java.lang.Boolean.FALSE
        } else {
          null
        }
      } else {
        null
      }
    }

    override def defaultValue(row: Row): StratumCache = {
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
      val sr = new StratumCache(1, 1, reservoir, 1, initShortFall)
      updateStratumStats(sr, row, updateSampleStats = true, replacedRow = null)
      maxSamples.compareAndSet(0, 1)
      batchSamples.incrementAndGet()
      slotSize.incrementAndGet()
      sr
    }

    override def mergeValue(row: Row, sr: StratumReservoir) = {
      val sc = sr.asInstanceOf[StratumCache]
      // else update meta information in current stratum
      val reservoirCapacity = cacheSize.get + sc.prevShortFall
      if (sc.reservoirSize >= reservoirCapacity) {
        val rnd = rng.nextInt(sc.count.toInt)
        // pick up this row with probability of reservoirCapacity/totalSize
        if (rnd < reservoirCapacity) {
          // replace a random row in reservoir
          sc.reservoir(rng.nextInt(reservoirCapacity)) =
              newMutableRow(row, processSelected)
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
          val newReservoir = new Array[MutableRow](math.max(math.min(
            reservoirCapacity, reservoirLen + (reservoirLen >>> 1) + 1),
            cacheSize.get))
          Utils.fillArray(newReservoir, EMPTY_ROW, reservoirLen,
            newReservoir.length)
          System.arraycopy(sc.reservoir, 0, newReservoir,
            0, reservoirLen)
          sc.reservoir = newReservoir
        }
        sc.reservoir(sc.reservoirSize) = newMutableRow(row, processSelected)
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

  private def flushCache[U](init: U, process: (U, Row) => U,
      // first acquire all the segment write locks
      // so no concurrent processors are in progress
      endBatch: U => U): U = strata.writeLock { segs =>

    val nsamples = batchSamples.get
    val numStrata = strata.size

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
          val newPBatch = new mutable.ArrayBuffer[Row](pendingLen)
          pendingBatch.set(newPBatch)
          endBatch(segs.foldLeft(u)(processSegment(prevCacheSize, fullReset)))
        } else if ((totalSamples << 1) > batchSize) {
          // some batches can be flushed but rest to be moved to pendingBatch
          val u = pbatch.foldLeft(init)(process)
          val newPBatch = new mutable.ArrayBuffer[Row](pendingLen)
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
          val res = endBatch(segs.foldLeft(u)(processAndCopyToBuffer(
            prevCacheSize, fullReset, numToFlush, newPBatch)))
          // pendingBatch is COW so need to replace in final shape
          pendingBatch.set(newPBatch)
          res
        } else {
          // copy existing to newBatch
          val newPBatch = new mutable.ArrayBuffer[Row](pendingLen)
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

  override def append[U](rows: Iterator[Row], processSelected: Any => Any,
      init: U, processFlush: (U, Row) => U,
      endBatch: U => U): U = {
    if (rows.hasNext) {
      val processedResult = new ProcessRows(processSelected, processFlush,
        endBatch, init)
      strata.bulkChangeValues(rows, processedResult)
      processedResult.result
    } else init
  }

  override def sample(items: Iterator[Row], flush: Boolean): Iterator[Row] = {
    // use "batchSize" to determine the sample buffer size
    val batchSize = BUFSIZE
    val sampleBuffer = new mutable.ArrayBuffer[Row](batchSize / 10)

    new GenerateFlatIterator[Row, Boolean](finished => {
      val sbuffer = sampleBuffer
      if (sbuffer.nonEmpty) sbuffer.clear()
      val processRows = new ProcessRows[Unit](null, (_, sampledRow) => {
        sbuffer += sampledRow
      }, identity, ())

      var flushed = false
      while (!flushed && items.hasNext) {
        if (strata.changeValue(items.next(), processRows) == null) {
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

  override def clone: StratifiedSamplerErrorLimit =
    new StratifiedSamplerErrorLimit(qcs, name, schema, cacheSize,
      errorLimit, confidence, errorLimitColumn, cacheBatchSize,
      timeSeriesColumn, timeInterval)
}

/**
 * An extension to StratumReservoir to also track the mean, variance of full
 * population as well as the mean of sampling to determine if the error limit
 * is being honoured.
 *
 * Note that batchSize field in this extension is used to account for total
 * number of rows that have non-null value in the column chosen for error limit
 * checks. This is used to keep running mean in stratumMean. The original batch
 * size field for total number of rows seen in the stratum in current batch
 * is taken care by parent StatVarianceCounter.count field.
 */
private final class StratumCache(_totalSamples: Int, _batchSize: Int,
    _reservoir: Array[MutableRow], _reservoirSize: Int, _prevShortFall: Int,
    var stratumMean: Double = 0)
    extends StratumReservoir(_totalSamples, _batchSize, _reservoir,
      _reservoirSize, _prevShortFall) with StatVarianceCounter {

  override def batchTotalSize = count.toInt

  override def clearBatchTotalSize(): Unit = {
    super.init(0, 0, 0)
    batchSize = 0
    this.stratumMean = 0
  }

  override def copy() = sys.error("StratumCache.copy: unexpected call")
}
