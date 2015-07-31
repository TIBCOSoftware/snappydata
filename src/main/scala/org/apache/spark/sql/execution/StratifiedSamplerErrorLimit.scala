package org.apache.spark.sql.execution

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import scala.collection.mutable
import scala.language.reflectiveCalls

import org.apache.spark.sql.catalyst.expressions.{MutableRow, Row}
import org.apache.spark.sql.collection.{ChangeValue, GenerateFlatIterator, SegmentMap, Utils}
import org.apache.spark.sql.execution.StratifiedSampler._
import org.apache.spark.sql.sources.{CastDouble, CastLongTime}
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
    val initCacheSize: Int,
    val errorLimitColumn: Int,
    val errorLimitPercent: Double,
    val cacheBatchSize: Int,
    val timeSeriesColumn: Int,
    val timeInterval: Long)
    extends StratifiedSampler(qcs, name, schema) with CastLongTime {

  private val batchSamples = new AtomicInteger
  // initialize timeSlotStart to MAX so that it will always be set first
  // time around for the slot (since every valid time will be less)
  private val timeSlotStart = new AtomicLong(Long.MaxValue)
  private val timeSlotEnd = new AtomicLong

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

  private def updateTimeSlot(row: Row, useCurrentTimeIfNoColumn: Boolean) {
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

  private final class ProcessRows[U](val processSelected: Array[Any => Any],
      val processFlush: (U, Row) => U,
      val endBatch: U => U, var result: U)
      extends ChangeValue[Row, StratumReservoir] with CastDouble {

    override def keyCopy(row: Row) = row.copy()

    override def doubleColumnType: DataType = schema(errorLimitColumn).dataType

    /** Update the stats required to honour error limit for a new base row */
    def updateStratumStats(sc: StratumCacheWithStats, row: Row,
        updateSampleStats: Boolean, replacedRow: Row): Unit = {
      var prevVal: Double = 0
      val errCol = errorLimitColumn
      val statVal = toDouble(row, errCol, Double.NegativeInfinity)
      if (statVal != Double.NegativeInfinity) {
        val delta = statVal - sc.populationMean
        sc.populationSize += 1
        sc.populationMean += delta / sc.populationSize
        sc.populationNVariance += delta * (statVal - sc.populationMean)
        if (updateSampleStats) {
          if ((replacedRow ne EMPTY_ROW) && {
            prevVal = toDouble(replacedRow, errCol, Double.NegativeInfinity)
            prevVal
          } != Double.NegativeInfinity) {
            sc.sampleMean += (statVal - prevVal) / sc.sampleSize
          } else {
            sc.sampleSize += 1
            //require(sc.sampleSize <= sc.reservoirSize)
            sc.sampleMean += (statVal - sc.sampleMean) / sc.sampleSize
          }
        }
      } else if (updateSampleStats && (replacedRow ne EMPTY_ROW) && {
        prevVal = toDouble(replacedRow, errCol, Double.NegativeInfinity)
        prevVal
      } != Double.NegativeInfinity) {
        // replacing a row with non-null value with null value
        sc.sampleSize -= 1
        sc.sampleMean -= (prevVal - sc.sampleMean) / sc.sampleSize
      }
    }

    /** check if error limit has been exceeded by more than 10% for a stratum */
    def stratumErrorLimitExceeded(sc: StratumCacheWithStats): Boolean = {
      val reqErrorPercent = errorLimitPercent
      val populationMean = sc.populationMean
      val meanError = sc.sampleMean - populationMean
      val meanErrorPercent = math.abs(meanError / populationMean) * 100.0
      (meanErrorPercent > reqErrorPercent) &&
          (meanErrorPercent - reqErrorPercent) > (0.1 * reqErrorPercent)
      /*
      // check for large variance but only when we detect significant error
      // this is to hopefully also deal with aggregates other than AVG/SUM
      if ((meanErrorPercent * 2.0) > reqErrorPercent) {
        val numSamples = sc.sampleSize.toDouble
        val populationSize = sc.populationSize.toDouble
        val stdvar = (sc.populationNVariance / (populationSize * numSamples)) *
            ((populationSize - numSamples) / populationSize)
        val errorPercent =
          if (stdvar > (meanError * meanError)) {
            // not using any confidence factor here so sticking to factor of
            // 1.0 that corresponds to confidence of ~70% which is good enough
            math.sqrt(stdvar) * 100.0 / populationMean
          } else {
            meanErrorPercent
          }
        // check if limit is within 10% of requested error limit
        (errorPercent > reqErrorPercent) &&
            (errorPercent - reqErrorPercent) > (0.1 * reqErrorPercent)
      } else {
        false
      }
      */
    }

    override def defaultValue(row: Row): StratumCacheWithStats = {
      val capacity = initCacheSize
      val reservoir = new Array[MutableRow](capacity)
      reservoir(0) = newMutableRow(row, processSelected)
      Utils.fillArray(reservoir, EMPTY_ROW, 1, capacity)
      // update timeSlot start and end
      if (timeInterval > 0) {
        updateTimeSlot(row, useCurrentTimeIfNoColumn = true)
      }
      val sr = new StratumCacheWithStats(reservoir, 1, 1)
      updateStratumStats(sr, row, updateSampleStats = true, EMPTY_ROW)
      batchSamples.incrementAndGet()
      sr
    }

    override def mergeValue(row: Row,
        sr: StratumReservoir): StratumCacheWithStats = {
      val sc = sr.asInstanceOf[StratumCacheWithStats]
      val reservoir = sc.reservoir
      val reservoirSize = sc.reservoirSize
      val reservoirLen = reservoir.length
      // else update meta information in current stratum
      sc.batchTotalSize += 1
      if (reservoirSize >= reservoirLen) {
        val reservoirOffset = sc.reservoirOffset
        val curReservoirLen = reservoirLen - reservoirOffset
        val scSize = sc.batchTotalSize
        val rnd = rng.nextInt(scSize)
        var replacedRow: Row = EMPTY_ROW
        val updateSampleStats = rnd < curReservoirLen
        // pick up this row with probability of reservoirCapacity/totalSize
        if (updateSampleStats) {
          // replace a random row in reservoir
          val replaceIndex = reservoirOffset + rng.nextInt(curReservoirLen)
          replacedRow = reservoir(replaceIndex)
          reservoir(replaceIndex) = newMutableRow(row, processSelected)
          // update timeSlot start and end
          if (timeInterval > 0) {
            updateTimeSlot(row, useCurrentTimeIfNoColumn = false)
          }
        }
        // update stratum stats
        updateStratumStats(sc, row, updateSampleStats, replacedRow)
        // check for error limit every 10 base rows
        if ((scSize % 10) == 9) {
          // check if stratum error limit has been exceeded (after some
          //   minimal sampling), but also check for full cache flush
          if (sc.populationSize > (sc.sampleSize << 2) &&
              stratumErrorLimitExceeded(sc)) {

            /*
            if (checkCacheFlush() != null) {
              // for this case no need to increase reservoir size even if error
              // limit has been exceeded since the whole cache will be flushed
              return null
            }
            */
            // increase reservoir size but separate out already collected
            // samples that should not be considered in subsequent reservoir
            // sampling for this stratum (using prevShortFall as marker)

            val meanErrorRatio = math.abs(sc.sampleMean /
                sc.populationMean - 1.0)
            val errorLimitRatio = errorLimitPercent / 100.0
            val meanErrorOverflow = meanErrorRatio / errorLimitRatio - 1.0
            if (meanErrorOverflow <= 0) {
              return sc
            }
            // Simplistic increase to get new size of reservoir.
            // This will be compared against values obtained from population
            // variance using some heuristics.
            val sampleSize1 = reservoirLen + math.min(math.max(
              meanErrorOverflow, 0.1), 1.0) * reservoirLen
            // Use population variance to estimate the new size.
            // First use z/t factor of 1.0 which is ~70% confidence which works
            // well in most cases.
            val maxError = errorLimitPercent * sc.populationMean / 100.0
            val maxErrorSquared = maxError * maxError
            val populationSize = sc.populationSize.toDouble
            val populationVarianceBySize = sc.populationNVariance /
                (populationSize * populationSize)
            var sampleSize2 = populationSize /
                (maxErrorSquared / populationVarianceBySize + 1.0)
            //val sampleSz2 = sampleSize2
            // also adjust for null values (should this be proportional?)
            sampleSize2 += (reservoirSize - sc.sampleSize)
            // if size is below 30, then need to use t-factor which will not
            // be very accurate so use the simplistic increase in reservoir
            //var sampleSize3 = 0.0
            val newReservoirSize =
              math.round(if (sampleSize2 <= 30.0) {
                sampleSize1
              } else {
                // if calculated size is not large enough compared to current
                // length but actual mean error is large, then recalculate
                // with 95% confidence
                if (sampleSize2 < ((reservoirLen >>> 3) * 9)) {
                  // if variance if small and actual mean error not too large
                  // then don't change reservoir size hoping that future
                  // sampling with same size will bring the error within limits
                  if (sampleSize2 <= reservoirLen &&
                      (meanErrorRatio * 2.0) <= errorLimitRatio) {
                    return sc
                  }
                  sampleSize2 = populationSize / (maxErrorSquared /
                      (populationVarianceBySize * Utils.Z95Squared) + 1.0)
                  // also adjust for null values (should this be proportional?)
                  sampleSize2 += (reservoirSize - sc.sampleSize)
                  //sampleSize3 = sampleSize2
                }
                if (sampleSize2 < sampleSize1) {
                  if (sampleSize2 > reservoirLen) {
                    sampleSize2
                  } else {
                    // use minimal increase in size
                    (reservoirLen / 10) * 11 + 1
                  }
                } else {
                  sampleSize1
                }
              }).toInt

            /*
            val key = Utils.projectColumns(row, qcs, schema, convertToScalaRow = true)
            if (key.equals(new GenericRow(Array(2007, 6, "9E")))) {
              println(s"SW:1: newSize=$newReservoirSize from $reservoirLen sampleSize2=$sampleSize2 sampleSz2=$sampleSz2 sampleSize3=$sampleSize3 sampleMean=${sc.sampleMean} sampleSize=${sc.sampleSize} popMean=${sc.populationMean} popSize=${sc.populationSize} with errorPercent=${math.abs(sc.sampleMean - sc.populationMean) * 100.0 / sc.populationMean}")
            } else if (key.equals(new GenericRow(Array(2007, 8, "9E")))) {
              println(s"SW:2: newSize=$newReservoirSize from $reservoirLen sampleSize2=$sampleSize2 sampleSz2=$sampleSz2 sampleSize3=$sampleSize3 sampleMean=${sc.sampleMean} sampleSize=${sc.sampleSize} popMean=${sc.populationMean} popSize=${sc.populationSize} with errorPercent=${math.abs(sc.sampleMean - sc.populationMean) * 100.0 / sc.populationMean}")
            }
            */
            val newReservoir = new Array[MutableRow](newReservoirSize)
            Utils.fillArray(newReservoir, EMPTY_ROW, reservoirLen,
              newReservoirSize)
            System.arraycopy(reservoir, 0, newReservoir, 0, reservoirLen)
            sc.reservoir = newReservoir

            /*
            // fill in the ratio column in previous reservoir first
            val ratio = sc.calculateWeightageColumn()
            val lastColumnIndex = schema.length - 1
            var index = reservoirOffset
            while (index < reservoirLen) {
              fillWeightageIfAbsent(newReservoir, index, ratio, lastColumnIndex)
              index += 1
            }

            sc.reservoirOffset = reservoirLen
            // reset the batch total size to start reservoir sampling afresh
            sc.batchTotalSize = 0
            */
          }
        }
        sc
      } else {
        // reservoir has empty slots so fill them up first
        reservoir(reservoirSize) = newMutableRow(row, processSelected)
        sc.reservoirSize += 1

        // update timeSlot start and end
        if (timeInterval > 0) {
          updateTimeSlot(row, useCurrentTimeIfNoColumn = false)
        }
        // update stratum stats
        updateStratumStats(sc, row, updateSampleStats = true, EMPTY_ROW)

        batchSamples.incrementAndGet()
        sc
      }
    }

    override def segmentAbort(seg: SegmentMap[Row, StratumReservoir]) = {
      strata.synchronized {
        // top-level synchronized above to avoid possible deadlocks with
        // segment locks if two threads are trying to drain cache concurrently

        val nsamples = batchSamples.get
        if (nsamples > 0) {
          result = flushCache(force = false, result, processFlush, endBatch)
        }
      }
      false
    }
  }

  /** Check whether the cache needs to be flushed. This should be invoked
    * whenever there is a potential significant increase in memory consumption
    *
    * @return java.lang.Boolean.TRUE if cache needs to be flushed and fully
    *         reset, java.lang.Boolean.FALSE if cache needs to be flushed but
    *         no full reset, and null if cache does not need to be flushed
    */
  def checkCacheFlush(force: Boolean): java.lang.Boolean = {
    // now that there is no "fraction" as reference, this will flush
    // the cache based on following criteria (cannot grow indefinitely)
    // 1) if time-slot is defined and is over then flush in any case
    // 2) if cache has grown beyond 10*cacheBatchSize then flush (regardless
    //    of time-slot defined or not)
    // 3) subject 2) to minimum 10 rows per stratum on an average
    // The above constant 10 is hardcoded and making them configurable
    // will be too confusing to the users

    if (timeInterval > 0) {
      // in case the current timeSlot is over, reset maxSamples
      // (thus causing shortFall to clear up)
      val tsEnd = timeSlotEnd.get
      if ((tsEnd != 0) && ((tsEnd - timeSlotStart.get) >= timeInterval)) {
        return java.lang.Boolean.TRUE
      }
    }
    val tableSize = batchSamples.get
    if (tableSize > (cacheBatchSize * 10) && tableSize > (strata.size * 10)) {
      java.lang.Boolean.FALSE
    } else if (force) {
      java.lang.Boolean.FALSE
    } else {
      null
    }
  }

  private def flushCache[U](force: Boolean, init: U, process: (U, Row) => U,
      // first acquire all the segment write locks
      // so no concurrent processors are in progress
      endBatch: U => U): U = strata.writeLock { segs =>

    val nsamples = batchSamples.get
    val fullReset = checkCacheFlush(force)
    // check if flush is to be really done by this thread (another concurrent
    //   thread may have done the flush already)
    if (nsamples == 0 || fullReset == null) {
      return init
    }

    /*
    println("\n")
    val tsr1 = strata(new GenericRow(Array(2007, 6, UTF8String("9E"))))
    if (tsr1 != null) {
      val sc = tsr1.asInstanceOf[StratumCacheWithStats]
      println(s"SW:**** size=${sc.reservoirSize} sampleMean=${sc.sampleMean} sampleSize=${sc.sampleSize} popMean=${sc.populationMean} popSize=${sc.populationSize} with errorPercent=${math.abs(sc.sampleMean - sc.populationMean) * 100.0 / sc.populationMean}")
    }
    val tsr2 = strata(new GenericRow(Array(2007, 8, UTF8String("9E"))))
    if (tsr2 != null) {
      val sc = tsr2.asInstanceOf[StratumCacheWithStats]
      println(s"SW:**** size=${sc.reservoirSize} sampleMean=${sc.sampleMean} sampleSize=${sc.sampleSize} popMean=${sc.populationMean} popSize=${sc.populationSize} with errorPercent=${math.abs(sc.sampleMean - sc.populationMean) * 100.0 / sc.populationMean}")
    }
    */

    val numStrata = strata.size
    // if more than 50% of keys are empty, then clear the whole map
    val emptyStrata = segs.foldLeft(0)(
      _ + _.valuesIterator.count(_.reservoir.isEmpty))

    if (timeInterval > 0) {
      // update the timeSlot with current time if no timeSeries column
      // has been provided
      if (timeSeriesColumn < 0) {
        updateTimeSlot(EMPTY_ROW, useCurrentTimeIfNoColumn = true)
      }
      if (fullReset) {
        // reset timeSlot start and end
        timeSlotStart.set(Long.MaxValue)
        timeSlotEnd.set(0)
      }
    }
    batchSamples.set(0)

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
          endBatch(segs.foldLeft(u)(processSegment(initCacheSize, fullReset)))
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
            initCacheSize, fullReset, numToFlush, newPBatch)))
          // pendingBatch is COW so need to replace in final shape
          pendingBatch.set(newPBatch)
          res
        } else {
          // copy existing to newBatch since pendingBatch is COW
          val newPBatch = new mutable.ArrayBuffer[Row](pendingLen)
          pbatch.copyToBuffer(newPBatch)
          // move collected samples into new pendingBatch
          segs.foldLeft(newPBatch)(foldDrainSegment(initCacheSize,
            fullReset, _ += _))
          // pendingBatch is COW so need to replace in final shape
          pendingBatch.set(newPBatch)
          init
        }
      } else {
        endBatch(segs.foldLeft(init)(processSegment(initCacheSize, fullReset)))
      }
    }
    if (numStrata < (emptyStrata << 1)) {
      strata.clear()
    }
    result
  }

  /** not used for this implementation so return init size */
  override protected def strataReservoirSize: Int = initCacheSize

  override def append[U](rows: Iterator[Row],
      processSelected: Array[Any => Any],
      init: U, processFlush: (U, Row) => U, endBatch: U => U): U = {
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
        flushCache[Unit](force = true, (), (_, sampledRow) => {
          sbuffer += sampledRow
        }, identity)
        (sbuffer.iterator, true)
      }
    }, false)
  }

  override def clone: StratifiedSamplerErrorLimit =
    new StratifiedSamplerErrorLimit(qcs, name, schema, initCacheSize,
      errorLimitColumn, errorLimitPercent, cacheBatchSize,
      timeSeriesColumn, timeInterval)
}

/**
 * An extension to StratumReservoir to also track the mean, variance of full
 * population as well as the mean of sampling to determine if the error limit
 * is being honoured.
 *
 * This extension allows for restarting reservoir build by using reservoirOffset
 * as marker for previous reservoir (thus offset of new reservoir). Also the
 * batchTotalSize field for that case is the number of rows seen since the
 * reservoir build was last restarted/started.
 */
private final class StratumCacheWithStats(_reservoir: Array[MutableRow],
    _reservoirSize: Int, _batchTotalSize: Int, var reservoirOffset: Int = 0,
    // need a separate sampleSize for the non-null values of error limit column
    // not extending StatVarianceCounter to avoid Long count field
    var sampleSize: Int = 0, var populationSize: Int = 0,
    var sampleMean: Double = 0, var populationMean: Double = 0,
    var populationNVariance: Double = 0)
    extends StratumReservoir(_reservoir, _reservoirSize, _batchTotalSize) {

  override def numSamples: Int = reservoirSize - reservoirOffset

  override def reset(prevReservoirSize: Int, newReservoirSize: Int,
      fullReset: Boolean): Unit = {
    // reset transient data
    super.reset(prevReservoirSize, newReservoirSize, fullReset)
    this.reservoirOffset = 0
    this.sampleSize = 0
    this.populationSize = 0
    this.sampleMean = 0
    this.populationMean = 0
    this.populationNVariance = 0
  }
}
