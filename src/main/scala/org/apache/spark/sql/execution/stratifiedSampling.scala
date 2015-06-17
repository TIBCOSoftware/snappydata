package org.apache.spark.sql.execution

import java.util.Random
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.collection._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, Row, SampleDataFrame}
import org.apache.spark.{Logging, Partition, SparkEnv, TaskContext}

import scala.collection.mutable
import scala.language.reflectiveCalls

final class SamplePartition(val parent: Partition, override val index: Int,
                            val host: String, var isLastHostPartition: Boolean)
  extends Partition with Serializable {

  override def toString = s"SamplePartition($index, $host)"
}

final class StratifiedSampledRDD(@transient parent: RDD[Row],
                                 qcs: Array[Int],
                                 cacheBatchSize: Int,
                                 options: Map[String, Any],
                                 schema: StructType)
  extends RDD[Row](parent) with Serializable {

  var hostPartitions: Map[String, Array[Int]] = _

  override def getPartitions: Array[Partition] = {
    val peers = SparkEnv.get.blockManager.master.getMemoryStatus.keySet.map(
      _.host)
    val npeers = peers.size
    if (npeers > 0) {
      val numberedPeers = peers.toArray
      // Split partitions executor-wise:
      //  1) first prefer the parent's first valid preference if any
      //  2) if no parent preference then in round-robin
      // So number of partitions assigned to each executor are known and wait
      // on each executor accordingly to drain the remaining cache.
      val parentPartitions = parent.partitions
      val partitions = parentPartitions.indices.map { index =>
        val ppart = parentPartitions(index)
        val plocs = firstParent[Row].preferredLocations(ppart)
        // get the first preferred location in the peers, else if none
        // found then use default round-robin policy among peers
        plocs.collectFirst { case host if peers contains host =>
          new SamplePartition(ppart, index, host, isLastHostPartition = false)
        }.getOrElse(new SamplePartition(ppart, index,
          numberedPeers(index % npeers), isLastHostPartition = false))
      }
      val partitionOrdering = Ordering[Int].on[SamplePartition](_.index)
      hostPartitions = partitions.groupBy(_.host).map { case (k, v) =>
        val sortedPartitions = v.sorted(partitionOrdering)
        // mark the last partition in each host as the last one that should
        // also be necessarily scheduled on that host
        sortedPartitions.last.isLastHostPartition = true
        (k, sortedPartitions.map(_.index).toArray)
      }
      partitions.toArray[Partition]
    }
    else {
      hostPartitions = Map.empty
      Array.empty[Partition]
    }
  }

  override def compute(split: Partition,
                       context: TaskContext): Iterator[Row] = {
    val blockManager = SparkEnv.get.blockManager
    val part = split.asInstanceOf[SamplePartition]
    val thisHost = blockManager.blockManagerId.host
    // use -ve cacheBatchSize to indicate that no additional batching is to be
    // done but still pass the value through for size increases
    val sampler = StratifiedSampler(options, qcs, "_rdd_" + id, -cacheBatchSize,
      schema, cached = true)
    val numSamplers = hostPartitions(part.host).length

    if (part.host != thisHost) {
      // if this is the last partition then it has to be necessarily scheduled
      // on specified host
      if (part.isLastHostPartition) {
        throw new IllegalStateException(
          s"Expected to execute on ${part.host} but is on $thisHost")
      }
      else {
        // this has been scheduled from some other target node so increment
        // the number of expected samplers but don't fail
        if (!sampler.numSamplers.compareAndSet(0, numSamplers + 1)) {
          sampler.numSamplers.incrementAndGet()
        }
      }
    }
    else {
      sampler.numSamplers.compareAndSet(0, numSamplers)
    }
    if (part.isLastHostPartition) {
      // If we are the last partition on this host, then wait for all
      // others to finish and then drain the remaining cache. The flag is
      // persistent so that any other stray additional partitions will also
      // do the flush.
      sampler.setFlushStatus(true)
    }
    sampler.numThreads.incrementAndGet()
    try {
      sampler.sample(firstParent[Row].iterator(part.parent, context))
    } finally {
      sampler.numThreads.decrementAndGet()
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(split.asInstanceOf[SamplePartition].host)
  }
}

object StratifiedSampler {

  private final val globalMap = new mutable.HashMap[String, StratifiedSampler]
  private final val mapLock = new ReentrantReadWriteLock

  private final val MAX_FLUSHED_MAP_AGE_SECS = 600
  private final val MAX_FLUSHED_MAP_SIZE = 1024
  private final val flushedMaps = new mutable.LinkedHashMap[String, Long]

  final val BUFSIZE = 1000
  final val EMPTY_RESERVOIR = Array.empty[MutableRow]
  final val EMPTY_ROW = new GenericMutableRow(Array[Any]())
  final val LONG_ONE = Long.box(1)

  implicit class StringExtensions(val s: String) extends AnyVal {
    def ci = new {
      def unapply(other: String) = s.equalsIgnoreCase(other)
    }
  }

  def apply(options: Map[String, Any], qcsi: Array[Int],
            nameSuffix: String, cacheBatchSize: Int,
            schema: StructType, cached: Boolean) = {
    val nameTest = "name".ci
    val qcsTest = "qcs".ci
    val fracTest = "fraction".ci
    val reservoirSizeTest = "strataReservoirSize".ci
    val timeSeriesColumnTest = "timeSeriesColumn".ci
    val timeIntervalTest = "timeInterval".ci

    val timeIntervalSpec = "([0-9]+)(s|m|h)".r
    val cols = schema.fieldNames

    // using a default strata size of 104 since 100 is taken as the normal
    // limit for assuming a Gaussian distribution (e.g. see MeanEvaluator)
    val defaultStrataSize = 104
    // Using foldLeft to read key-value pairs and build into the result
    // tuple of (qcs, fraction, strataReservoirSize) like an aggregate.
    // This "aggregate" simply keeps the last values for the corresponding
    // keys as found when folding the map.
    val (nm, fraction, strataSize, tsCol, timeInterval) = options.foldLeft(
      "", 0.0, defaultStrataSize, -1, 0) {
      case ((n, fr, sz, ts, ti), (opt, optV)) =>
        opt match {
          case qcsTest() => (n, fr, sz, ts, ti) // ignore
          case nameTest() => (optV.toString, fr, sz, ts, ti)
          case fracTest() => optV match {
            case fd: Double => (n, fd, sz, ts, ti)
            case fs: String => (n, fs.toDouble, sz, ts, ti)
            case ff: Float => (n, ff.toDouble, sz, ts, ti)
            case fi: Int => (n, fi.toDouble, sz, ts, ti)
            case fl: Long => (n, fl.toDouble, sz, ts, ti)
            case _ => throw new AnalysisException(
              s"StratifiedSampler: Cannot parse double 'fraction'=$optV")
          }
          case reservoirSizeTest() => optV match {
            case si: Int => (n, fr, si, ts, ti)
            case ss: String => (n, fr, ss.toInt, ts, ti)
            case sl: Long => (n, fr, sl.toInt, ts, ti)
            case _ => throw new AnalysisException(
              s"StratifiedSampler: Cannot parse int 'strataReservoirSize'=$optV")
          }
          case timeSeriesColumnTest() => optV match {
            case tss: String => (n, fr, sz,
              SampleDataFrame.columnIndex(tss, cols), ti)
            case tsi: Int => (n, fr, sz, tsi, ti)
            case _ => throw new AnalysisException(
              s"StratifiedSampler: Cannot parse 'timeSeriesColumn'=$optV")
          }
          case timeIntervalTest() => optV match {
            case tii: Int => (n, fr, sz, ts, tii)
            case til: Long => (n, fr, sz, ts, til.toInt)
            case tis: String => tis match {
              case timeIntervalSpec(interval, unit) =>
                unit match {
                  case "s" => (n, fr, sz, ts, interval.toInt)
                  case "m" => (n, fr, sz, ts, interval.toInt * 60)
                  case "h" => (n, fr, sz, ts, interval.toInt * 3600)
                  case _ => throw new AssertionError(
                    s"unexpected regex match 'unit'=$unit")
                }
              case _ => throw new AnalysisException(
                s"StratifiedSampler: Cannot parse 'timeInterval'=$tis")
            }
            case _ => throw new AnalysisException(
              s"StratifiedSampler: Cannot parse 'timeInterval'=$optV")
          }
          case _ => throw new AnalysisException(
            s"StratifiedSampler: Unknown option '$opt'")
        }
    }

    val qcs =
      if (qcsi.isEmpty) SampleDataFrame.resolveQCS(options, schema.fieldNames)
      else qcsi
    val name = nm + nameSuffix
    if (cached && name.nonEmpty) {
      lookupOrAdd(qcs, name, fraction, strataSize, cacheBatchSize,
        tsCol, timeInterval, schema)
    }
    else {
      newSampler(qcs, name, fraction, strataSize, cacheBatchSize,
        tsCol, timeInterval, schema)
    }
  }

  def apply(name: String): Option[StratifiedSampler] = {
    SegmentMap.lock(mapLock.readLock) {
      globalMap.get(name)
    }
  }

  private[sql] def lookupOrAdd(qcs: Array[Int], name: String,
                               fraction: Double, strataSize: Int,
                               cacheBatchSize: Int, tsCol: Int,
                               timeInterval: Int, schema: StructType) = {
    // not using getOrElse in one shot to allow taking only read lock
    // for the common case, then release it and take write lock if new
    // sampler has to be added
    SegmentMap.lock(mapLock.readLock) {
      globalMap.get(name)
    } match {
      case Some(sampler) => sampler
      case None =>
        // insert into global map but double-check after write lock
        SegmentMap.lock(mapLock.writeLock) {
          globalMap.getOrElse(name, {
            val sampler = newSampler(qcs, name, fraction, strataSize,
              cacheBatchSize, tsCol, timeInterval, schema)
            // if the map has been removed previously, then mark as flushed
            sampler.setFlushStatus(flushedMaps.contains(name))
            globalMap(name) = sampler
            sampler
          })
        }
    }
  }

  def removeSampler(name: String, markFlushed: Boolean): Unit =
    SegmentMap.lock(mapLock.writeLock) {
      globalMap.remove(name)
      if (markFlushed) {
        // make an entry in the flushedMaps list with the current time
        // for expiration later if the map becomes large
        flushedMaps.put(name, System.currentTimeMillis())
        // clear old values if map is too large and expiration
        // has been hit for one or more older entries
        if (flushedMaps.size > MAX_FLUSHED_MAP_SIZE) {
          val expireTime = System.currentTimeMillis() -
            (MAX_FLUSHED_MAP_AGE_SECS * 1000L)
          flushedMaps.takeWhile(_._2 <= expireTime).keysIterator.foreach(
            flushedMaps.remove)
        }
      }
    }

  private def newSampler(qcs: Array[Int], name: String, fraction: Double,
                         strataSize: Int, cacheBatchSize: Int, tsCol: Int,
                         timeInterval: Int, schema: StructType) = {
    if (qcs.isEmpty)
      throw new AnalysisException(SampleDataFrame.ERROR_NO_QCS)
    else if (tsCol >= 0 && timeInterval <= 0)
      throw new AnalysisException("StratifiedSampler: no timeInterval for " +
        "timeSeriesColumn=" + schema(tsCol).name)
    else if (fraction > 0.0)
      new StratifiedSamplerCached(qcs, name, schema,
        new AtomicInteger(strataSize), fraction, cacheBatchSize,
        tsCol, timeInterval)
    else if (strataSize > 0)
      new StratifiedSamplerReservoir(qcs, name, schema, strataSize)
    else throw new AnalysisException("StratifiedSampler: " +
      s"'fraction'=$fraction 'strataReservoirSize'=$strataSize")
  }

  def compareOrderAndSet(atomicVal: AtomicLong, compareTo: Long,
                         getMax: Boolean): Boolean = {
    while (true) {
      val v = atomicVal.get
      val cmp = if (getMax) compareTo > v else compareTo < v
      if (cmp) {
        if (atomicVal.compareAndSet(v, compareTo)) {
          return true
        }
      } else return false
    }
    false
  }

  def readReservoir(reservoir: Array[MutableRow], pos: Int,
                    ratio: Long, lastIndex: Int) = {
    // fill in the weight ratio column
    val row = reservoir(pos)
    row(lastIndex) = ratio
    row
  }
}

import StratifiedSampler._

abstract class StratifiedSampler(val qcs: Array[Int], val name: String,
                                 val schema: StructType)
  extends Serializable with Cloneable with Logging {

  type ReservoirSegment = MultiColumnOpenHashMap[StrataReservoir]

  /**
   * Map of each strata key (i.e. a unique combination of values of columns
   * in qcs) to related metadata and reservoir
   */
  protected final val stratas = {
    val types = qcs.map(schema(_).dataType)
    val numColumns = qcs.length
    val columnHandler = MultiColumnOpenHashSet.newColumnHandler(qcs,
      types, numColumns)
    val hasher = { row: Row => columnHandler.hash(row) }
    new ConcurrentSegmentedHashMap[Row, StrataReservoir, ReservoirSegment](
      (initialCapacity, loadFactor) => new ReservoirSegment(qcs, types,
        numColumns, initialCapacity, loadFactor), hasher)
  }

  /** Random number generator for sampling. */
  protected final val rng = new Random()

  private[sql] final val numSamplers = new AtomicInteger
  private[sql] final val numThreads = new AtomicInteger

  /**
   * Store pending values to be flushed in a separate buffer so that we
   * do not end up creating too small CachedBatches
   */
  protected final val pendingBatch = new mutable.ArrayBuffer[Row] {

    private final val lock = new ReentrantReadWriteLock()

    def readLock[U](f: => U): U = {
      val rlock = lock.readLock()
      rlock.lock()
      try {
        f
      } finally {
        rlock.unlock()
      }
    }

    def writeLock[U](f: => U): U = {
      val wlock = lock.writeLock()
      wlock.lock()
      try {
        f
      } finally {
        wlock.unlock()
      }
    }
  }

  def setSeed(seed: Long) {
    rng.setSeed(seed)
  }

  protected def strataReservoirSize: Int

  protected final def newMutableRow(parentRow: Row,
                                    process: Any => Any): MutableRow = {
    val row =
      if (process == null) parentRow else process(parentRow).asInstanceOf[Row]
    // add the weight column
    row match {
      case r: GenericRow =>
        val lastIndex = r.length
        val newRow = new Array[Any](lastIndex + 1)
        System.arraycopy(r.values, 0, newRow, 0, lastIndex)
        newRow(lastIndex) = LONG_ONE
        new GenericMutableRow(newRow)
      case _ =>
        val lastIndex = row.length
        val newRow = new GenericMutableRow(lastIndex + 1)
        var index = 0
        do {
          newRow(index) = row(index)
          index += 1
        } while (index < lastIndex)
        newRow(lastIndex) = LONG_ONE
        newRow
    }
  }

  def append[U](rows: Iterator[Row], processSelected: Any => Any,
                init: U, processFlush: (U, Row) => U, endBatch: U => U): U

  def sample(items: Iterator[Row]): Iterator[Row]

  protected final val flushStatus = new AtomicBoolean

  def setFlushStatus(doFlush: Boolean) = flushStatus.set(doFlush)

  def iterator: Iterator[Row] = {
    val sampleBuffer = new mutable.ArrayBuffer[Row](BUFSIZE)
    stratas.foldSegments(Iterator[Row]()) { (iter, seg) =>
      iter ++ {
        if (sampleBuffer.nonEmpty) sampleBuffer.clear()
        SegmentMap.lock(seg.readLock()) {
          seg.foldValues[Unit]()(foldReservoir(0, false, false, { (_, row) =>
            sampleBuffer += row
          }))
        }
        sampleBuffer.iterator
      }
    } ++ pendingBatch.readLock {
      if (pendingBatch.nonEmpty) pendingBatch.toArray.iterator
      else Iterator.empty
    }
  }

  protected final def foldDrainSegment[U](prevReservoirSize: Int,
                                          fullReset: Boolean,
                                          process: (U, Row) => U)
                                         (init: U, seg: ReservoirSegment): U = {
    seg.foldValues(init)(foldReservoir(prevReservoirSize, doReset = true,
      fullReset, process))
  }

  protected final def foldReservoir[U]
  (prevReservoirSize: Int, doReset: Boolean, fullReset: Boolean,
   process: (U, Row) => U)(sr: StrataReservoir, init: U): U = {
    // imperative code segment below for best efficiency
    var v = init
    val reservoir = sr.reservoir
    val nsamples = sr.reservoirSize
    val ratio = sr.prepareToRead(nsamples)
    val lastIndex = schema.length - 1
    var index = 0
    while (index < nsamples) {
      v = process(v, readReservoir(reservoir, index, ratio, lastIndex))
      index += 1
    }
    // reset transient data
    if (doReset) {
      sr.reset(prevReservoirSize, strataReservoirSize, fullReset)
    }
    v
  }

  protected def waitForSamplers(waitUntil: Int, maxTime: Long): Unit = {
    val startTime = System.currentTimeMillis
    numSamplers.synchronized {
      while (numSamplers.get > waitUntil && (numThreads.get > waitUntil ||
        maxTime <= 0 || (System.currentTimeMillis - startTime) <= maxTime))
        numSamplers.wait(100)
    }
  }
}

// TODO: optimize by having metadata as multiple columns like key;
// TODO: add a good sparse array implementation

/**
 * For each strata (i.e. a unique set of values for QCS), keep a set of
 * meta-data including number of samples collected, total number of rows
 * in the strata seen so far, the QCS key, reservoir of samples etc.
 */
final class StrataReservoir(var totalSamples: Int, var batchTotalSize: Int,
                            var reservoir: Array[MutableRow],
                            var reservoirSize: Int, var prevShortFall: Int) {

  self =>

  def iterator(prevReservoirSize: Int, newReservoirSize: Int,
               columns: Int, doReset: Boolean,
               fullReset: Boolean): Iterator[MutableRow] = {
    new Iterator[MutableRow] {

      final val reservoir = self.reservoir
      final val nsamples = self.reservoirSize
      final val ratio = prepareToRead(nsamples)
      final val lastIndex = columns - 1
      var pos = 0

      override def hasNext: Boolean = {
        if (pos < nsamples) {
          true
        } else if (doReset) {
          self.reset(prevReservoirSize, newReservoirSize, fullReset)
          false
        } else {
          false
        }
      }

      override def next() = {
        val v = readReservoir(reservoir, pos, ratio, lastIndex)
        pos += 1
        v
      }
    }
  }

  def prepareToRead(nsamples: Int): Long = {
    // calculate the weight ratio column
    if (nsamples > 0) {
      // combine the two integers into a long
      // higher order is number of samples (which is expected to remain mostly
      //   constant will result in less change)
      (nsamples.asInstanceOf[Long] << 32L) |
        self.batchTotalSize.asInstanceOf[Long]
    }
    else 0
  }

  def reset(prevReservoirSize: Int, newReservoirSize: Int,
            fullReset: Boolean): Unit = {

    if (newReservoirSize > 0) {
      val nsamples = self.reservoirSize
      // reset transient data

      // check for the end of current time-slot; if it has ended, then
      // also reset the "shortFall" and other such history
      if (fullReset) {
        self.totalSamples = 0
        self.prevShortFall = 0
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
          prevShortFall <= (prevReservoirSize + reservoirSize)) {
          prevShortFall += (prevReservoirSize - reservoirSize)
        }
        */
        self.prevShortFall += (prevReservoirSize - nsamples)
      }
      // shrink reservoir back to strataReservoirSize if required to avoid
      // growing possibly without bound (in case some strata consistently
      //   gets small number of total rows less than sample size)
      if (self.reservoir.length == newReservoirSize) {
        Utils.fillArray(reservoir, EMPTY_ROW, 0, nsamples)
      } else if (nsamples <= 2 && newReservoirSize > 3) {
        // empty the reservoir since it did not receive much data in last round
        self.reservoir = EMPTY_RESERVOIR
      } else {
        self.reservoir = new Array[MutableRow](newReservoirSize)
      }
      self.reservoirSize = 0
      self.batchTotalSize = 0
    }
  }
}

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

  private def tsColumnTime(row: Row): Long = {
    if (timeSeriesColumn >= 0) {
      val ts = row.get(timeSeriesColumn)
      ts match {
        case tl: Long => tl
        case ti: Int => ti.toLong
        case td: java.util.Date => td.getTime
        case _ => throw new AnalysisException(
          s"StratifiedSampler: Cannot parse 'timeSeriesColumn'=$ts")
      }
    } else {
      System.currentTimeMillis
    }
  }

  private def setTimeSlot(row: Row) = {
    val timeSlot = tsColumnTime(row)

    compareOrderAndSet(timeSlotStart, timeSlot,
      getMax = false)
    compareOrderAndSet(timeSlotEnd, timeSlot,
      getMax = true)
  }

  private final class ProcessRows[U](val processSelected: Any => Any,
                                     val processFlush: (U, Row) => U,
                                     val endBatch: U => U, var result: U)
    extends ChangeValue[Row, StrataReservoir] {

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
        setTimeSlot(row)
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
          sr.reservoir(rng.nextInt(reservoirCapacity)) = newMutableRow(row,
            processSelected)
          // update timeSlot start and end
          if (timeInterval > 0) {
            setTimeSlot(row)
          }
        }
        if (batchSamples.get >= (fraction * slotSize.incrementAndGet())) {
          sr
        }
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
          Utils.fillArray(newReservoir, EMPTY_ROW, reservoirLen, newReservoir.length)
          System.arraycopy(sr.reservoir, 0, newReservoir,
            0, reservoirLen)
          sr.reservoir = newReservoir
        }
        sr.reservoir(sr.reservoirSize) = newMutableRow(row, processSelected)
        sr.reservoirSize += 1
        sr.totalSamples += 1

        // update timeSlot start and end
        if (timeInterval > 0) {
          setTimeSlot(row)
        }

        compareOrderAndSet(maxSamples, sr.totalSamples, getMax = true)
        batchSamples.incrementAndGet()
        slotSize.incrementAndGet()
        sr
      }
    }

    override def segmentEnd(seg: SegmentMap[Row, StrataReservoir]): Unit = {}

    override def segmentAbort(seg: SegmentMap[Row, StrataReservoir]): Boolean = {
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
            this.cacheSize.set(
              math.max(4, (newCacheSize * 2 + prevCacheSize) / 3))
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

  override def sample(items: Iterator[Row]): Iterator[Row] = {
    // break up input into batches of "batchSize" and bulk sample each batch
    val batchSize = BUFSIZE
    val sampleBuffer = new mutable.ArrayBuffer[Row](math.min(batchSize,
      (batchSize * fraction * 10).toInt))

    def sampleIterator(iter: Iterator[Row],
                       doFlush: Boolean): Iterator[Row] = {
      val sbuffer = sampleBuffer
      if (sbuffer.nonEmpty) sbuffer.clear()
      // bulk sample the buffer
      append[Unit](iter, null, (), { (_, sampledRow) =>
        sbuffer += sampledRow; ()
      }, identity)
      if (doFlush) {
        flushCache[Unit]((), { (_, sampledRow) =>
          sampleBuffer += sampledRow; ()
        }, identity)
      }
      sbuffer.iterator
    }

    val batchIter = new SlicedIterator(items)
    new GenerateFlatIterator[Row, Boolean]({ finished =>
      batchIter.setSlice(0, batchSize)
      if (batchIter.hasNext) (sampleIterator(batchIter, doFlush = false), false)
      else if (finished) {
        // if required notify any other waiting samplers that iteration is done
        if (numSamplers.decrementAndGet() == 1) numSamplers.synchronized {
          numSamplers.notifyAll()
        }
        (GenerateFlatIterator.TERMINATE, true)
      }
      else if (flushStatus.get) {
        // wait for all other partitions to flush the cache
        waitForSamplers(1, 5000)
        // remove sampler used only for DataFrame => DataFrame transformation
        removeSampler(name, markFlushed = true)
        (sampleIterator(batchIter, doFlush = true), true)
      }
      else (sampleIterator(batchIter, doFlush = false), true)
    }, false)
  }

  override def clone: StratifiedSamplerCached = new StratifiedSamplerCached(
    qcs, name, schema, cacheSize, fraction, cacheBatchSize,
    timeSeriesColumn, timeInterval)
}

final class StratifiedSamplerReservoir(override val qcs: Array[Int],
                                       override val name: String,
                                       override val schema: StructType,
                                       private val reservoirSize: Int)
  extends StratifiedSampler(qcs, name, schema) {

  private final class ProcessRows(val processSelected: Any => Any)
    extends ChangeValue[Row, StrataReservoir] {

    override def defaultValue(row: Row) = {
      val strataSize = reservoirSize
      // create new strata if required
      val reservoir = new Array[MutableRow](strataSize)
      reservoir(0) = newMutableRow(row, processSelected)
      Utils.fillArray(reservoir, EMPTY_ROW, 1, strataSize)
      new StrataReservoir(1, 1, reservoir, 1, 0)
    }

    override def mergeValue(row: Row, sr: StrataReservoir): StrataReservoir = {
      // else update meta information in current strata
      val strataSize = reservoirSize
      sr.batchTotalSize += 1
      if (sr.reservoirSize >= strataSize) {
        // copy into the reservoir as per probability (strataSize/totalSize)
        val rnd = rng.nextInt(sr.batchTotalSize)
        if (rnd < strataSize) {
          // pick up this row and replace a random one from reservoir
          sr.reservoir(rng.nextInt(strataSize)) = newMutableRow(row,
            processSelected)
        }
      } else {
        // always copy into the reservoir for this case
        sr.reservoir(sr.reservoirSize) = newMutableRow(row, processSelected)
        sr.reservoirSize += 1
      }
      sr
    }

    override def segmentEnd(segment: SegmentMap[Row, StrataReservoir]): Unit = {}

    override def segmentAbort(segment: SegmentMap[Row, StrataReservoir]) = false
  }

  override protected def strataReservoirSize: Int = reservoirSize

  override def append[U](rows: Iterator[Row], processSelected: Any => Any,
                         init: U, processFlush: (U, Row) => U,
                         endBatch: U => U): U = {
    if (rows.hasNext) {
      stratas.bulkChangeValues(rows, new ProcessRows(processSelected))
    }
    init
  }

  override def sample(items: Iterator[Row]): Iterator[Row] = {
    // break up into batches of some size
    val batchSize = BUFSIZE
    val buffer = new mutable.ArrayBuffer[Row](batchSize)
    items.foreach { row =>
      if (buffer.length < batchSize) {
        buffer += row
      } else {
        // bulk append to sampler
        append[Unit](buffer.iterator, null, (), null, null)
        buffer.clear()
      }
    }
    // append any remaining in buffer
    if (buffer.nonEmpty) {
      append[Unit](buffer.iterator, null, (), null, null)
    }

    if (flushStatus.get) {
      // iterate over all the strata reservoirs for marked partition
      waitForSamplers(1, 5000)
      // remove sampler used only for DataFrame => DataFrame transformation
      removeSampler(name, markFlushed = true)
      // at this point we don't have a problem with concurrency
      val columns = schema.length
      stratas.flatMap(_.valuesIterator.flatMap(_.iterator(reservoirSize,
        reservoirSize, columns, doReset = true, fullReset = false)))
    }
    else {
      if (numSamplers.decrementAndGet() == 1) numSamplers.synchronized {
        numSamplers.notifyAll()
      }
      Iterator.empty
    }
  }

  override def clone: StratifiedSamplerReservoir =
    new StratifiedSamplerReservoir(qcs, name, schema, reservoirSize)
}
