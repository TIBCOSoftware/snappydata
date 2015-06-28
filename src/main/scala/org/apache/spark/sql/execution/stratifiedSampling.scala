package org.apache.spark.sql.execution

import java.util.Random
import java.util.concurrent.atomic.{AtomicReference, AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable
import scala.language.reflectiveCalls

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.collection._
import org.apache.spark.sql.execution.StratifiedSampler._
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.{AnalysisException, Row, SampleDataFrame}
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{Logging, Partition, SparkEnv, TaskContext}

case class StratifiedSample(var options: Map[String, Any],
    @transient override val child: logical.LogicalPlan)
    // pre-compute QCS because it is required by
    // other API driven from driver
    (val qcs: Array[Int] = SampleDataFrame.resolveQCS(
      options, child.schema.fieldNames))
    extends logical.UnaryNode {

  /**
   * StratifiedSample will add one additional column for the ratio of total
   * rows seen for a stratum to the number of samples picked.
   */
  override val output = child.output :+ AttributeReference(
    SampleDataFrame.WEIGHTAGE_COLUMN_NAME, LongType, nullable = false)()

  override protected final def otherCopyArgs: Seq[AnyRef] = Seq(qcs)

  /**
   * Perform stratified sampling given a Query-Column-Set (QCS). This variant
   * can also use a fixed fraction to be sampled instead of fixed number of
   * total samples since it is also designed to be used with streaming data.
   */
  case class Execute(override val child: SparkPlan,
      override val output: Seq[Attribute]) extends UnaryNode {

    protected override def doExecute(): RDD[Row] =
      new StratifiedSampledRDD(child.execute(), qcs,
        sqlContext.conf.columnBatchSize, options, schema)
  }

  def getExecution(plan: SparkPlan) = Execute(plan, output)
}

private final class ExecutorPartitionInfo(val blockId: BlockManagerId,
    val remainingMem: Long, var remainingPartitions: Double)
    extends java.lang.Comparable[ExecutorPartitionInfo] {

  /** update "remainingPartitions" and enqueue again */
  def usePartition(queue: java.util.PriorityQueue[ExecutorPartitionInfo],
      removeFromQueue: Boolean) = {
    if (removeFromQueue) queue.remove(this)
    remainingPartitions -= 1.0
    queue.offer(this)
    this
  }

  override def compareTo(other: ExecutorPartitionInfo): Int = {
    // reverse the order so that max partitions is at head of queue
    java.lang.Double.compare(other.remainingPartitions, remainingPartitions)
  }
}

final class SamplePartition(val parent: Partition, override val index: Int,
    @transient private[this] val _partInfo: ExecutorPartitionInfo,
    var isLastHostPartition: Boolean) extends Partition with Serializable {

  val blockId = _partInfo.blockId

  def hostExecutorId = Utils.getHostExecutorId(blockId)

  override def toString =
    s"SamplePartition($index, $blockId, isLast=$isLastHostPartition)"
}

final class StratifiedSampledRDD(@transient parent: RDD[Row],
    qcs: Array[Int],
    cacheBatchSize: Int,
    options: Map[String, Any],
    schema: StructType)
    extends RDD[Row](parent) with Serializable {

  var executorPartitions: Map[BlockManagerId, IndexedSeq[Int]] = Map.empty

  /**
   * This method does detailed scheduling itself which is required given that
   * the sample cache is not managed by Spark's scheduler implementations.
   * Depending on the amount of memory reported as remaining, we will assign
   * appropriate weight to that executor.
   */
  override def getPartitions: Array[Partition] = {
    val peerExecutorMap = new mutable.HashMap[String,
        mutable.ArrayBuffer[ExecutorPartitionInfo]]()
    var totalMemory = 1L
    for ((blockId, (max, remaining)) <- Utils.getAllExecutorsMemoryStatus(
      sparkContext)) {
      peerExecutorMap.getOrElseUpdate(blockId.host,
        new mutable.ArrayBuffer[ExecutorPartitionInfo](4)) +=
          new ExecutorPartitionInfo(blockId, remaining, 0)
      totalMemory += remaining
    }
    if (peerExecutorMap.nonEmpty) {
      // Split partitions executor-wise:
      //  1) assign number of partitions to each executor in proportion
      //     to amount of remaining memory on the executor
      //  2) use a priority queue to order the hosts
      //  3) first prefer the parent's hosts and select the executor with
      //     maximum remaining partitions to be assigned
      //  4) if no parent preference then select head of priority queue
      // So number of partitions assigned to each executor are known and wait
      // on each executor accordingly to drain the remaining cache.
      val parentPartitions = parent.partitions
      // calculate the approx number of partitions for each executor in
      // proportion to its total remaining memory and place in priority queue
      val queue = new java.util.PriorityQueue[ExecutorPartitionInfo]
      val numPartitions = parentPartitions.length
      peerExecutorMap.values.foreach(_.foreach { partInfo =>
        partInfo.remainingPartitions =
            (partInfo.remainingMem.toDouble * numPartitions) / totalMemory
        queue.offer(partInfo)
      })
      val partitions = (0 until numPartitions).map { index =>
        val ppart = parentPartitions(index)
        val plocs = firstParent[Row].preferredLocations(ppart)
        // get the "best" one as per the maximum number of remaining partitions
        // to be assigned from among the parent partition's preferred locations
        // (that can be hosts or executors), else if none found then use the
        // head of priority queue among available peers to get the
        // "best" available executor

        // first find all executors for preferred hosts of parent partition
        plocs.flatMap { loc =>
          val underscoreIndex = loc.indexOf('_')
          if (underscoreIndex >= 0) {
            // if the preferred location is already an executorId then search
            // in available executors for its host
            val host = loc.substring(0, underscoreIndex)
            val executorId = loc.substring(underscoreIndex + 1)
            val executors = peerExecutorMap.getOrElse(host, Iterator.empty)
            executors.find(_.blockId.executorId == executorId) match {
              // executorId found so return the single value
              case Some(executor) => Iterator(executor)
              // executorId not found so return all executors for its host
              case None => executors
            }
          } else {
            peerExecutorMap.getOrElse(loc, Iterator.empty)
          }
          // reduceLeft below will find the executor with max number of
          // remaining partitions assigned to it
        }.reduceLeftOption((pm, p) => if (p.compareTo(pm) >= 0) pm else p).map {
          // get the SamplePartition for the "best" executor
          partInfo => new SamplePartition(ppart, index, partInfo.usePartition(
            queue, removeFromQueue = true), isLastHostPartition = false)
          // queue.poll below will pick "best" one from the head of queue when
          // no valid executor found from parent preferred locations
        }.getOrElse(new SamplePartition(ppart, index, queue.poll().usePartition(
          queue, removeFromQueue = false), isLastHostPartition = false))
      }
      val partitionOrdering = Ordering[Int].on[SamplePartition](_.index)
      executorPartitions = partitions.groupBy(_.blockId).map { case (k, v) =>
        val sortedPartitions = v.sorted(partitionOrdering)
        // mark the last partition in each host as the last one that should
        // also be necessarily scheduled on that host
        sortedPartitions.last.isLastHostPartition = true
        (k, sortedPartitions.map(_.index))
      }
      partitions.toArray[Partition]
    } else {
      executorPartitions = Map.empty
      Array.empty[Partition]
    }
  }

  override def compute(split: Partition,
      context: TaskContext): Iterator[Row] = {
    val part = split.asInstanceOf[SamplePartition]
    val thisBlockId = SparkEnv.get.blockManager.blockManagerId
    // use -ve cacheBatchSize to indicate that no additional batching is to be
    // done but still pass the value through for size increases
    val sampler = StratifiedSampler(options, qcs, "_rdd_" + id, -cacheBatchSize,
      schema, cached = true)
    val numSamplers = executorPartitions(part.blockId).length

    if (part.blockId != thisBlockId) {
      // if this is the last partition then it has to be necessarily scheduled
      // on specified host
      if (part.isLastHostPartition) {
        throw new IllegalStateException(
          s"Unexpected execution of $part on $thisBlockId")
      } else {
        // this has been scheduled from some other target node so increment
        // the number of expected samplers but don't fail
        logWarning(s"Unexpected execution of $part on $thisBlockId")
        if (!sampler.numSamplers.compareAndSet(0, numSamplers + 1)) {
          sampler.numSamplers.incrementAndGet()
        }
      }
    } else {
      sampler.numSamplers.compareAndSet(0, numSamplers)
    }
    sampler.numThreads.incrementAndGet()
    try {
      sampler.sample(firstParent[Row].iterator(part.parent, context),
        // If we are the last partition on this host, then wait for all
        // others to finish and then drain the remaining cache. The flag
        // is set persistently by this last thread so that any other stray
        // additional partitions will also do the flush.
        part.isLastHostPartition || sampler.flushStatus.get)
    } finally {
      sampler.numThreads.decrementAndGet()
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    Seq(split.asInstanceOf[SamplePartition].hostExecutorId)
}

object StratifiedSampler {

  private final val globalMap = new mutable.HashMap[String, StratifiedSampler]
  private final val mapLock = new ReentrantReadWriteLock

  private final val MAX_FLUSHED_MAP_AGE_SECS = 300
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
    val reservoirSizeTest = "stratumReservoirSize".ci
    val timeSeriesColumnTest = "timeSeriesColumn".ci
    val timeIntervalTest = "timeInterval".ci

    val timeIntervalSpec = "([0-9]+)(s|m|h)".r
    val cols = schema.fieldNames

    // using a default stratum size of 104 since 100 is taken as the normal
    // limit for assuming a Gaussian distribution (e.g. see MeanEvaluator)
    val defaultStratumSize = 104
    // Using foldLeft to read key-value pairs and build into the result
    // tuple of (qcs, fraction, stratumReservoirSize) like an aggregate.
    // This "aggregate" simply keeps the last values for the corresponding
    // keys as found when folding the map.
    val (nm, fraction, stratumSize, tsCol, timeInterval) = options.
        foldLeft("", 0.0, defaultStratumSize, -1, 0) {
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
              s"StratifiedSampler: Cannot parse 'stratumReservoirSize'=$optV")
          }
          case timeSeriesColumnTest() => optV match {
            case tss: String => (n, fr, sz, Utils.columnIndex(tss, cols), ti)
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
      lookupOrAdd(qcs, name, fraction, stratumSize, cacheBatchSize,
        tsCol, timeInterval, schema)
    } else {
      newSampler(qcs, name, fraction, stratumSize, cacheBatchSize,
        tsCol, timeInterval, schema)
    }
  }

  def apply(name: String): Option[StratifiedSampler] = {
    SegmentMap.lock(mapLock.readLock) {
      globalMap.get(name)
    }
  }

  private[sql] def lookupOrAdd(qcs: Array[Int], name: String,
      fraction: Double, stratumSize: Int,
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
            val sampler = newSampler(qcs, name, fraction, stratumSize,
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
        flushedMaps.put(name, System.nanoTime)
        // clear old values if map is too large and expiration
        // has been hit for one or more older entries
        if (flushedMaps.size > MAX_FLUSHED_MAP_SIZE) {
          val expireTime = System.nanoTime -
              (MAX_FLUSHED_MAP_AGE_SECS * 1000000000L)
          flushedMaps.takeWhile(_._2 <= expireTime).keysIterator.
              foreach(flushedMaps.remove)
        }
      }
    }

  private def newSampler(qcs: Array[Int], name: String, fraction: Double,
      stratumSize: Int, cacheBatchSize: Int, tsCol: Int,
      timeInterval: Int, schema: StructType) = {
    if (qcs.isEmpty) {
      throw new AnalysisException(SampleDataFrame.ERROR_NO_QCS)
    } else if (tsCol >= 0 && timeInterval <= 0) {
      throw new AnalysisException("StratifiedSampler: no timeInterval for " +
          "timeSeriesColumn=" + schema(tsCol).name)
    } else if (fraction > 0.0) {
      new StratifiedSamplerCached(qcs, name, schema,
        new AtomicInteger(stratumSize), fraction, cacheBatchSize,
        tsCol, timeInterval)
    } else if (stratumSize > 0) {
      new StratifiedSamplerReservoir(qcs, name, schema, stratumSize)
    } else {
      throw new AnalysisException("StratifiedSampler: " +
          s"'fraction'=$fraction 'stratumReservoirSize'=$stratumSize")
    }
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

abstract class StratifiedSampler(val qcs: Array[Int], val name: String,
    val schema: StructType)
    extends Serializable with Cloneable with Logging {

  type ReservoirSegment = MultiColumnOpenHashMap[StratumReservoir]

  /**
   * Map of each stratum key (i.e. a unique combination of values of columns
   * in qcs) to related metadata and reservoir
   */
  protected final val strata = {
    val types = qcs.map(schema(_).dataType)
    val numColumns = qcs.length
    val columnHandler = MultiColumnOpenHashSet.newColumnHandler(qcs,
      types, numColumns)
    val hasher = { row: Row => columnHandler.hash(row) }
    new ConcurrentSegmentedHashMap[Row, StratumReservoir, ReservoirSegment](
      (initialCapacity, loadFactor) => new ReservoirSegment(qcs, types,
        numColumns, initialCapacity, loadFactor), hasher)
  }

  /** Random number generator for sampling. */
  protected final val rng =
    new Random(org.apache.spark.util.Utils.random.nextLong)

  private[sql] final val numSamplers = new AtomicInteger
  private[sql] final val numThreads = new AtomicInteger

  /**
   * Store pending values to be flushed in a separate buffer so that we
   * do not end up creating too small CachedBatches.
   *
   * Note that this mini-cache is copy-on-write (to avoid copy-on-read for
   * readers) so the buffer inside should never be changed rather the whole
   * buffer replaced if required. This should happen only inside flushCache.
   */
  protected final val pendingBatch = new AtomicReference[
      mutable.ArrayBuffer[Row]](new mutable.ArrayBuffer[Row])

  protected def stratumReservoirSize: Int

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

  def sample(items: Iterator[Row], flush: Boolean): Iterator[Row]

  private[sql] final val flushStatus = new AtomicBoolean

  def setFlushStatus(doFlush: Boolean) = flushStatus.set(doFlush)

  def iterator: Iterator[Row] = {
    val sampleBuffer = new mutable.ArrayBuffer[Row](BUFSIZE)
    strata.foldSegments(Iterator[Row]()) { (iter, seg) =>
      iter ++ {
        if (sampleBuffer.nonEmpty) sampleBuffer.clear()
        SegmentMap.lock(seg.readLock()) {
          seg.foldValues[Unit]((), foldReservoir(0, false, false, { (_, row) =>
            sampleBuffer += row
          }))
        }
        sampleBuffer.iterator
      }
    } ++ {
      val pbatch = this.pendingBatch.get()
      if (pbatch.nonEmpty) pbatch.iterator
      else Iterator.empty
    }
  }

  protected final def foldDrainSegment[U](prevReservoirSize: Int,
      fullReset: Boolean,
      process: (U, Row) => U)
      (init: U, seg: ReservoirSegment): U = {
    seg.foldValues(init, foldReservoir(prevReservoirSize, doReset = true,
      fullReset, process))
  }

  protected final def foldReservoir[U]
  (prevReservoirSize: Int, doReset: Boolean, fullReset: Boolean,
      process: (U, Row) => U)(sr: StratumReservoir, init: U): U = {
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
      sr.reset(prevReservoirSize, stratumReservoirSize, fullReset)
    }
    v
  }

  protected def waitForSamplers(waitUntil: Int, maxMillis: Long) {
    val startTime = System.currentTimeMillis
    numThreads.decrementAndGet()
    try {
      numSamplers.synchronized {
        while (numSamplers.get > waitUntil &&
            (numThreads.get > 0 || maxMillis <= 0 ||
                (System.currentTimeMillis - startTime) <= maxMillis))
          numSamplers.wait(100)
      }
    } finally {
      numThreads.incrementAndGet()
    }
  }
}

// TODO: optimize by having metadata as multiple columns like key;
// TODO: add a good sparse array implementation

/**
 * For each stratum (i.e. a unique set of values for QCS), keep a set of
 * meta-data including number of samples collected, total number of rows
 * in the stratum seen so far, the QCS key, reservoir of samples etc.
 */
class StratumReservoir(final var totalSamples: Int,
    final var batchTotalSize: Int,
    final var reservoir: Array[MutableRow],
    final var reservoirSize: Int,
    final var prevShortFall: Int) {

  self =>

  final def iterator(prevReservoirSize: Int, newReservoirSize: Int,
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

  final def prepareToRead(nsamples: Int): Long = {
    // calculate the weight ratio column
    if (nsamples > 0) {
      // combine the two integers into a long
      // higher order is number of samples (which is expected to remain mostly
      //   constant will result in less change)
      (nsamples.asInstanceOf[Long] << 32L) |
          self.batchTotalSize.asInstanceOf[Long]
    } else 0
  }

  final def reset(prevReservoirSize: Int, newReservoirSize: Int,
      fullReset: Boolean) {

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
      // shrink reservoir back to stratumReservoirSize if required to avoid
      // growing possibly without bound (in case some stratum consistently
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
