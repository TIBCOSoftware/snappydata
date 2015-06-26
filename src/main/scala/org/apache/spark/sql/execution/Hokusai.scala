package org.apache.spark.sql.execution

import java.util.concurrent.locks.ReentrantReadWriteLock

import io.snappydata.util.NumberUtils
import org.apache.spark.sql.execution.cms.CountMinSketch
import java.util.{ Timer, TimerTask }
import scala.collection.mutable.{ ListBuffer, MutableList, Stack }
import scala.reflect.ClassTag
import scala.collection.mutable.MutableList
import scala.collection.mutable.ListBuffer
import scala.math
import scala.collection.mutable.Stack
import scala.util.Random

// TODO Make sure M^t and A^t coincide  I think the timeAggregation may run left to right, but the
//  item aggregation might run the other way in my impl????
/**
 * Implements the algorithms and data structures from "Hokusai -- Sketching
 * Streams in Real Time", by Sergiy Matusevych, Alexander Smola, Amr Ahmed.
 * http://www.auai.org/uai2012/papers/231.pdf
 *
 * Aggregates state, so this is a mutable class.
 *
 * Since we are all still learning scala, I thought I'd explain the use of
 * implicits in this file.   TimeAggregation takes an implicit constructor
 * parameter:
 *    TimeAggregation[T]()(implicit val cmsMonoid: CMSMonoid[T])
 * The purpose for that is:
 * + In Algebird, a CMSMonoid[T] is a factory for creating instances of CMS[T]
 * + TimeAggregation needs to occasionally make new CMS instances, so it will
 *   use the factory
 * + By making it an implicit (and in the curried param), the outer context of
 *   the TimeAggregation can create/ensure that the factory is there.
 * + Hokusai[T] will be the "outer context" so it can handle that for
 *   TimeAggregation
 *
 *
 * TODO
 * 1. Decide if the underlying CMS should be mutable (save memory) or
 *    functional (algebird) I'm afraid that with the functional approach,
 *    and having so many, every time we merge two CMS, we create a third
 *    and that is wasteful of memory or may take too much memory. If we go
 *    with a mutable CMS, we have to either make stream-lib's serializable,
 *    or make our own.
 *
 * 2. Clean up intrusion of algebird shenanigans in the code (implicit
 *    factories etc)
 *
 * 3. Decide on API for managing data and time.  Do we increment time in a
 *    separate operation or add a time parameter to addData()?
 *
 * 4. Decide if we want to be mutable or functional in this datastruct.
 *    Current version is mutable.
 */
class Hokusai[T: ClassTag](cmsParams: CMSParams, windowSize: Long, epoch0: Long,
  startIntervalGenerator: Boolean = true) {
  //assert(NumberUtils.isPowerOfTwo(numIntervals))

  private val intervalGenerator = new Timer()

  val rwLock = new ReentrantReadWriteLock()
  val readLock = rwLock.readLock
  val writeLock = rwLock.writeLock
  val mergeCreator: ((Array[CountMinSketch[T]]) => CountMinSketch[T]) = (estimators) => CountMinSketch.merge[T](estimators: _*)
  def this(cmsParams: CMSParams, windowSize: Long) = this(cmsParams, windowSize,
    System.currentTimeMillis())

  val timeEpoch = new TimeEpoch(windowSize, epoch0)

  val taPlusIa = new TimeAndItemAggregation()
  // Current data accummulates into mBar until the next time tick
  var mBar: CountMinSketch[T] = createZeroCMS(0)

  if (startIntervalGenerator) {
    intervalGenerator.schedule(createTimerTask(this.increment), 0, windowSize)
  }

  private val queryTillLastN_Case2: (T, Int) => Option[Long] = (item: T, sumUpTo: Int) => Some(this.taPlusIa.queryBySummingTimeAggregates(item, sumUpTo))

  private val queryTillLastN_Case1: (T) => () => Option[Long] = (item: T) => () => Some(this.taPlusIa.ta.aggregates(0).estimateCount(item))

  private val queryTillLastN_Case3: (T, Int, Int, Int, Int) => Option[Long] = (item: T, lastNIntervals: Int, totalIntervals: Int, n: Int, nQueried: Int) =>
    if (lastNIntervals > totalIntervals) {
      Some(this.taPlusIa.queryBySummingTimeAggregates(item, n))
    } else {
      val nearestPowerOf2Num = NumberUtils.nearestPowerOf2LE(lastNIntervals)
      var count = this.taPlusIa.queryBySummingTimeAggregates(item,
        NumberUtils.isPowerOf2(nearestPowerOf2Num))
      // the remaining interval will lie in the time interval range
      val lengthOfLastInterval = nearestPowerOf2Num
      val residualIntervals = lastNIntervals - nearestPowerOf2Num
      if (residualIntervals > (lengthOfLastInterval * 3) / 4) {
        //it would be better to find the time aggregation of last interval - the other intervals)
        count += this.taPlusIa.queryTimeAggregateForInterval(item, lengthOfLastInterval)
        count -= this.taPlusIa.basicQuery(lastNIntervals + 1 to (2 * nearestPowerOf2Num).asInstanceOf[Int],
          item, nearestPowerOf2Num.asInstanceOf[Int], nearestPowerOf2Num.asInstanceOf[Int] * 2)
      } else {
        count += this.taPlusIa.basicQuery(nearestPowerOf2Num.asInstanceOf[Int] + 1 to lastNIntervals, item,
          nearestPowerOf2Num.asInstanceOf[Int],
          nearestPowerOf2Num.asInstanceOf[Int] * 2)

      }

      Some(count)
    }

  private val queryTillLastN_Case4: (T, Int, Int, Int, Int) => Option[Long] = (item: T, lastNIntervals: Int, totalIntervals: Int, n: Int, nQueried: Int) => {
    val lastNIntervalsToQuery = if (lastNIntervals > totalIntervals) {
      totalIntervals
    } else {
      lastNIntervals
    }

    val (bestPath, computedIntervalLength) = this.taPlusIa.intervalTracker.identifyBestPath(lastNIntervalsToQuery,
      true)

    val skipLastInterval = if (computedIntervalLength > lastNIntervalsToQuery) {
      //this means that last one or many intervals lie within a time interval range
      //drop  the last interval from the count of  best path as it needs to be handled separately
      bestPath.last
    } else {
      -1
    }

    var total = bestPath.aggregate[Long](0)((total, interval) => {
      total +
        (if (interval != skipLastInterval) {
          this.taPlusIa.queryTimeAggregateForInterval(item, interval)
        } else {
          0
        })
    }, _ + _)

    // Add the first item representing interval 1
    total += this.taPlusIa.ta.aggregates(0).estimateCount(item)

    if (computedIntervalLength > lastNIntervalsToQuery) {
      // start individual query from interval
      // The accuracy becomes very poor if we query the first interval 1 using entity
      //aggregates . So subtraction approach needs to be looked at more carefully
      val residualLength = lastNIntervalsToQuery - (computedIntervalLength - skipLastInterval)
      if (residualLength > (skipLastInterval * 3) / 4) {
        //it will be better to add the whole time aggregate & substract the other intervals

        total += this.taPlusIa.queryTimeAggregateForInterval(item, skipLastInterval)
        val begin = (lastNIntervals + 1).asInstanceOf[Int]
        val end = computedIntervalLength.asInstanceOf[Int]
        total -= this.taPlusIa.basicQuery(begin to end,
          item, skipLastInterval.asInstanceOf[Int], computedIntervalLength.asInstanceOf[Int])

      } else {
        val begin = (computedIntervalLength - skipLastInterval + 1).asInstanceOf[Int]

        total += this.taPlusIa.basicQuery(begin to lastNIntervalsToQuery,
          item, skipLastInterval.asInstanceOf[Int], computedIntervalLength.asInstanceOf[Int])
      }
    }
    Some(total)
  }

  def increment(): Unit = {
    this.writeLock.lockInterruptibly()
    try {
      timeEpoch.increment()
      this.taPlusIa.increment(mBar, timeEpoch.t)
      mBar = createZeroCMS(0)
    } finally {
      this.writeLock.unlock()
    }
  }

  // For testing.  This follows spark one update per batch model?  But
  // we may need to control for time a bit more carefully?
  def addEpochData(data: Seq[T]) = {
    accummulate(data)
  }

  def addEpochData(data: scala.collection.Map[T, Long]) = {
    accummulate(data)
  }

  // Get the frequency estimate for key in epoch from the CMS
  // If there is no data for epoch, returns None
  /*def query(epoch: Long, key: T): Option[Long] =
    // TODO I don't like passing numIntervals in .....
    timeEpoch.jForTimestamp(epoch, numIntervals).flatMap(ta.query(_, key))*/

  def queryTillTime(epoch: Long, key: T): Option[Long] = {
    this.executeInReadLock(
      this.timeEpoch.timestampToInterval(epoch).flatMap(x =>

        if (x > timeEpoch.t) {
          None
        } else {
          this.queryTillLastNIntervals(this.taPlusIa.convertIntervalBySwappingEnds(x.asInstanceOf[Int]).asInstanceOf[Int],
            key)
        }),
      true)

  }

  def queryAtTime(epoch: Long, key: T): Option[Long] = {

    this.executeInReadLock(
      this.timeEpoch.timestampToInterval(epoch).flatMap(x =>
        if (x > timeEpoch.t) {
          None
        } else {
          this.taPlusIa.queryAtInterval(this.taPlusIa.convertIntervalBySwappingEnds(x.asInstanceOf[Int]).asInstanceOf[Int],
            key)
        }), true)

  }

  def queryBetweenTime(epochFrom: Long, epochTo: Long, key: T): Option[Long] = {

    this.executeInReadLock(
      {
        val (later, earlier) = convertEpochToIntervals(epochFrom, epochTo) match {
          case Some(x) => x
          case None => return None
        }
        this.taPlusIa.queryBetweenIntervals(later, earlier, key)
      }, true)

  }

  def convertEpochToIntervals(epochFrom: Long, epochTo: Long): Option[(Int, Int)] = {
    val fromInterval = this.timeEpoch.timestampToInterval(epochFrom) match {
      case Some(x) => x
      case None => return None
    }

    val toInterval = this.timeEpoch.timestampToInterval(epochTo) match {
      case Some(x) => x
      case None => return None
    }

    if (fromInterval > timeEpoch.t || toInterval > timeEpoch.t) {
      return None
    }
    if (fromInterval > toInterval) {
      Some((fromInterval, toInterval))
    } else {
      Some((toInterval, fromInterval))
    }
  }

  //def accummulate(data: Seq[Long]): Unit = mBar = mBar ++ cmsMonoid.create(data)
  def accummulate(data: Seq[T]): Unit = this.executeInReadLock {
    data.foreach(i => mBar.add(i, 1L))
  }

  def accummulate(data: scala.collection.Map[T, Long]): Unit =
    this.executeInReadLock {
      data.foreach { case (item, count) => mBar.add(item, count) }
    }

  protected def executeInReadLock[T](body: => T, lockInterruptibly: Boolean = false): T = {
    if (lockInterruptibly) {
      this.readLock.lockInterruptibly()
    } else {
      this.readLock.lock()
    }
    try {
      body
    } finally {
      this.readLock.unlock()
    }
  }

  def queryTillLastNIntervals(lastNIntervals: Int, item: T): Option[Long] =
    this.taPlusIa.queryLastNIntervals[Option[Long]](lastNIntervals, queryTillLastN_Case1(item),
      queryTillLastN_Case2(item, _),
      queryTillLastN_Case3(item, _, _, _, _),
      queryTillLastN_Case4(item, _, _, _, _))

  private def createTimerTask(taskBody: => Unit): TimerTask = new TimerTask() {
    override def run() {
      taskBody
    }
  }

  def createZeroCMS(intervalFromLast: Int): CountMinSketch[T] =
    Hokusai.newZeroCMS[T](cmsParams.depth, cmsParams.width, cmsParams.hashA)

  override def toString = s"Hokusai[ta=${taPlusIa}, mBar=${mBar}]"

  class ItemAggregation {
    val aggregates = new MutableList[CountMinSketch[T]]()

    def increment(cms: CountMinSketch[T], t: Long): Unit = {
      //TODo :Asif : Take care of long being casted to int
      // Since the indexes in aggregates are 0 based, A^1 is obtained by A(0)
      aggregates += cms
      for (k <- 1 to math.floor(Hokusai.log2X(t)).asInstanceOf[Int]) {
        val compressIndex = t.asInstanceOf[Int] - math.pow(2, k).asInstanceOf[Int] - 1
        val compressCMS = aggregates(compressIndex)
        if (compressCMS.width > 512) {
          this.aggregates.update(compressIndex, compressCMS.compress)
        }
      }
    }

  }
  /*
val a: Array[Option[CountMinSketch]] = Array.fill(numIntervals) { None } // This is A from the paper
def algo3(): Unit = {

  val ln = a.length
  val l = Hokusai.ilog2(h.timeUnits - 1)

      h.liveItems++

      if h.liveItems >= 1<<h.intervals {
        // kill off the oldest live interval
        h.itemAggregate[ln-h.liveItems+1] = nil
        h.liveItems--
      }

      for k := 1; k < l; k++ {
        // itemAggregation[t] is the data array for time t
        sk := h.itemAggregate[ln-1<<uint(k)]
        // FIXME(dgryski): can we avoid this check by be smarter about loop bounds?
        if sk != nil {
          sk.Compress()
        }
      }
      h.itemAggregate = append(h.itemAggregate, h.sk.Clone())

      }
    */

  /**
   * Data Structures and Algorithms to maintain Time Aggregation from the paper.
   * Time is modeled as a non-decreasing integer starting at 0.
   *
   * The type parameter, T, is the key type.  This needs to be numeric.  Typical
   * value is Long, but Short can cut down on size of resulting data structure,
   * but increased chance of collision.  BigInt is another possibility.
   *
   *
   * From Theorem 4 in the Paper:
   *
   * At time t, the sketch M^j contains statistics for the period
   * [t-delta, t-delta-2^j] where delta = t mod 2^j
   *
   * The following shows an example of how data ages through m() as t
   * starts at 0 and increases:
   *
   * {{{
   *    === t = 0
   *      t=0  j=0 m is EMPTY
   *    === t = 1
   *      t=1  j=0 m(0)=[0, 1) # secs in m(0): 1
   *    === t = 2
   *      t=2  j=0 m(0)=[1, 2) # secs in m(0): 1
   *      t=2  j=1 m(1)=[0, 2) # secs in m(1): 2
   *    === t = 3
   *      t=3  j=0 m(0)=[2, 3) # secs in m(0): 1
   *      t=3  j=1 m(1)=[0, 2) # secs in m(1): 2
   *    === t = 4
   *      t=4  j=0 m(0)=[3, 4) # secs in m(0): 1
   *      t=4  j=1 m(1)=[2, 4) # secs in m(1): 2
   *      t=4  j=2 m(2)=[0, 4) # secs in m(2): 4
   *    === t = 5
   *      t=5  j=0 m(0)=[4, 5) # secs in m(0): 1
   *      t=5  j=1 m(1)=[2, 4) # secs in m(1): 2
   *      t=5  j=2 m(2)=[0, 4) # secs in m(2): 4
   *    === t = 6
   *      t=6  j=0 m(0)=[5, 6) # secs in m(0): 1
   *      t=6  j=1 m(1)=[4, 6) # secs in m(1): 2
   *      t=6  j=2 m(2)=[0, 4) # secs in m(2): 4
   *    === t = 7
   *      t=7  j=0 m(0)=[6, 7) # secs in m(0): 1
   *      t=7  j=1 m(1)=[4, 6) # secs in m(1): 2
   *      t=7  j=2 m(2)=[0, 4) # secs in m(2): 4
   *    === t = 8
   *      t=8  j=0 m(0)=[7, 8) # secs in m(0): 1
   *      t=8  j=1 m(1)=[6, 8) # secs in m(1): 2
   *      t=8  j=2 m(2)=[4, 8) # secs in m(2): 4
   *      t=8  j=3 m(3)=[0, 8) # secs in m(3): 8
   *    === t = 9
   *      t=9  j=0 m(0)=[8, 9) # secs in m(0): 1
   *      t=9  j=1 m(1)=[6, 8) # secs in m(1): 2
   *      t=9  j=2 m(2)=[4, 8) # secs in m(2): 4
   *      t=9  j=3 m(3)=[0, 8) # secs in m(3): 8
   * }}}
   *
   * @param numIntervals The number of sketches to keep in the exponential backoff.
   *        the last one will have a sketch of all data.  Default value is 16.
   */
  class TimeAggregation {
    val aggregates = new MutableList[CountMinSketch[T]]()
    // The terse variable names follow the variable names in the paper
    // (minus capitalization)

    // The array of exponentially decaying CMSs. In the paper, this is called
    // M^j, but here we call it m(j). Each subsequent one covers a time
    // period twice as long as the previous We will lazily initialize as time
    // progresses, to keep memory down

    // Increments time step by one unit, and compresses the sketches to
    // maintain the invariants.  This is Algorithm 2 in the paper.
    /*override def increment(cms: CountMinSketch[T], t: Long): Unit = {
    if (NumberUtils.isPowerOfTwo(t.asInstanceOf[Int])) {
      // Make a dummy  entry at the last  position
      this.aggregates += new CountMinSketch[T](cmsParams.depth, cmsParams.width, cmsParams.hashA)
    }
    var mBar = cms
    (0 to Hokusai.maxJ(t)) foreach ({ j =>
      val temp = mBar
      val mj = this.aggregates.get(j) match {
        case Some(map) => map
        case None => {
          //make a place so update can happen correctly
          throw new IllegalStateException("The index should have had a CMS")
          //new CountMinSketch[T](cmsParams.depth, cmsParams.width, cmsParams.hashA) //mZero
        }
      }
      mBar = CountMinSketch.merge[T](mBar, mj)
      this.aggregates.update(j, temp)
    })

  }*/

    def query(index: Int, key: T): Option[Long] = this.aggregates.get(index).map(_.estimateCount(key))

    def increment(cms: CountMinSketch[T], t: Long, rangeToAggregate: Int): Unit = {
      if (t == 1 || t == 2) {
        cms +=: this.aggregates
      } else {
        val powerOf2 = NumberUtils.isPowerOf2(t.asInstanceOf[Int] - 1)
        if (powerOf2 != -1) {
          // Make a dummy  entry at the last  position
          this.aggregates += createZeroCMS(powerOf2)
        }
        var mBar = cms

        (0 to rangeToAggregate) foreach ({ j =>
          val temp = mBar
          val mj = this.aggregates.get(j) match {
            case Some(map) => map
            case None => {
              throw new IllegalStateException("The index should have had a CMS")

            }
          }
          if (j != 0) {

            mBar = mergeCreator(Array[CountMinSketch[T]](mBar, mj))
            //CountMinSketch.merge[T](mBar, mj)
          } else {
            mBar = mj
          }
          this.aggregates.update(j, temp)
        })
      }

    }

    override def toString =
      s"TimeAggregation[${this.aggregates.mkString("M=[", ", ", "]")}]"
  }

  class TimeAndItemAggregation {
    // val aggregates = new MutableList[CountMinSketch[T]]()
    val ta = new TimeAggregation()
    val ia = new ItemAggregation()
    val intervalTracker: IntervalTracker = new IntervalTracker()

    def increment(mBar: CountMinSketch[T], t: Long): Unit = {
      val rangeToAggregate = if (NumberUtils.isPowerOfTwo(t.asInstanceOf[Int])) {
        1
      } else if (NumberUtils.isPowerOfTwo(t.asInstanceOf[Int] - 1)) {
        NumberUtils.isPowerOf2(t - 1) + 1
      } else {
        intervalTracker.numSaturatedSize
      }
      ta.increment(mBar, t, rangeToAggregate)
      ia.increment(mBar, t)
      //this.basicIncrement(mBar, t, rangeToAggregate)
      this.intervalTracker.updateIntervalLinks(t)
    }
    /*
  private def basicIncrement(cms: CountMinSketch[T], t: Long, rangeToAggregate: Int): Unit = {
    if (t == 1 ) {
      cms +=: this.aggregates
    }else if(t ==2) {
      cms +=: this.aggregates
      this.aggregates.update(1, this.aggregates.last.compress)
    }
    else if (t > 2) {
      var s = cms
      (0 to rangeToAggregate) foreach { j =>
        
        val temp = s
        val sJ = this.aggregates.get(j) match {
          case Some(map) => map
          case None => {
            this.aggregates += null
            new CountMinSketch[T](cmsParams.depth, s.width, cmsParams.hashA)
          }
        }
        if (j != 0) {
         s = CountMinSketch.merge(s, sJ)
         if(s.width >1) {
         s = s.compress
         }
        } else {
          s = sJ.compress
          
        }
        
        this.aggregates.update(j, temp)
      }

    }

    

    //this.intervalTracker.updateIntervalLinks(t)

  }*/
    /*
  private def basicIncrement(cms: CountMinSketch[T], t: Long, rangeToAggregate: Int): Unit = {
    if (t == 1) {
      cms.compress +=: this.aggregates
    } else if (t == 2) {
      val prev = this.aggregates.head.compress
      this.aggregates.update(0, prev)
      cms.compress +=: this.aggregates
    } else {
      var s = cms
      (0 to rangeToAggregate) foreach { j =>
        if (s.width > 1) {
          s = s.compress
        }
        val temp = s
        val sJ = this.aggregates.get(j) match {
          case Some(map) => map
          case None => {
            this.aggregates += null
            new CountMinSketch[T](cmsParams.depth, s.width, cmsParams.hashA)
          }
        }
        if (j != 0) {
          s = CountMinSketch.merge(s, sJ)
        } else {
          s = sJ
        }

        this.aggregates.update(j, temp)
      }

    }

    //this.intervalTracker.updateIntervalLinks(t)

  }*/

    def queryAtInterval(lastNthInterval: Int, key: T): Option[Long] = {
      if (lastNthInterval == 1) {
        Some(this.ta.aggregates(0).estimateCount(key))
      } else {
        // Identify the best path which contains the last interval
        val (bestPath, computedIntervalLength) = this.intervalTracker.identifyBestPath(lastNthInterval,
          true)
        val lastIntervalRange = bestPath.last.asInstanceOf[Int]
        if (lastIntervalRange == 1) {
          Some(ta.aggregates(1).estimateCount(key))
        } else {
          Some(this.basicQuery(lastNthInterval to lastNthInterval,
            key, lastIntervalRange, computedIntervalLength.asInstanceOf[Int]))
        }

      }

    }

    def queryBetweenIntervals(later: Int, earlier: Int, key: T): Option[Long] = {
      val fromLastNInterval = this.convertIntervalBySwappingEnds(later)
      val tillLastNInterval = this.convertIntervalBySwappingEnds(earlier)
      if (fromLastNInterval == 1 && tillLastNInterval == 1) {
        Some(this.ta.aggregates(0).estimateCount(key))
      } else {
        // Identify the best path
        val (bestPath, computedIntervalLength) = this.intervalTracker.identifyBestPath(tillLastNInterval.asInstanceOf[Int],
          true, 1, fromLastNInterval.asInstanceOf[Int])

        var truncatedSeq = bestPath
        var start = if (fromLastNInterval == 1) {
          fromLastNInterval + 1
        } else {
          fromLastNInterval
        }
        var taIntervalStartsAt = computedIntervalLength - bestPath.aggregate[Long](0)(_ + _, _ + _) + 1
        var finalTotal = bestPath.aggregate[Long](0)((total, interval) => {
          val lengthToDrop = truncatedSeq.head
          truncatedSeq = truncatedSeq.drop(1)
          val lengthTillInterval = computedIntervalLength - truncatedSeq.aggregate[Long](0)(_ + _, _ + _)
          val end = if (lengthTillInterval > tillLastNInterval) {
            tillLastNInterval
          } else {
            lengthTillInterval
          }

          val total1 = if (start == taIntervalStartsAt && end == lengthTillInterval) {
            // can add the time series aggregation as whole interval is needed
            ta.aggregates(NumberUtils.isPowerOf2(interval) + 1).estimateCount(key)
          } else {
            basicQuery(start.asInstanceOf[Int] to end.asInstanceOf[Int],
              key, interval.asInstanceOf[Int], lengthTillInterval.asInstanceOf[Int])
          }
          start = lengthTillInterval + 1
          taIntervalStartsAt += lengthToDrop
          total + total1
        }, _ + _)

        if (fromLastNInterval == 1) {
          finalTotal += this.ta.aggregates(0).estimateCount(key)
        }
        Some(finalTotal)
      }

    }

    def queryLastNIntervals[B](lastNIntervals: Int, queryTillLastN_Case1: () => B,
      queryTillLastN_Case2: (Int) => B,
      queryTillLastN_Case3: (Int, Int, Int, Int) => B,
      queryTillLastN_Case4: (Int, Int, Int, Int) => B): B = {
      if (lastNIntervals == 1) {
        return queryTillLastN_Case1()
      }

      //check the total number of intervals excluding the current ( in progress).
      //The current interval counts only at the end of the current interval
      val totalIntervals = this.ia.aggregates.size
      // If total number of intervals is some power of 2, then all intervals are segregated &
      // there is no overlap
      val n = NumberUtils.isPowerOf2(totalIntervals)
      val nQueried = NumberUtils.isPowerOf2(lastNIntervals)
      if (n != -1 && nQueried != -1) {
        val sumUpTo = math.min(n, nQueried)
        //Some(queryBySummingTimeAggregates(item, sumUpTo))
        queryTillLastN_Case2(sumUpTo)
      } else if (n != -1) {
        // the total intervals are power of 2 , but the queried up to interval is not
        // In which case we find the nearest interval, which is power of 2 and sum up
        // those time aggregates & approximate the remaining using interpolation
        queryTillLastN_Case3(lastNIntervals, totalIntervals, n, nQueried)

      } else {
        queryTillLastN_Case4(lastNIntervals, totalIntervals, n, nQueried)
      }

    }

    /**
     * @param tIntervalstoQuery the tIntervals range to query  as per the time aggregates. That is
     * the interval range is the intervals in the past relative to most recent interval
     *  Note that
     * time aggregates are from right to left, that is most recent interval is at the 0th position
     *  , while item aggregates are from left to right, that is most recent interval is at the end
     */
    def basicQuery(tIntervalsToQuery: Range, key: T, taIntervalWithInstant: Int,
      totalComputedIntervalLength: Int): Long = {
      //handle level =1
      val totalIntervals = this.ia.aggregates.size
      val jStar = NumberUtils.isPowerOf2(taIntervalWithInstant) + 1

      val beginingOfRangeAsPerIA =
        this.convertIntervalBySwappingEnds(totalComputedIntervalLength)

      val endRangeAsPerIA = beginingOfRangeAsPerIA + taIntervalWithInstant - 1
      val mJStar = this.ta.aggregates.get(jStar).get
      var total: Long = 0
      var n: Array[Long] = null
      val bStart = beginingOfRangeAsPerIA.asInstanceOf[Int]
      val bEnd = endRangeAsPerIA.asInstanceOf[Int]
      val mjStarCount = mJStar.estimateCount(key)
      tIntervalsToQuery foreach {
        j =>
          val intervalNumRelativeToIA = convertIntervalBySwappingEnds(j).asInstanceOf[Int]
          val cmsAtT = this.ia.aggregates.get(intervalNumRelativeToIA - 1).get
          val hashes = cmsAtT.getIHashesFor(key)
          val nTilda = calcNTilda(cmsAtT, hashes)
          val width = if (j <= 2) {
            cmsParams.width
          } else {
            cmsParams.width - Hokusai.ilog2(j - 1) + 1
          }
          total += (if (nTilda > math.E * intervalNumRelativeToIA / (1 << width)
            && (nTilda < mjStarCount)) {
            nTilda
          } else {
            if (n == null) {
              n = this.queryBySummingEntityAggregates(key, bStart - 1, bEnd - 1)
            }
            calcNCarat(key, jStar, cmsAtT, hashes, n, mJStar)
          })

      }
      total

    }

    /**
     * Converts the last n th interval to interval from begining
     * For example, the most recent interval is say 8th.
     * 8, 7, 6 , 5 , 4 , 3 , 2 ,1
     * last 3rd interval will be converted to 6th interval from the begining
     * The starting interval is 1 based.
     *
     * or
     * Converts the  n th interval from 1st interal onwards to interval number from the end
     * i.e lastNth interval from the most recent interval;
     * For example, the most recent interval is say 8th.
     * 8, 7, 6 , 5 , 4 , 3 , 2 ,1
     * 3rd interval will be converted to 6th interval from the end that is 6th most recent interval
     * The starting interval is 1 based.
     */
    def convertIntervalBySwappingEnds(intervalNumber: Long) =
      this.ia.aggregates.size - intervalNumber + 1

    def queryBySummingTimeAggregates(item: T, sumUpTo: Int): Long = {
      var count: Long = 0
      (0 to sumUpTo) foreach {
        j => count += this.ta.aggregates.get(j).get.estimateCount(item)
      }

      count
    }

    def queryTimeAggregateForInterval(item: T, interval: Long): Long = {
      assert(NumberUtils.isPowerOf2(interval) != -1)
      ta.aggregates(NumberUtils.isPowerOf2(interval) + 1).estimateCount(item)
    }

    private def queryBySummingEntityAggregates(item: T, startIndex: Int, sumUpTo: Int): Array[Long] = {
      var n = Array.ofDim[Long](cmsParams.depth)

      (startIndex to sumUpTo) foreach {
        j =>
          {
            val cms = this.ia.aggregates.get(j).get
            val hashes = cms.getIHashesFor(item)
            0 until n.length foreach { i =>
              n(i) = n(i) + cms.table(i)(hashes(i))
            }
          }
      }

      n
    }

    private def calcNTilda(cms: CountMinSketch[T],
      hashes: Array[Int]): Long = {
      var res = scala.Long.MaxValue;
      for (i <- 0 until cms.depth) {
        res = Math.min(res, cms.table(i)(hashes(i)));
      }
      return res;
    }

    private def calcNCarat(key: T, jStar: Int, cmsAtT: CountMinSketch[T],
      hashesForTime: Array[Int], sumOverEntities: Array[Long], mJStar: CountMinSketch[T]): Long = {

      val mjStarHashes = mJStar.getIHashesFor(key)
      var res = scala.Long.MaxValue;
      var m: Long = scala.Long.MaxValue;
      var c: Long = scala.Long.MaxValue;
      var b: Long = scala.Long.MaxValue;
      // since the indexes are zero based

      for (i <- 0 until cmsAtT.depth) {
        if (sumOverEntities(i) != 0) {
          res = Math.min(res, (mJStar.table(i)(mjStarHashes(i)) * cmsAtT.table(i)(hashesForTime(i))) / sumOverEntities(i))
        }
      }

      return if (res == scala.Long.MaxValue) {
        0
      } else {
        res
      }

    }

  }
}

/**
 * Manages a time epoch and how to index into it.
 */
class TimeEpoch(val windowSize: Long, val epoch0: Long) {
  val MAX_J = 16 // Using Int.MaxValue is waayyyyyyyyyyyy to computationally expensive
  // we are talking exponents here, so 2^64 ought to be big enough for anyone?
  var t: Long = 0 // t: The current time epoch

  // TODO: Right now, we treat time as going up one tick per batch, but
  // windowSize and epoch0 are put in place so we can have a time-tick be
  // any number of seconds.  Not really used yet.
  // val epoch0 = System.currentTimeMillis() // The oldest timestamp known to this TimeAggregation
  //val windowSize = 1L // How many time units we increment in an increment

  def increment() = t = t + 1

  // For a given timestamp, what is the smallest index into m which
  // contains data for the timestamp?
  // TODO: This essentially searches O(log n) time periods for the correct one
  // Perhaps there is an O(1) way to calculate?
  def timestampToInterval(ts: Long): Option[Int] = {
    if (ts < epoch0) return None
    if (ts == epoch0 && t >= 1) {
      Some(1)
    } else {
      /*val interval = if((ts - epoch0) % windowSize == 0) {
      (ts - epoch0)/windowSize
    }else {
      (ts - epoch0)/windowSize + 1
    }*/
      val interval = (ts - epoch0) / windowSize + 1
      Some(interval.asInstanceOf[Int])

    }

    // This is bad: Searches...but this will generate correct test cases!
    /*
    (0 until MAX_J) foreach ({ j =>
      val tp = timePeriodForJ(t, j)
      tp map { x =>
        if (x._1 <= ts1 && ts1 < x._2)
          return Some(j)
      }
    })
    None*/
  }

  /*
  def timePeriodForJ(startTime: Long, j: Int): Option[(Long, Long)] = {
    if (startTime < 0 || j < 0)
      return None

    val twoToJ = 1L << j
    val delta = startTime % twoToJ
    val p1 = startTime - delta
    val p2 = startTime - delta - twoToJ
    if (p2 >= 0L) Some(p2, p1) else None
  }*/

}

// TODO Better handling of params and construction (both delta/eps and d/w support)
case class CMSParams(width: Int, depth: Int, seed: Int = 123) {
  val hashA = createHashA

  private def createHashA: Array[Long] = {
    val r = new Random(seed)
    Array.fill[Long](depth)(r.nextInt(Int.MaxValue))
  }
}

class IntervalTracker {
  private var head: IntervalLink = null
  private val unsaturatedIntervals: Stack[IntervalLink] = new Stack[IntervalLink]()

  def identifyBestPath(lastNIntervals: Int,
    encompassLastInterval: Boolean = false,
    prevTotal: Long = 1,
    startingFromInterval: Int = 1): (Seq[Long], Long) = {
    //The first 1 interval is outside the LinkedInterval but will always be there separate
    //But the computed interval length includes the 1st interval
    this.head.identifyBestPath(lastNIntervals, prevTotal, encompassLastInterval, startingFromInterval)
  }

  def numSaturatedSize: Int = {
    var len = 0

    var start = this.unsaturatedIntervals.top
    while (start != null) {
      len += 1
      start = start.prevLink
    }
    len
  }

  def updateIntervalLinks(t: Long): Unit = {
    if (t > 2) {
      val lastIntervalPowerOf2 = NumberUtils.isPowerOf2(t - 1)
      if (lastIntervalPowerOf2 != -1) {
        // create new Interval Link , with values
        //2^n+1
        this.unsaturatedIntervals.clear()
        val seq = new ListBuffer[Long]()
        var a = 1
        seq += 1
        for (i <- 1 to lastIntervalPowerOf2) {
          a *= 2
          seq += a
        }
        this.head = new IntervalLink(seq)
        this.unsaturatedIntervals.push(this.head)
      } else {
        val topUnsaturated = this.unsaturatedIntervals.top
        this.head = topUnsaturated.buildBackward
        if (topUnsaturated.isSaturated) {
          this.unsaturatedIntervals.pop()
        }
        if (!this.head.isSaturated) {
          this.unsaturatedIntervals.push(this.head)
        }
      }
    } else if (t == 2) {
      val seq = new ListBuffer[Long]()
      seq += 1
      this.head = new IntervalLink(seq)
    }

  }

  /**
   * Gets all the possible paths which can be calculated from the
   * given state of intervals.
   */
  def getAllPossiblePaths: Seq[Seq[Int]] = {
    var all: Seq[Seq[Int]] = Seq[Seq[Int]]()
    var start = this.head
    var last = Seq[Int](1)
    all = all.+:(last)
    while (start != null) {
      (0 to start.intervals.length - 2) foreach {
        i =>
          val elem = start.intervals(i)
          val path = last :+ elem.asInstanceOf[Int]
          all = all.+:(path)
      }
      val path = last :+ start.intervals.last.asInstanceOf[Int]
      all = all.+:(path)

      last = last.+:(start.intervals.last.asInstanceOf[Int])
      start = start.nextLink
    }

    all
  }

  private class IntervalLink(val intervals: ListBuffer[Long]) {

    def this(interval: Long) = this(ListBuffer(interval))
    // intervals having same start
    //The max interval will form the link to the next

    var nextLink: IntervalLink = null
    var prevLink: IntervalLink = null

    def buildBackward(): IntervalLink = {
      val topInterval = this.intervals.remove(0)
      val newHead = new IntervalLink(topInterval)
      //subsume the previous intervals
      if (prevLink != null) {
        prevLink.subsume(newHead.intervals)
      }
      this.prevLink = newHead
      newHead.nextLink = this
      newHead
    }

    def subsume(subsumer: ListBuffer[Long]) {
      this.intervals ++=: subsumer
      if (prevLink != null) {
        prevLink.subsume(subsumer)
      }
    }

    def isSaturated: Boolean = this.intervals.size == 1

    def identifyBestPath(lastNIntervals: Int, prevTotal: Long,
      encompassLastInterval: Boolean, startingFromInterval: Int): (Seq[Long], Long) = {
      val last = this.intervals.last
      if (last + prevTotal == lastNIntervals) {
        (Seq[Long](last), last + prevTotal)
      } else if (last + prevTotal < lastNIntervals) {
        if (this.nextLink != null) {
          val (seq, total) = this.nextLink.identifyBestPath(lastNIntervals, last + prevTotal,
            encompassLastInterval, startingFromInterval)
          if (prevTotal + last >= startingFromInterval) {
            (last +: seq, total)
          } else {
            (seq, total)
          }
        } else {
          (Seq[Long](last), last + prevTotal)
        }
      } else {
        if (encompassLastInterval) {
          this.intervals.find { x => x + prevTotal >= lastNIntervals } match {
            case Some(x) => if (x + prevTotal >= startingFromInterval) {
              (Seq[Long](x), x + prevTotal)
            } else {
              (Seq.empty, x + prevTotal)
            }
            case None => (Seq.empty, prevTotal)
          }
        } else {
          this.intervals.reverse.find { x => x + prevTotal <= lastNIntervals } match {
            case Some(x) => {
              if (x + prevTotal >= startingFromInterval) {
                (Seq[Long](x), x + prevTotal)
              } else {
                (Seq.empty, x + prevTotal)
              }
            }
            case None => (Seq.empty, prevTotal)
          }
        }
      }

    }
  }

}
object Hokusai {

  def log2X(X: Long): Double = math.log10(X) / math.log10(2)

  def newZeroCMS[T: ClassTag](depth: Int, width: Int, hashA: Array[Long]) =
    new CountMinSketch[T](depth, width, hashA)

  // @return the max i such that t % 2^i is zero
  // from the paper (I think the paper has a typo, and "i" should be "t"):
  //    argmax {l where i mod 2^l == 0}
  def maxJ(t: Long): Int = {
    if (t <= 1) return 0
    var l = 0
    while (t % (1 << l) == 0) l = l + 1
    l - 1
  }

  def ilog2(value: Int): Int = {
    var r = 0
    var v = value
    while (v != 0) {
      v = v >> 1
      r = r + 1
    }
    r
  }

}
