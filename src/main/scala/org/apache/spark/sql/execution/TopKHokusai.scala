package org.apache.spark.sql.execution

import java.util.concurrent.locks.ReentrantReadWriteLock

import io.snappydata.util.NumberUtils
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.collection.{ SegmentMap, BoundedSortedSet }
import org.apache.spark.sql.execution.cms.{ CountMinSketch, TopKCMS }
import org.apache.spark.sql.types.{ StructField, StructType }
import scala.collection.mutable
import scala.reflect.ClassTag

class TopKHokusai[T: ClassTag](cmsParams: CMSParams, val windowSize: Long, val epoch0: Long,
  val topKActual: Int, startIntervalGenerator: Boolean = true)
  extends Hokusai[T](cmsParams, windowSize, epoch0, startIntervalGenerator) {
  val topKInternal = topKActual * 2
  private val queryTillLastNTopK_Case1: () => Array[(T, Long)] = () => {
    val topKCMS = this.taPlusIa.ta.aggregates(0).asInstanceOf[TopKCMS[T]]
    topKCMS.getTopK
  }

  private val queryTillLastNTopK_Case2: (Int) => Array[(T, Long)] = (sumUpTo: Int) => sortAndBound(getTopKBySummingTimeAggregates(sumUpTo))

  private val queryTillLastNTopK_Case3: (Int, Int, Int, Int) => Array[(T, Long)] = (lastNIntervals: Int, totalIntervals: Int, n: Int, nQueried: Int) =>
    if (lastNIntervals > totalIntervals) {
      sortAndBound(getTopKBySummingTimeAggregates(n))
    } else {
      val nearestPowerOf2NumGE = NumberUtils.nearestPowerOf2GE(lastNIntervals)
      val nearestPowerOf2Num = NumberUtils.nearestPowerOf2LE(lastNIntervals)
      //get all the unioned top k keys.
      val estimators = this.taPlusIa.ta.aggregates.slice(0,
        NumberUtils.isPowerOf2(nearestPowerOf2NumGE) + 1)
      val unionedTopKKeys = TopKCMS.getUnionedTopKKeysFromEstimators(estimators)
      //get the top k count till the last but one interval
      val mappings = getTopKBySummingTimeAggregates(NumberUtils.isPowerOf2(nearestPowerOf2Num),
        unionedTopKKeys)

      // the remaining interval will lie in the time interval range
      val lengthOfLastInterval = nearestPowerOf2Num

      val residualIntervals = lastNIntervals - nearestPowerOf2Num
      if (residualIntervals > (lengthOfLastInterval * 3) / 4) {
        //it would be better to find the time aggregation of last interval - the other intervals)
        unionedTopKKeys.foreach { item: T =>
          var total = this.queryTimeAggregateForInterval(item, lengthOfLastInterval)
          var count = this.taPlusIa.basicQuery(lastNIntervals + 1 to (2 * nearestPowerOf2Num).asInstanceOf[Int],
            item, nearestPowerOf2Num.asInstanceOf[Int], nearestPowerOf2Num.asInstanceOf[Int] * 2)
          if (count < total) {
            total = total - count
          } else {
            // what to do? ignore as count is abnormally high
          }
          val prevCount = mappings.getOrElse[java.lang.Long](item, 0)
          mappings += (item -> (prevCount + count))
        }

      } else {
        unionedTopKKeys.foreach { item: T =>
          val count = this.taPlusIa.basicQuery(nearestPowerOf2Num.asInstanceOf[Int] + 1 to lastNIntervals,
            item, nearestPowerOf2Num.asInstanceOf[Int], nearestPowerOf2Num.asInstanceOf[Int] * 2)
          val prevCount = mappings.getOrElse[java.lang.Long](item, 0)
          mappings += (item -> (prevCount + count))
        }

      }
      sortAndBound(mappings)
    }

  private val queryTillLastNTopK_Case4: (Int, Int, Int, Int) => Array[(T, Long)] = (lastNIntervals: Int, totalIntervals: Int, n: Int, nQueried: Int) =>
    { // the number of intervals so far elapsed is not of form 2 ^n. So the time aggregates are
      // at various stages of overlaps
      //Identify the total range of intervals by identifying the highest 2^n , greater than or equal to
      // the interval

      val lastNIntervalsToQuery = if (lastNIntervals > totalIntervals) {
        totalIntervals
      } else {
        lastNIntervals
      }

      val (bestPath, computedIntervalLength) = this.taPlusIa.intervalTracker.identifyBestPath(lastNIntervalsToQuery,
        true)
      // get all the unified top k keys of all the intervals in the path
      var estimators = bestPath.map { interval => taPlusIa.ta.aggregates(NumberUtils.isPowerOf2(interval) + 1).asInstanceOf[TopKCMS[T]] }
      estimators = this.taPlusIa.ta.aggregates(0).asInstanceOf[TopKCMS[T]] +: estimators
      val unifiedTopKKeys = TopKCMS.getUnionedTopKKeysFromEstimators(estimators)

      val skipLastInterval = if (computedIntervalLength > lastNIntervalsToQuery) {
        //this means that last one or many intervals lie within a time interval range
        //drop  the last interval from the count of  best path as it needs to be handled separately
        bestPath.last
      } else {
        -1
      }
      if (skipLastInterval != -1) {
        estimators = estimators.dropRight(1)
      }
      val topKs = TopKCMS.getCombinedTopKFromEstimators(estimators, unifiedTopKKeys)

      if (computedIntervalLength > lastNIntervalsToQuery) {
        // start individual query from interval
        // The accuracy becomes very poor if we query the first interval 1 using entity
        //aggregates . So subtraction approach needs to be looked at more carefully
        val residualLength = lastNIntervalsToQuery - (computedIntervalLength - skipLastInterval)
        if (residualLength > (skipLastInterval * 3) / 4) {
          //it will be better to add the whole time aggregate & substract the other intervals
          unifiedTopKKeys.foreach { item: T =>
            val total = this.queryTimeAggregateForInterval(item, skipLastInterval)
            val prevCount = topKs.getOrElse[java.lang.Long](item, 0)
            topKs += (item -> (total + prevCount))
          }

          val begin = (lastNIntervals + 1).asInstanceOf[Int]
          val end = computedIntervalLength.asInstanceOf[Int]
          unifiedTopKKeys.foreach { item: T =>
            val total = this.taPlusIa.basicQuery(begin to end,
              item, skipLastInterval.asInstanceOf[Int], computedIntervalLength.asInstanceOf[Int])
            val prevCount = topKs.getOrElse[java.lang.Long](item, 0)
            if (prevCount > total) {
              topKs += (item -> (prevCount - total))
            } else {
              ///ignore the values as they are abnormal. what to do?....
            }
          }

        } else {
          val begin = (computedIntervalLength - skipLastInterval + 1).asInstanceOf[Int]
          unifiedTopKKeys.foreach { item: T =>
            val total = this.taPlusIa.basicQuery(begin to lastNIntervalsToQuery,
              item, skipLastInterval.asInstanceOf[Int], computedIntervalLength.asInstanceOf[Int])
            val prevCount = topKs.getOrElse[java.lang.Long](item, 0)
            topKs += (item -> (total + prevCount))
          }
        }
      }
      /*val temp = new BoundedSortedSet[T](10000, false)
      topKs.foreach(temp.add(_))
      System.out.println(temp)*/
      sortAndBound(topKs)

    }

  override val mergeCreator: ((Array[CountMinSketch[T]]) => CountMinSketch[T]) = estimators =>
    TopKCMS.merge[T](estimators(1).asInstanceOf[TopKCMS[T]].topKInternal * 2, estimators)

  def this(cmsParams: CMSParams, windowSize: Long, topK: Int) = this(cmsParams, windowSize,
    System.currentTimeMillis(), topK)

  def getTopKTillTime(epoch: Long): Option[Array[(T, Long)]] = {
    this.executeInReadLock(
      this.timeEpoch.timestampToInterval(epoch).flatMap(x => {
        val interval = if (x > timeEpoch.t) {
          timeEpoch.t
        } else {
          x
        }
        Some(this.taPlusIa.queryLastNIntervals[Array[(T, Long)]](
          this.taPlusIa.convertIntervalBySwappingEnds(interval.asInstanceOf[Int]).asInstanceOf[Int],
          queryTillLastNTopK_Case1, queryTillLastNTopK_Case2, queryTillLastNTopK_Case3,
          queryTillLastNTopK_Case4))

      }), true)

  }

  def getTopKForCurrentInterval: Option[Array[(T, Long)]] = this.executeInReadLock(
    {
      Some(this.mBar.asInstanceOf[TopKCMS[T]].getTopK)
    }, true)

  def getTopKBetweenTime(epochFrom: Long, epochTo: Long): Option[Array[(T, Long)]] = {
    this.executeInReadLock(
      {
        val (later, earlier) = convertEpochToIntervals(epochFrom, epochTo) match {
          case Some(x) => x
          case None => return None
        }
        Some(this.getTopKBetweenTime(later, earlier))
      }, true)
  }

  def getTopKBetweenTime(later: Int, earlier: Int): Array[(T, Long)] = {
    val fromLastNInterval = this.taPlusIa.convertIntervalBySwappingEnds(later)
    val tillLastNInterval = this.taPlusIa.convertIntervalBySwappingEnds(earlier)
    if (fromLastNInterval == 1 && tillLastNInterval == 1) {
      queryTillLastNTopK_Case1()
    } else {
      // Identify the best path
      val (bestPath, computedIntervalLength) = this.taPlusIa.intervalTracker.identifyBestPath(tillLastNInterval.asInstanceOf[Int],
        true, 1, fromLastNInterval.asInstanceOf[Int])
      var estimators = bestPath.map { interval => taPlusIa.ta.aggregates(NumberUtils.isPowerOf2(interval) + 1).asInstanceOf[TopKCMS[T]] }
      if (fromLastNInterval == 1) {
        estimators = this.taPlusIa.ta.aggregates(0).asInstanceOf[TopKCMS[T]] +: estimators
      }
      val unifiedTopKKeys = TopKCMS.getUnionedTopKKeysFromEstimators(estimators)

      var truncatedSeq = bestPath
      var start = if (fromLastNInterval == 1) {
        fromLastNInterval + 1
      } else {
        fromLastNInterval
      }
      val topKs = scala.collection.mutable.HashMap[T, java.lang.Long]()
      var taIntervalStartsAt = computedIntervalLength - bestPath.aggregate[Long](0)(_ + _, _ + _) + 1

      bestPath.foreach { interval =>
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
          val topKCMS = this.taPlusIa.ta.aggregates(NumberUtils.isPowerOf2(interval) + 1).asInstanceOf[TopKCMS[T]]
          TopKCMS.getCombinedTopKFromEstimators(Array(topKCMS), unifiedTopKKeys, topKs)
        } else {
          unifiedTopKKeys.foreach { item: T =>
            val total = this.taPlusIa.basicQuery(start.asInstanceOf[Int] to end.asInstanceOf[Int],
              item, interval.asInstanceOf[Int], lengthTillInterval.asInstanceOf[Int])
            val prevCount = topKs.getOrElse[java.lang.Long](item, 0)
            topKs += (item -> (total + prevCount))
          }
        }
        start = lengthTillInterval + 1
        taIntervalStartsAt += lengthToDrop

      }

      if (fromLastNInterval == 1) {
        TopKCMS.getCombinedTopKFromEstimators(
          Array(this.taPlusIa.ta.aggregates(0).asInstanceOf[TopKCMS[T]]), unifiedTopKKeys, topKs)

      }
      sortAndBound(topKs)
    }

  }

  def queryTimeAggregateForInterval(item: T, interval: Long): Long = {
    assert(NumberUtils.isPowerOf2(interval) != -1)
    val topKCMS = this.taPlusIa.ta.aggregates(NumberUtils.isPowerOf2(interval) + 1).asInstanceOf[TopKCMS[T]]
    topKCMS.getFromTopKMap(item).getOrElse(this.taPlusIa.queryTimeAggregateForInterval(item, interval))
  }

  private def getTopKBySummingTimeAggregates(sumUpTo: Int, setOfTopKKeys: scala.collection.mutable.Set[T] = null): scala.collection.mutable.Map[T, java.lang.Long] = {

    val estimators = this.taPlusIa.ta.aggregates.slice(0, sumUpTo + 1)

    val topKs = TopKCMS.getCombinedTopKFromEstimators(estimators,

      if (setOfTopKKeys != null) {
        setOfTopKKeys
      } else {
        TopKCMS.getUnionedTopKKeysFromEstimators(estimators)
      })
    topKs

  }

  private def sortAndBound[T](topKs: scala.collection.mutable.Map[T, java.lang.Long]): Array[(T, Long)] = {
    val sortedData = new BoundedSortedSet[T](this.topKActual, false)
    topKs.foreach(sortedData.add(_))
    val iter = sortedData.iterator
    //topKs.foreach(sortedData.add(_))
    Array.fill[(T, Long)](sortedData.size())({
      val (key, value) = iter.next
      (key, value.longValue())

    })

  }

  override def createZeroCMS(powerOf2: Int): CountMinSketch[T] =
    if (powerOf2 == 0) {
      //TODO: fix this
      val x = if (this.topKInternal == 0) {
        2 * this.topKActual
      } else {
        this.topKInternal
      }
      TopKHokusai.newZeroCMS[T](cmsParams.depth, cmsParams.width, cmsParams.hashA, topKActual, x)
    } else {
      TopKHokusai.newZeroCMS[T](cmsParams.depth, cmsParams.width, cmsParams.hashA, topKActual,
        topKInternal * (powerOf2 + 1))
    }

}

object TopKHokusai {
  // TODO: Resolve the type of TopKHokusai
  private final val topKMap = new mutable.HashMap[String, TopKHokusai[Any]]
  private final val mapLock = new ReentrantReadWriteLock

  def newZeroCMS[T: ClassTag](depth: Int, width: Int, hashA: Array[Long], topKActual: Int, topKInternal: Int) =
    new TopKCMS[T](topKActual, topKInternal, depth, width, hashA)
  def apply(name: String): Option[TopKHokusai[Any]] = {
    SegmentMap.lock(mapLock.readLock) {
      topKMap.get(name)
    }
  }

  def apply(name: String, confidence: Double,
    eps: Double, size: Int, timeInterval: Long) = {
    lookupOrAdd(name, confidence, eps, size, timeInterval)
  }

  private[sql] def lookupOrAdd(name: String, confidence: Double, eps: Double,
    size: Int, timeInterval: Long): TopKHokusai[Any] = {
    SegmentMap.lock(mapLock.readLock) {
      topKMap.get(name)
    } match {
      case Some(topk) => topk
      case None =>
        // insert into global map but double-check after write lock
        SegmentMap.lock(mapLock.writeLock) {
          topKMap.getOrElse(name, {
            // TODO: why is seed needed as an input param

            val depth = Math.ceil(-Math.log(1 - confidence) / Math.log(2)).toInt
            val width = Math.ceil(2 / eps).toInt

            val cmsParams = CMSParams(width, depth)

            val topk = new TopKHokusai[Any](cmsParams: CMSParams,
              timeInterval: Long, System.currentTimeMillis(), size)
            topKMap(name) = topk
            topk
          })
        }
    }
  }
}

class TopKHokusaiWrapper[T](val name: String, val confidence: Double, val eps: Double,
  val size: Int, val timeInterval: Long,
  val schema: StructType, val key: StructField) extends Serializable

object TopKHokusaiWrapper {

  implicit class StringExtensions(val s: String) extends AnyVal {
    def ci = new {
      def unapply(other: String) = s.equalsIgnoreCase(other)
    }
  }

  def apply(name: String, options: Map[String, Any],
    schema: StructType): TopKHokusaiWrapper[Any] = {
    val keyTest = "key".ci
    val timeIntervalTest = "timeInterval".ci
    val confidenceTest = "confidence".ci
    val epsTest = "eps".ci
    val sizeTest = "size".ci

    val cols = schema.fieldNames

    // using a default strata size of 104 since 100 is taken as the normal
    // limit for assuming a Gaussian distribution (e.g. see MeanEvaluator)
    val defaultStrataSize = 104
    // Using foldLeft to read key-value pairs and build into the result
    // tuple of (qcs, fraction, strataReservoirSize) like an aggregate.
    // This "aggregate" simply keeps the last values for the corresponding
    // keys as found when folding the map.
    val (key, timeInterval, confidence, eps, size) = options.
      foldLeft("", 5L, 0.95, 0.01, 100) {
        case ((k, ti, cf, e, s), (opt, optV)) =>
          opt match {
            case keyTest() => (optV.toString, ti, cf, e, s)
            case confidenceTest() => optV match {
              case fd: Double => (k, ti, fd, e, s)
              case fs: String => (k, ti, fs.toDouble, e, s)
              case ff: Float => (k, ti, ff.toDouble, e, s)
              case fi: Int => (k, ti, fi.toDouble, e, s)
              case fl: Long => (k, ti, fl.toDouble, e, s)
              case _ => throw new AnalysisException(
                s"TopKCMS: Cannot parse double 'confidence'=$optV")
            }
            case epsTest() => optV match {
              case fd: Double => (k, ti, cf, fd, s)
              case fs: String => (k, ti, cf, fs.toDouble, s)
              case ff: Float => (k, ti, cf, ff.toDouble, s)
              case fi: Int => (k, ti, cf, fi.toDouble, s)
              case fl: Long => (k, ti, cf, fl.toDouble, s)
              case _ => throw new AnalysisException(
                s"TopKCMS: Cannot parse double 'eps'=$optV")
            }

            case timeIntervalTest() => optV match {
              case si: Int => (k, si.toLong, cf, e, s)
              case ss: String => (k, ss.toLong, cf, e, s)
              case sl: Long => (k, sl, cf, e, s)
              case _ => throw new AnalysisException(
                s"TopKCMS: Cannot parse int 'timeInterval'=$optV")
            }
            case sizeTest() => optV match {
              case si: Int => (k, ti, cf, e, si)
              case ss: String => (k, ti, cf, e, ss.toInt)
              case sl: Long => (k, ti, cf, e, sl.toInt)
              case _ => throw new AnalysisException(
                s"TopKCMS: Cannot parse int 'size'=$optV")
            }
          }
      }
    new TopKHokusaiWrapper(name, confidence, eps, size, timeInterval,
      schema, schema(key))

  }

}