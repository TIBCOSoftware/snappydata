package io.snappydata.core

import scala.reflect.ClassTag
import io.snappydata.core.cms.CountMinSketch
import io.snappydata.core.cms.TopKCMS
import io.snappydata.util.NumberUtils
import io.snappydata.util.BoundedSortedSet

class TopKHokusai[T: ClassTag](cmsParams: CMSParams, windowSize: Long, epoch0: Long, val topK: Int)
  extends Hokusai[T](cmsParams, windowSize, epoch0) {

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
      unionedTopKKeys.foreach { item: T =>
        val count = this.taPlusIa.basicQuery(nearestPowerOf2Num.asInstanceOf[Int] + 1 to lastNIntervals,
          item,
          nearestPowerOf2Num.asInstanceOf[Int],
          nearestPowerOf2Num.asInstanceOf[Int] * 2)
        val prevCount = mappings.getOrElse[java.lang.Long](item, 0)
        mappings += (item -> (prevCount + count))
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
        val begin = (computedIntervalLength - skipLastInterval + 1).asInstanceOf[Int]
        unifiedTopKKeys.foreach { item: T =>
          val total = this.taPlusIa.basicQuery(begin to lastNIntervalsToQuery,
            item, skipLastInterval.asInstanceOf[Int], computedIntervalLength.asInstanceOf[Int])
          val prevCount = topKs.getOrElse[java.lang.Long](item, 0)
          topKs += (item -> (total + prevCount))
        }
      }
      sortAndBound(topKs)
    }

  override val mergeCreator: (Array[CountMinSketch[T]] => CountMinSketch[T]) = estimators => {
    TopKCMS.merge[T](estimators(1).asInstanceOf[TopKCMS[T]].topK, estimators)
  }

  def this(cmsParams: CMSParams, windowSize: Long, topK: Int) = this(cmsParams, windowSize,
    System.currentTimeMillis(), topK)

  def getTopKTillTime(epoch: Long): Option[Array[(T, Long)]] = {
    this.readLock.lockInterruptibly()
    try {
      this.timeEpoch.timestampToInterval(epoch).flatMap(x =>

        if (x > timeEpoch.t) {
          None
        } else {
          Some(this.taPlusIa.queryLastNIntervals[Array[(T, Long)]](
            this.taPlusIa.convertIntervalBySwappingEnds(x.asInstanceOf[Int]).asInstanceOf[Int],
            queryTillLastNTopK_Case1, queryTillLastNTopK_Case2, queryTillLastNTopK_Case3,
            queryTillLastNTopK_Case4))
        })
    } finally {
      this.readLock.unlock()
    }
  }

  def getTopKBetweenTime(epochFrom: Long, epochTo: Long, key: T): Option[Array[(T, Long)]] = {
    this.readLock.lockInterruptibly()
    try {

      val (later, earlier) = convertEpochToIntervals(epochFrom, epochTo) match {
        case Some(x) => x
        case None => return None
      }
      Some(this.getTopKBetweenTime(later, earlier, key))
    } finally {
      this.readLock.unlock()
    }
  }

  def getTopKBetweenTime(later: Int, earlier: Int, key: T): Array[(T, Long)] = {
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
              key, interval.asInstanceOf[Int], lengthTillInterval.asInstanceOf[Int])
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
    val sortedData = new BoundedSortedSet[T](this.topK)
    topKs.foreach(sortedData.add(_))
    val iter = sortedData.iterator
    //topKs.foreach(sortedData.add(_))
    Array.fill[(T, Long)](sortedData.size())({
      val (key, value) = iter.next
      (key, value.longValue())

    })

  }

  override def createZeroCMS(powerOf2: Int): CountMinSketch[T] = 
    if(powerOf2 == 0) {
      TopKHokusai.newZeroCMS[T](cmsParams.depth, cmsParams.width, cmsParams.hashA, topK)
    }else {
      TopKHokusai.newZeroCMS[T](cmsParams.depth, cmsParams.width, cmsParams.hashA, topK*(powerOf2+1))
    }

}

object TopKHokusai {
  def newZeroCMS[T: ClassTag](depth: Int, width: Int, hashA: Array[Long], topK: Int) =
    new TopKCMS[T](topK, depth, width, hashA)
}