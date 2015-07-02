package org.apache.spark.sql.execution

import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable
import scala.language.reflectiveCalls
import scala.reflect.ClassTag

import io.snappydata.util.NumberUtils
import io.snappydata.util.gnu.trove.impl.PrimeFinder
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.collection.Utils._
import org.apache.spark.sql.collection.{BoundedSortedSet, SegmentMap}
import org.apache.spark.sql.execution.cms.{CountMinSketch, TopKCMS}
import org.apache.spark.sql.sources.CastLongTime
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.util.collection.OpenHashSet

final class TopKHokusai[T: ClassTag](cmsParams: CMSParams, val windowSize: Long,
    val epoch0: Long, val topKActual: Int, startIntervalGenerator: Boolean)
  extends Hokusai[T](cmsParams, windowSize, epoch0, startIntervalGenerator) {
  val topKInternal = topKActual * 2

  private val queryTillLastNTopK_Case1: (Array[T]) => () => Array[(T, Long)] = (combinedKeys: Array[T]) => {
    () =>
      {
        val topKCMS = this.taPlusIa.ta.aggregates(0).asInstanceOf[TopKCMS[T]]
        if (combinedKeys != null) {
          sortAndBound(TopKCMS.getCombinedTopKFromEstimators(Array(topKCMS),
            scala.collection.mutable.Set(combinedKeys: _*)))
        } else {
          topKCMS.getTopK
        }
      }
  }

  private val queryTillLastNTopK_Case2: (Int, Array[T]) => Array[(T, Long)] = (sumUpTo: Int, combinedTopKKeys: Array[T]) =>
    {
      val combinedKeys = if (combinedTopKKeys != null) {
        scala.collection.mutable.Set(combinedTopKKeys: _*)
      } else {
        null
      }
      sortAndBound(getTopKBySummingTimeAggregates(sumUpTo, combinedKeys))
    }

  private val queryTillLastNTopK_Case3: (Int, Int, Int, Int, Array[T]) => Array[(T, Long)] = (lastNIntervals: Int,
    totalIntervals: Int, n: Int, nQueried: Int, combinedTopKKeys: Array[T]) =>
    if (lastNIntervals > totalIntervals) {
      val topKKeys = if (combinedTopKKeys != null) {
        scala.collection.mutable.Set(combinedTopKKeys: _*)
      } else {
        null
      }
      sortAndBound(getTopKBySummingTimeAggregates(n, topKKeys))
    } else {
      val nearestPowerOf2NumGE = NumberUtils.nearestPowerOf2GE(lastNIntervals)
      val nearestPowerOf2Num = NumberUtils.nearestPowerOf2LE(lastNIntervals)
      //get all the unioned top k keys.
      val estimators = this.taPlusIa.ta.aggregates.slice(0,
        NumberUtils.isPowerOf2(nearestPowerOf2NumGE) + 1)
      val unionedTopKKeys = if (combinedTopKKeys != null) {
        scala.collection.mutable.Set(combinedTopKKeys: _*)
      } else {
        TopKCMS.getUnionedTopKKeysFromEstimators(estimators)
      }
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

  private val queryTillLastNTopK_Case4: (Int, Int, Int, Int, Array[T]) => Array[(T, Long)] =
    (lastNIntervals: Int, totalIntervals: Int, n: Int, nQueried: Int, combinedTopKKeys: Array[T]) =>
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
        val unifiedTopKKeys = if (combinedTopKKeys != null) {
          scala.collection.mutable.Set(combinedTopKKeys: _*)
        } else {
          TopKCMS.getUnionedTopKKeysFromEstimators(estimators)
        }

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

  private val combinedKeysTillLastNTopK_Case1: () => Array[T] = () => {
    val topKCMS = this.taPlusIa.ta.aggregates(0).asInstanceOf[TopKCMS[T]]
    TopKCMS.getUnionedTopKKeysFromEstimators(Array(topKCMS)).toArray
  }

  private val combinedKeysTillLastNTopK_Case2: (Int) => Array[T] = (sumUpTo: Int) =>
    this.getTopKKeysBySummingTimeAggregates(sumUpTo).toArray

  private val combinedKeysTillLastNTopK_Case3: (Int, Int, Int, Int) => Array[T] = (lastNIntervals: Int,
    totalIntervals: Int, n: Int, nQueried: Int) =>
    if (lastNIntervals > totalIntervals) {
      this.getTopKKeysBySummingTimeAggregates(n).toArray

    } else {
      val nearestPowerOf2NumGE = NumberUtils.nearestPowerOf2GE(lastNIntervals)
      val nearestPowerOf2Num = NumberUtils.nearestPowerOf2LE(lastNIntervals)
      //get all the unioned top k keys.
      val estimators = this.taPlusIa.ta.aggregates.slice(0,
        NumberUtils.isPowerOf2(nearestPowerOf2NumGE) + 1)
      TopKCMS.getUnionedTopKKeysFromEstimators(estimators).toArray
    }

  private val combinedKeysTillLastNTopK_Case4: (Int, Int, Int, Int) => Array[T] =
    (lastNIntervals: Int, totalIntervals: Int, n: Int, nQueried: Int) =>
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
        TopKCMS.getUnionedTopKKeysFromEstimators(estimators).toArray

      }

  override val mergeCreator: ((Array[CountMinSketch[T]]) => CountMinSketch[T]) = estimators =>
    TopKCMS.merge[T](estimators(1).asInstanceOf[TopKCMS[T]].topKInternal * 2, estimators)

  def getTopKTillTime(epoch: Long, combinedKeys: Array[T] = null): Option[Array[(T, Long)]] = {
    this.executeInReadLock(
      this.timeEpoch.timestampToInterval(epoch).flatMap(x => {
        val interval = if (x > timeEpoch.t) {
          timeEpoch.t
        } else {
          x
        }
        Some(this.taPlusIa.queryLastNIntervals[Array[(T, Long)]](
          this.taPlusIa.convertIntervalBySwappingEnds(interval.asInstanceOf[Int]).asInstanceOf[Int],
          queryTillLastNTopK_Case1(combinedKeys), queryTillLastNTopK_Case2(_, combinedKeys),
          queryTillLastNTopK_Case3(_, _, _, _, combinedKeys),
          queryTillLastNTopK_Case4(_, _, _, _, combinedKeys)))

      }), true)

  }

  def getTopKForCurrentInterval: Option[Array[(T, Long)]] =
    this.executeInReadLock({
      Some(this.mBar.asInstanceOf[TopKCMS[T]].getTopK)
    }, true)

  def getTopKKeysForCurrentInterval: OpenHashSet[T] =
    this.executeInReadLock({
      this.mBar.asInstanceOf[TopKCMS[T]].getTopKKeys
    }, true)

  def getForKeysInCurrentInterval(combinedKeys: Array[T]): Array[(T, Long)] =
    this.executeInReadLock({
      val cms = this.mBar
      combinedKeys.map { k => (k, cms.estimateCount(k)) }
    }, true)

  def getCombinedTopKKeysBetweenTime(epochFrom: Long, epochTo: Long): Option[Array[T]] = {
    this.executeInReadLock(
      {
        val (later, earlier) = convertEpochToIntervals(epochFrom, epochTo) match {
          case Some(x) => x
          case None => return None
        }
        Some(this.getCombinedTopKKeysBetween(later, earlier))
      }, true)
  }

  def getCombinedTopKKeysTillTime(epoch: Long): Option[Array[T]] = {
    this.executeInReadLock(
      this.timeEpoch.timestampToInterval(epoch).flatMap(x => {
        val interval = if (x > timeEpoch.t) {
          timeEpoch.t
        } else {
          x
        }
        Some(this.taPlusIa.queryLastNIntervals[Array[T]](
          this.taPlusIa.convertIntervalBySwappingEnds(interval.asInstanceOf[Int]).asInstanceOf[Int],
          combinedKeysTillLastNTopK_Case1, combinedKeysTillLastNTopK_Case2(_),
          combinedKeysTillLastNTopK_Case3(_, _, _, _),
          combinedKeysTillLastNTopK_Case4(_, _, _, _)))

      }), true)

  }

  def getTopKBetweenTime(epochFrom: Long, epochTo: Long,
      combinedTopKKeys: Array[T] = null): Option[Array[(T, Long)]] =
    this.executeInReadLock({
      val (later, earlier) = convertEpochToIntervals(epochFrom, epochTo) match {
        case Some(x) => x
        case None => return None
      }
      Some(this.getTopKBetweenTime(later, earlier, combinedTopKKeys))
    }, true)

  def getTopKKeysBetweenTime(epochFrom: Long, epochTo: Long): Option[OpenHashSet[T]] =
    this.executeInReadLock({
      val (later, earlier) = convertEpochToIntervals(epochFrom, epochTo) match {
        case Some(x) => x
        case None => return None
      }
      // TODO: could be optimized to return only the keys
      val topK = this.getCombinedTopKKeysBetween(later, earlier)
      val result = new OpenHashSet[T](topK.length)
      topK.foreach { v => result.add(v) }
      Some(result)
    }, true)

  def getTopKBetweenTime(later: Int, earlier: Int, combinedTopKKeys: Array[T]): Array[(T, Long)] = {
    val fromLastNInterval = this.taPlusIa.convertIntervalBySwappingEnds(later)
    val tillLastNInterval = this.taPlusIa.convertIntervalBySwappingEnds(earlier)
    if (fromLastNInterval == 1 && tillLastNInterval == 1) {
      queryTillLastNTopK_Case1(combinedTopKKeys)()
    } else {
      // Identify the best path
      val (bestPath, computedIntervalLength) = this.taPlusIa.intervalTracker.identifyBestPath(tillLastNInterval.asInstanceOf[Int],
        true, 1, fromLastNInterval.asInstanceOf[Int])
      var estimators = bestPath.map { interval => taPlusIa.ta.aggregates(NumberUtils.isPowerOf2(interval) + 1).asInstanceOf[TopKCMS[T]] }
      if (fromLastNInterval == 1) {
        estimators = this.taPlusIa.ta.aggregates(0).asInstanceOf[TopKCMS[T]] +: estimators
      }
      val unifiedTopKKeys = if (combinedTopKKeys != null) {
        scala.collection.mutable.Set(combinedTopKKeys: _*)
      } else {
        TopKCMS.getUnionedTopKKeysFromEstimators(estimators)
      }

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

  def getCombinedTopKKeysBetween(later: Int, earlier: Int): Array[T] = {
    val fromLastNInterval = this.taPlusIa.convertIntervalBySwappingEnds(later)
    val tillLastNInterval = this.taPlusIa.convertIntervalBySwappingEnds(earlier)
    if (fromLastNInterval == 1 && tillLastNInterval == 1) {
      val topKCMS = this.taPlusIa.ta.aggregates(0).asInstanceOf[TopKCMS[T]]
      TopKCMS.getUnionedTopKKeysFromEstimators(Array(topKCMS)).toArray
    } else {
      // Identify the best path
      val (bestPath, computedIntervalLength) = this.taPlusIa.intervalTracker.identifyBestPath(tillLastNInterval.asInstanceOf[Int],
        true, 1, fromLastNInterval.asInstanceOf[Int])
      var estimators = bestPath.map { interval => taPlusIa.ta.aggregates(NumberUtils.isPowerOf2(interval) + 1).asInstanceOf[TopKCMS[T]] }
      if (fromLastNInterval == 1) {
        estimators = this.taPlusIa.ta.aggregates(0).asInstanceOf[TopKCMS[T]] +: estimators
      }
      val unifiedTopKKeys = TopKCMS.getUnionedTopKKeysFromEstimators(estimators)
      unifiedTopKKeys.toArray
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

  private def getTopKKeysBySummingTimeAggregates(sumUpTo: Int): scala.collection.Set[T] = {
    val estimators = this.taPlusIa.ta.aggregates.slice(0, sumUpTo + 1)
    TopKCMS.getUnionedTopKKeysFromEstimators(estimators)
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

  def apply(name: String, confidence: Double, eps: Double, size: Int,
      tsCol: Int, timeInterval: Long, epoch0: () => Long) = {
    lookupOrAdd(name, confidence, eps, size, tsCol, timeInterval, epoch0)
  }

  private[sql] def lookupOrAdd(name: String, confidence: Double, eps: Double,
      size: Int, tsCol: Int, timeInterval: Long,
      epoch0: () => Long): TopKHokusai[Any] = {
    SegmentMap.lock(mapLock.readLock) {
      topKMap.get(name)
    } match {
      case Some(topk) => topk
      case None =>
        // insert into global map but double-check after write lock
        SegmentMap.lock(mapLock.writeLock) {
          topKMap.getOrElse(name, {
            val depth = Math.ceil(-Math.log(1 - confidence) / Math.log(2)).toInt
            //val width = PrimeFinder.nextPrime(Math.ceil(2 / eps).toInt)
            val width = NumberUtils.nearestPowerOf2GE(Math.ceil(2 / eps).toInt)

            val cmsParams = CMSParams(width, depth)

            val topk = new TopKHokusai[Any](cmsParams, timeInterval,
              epoch0(), size, false/*timeInterval > 0 && tsCol < 0*/)
            topKMap(name) = topk
            topk
          })
        }
    }
  }
}

final class TopKHokusaiWrapper[T](val name: String, val confidence: Double,
    val eps: Double, val size: Int, val timeSeriesColumn: Int,
    val timeInterval: Long, val schema: StructType, val key: StructField,
    val frequencyCol: Option[StructField], val epoch : Long)
    extends CastLongTime with Serializable {

  override protected def getNullMillis(getDefaultForNull: Boolean) =
    if (getDefaultForNull) System.currentTimeMillis() else -1L

  override def timeColumnType: Option[DataType] = {
    if (timeSeriesColumn >= 0) {
      Some(schema(timeSeriesColumn).dataType)
    } else {
      None
    }
  }

  override def module: String = "TopKHokusai"
}

object TopKHokusaiWrapper {

  def apply(name: String, options: Map[String, Any],
      schema: StructType): TopKHokusaiWrapper[Any] = {
    val keyTest = "key".ci
    val timeSeriesColumnTest = "timeSeriesColumn".ci
    val timeIntervalTest = "timeInterval".ci
    val confidenceTest = "confidence".ci
    val epsTest = "eps".ci
    val sizeTest = "size".ci
    val frequencyColTest = "frequencyCol".ci
    val epochTest = "epoch".ci
    val cols = schema.fieldNames

    // Using foldLeft to read key-value pairs and build into the result
    // tuple of (key, confidence, eps, size, frequencyCol) like an aggregate.
    // This "aggregate" simply keeps the last values for the corresponding
    // keys as found when folding the map.
    val (key, tsCol, timeInterval, confidence, eps, size, frequencyCol, epoch) =
      options.foldLeft("", -1, 5L, 0.95, 0.01, 100, "", -1L) {
        case ((k, ts, ti, cf, e, s, fr, ep), (opt, optV)) =>
          opt match {
            case keyTest() => (optV.toString, ts, ti, cf, e, s, fr, ep)
            case confidenceTest() => optV match {
              case fd: Double => (k, ts, ti, fd, e, s, fr, ep)
              case fs: String => (k, ts, ti, fs.toDouble, e, s, fr, ep)
              case ff: Float => (k, ts, ti, ff.toDouble, e, s, fr, ep)
              case fi: Int => (k, ts, ti, fi.toDouble, e, s, fr, ep)
              case fl: Long => (k, ts, ti, fl.toDouble, e, s, fr, ep)
              case _ => throw new AnalysisException(
                s"TopKCMS: Cannot parse double 'confidence'=$optV")
            }
            case epsTest() => optV match {
              case fd: Double => (k, ts, ti, cf, fd, s, fr, ep)
              case fs: String => (k, ts, ti, cf, fs.toDouble, s, fr, ep)
              case ff: Float => (k, ts, ti, cf, ff.toDouble, s, fr, ep)
              case fi: Int => (k, ts, ti, cf, fi.toDouble, s, fr, ep)
              case fl: Long => (k, ts, ti, cf, fl.toDouble, s, fr, ep)
              case _ => throw new AnalysisException(
                s"TopKCMS: Cannot parse double 'eps'=$optV")
            }
            case timeSeriesColumnTest() => optV match {
              case tss: String => (k, columnIndex(tss, cols), ti, cf, e, s, fr, ep)
              case tsi: Int => (k, tsi, ti, cf, e, s, fr, ep)
              case _ => throw new AnalysisException(
                s"TopKCMS: Cannot parse 'timeSeriesColumn'=$optV")
            }
            case timeIntervalTest() =>
              (k, ts, parseTimeInterval(optV, "TopKCMS"), cf, e, s, fr, ep)
            case sizeTest() => optV match {
              case si: Int => (k, ts, ti, cf, e, si, fr, ep)
              case ss: String => (k, ts, ti, cf, e, ss.toInt, fr, ep)
              case sl: Long => (k, ts, ti, cf, e, sl.toInt, fr, ep)
              case _ => throw new AnalysisException(
                s"TopKCMS: Cannot parse int 'size'=$optV")
            }
            case epochTest() => optV match {
              case si: Int => (k, ts, ti, cf, e, s, fr, si.toLong)
              case ss: String => (k, ts, ti, cf, e, s, fr, ss.toLong)
              case sl: Long => (k, ts, ti, cf, e, s, fr, sl)
              case _ => throw new AnalysisException(
                s"TopKCMS: Cannot parse int 'size'=$optV")
            }
            case frequencyColTest() => (k, ts, ti, cf, e, s, optV.toString, ep)
          }
      }
    new TopKHokusaiWrapper(name, confidence, eps, size, tsCol, timeInterval,
      schema, schema(key),
      if (frequencyCol.isEmpty) None else Some(schema(frequencyCol)), epoch)
  }
}
