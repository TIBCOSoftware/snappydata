package org.apache.spark.sql.execution

import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable
import scala.language.reflectiveCalls
import scala.reflect.ClassTag
import scala.reflect.runtime.{ universe => ru }
import io.snappydata.util.NumberUtils
import org.apache.spark.sql.collection.{ BoundedSortedSet, SegmentMap }
import org.apache.spark.sql.execution.cms.{ CountMinSketch, TopKCMS }
import org.apache.spark.util.collection.OpenHashSet
import com.esotericsoftware.kryo.KryoSerializable
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Output
import com.esotericsoftware.kryo.io.Input
import scala.collection.mutable.MutableList

final class TopKHokusai[T](cmsParams: CMSParams, windowSize: Long,
  epoch0: Long, val topKActual: Int, taAgg: MutableList[CountMinSketch[T]],
  iaAgg: MutableList[CountMinSketch[T]], intervalTracker: IntervalTracker, initialInterval: Long)(implicit val ev: ClassTag[T])
  extends Hokusai[T](cmsParams, windowSize, epoch0, taAgg, iaAgg, intervalTracker,
    initialInterval) with TopK {

  def this(cmsParams: CMSParams, windowSize: Long,
    epoch0: Long, topKActual: Int)(implicit t: ClassTag[T]) = this(cmsParams, windowSize,
    epoch0, topKActual, new MutableList[CountMinSketch[T]],
    new MutableList[CountMinSketch[T]], new IntervalTracker(), 0)

  var topKInternal = topKActual * 2

  private val queryTillLastNTopK_Case1: (Array[T]) => () => Array[(T, Approximate)] = (combinedKeys: Array[T]) => {
    () =>
      {
        val topKCMS = this.taPlusIa.ta.aggregates(0).asInstanceOf[TopKCMS[T]]
        if (combinedKeys != null) {
          sortAndBound(TopKCMS.getCombinedTopKFromEstimators(Array(topKCMS),
            combinedKeys))
        } else {
          topKCMS.getTopK
        }
      }
  }

  private val queryTillLastNTopK_Case2: (Int, Array[T]) => Array[(T, Approximate)] =
    (sumUpTo: Int, combinedTopKKeys: Array[T]) => {
      val combinedKeys: Iterable[T] = if (combinedTopKKeys != null) {
        combinedTopKKeys
      } else {
        null
      }
      sortAndBound(getTopKBySummingTimeAggregates(sumUpTo, combinedKeys))
    }

  private val queryTillLastNTopK_Case3: (Int, Int, Int, Int, Array[T]) => Array[(T, Approximate)] = (lastNIntervals: Int,
    totalIntervals: Int, n: Int, nQueried: Int, combinedTopKKeys: Array[T]) =>
    if (lastNIntervals > totalIntervals) {
      val topKKeys: Iterable[T] = if (combinedTopKKeys != null) {
        combinedTopKKeys
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
      val unionedTopKKeys: Iterable[T] = if (combinedTopKKeys != null) {
        combinedTopKKeys
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
          val count = this.taPlusIa.basicQuery(lastNIntervals + 1 to (2 * nearestPowerOf2Num).asInstanceOf[Int],
            item, nearestPowerOf2Num.asInstanceOf[Int], nearestPowerOf2Num.asInstanceOf[Int] * 2)
          if (count < total) {
            total = total - count
          } else {
            // what to do? ignore as count is abnormally high
          }
          val prevCount = mappings.getOrElse[Approximate](item,
            Approximate.zeroApproximate(cmsParams.confidence))
          mappings += (item -> (prevCount + count))
        }

      } else {
        unionedTopKKeys.foreach { item: T =>
          val count = this.taPlusIa.basicQuery(nearestPowerOf2Num.asInstanceOf[Int] + 1 to lastNIntervals,
            item, nearestPowerOf2Num.asInstanceOf[Int], nearestPowerOf2Num.asInstanceOf[Int] * 2)
          val prevCount = mappings.getOrElse[Approximate](item,
            Approximate.zeroApproximate(cmsParams.confidence))
          mappings += (item -> (prevCount + count))
        }

      }
      sortAndBound(mappings)
    }

  private val queryTillLastNTopK_Case4: (Int, Int, Int, Int, Array[T]) => Array[(T, Approximate)] =
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
        val unifiedTopKKeys: Iterable[T] = if (combinedTopKKeys != null) {
          combinedTopKKeys
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
              val prevCount = topKs.getOrElse[Approximate](item,
                Approximate.zeroApproximate(cmsParams.confidence))
              topKs += (item -> (total + prevCount))
            }

            val begin = lastNIntervals + 1
            val end = computedIntervalLength.asInstanceOf[Int]
            unifiedTopKKeys.foreach { item: T =>
              val total = this.taPlusIa.basicQuery(begin to end,
                item, skipLastInterval.asInstanceOf[Int], computedIntervalLength.asInstanceOf[Int])
              val prevCount = topKs.getOrElse[Approximate](item,
                Approximate.zeroApproximate(cmsParams.confidence))
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
              val prevCount = topKs.getOrElse[Approximate](item, Approximate.zeroApproximate(cmsParams.confidence))
              topKs += (item -> (total + prevCount))
            }
          }
        }

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
      //val nearestPowerOf2Num = NumberUtils.nearestPowerOf2LE(lastNIntervals)
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

  def getTopKTillTime(epoch: Long, combinedKeys: Array[T] = null): Option[Array[(T, Approximate)]] = {

    this.timeEpoch.timestampToInterval(epoch).flatMap(x => {
      val interval = if (x > timeEpoch.t) {
        timeEpoch.t
      } else {
        x
      }
      Some(this.taPlusIa.queryLastNIntervals[Array[(T, Approximate)]](
        this.taPlusIa.convertIntervalBySwappingEnds(interval.asInstanceOf[Int]).asInstanceOf[Int],
        queryTillLastNTopK_Case1(combinedKeys), queryTillLastNTopK_Case2(_, combinedKeys),
        queryTillLastNTopK_Case3(_, _, _, _, combinedKeys),
        queryTillLastNTopK_Case4(_, _, _, _, combinedKeys)))

    })

  }

  def getTopKForCurrentInterval: Option[Array[(T, Approximate)]] =
    Some(this.mBar.asInstanceOf[TopKCMS[T]].getTopK)

  def getTopKKeysForCurrentInterval: OpenHashSet[T] =
    this.mBar.asInstanceOf[TopKCMS[T]].getTopKKeys

  def getTopKInCurrentInterval[T](): Array[(T, Approximate)] =
    this.mBar.asInstanceOf[TopKCMS[T]].getTopK

  def getCombinedTopKKeysBetweenTime(epochFrom: Long,
    epochTo: Long): Option[mutable.Set[T]] = {

    val (later, earlier) = this.timeEpoch.convertEpochToIntervals(epochFrom, epochTo) match {
      case Some(x) => x
      case None => return None
    }
    Some(this.getCombinedTopKKeysBetween(later, earlier))

  }

  def getCombinedTopKKeysTillTime(epoch: Long): Option[Array[T]] = {

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

    })

  }

  override def isStreamSummary: Boolean = false

  def getTopKBetweenTime(epochFrom: Long, epochTo: Long,
    combinedTopKKeys: Array[T] = null): Option[Array[(T, Approximate)]] =
    {
      val (later, earlier) = this.timeEpoch.convertEpochToIntervals(epochFrom, epochTo) match {
        case Some(x) if x._1 > taPlusIa.ia.aggregates.size => (taPlusIa.ia.aggregates.size, x._2)
        case Some(x) => x
        case None => return None
      }
      Some(this.getTopKBetweenTime(later, earlier, combinedTopKKeys))
    }

  def getTopKKeysBetweenTime(epochFrom: Long,
    epochTo: Long): Option[OpenHashSet[T]] =
    {
      val (later, earlier) = this.timeEpoch.convertEpochToIntervals(epochFrom, epochTo) match {
        case Some(x) => x
        case None => return None
      }
      val topKKeys = this.getCombinedTopKKeysBetween(later, earlier)
      if (topKKeys.nonEmpty) {
        val result = new OpenHashSet[T](topKKeys.size)
        topKKeys.foreach(result.add)
        Some(result)
      } else {
        None
      }
    }

  def getTopKBetweenTime(later: Int, earlier: Int, combinedTopKKeys: Array[T]): Array[(T, Approximate)] = {
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
      val unifiedTopKKeys: Iterable[T] = if (combinedTopKKeys != null) {
        combinedTopKKeys
      } else {
        TopKCMS.getUnionedTopKKeysFromEstimators(estimators)
      }

      var truncatedSeq = bestPath
      var start = if (fromLastNInterval == 1) {
        fromLastNInterval + 1
      } else {
        fromLastNInterval
      }
      val topKs = mutable.HashMap[T, Approximate]()
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

        if (start == taIntervalStartsAt && end == lengthTillInterval) {
          // can add the time series aggregation as whole interval is needed
          val topKCMS = this.taPlusIa.ta.aggregates(NumberUtils.isPowerOf2(interval) + 1).asInstanceOf[TopKCMS[T]]
          TopKCMS.getCombinedTopKFromEstimators(Array(topKCMS), unifiedTopKKeys, topKs)
        } else {
          unifiedTopKKeys.foreach { item: T =>
            val total = this.taPlusIa.basicQuery(start.asInstanceOf[Int] to end.asInstanceOf[Int],
              item, interval.asInstanceOf[Int], lengthTillInterval.asInstanceOf[Int])
            val prevCount = topKs.getOrElse[Approximate](item,
              Approximate.zeroApproximate(cmsParams.confidence))
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

  def getCombinedTopKKeysBetween(later: Int, earlier: Int): mutable.Set[T] = {
    val fromLastNInterval = this.taPlusIa.convertIntervalBySwappingEnds(later)
    val tillLastNInterval = this.taPlusIa.convertIntervalBySwappingEnds(earlier)
    if (fromLastNInterval == 1 && tillLastNInterval == 1) {
      val topKCMS = this.taPlusIa.ta.aggregates(0).asInstanceOf[TopKCMS[T]]
      TopKCMS.getUnionedTopKKeysFromEstimators(Array(topKCMS))
    } else {
      // Identify the best path
      val (bestPath, computedIntervalLength) = this.taPlusIa.intervalTracker.identifyBestPath(tillLastNInterval.asInstanceOf[Int],
        true, 1, fromLastNInterval.asInstanceOf[Int])
      var estimators = bestPath.map { interval => taPlusIa.ta.aggregates(NumberUtils.isPowerOf2(interval) + 1).asInstanceOf[TopKCMS[T]] }
      if (fromLastNInterval == 1) {
        estimators = this.taPlusIa.ta.aggregates(0).asInstanceOf[TopKCMS[T]] +: estimators
      }
      TopKCMS.getUnionedTopKKeysFromEstimators(estimators)
    }
  }

  def queryTimeAggregateForInterval(item: T, interval: Long): Approximate = {
    assert(NumberUtils.isPowerOf2(interval) != -1)
    val topKCMS = this.taPlusIa.ta.aggregates(NumberUtils.isPowerOf2(interval) + 1).asInstanceOf[TopKCMS[T]]
    topKCMS.getFromTopKMap(item).getOrElse(this.taPlusIa.queryTimeAggregateForInterval(item, interval))
  }

  private def getTopKBySummingTimeAggregates(sumUpTo: Int,
    setOfTopKKeys: Iterable[T] = null): mutable.Map[T, Approximate] = {

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

  private def sortAndBound[U](
    topKs: mutable.Map[U, Approximate]): Array[(U, Approximate)] = {
    val sortedData = new BoundedSortedSet[U, Approximate](this.topKActual, false)
    topKs.foreach(sortedData.add)
    val iter = sortedData.iterator
    //topKs.foreach(sortedData.add(_))
    Array.fill[(U, Approximate)](sortedData.size())({
      //val (key, value) = iter.next
      //(key, value.longValue())
      iter.next
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
      TopKHokusai.newZeroCMS[T](cmsParams.depth, cmsParams.width, cmsParams.hashA, topKActual, x,
        cmsParams.confidence, cmsParams.eps)
    } else {
      TopKHokusai.newZeroCMS[T](cmsParams.depth, cmsParams.width, cmsParams.hashA, topKActual,
        topKInternal * (powerOf2 + 1), cmsParams.confidence, cmsParams.eps)
    }

}

object TopKHokusai {
  // TODO: Resolve the type of TopKHokusai
  // private final val topKMap = new mutable.HashMap[String, mutable.HashMap[Int, TopK]]
  private final val mapLock = new ReentrantReadWriteLock

  def newZeroCMS[T: ClassTag](depth: Int, width: Int, hashA: Array[Long], topKActual: Int,
    topKInternal: Int, confidence: Double, eps: Double) =
    new TopKCMS[T](topKActual, topKInternal, depth, width, hashA, confidence, eps)

  /*
  def apply[T](name: String): Option[TopKHokusai[T]] = {
    SegmentMap.lock(mapLock.readLock) {
      topKMap.get(name) match {
        case Some(tk) => Some(tk.asInstanceOf[TopKHokusai[T]])
        case None => None
      }
    }
  }*/

  def write(kryo: Kryo, output: Output, obj: TopKHokusai[_]) {
    //TopK params
    kryo.writeObject(output, obj.ev)
    output.writeInt(obj.topKActual)
    CMSParams.write(kryo, output, obj.cmsParams)
    output.writeLong(obj.windowSize)
    output.writeLong(obj.epoch0)

    //Hokusai params
    val taAggs = obj.taPlusIa.ta.aggregates
    output.writeInt(taAggs.length)
    taAggs.foreach { x =>
      val bytes = CountMinSketch.serialize(x)
      output.writeInt(bytes.length)
      output.writeBytes(bytes)
    }

    val itemAggs = obj.taPlusIa.ia.aggregates
    output.writeInt(itemAggs.length)
    itemAggs.foreach { x =>
      val bytes = CountMinSketch.serialize(x)
      output.writeInt(bytes.length)
      output.writeBytes(bytes)
    }
    IntervalTracker.write(kryo, output, obj.taPlusIa.intervalTracker)
    val bytes = TopKCMS.serialize(obj.mBar.asInstanceOf[TopKCMS[_]])
    output.writeInt(bytes.length)
    output.writeBytes(bytes)
    output.writeLong(obj.timeEpoch.t)

  }

  def read(kryo: Kryo, input: Input): TopKHokusai[_] = {
    val classTag = kryo.readObject[ClassTag[Any]](input, classOf[ClassTag[Any]])
    val topKActual = input.readInt
    val cmsParams = CMSParams.read(kryo, input)
    val windowSize = input.readLong
    val epoch0 = input.readLong
    val lenTA = input.readInt
    val aggregatesTA = MutableList.fill(lenTA)({
      val bytes = input.readBytes(input.readInt)
      CountMinSketch.deserialize(bytes)(classTag)
    })
    val lenIA = input.readInt
    val aggregatesIA = MutableList.fill(lenIA)({
      val bytes = input.readBytes(input.readInt)
      CountMinSketch.deserialize(bytes)(classTag)
    })
    val intervalTracker = IntervalTracker.read(kryo, input)
    val bytes = input.readBytes(input.readInt)
    val mBar = TopKCMS.deserialize(bytes)(classTag)
    val t = input.readLong

    new TopKHokusai(cmsParams, windowSize,
      epoch0, topKActual, aggregatesTA, aggregatesIA,
      intervalTracker, t)(classTag)
  }

  def create[T: ClassTag](cms: CMSParams,
    size: Int, tsCol: Int, timeInterval: Long,
    epoch0: () => Long): TopK = new TopKHokusai[T](cms, timeInterval, epoch0(), size)

  def createDummy[T: ClassTag](isStreamSummary: Boolean): TopK = new TopKStub(isStreamSummary)

}


