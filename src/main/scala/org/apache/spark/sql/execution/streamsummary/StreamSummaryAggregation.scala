package org.apache.spark.sql.execution.streamsummary

import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import io.snappydata.util.com.clearspring.analytics.stream.{Counter, StreamSummary}
import org.apache.spark.sql.LockUtils.ReadWriteLock
import org.apache.spark.sql.TimeEpoch
import org.apache.spark.sql.collection.SegmentMap
import org.apache.spark.sql.execution.{Approximate, KeyFrequencyWithTimestamp}

/**
 * TopK using stream summary algorithm.
 *
 * Created by Hemant on 7/14/15.
 */
class StreamSummaryAggregation[T](val capacity: Int, val intervalSize: Long,
    val epoch0: Long, val maxIntervals: Int,
    val startIntervalGenerator: Boolean = false) {

  val rwlock = new ReadWriteLock()

  val streamAggregates = new mutable.MutableList[StreamSummary[T]]()

  val timeEpoch = new TimeEpoch(intervalSize, epoch0)

  for (x <- 1 to maxIntervals) {
    streamAggregates += new StreamSummary[T](capacity)
    timeEpoch.increment()
  }

  def addItems(data: ArrayBuffer[KeyFrequencyWithTimestamp[T]]) = {
    this.rwlock.executeInWriteLock({

      data.foreach(item => {
        val epoch = item.epoch
        if (epoch > 0) {
          // find the appropriate time and item aggregates and update them
          timeEpoch.timestampToInterval(epoch) match {
            case Some(interval) =>
              if (interval <= maxIntervals) {
                streamAggregates(interval - 1).offer(item.key,
                  item.frequency.toInt)
              } // else IGNORE
            case None => // IGNORE
          }
        }
        else {
          throw new UnsupportedOperationException("Support for intervals " +
              "for wall clock time for streams not available yet.")
        }
      })
    })
  }

  def queryIntervals(start: Int, end: Int, k: Int) = {
    val topkFromAllIntervals = new mutable.MutableList[Counter[T]]

    import scala.collection.JavaConversions._

    val lastInterval = if (end > maxIntervals) maxIntervals else end

    for (x <- start to lastInterval) {
      streamAggregates(x).topK(k).foreach(topkFromAllIntervals += _)
    }
    topkFromAllIntervals groupBy (_.getItem) map { x =>
      (x._1, new Approximate(x._2.foldLeft(0L)(_ + _.getError),
        x._2.foldLeft(0L)(_ + _.getCount), 0, 0))
    }
  }

  def getTopKBetweenTime(epochFrom: Long, epochTo: Long,
      k: Int): Option[Array[(T, Approximate)]] =
    this.rwlock.executeInReadLock({
      val (end, start) = this.timeEpoch.convertEpochToIntervals(epochFrom,
        epochTo) match {
        case Some(x) => x
        case None => return None
      }
      Some(this.queryIntervals(start - 1, end - 1, k).toArray)
    }, true)
}

object StreamSummaryAggregation {
  private final val topKMap = new mutable.HashMap[String, StreamSummaryAggregation[_]]
  private final val mapLock = new ReentrantReadWriteLock()

  def apply[T](name: String): Option[StreamSummaryAggregation[T]] = {
    SegmentMap.lock(mapLock.readLock) {
      topKMap.get(name) match {
        case Some(tk) => Some(tk.asInstanceOf[StreamSummaryAggregation[T]])
        case None => None
      }
    }
  }

  def apply[T: ClassTag](name: String, size: Int, tsCol: Int,
      timeInterval: Long, epoch0: () => Long, maxInterval: Int) = {
    lookupOrAdd[T](name, size, tsCol, timeInterval, epoch0, maxInterval)
  }

  private[sql] def lookupOrAdd[T: ClassTag](name: String, size: Int,
      tsCol: Int, timeInterval: Long, epoch0: () => Long,
      maxInterval: Int): StreamSummaryAggregation[T] = {
    SegmentMap.lock(mapLock.readLock) {
      topKMap.get(name)
    } match {
      case Some(topk) => topk.asInstanceOf[StreamSummaryAggregation[T]]
      case None =>
        // insert into global map but double-check after write lock
        SegmentMap.lock(mapLock.writeLock) {
          topKMap.getOrElse(name, {
            val topk = new StreamSummaryAggregation[T](size, timeInterval,
              epoch0(), maxInterval, timeInterval > 0 && tsCol < 0)
            topKMap(name) = topk
            topk
          }).asInstanceOf[StreamSummaryAggregation[T]]
        }
    }
  }
}
