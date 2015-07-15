package org.apache.spark.sql.execution.streamsummary

import io.snappydata.util.com.clearspring.analytics.stream.{Counter, StreamSummary}
import org.apache.spark.sql.LockUtils.ReadWriteLock
import org.apache.spark.sql.TimeEpoch
import org.apache.spark.sql.execution.{Approximate, KeyFrequencyWithTimestamp}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by hemantb on 7/14/15.
 */
class StreamSummaryAggregation[T](val capacity: Int, val intervalSize: Long, val epoch0: Long,
                                  val maxIntervals: Int, val startIntervalGenerator: Boolean = false) {

  val rwlock = new ReadWriteLock()


  val streamAggregates = new mutable.MutableList[StreamSummary[T]]()

  val timeEpoch = new TimeEpoch(intervalSize, epoch0)

  for (x <- 1 to maxIntervals)
    streamAggregates(x) = new StreamSummary[T]()

  def addItems(data: ArrayBuffer[KeyFrequencyWithTimestamp[T]]) = {
    this.rwlock.executeInWriteLock({

      data.foreach(item => {
        val epoch = item.epoch
        if (epoch > 0) {
          // find the appropriate time and item aggregates and update them
          timeEpoch.timestampToInterval(epoch) match {
            case Some(interval) => {
              if (interval <= maxIntervals)
                streamAggregates(interval).offer(item.key, item.frequency.toInt)
              // else IGNORE

            }
          }
        }
        else {
          throw new UnsupportedOperationException("Support for intervals for wall clock time for streams not available yet.")
        }
      })
    })
  }
  def queryIntervals (start:Int, end : Int, k: Int) = {
    val topkFromAllIntervals = new mutable.MutableList[Counter[T]]

    import scala.collection.JavaConversions._

    for (x <- start to end) {
      topkFromAllIntervals.addAll(streamAggregates(x).topK(k))
    }
    topkFromAllIntervals groupBy(_.getItem) map(x =>
      (x._1, x._2.foldLeft(0)(_ + _.getCount.toInt), x._2.foldLeft(0)(_ + _.getError.toInt))
      )
  }

  def getTopKBetweenTime(epochFrom: Long, epochTo: Long, k : Int,
                         combinedTopKKeys: Array[T] = null): Option[Seq[(T, Int, Int)]] =
    this.rwlock.executeInReadLock({
      val (start, end) = this.timeEpoch.convertEpochToIntervals(epochFrom, epochTo) match {
        case Some(x) => x
        case None => return None
      }
      Some(this.queryIntervals(start, end, k).toSeq)
    }, true)
}

