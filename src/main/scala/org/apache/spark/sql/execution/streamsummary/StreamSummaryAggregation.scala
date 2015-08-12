package org.apache.spark.sql.execution.streamsummary

import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import io.snappydata.util.com.clearspring.analytics.stream.{ Counter, StreamSummary }
import org.apache.spark.sql.LockUtils.ReadWriteLock
import org.apache.spark.sql.TimeEpoch
import org.apache.spark.sql.collection.SegmentMap
import org.apache.spark.sql.execution.{ Approximate, KeyFrequencyWithTimestamp }
import org.apache.spark.sql.execution.{ TopK, TopKStub }

/**
 * TopK using stream summary algorithm.
 *
 * Created by Hemant on 7/14/15.
 */
class StreamSummaryAggregation[T](val capacity: Int, val intervalSize: Long,
  val epoch0: Long, val maxIntervals: Int,
  val startIntervalGenerator: Boolean = false, partitionID: Int) extends TopK {

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
        } else {
          throw new UnsupportedOperationException("Support for intervals " +
            "for wall clock time for streams not available yet.")
        }
      })
    })
  }

  override def getPartitionID: Int = this.partitionID

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
  private final val topKMap = new mutable.HashMap[String, mutable.HashMap[Int, TopK]]
  private final val mapLock = new ReentrantReadWriteLock()
  /*
  def apply[T](name: String): Option[StreamSummaryAggregation[T]] = {
    SegmentMap.lock(mapLock.readLock) {
      topKMap.get(name) match {
        case Some(tk) => Some(tk.asInstanceOf[StreamSummaryAggregation[T]])
        case None => None
      }
    }
  }*/

  def apply[T: ClassTag](name: String, size: Int, tsCol: Int,
    timeInterval: Long, epoch0: () => Long, maxInterval: Int, partitionID: Int) = {
    lookupOrAdd[T](name, size, tsCol, timeInterval, epoch0, maxInterval, partitionID)
  }

  def lookupOrAddDummy(name: String, partitionID: Int): TopK = {
    SegmentMap.lock(mapLock.readLock) {
      topKMap.get(name)
    } match {
      case Some(partMap) => partMap.get(partitionID) match {
        case Some(topK) => topK
        case None =>
          SegmentMap.lock(mapLock.writeLock) {
            partMap.getOrElse(partitionID, {
              val topK = new TopKStub(partitionID)
              partMap(partitionID) = topK
              topK
            })
          }
      }
      case None =>
        // insert into global map but double-check after write lock
        SegmentMap.lock(mapLock.writeLock) {
          val partMap = topKMap.getOrElse(name, {
            val newPartMap = new mutable.HashMap[Int, TopK]
            topKMap(name) = newPartMap
            newPartMap
          })
          partMap.getOrElse(partitionID, {
            val topK = new TopKStub(partitionID)
            partMap(partitionID) = topK
            topK
          })
        }
    }
  }

  private[sql] def lookupOrAdd[T: ClassTag](name: String, size: Int,
    tsCol: Int, timeInterval: Long, epoch0: () => Long,
    maxInterval: Int, partitionID: Int): TopK = {
    SegmentMap.lock(mapLock.readLock) {
      topKMap.get(name)
    } match {
      case Some(partMap) =>
        SegmentMap.lock(mapLock.writeLock) {
          partMap.get(partitionID) match {
            case Some(topK) => topK match {
              case x: StreamSummaryAggregation[_] => x
              case _ =>
                val topKK = new StreamSummaryAggregation[T](size, timeInterval,
                  epoch0(), maxInterval, timeInterval > 0 && tsCol < 0, partitionID)
                partMap(partitionID) = topKK
                topKK
            }
            case None =>
              partMap.getOrElse(partitionID, {
                val topk = new StreamSummaryAggregation[T](size, timeInterval,
                  epoch0(), maxInterval, timeInterval > 0 && tsCol < 0, partitionID)
                partMap(partitionID) = topk
                topk
              })
          }
        }

      //topk.asInstanceOf[StreamSummaryAggregation[T]]
      case None =>
        SegmentMap.lock(mapLock.writeLock) {
          val partMap = topKMap.getOrElse(name, {
            val newPartMap = new mutable.HashMap[Int, TopK]
            topKMap(name) = newPartMap
            newPartMap
          })
          partMap.getOrElse(partitionID, {
            val topk = new StreamSummaryAggregation[T](size, timeInterval,
              epoch0(), maxInterval, timeInterval > 0 && tsCol < 0, partitionID)
            partMap(partitionID) = topk
            topk
          })
        }
    }

  }
}
