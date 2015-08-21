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
import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.io.Output

/**
 * TopK using stream summary algorithm.
 *
 * Created by Hemant on 7/14/15.
 */
class StreamSummaryAggregation[T](val capacity: Int, val intervalSize: Long,
  val epoch0: Long, val maxIntervals: Int,
  val streamAggregates: mutable.MutableList[StreamSummary[T]], initInterval: Long) extends TopK {

  def this(capacity: Int, intervalSize: Long,
    epoch0: Long, maxIntervals: Int) = this(capacity, intervalSize,
    epoch0, maxIntervals, new mutable.MutableList[StreamSummary[T]](), 0)

  val timeEpoch = new TimeEpoch(intervalSize, epoch0, initInterval)

  if (streamAggregates.length == 0) {
    for (x <- 1 to maxIntervals) {
      streamAggregates += new StreamSummary[T](capacity)
      timeEpoch.increment()
    }
  }

  def addItems(data: ArrayBuffer[KeyFrequencyWithTimestamp[T]]) = {
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

  }

  override def isStreamSummary: Boolean = true

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
    {
      val (end, start) = this.timeEpoch.convertEpochToIntervals(epochFrom,
        epochTo) match {
          case Some(x) => x
          case None => return None
        }
      Some(this.queryIntervals(start - 1, end - 1, k).toArray)
    }

}

object StreamSummaryAggregation {

  def create[T: ClassTag](size: Int,
    timeInterval: Long, epoch0: () => Long, maxInterval: Int): TopK =
    new StreamSummaryAggregation[T](size, timeInterval,
      epoch0(), maxInterval)

  def write(kryo: Kryo, output: Output, obj: StreamSummaryAggregation[_]) {
    output.writeInt(obj.capacity)
    output.writeLong(obj.intervalSize)
    output.writeLong(obj.epoch0)
    output.writeInt(obj.maxIntervals)
    output.writeInt(obj.streamAggregates.length)
    obj.streamAggregates.foreach { x => StreamSummary.write(kryo, output, x) }
    output.writeLong(obj.timeEpoch.t)
  }

  def read(kryo: Kryo, input: Input): StreamSummaryAggregation[_] = {

    val capacity = input.readInt
    val intervalSize = input.readLong
    val epoch0 = input.readLong
    val maxIntervals = input.readInt
    val len = input.readInt
    val streamAggregates = mutable.MutableList.fill[StreamSummary[Any]](len)(StreamSummary.read[Any](kryo, input))
    val t = input.readLong
    new StreamSummaryAggregation[Any](capacity, intervalSize, epoch0: Long, maxIntervals,
      streamAggregates, t)

  }
}
