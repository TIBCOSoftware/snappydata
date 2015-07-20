package org.apache.spark.sql.execution

import scala.reflect.ClassTag

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.streamsummary.StreamSummaryAggregation
import org.apache.spark.util.collection.OpenHashSet

object TopKRDD {

  /**
   * RDD to compute topK results from all executors where the full
   * set of keys across cluster has been pre-computed.
   */
  def resultRDD[T: ClassTag](name: String, startTime: Long,
      endTime: Long, combinedKeys: Array[T], sqlContext: SQLContext) = {
    Utils.mapExecutors[(T, Approximate)](sqlContext, { () =>
      val topKHokusai = TopKHokusai[T](name).getOrElse(
        throw new IllegalStateException(s"TopK named '$name' not found"))

      val arrayTopK =
        if (topKHokusai.windowSize == Long.MaxValue)
          Some(topKHokusai.getForKeysInCurrentInterval(combinedKeys))
        else
          topKHokusai.getTopKBetweenTime(startTime, endTime, combinedKeys)

      arrayTopK.map(_.toIterator).getOrElse(Iterator.empty)
    })
  }

  /**
   * RDD to collect topK keys from all the partitions (exactly one per executor)
   */
  def keysRDD[T: ClassTag](name: String, startTime: Long,
      endTime: Long, sqlContext: SQLContext) = {
    Utils.mapExecutors[OpenHashSet[T]](sqlContext, { () =>
      val topKHokusai = TopKHokusai[T](name).getOrElse(
        throw new IllegalStateException())

      val arrayTopK =
        if (topKHokusai.windowSize == Long.MaxValue)
          Some(topKHokusai.getTopKKeysForCurrentInterval)
        else
          topKHokusai.getTopKKeysBetweenTime(startTime, endTime)

      arrayTopK.map(Iterator(_)).getOrElse(Iterator.empty)
    })
  }

  /**
   * RDD to collect topK keys from all the partitions (exactly one per executor)
   */
  def streamRDD[T: ClassTag](name: String, startTime: Long,
      endTime: Long, sqlContext: SQLContext) = {
    Utils.mapExecutors[(T, Approximate)](sqlContext, { () =>

      val streamSummary = StreamSummaryAggregation[T](name).getOrElse(
        throw new IllegalStateException())

      val arrayTopK = streamSummary.getTopKBetweenTime(startTime, endTime,
        streamSummary.capacity)

      arrayTopK.map(_.toIterator).getOrElse(Iterator.empty)
    })
  }
}
