package org.apache.spark.sql.execution

import scala.reflect.ClassTag

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.collection.ExecutorLocalRDD
import org.apache.spark.sql.execution.streamsummary.StreamSummaryAggregation
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.{Partition, TaskContext}

/**
 * RDD to compute topK results from all executors where the full
 * set of keys across cluster has been pre-computed.
 */
class TopKResultRDD[T: ClassTag](name: String, startTime: Long,
    endTime: Long, combinedKeys: Array[T], sqlContext: SQLContext)
    extends ExecutorLocalRDD[(T, Approximate)](sqlContext.sparkContext) {

  override def compute(split: Partition,
      context: TaskContext): Iterator[(T, Approximate)] = {

    val topKHokusai = TopKHokusai[T](name).getOrElse(
      throw new IllegalStateException(s"TopK named '$name' not found"))

    val arrayTopK =
      if (topKHokusai.windowSize == Long.MaxValue)
        Some(topKHokusai.getForKeysInCurrentInterval(combinedKeys))
      else
        topKHokusai.getTopKBetweenTime(startTime, endTime, combinedKeys)

    arrayTopK.map(_.toIterator).getOrElse(Iterator.empty)
  }
}

/**
 * RDD to collect topK keys from all the partitions (exactly one per executor).
 */
class TopKKeysRDD[T: ClassTag](name: String, startTime: Long,
    endTime: Long, sqlContext: SQLContext)
    extends ExecutorLocalRDD[OpenHashSet[T]](sqlContext.sparkContext) {

  override def compute(split: Partition,
      context: TaskContext): Iterator[OpenHashSet[T]] = {

    val topKHokusai = TopKHokusai[T](name).getOrElse(
      throw new IllegalStateException())

    val arrayTopK =
      if (topKHokusai.windowSize == Long.MaxValue)
        Some(topKHokusai.getTopKKeysForCurrentInterval)
      else
        topKHokusai.getTopKKeysBetweenTime(startTime, endTime)

    arrayTopK.map(Iterator(_)).getOrElse(Iterator.empty)
  }
}

/**
 * RDD to collect topK keys from all the partitions (exactly one per executor).
 */
class TopKStreamRDD[T: ClassTag](name: String, startTime: Long,
    endTime: Long, sqlContext: SQLContext)
    extends ExecutorLocalRDD[(T, Approximate)](sqlContext.sparkContext) {

  override def compute(split: Partition,
      context: TaskContext): Iterator[(T, Approximate)] = {

    val streamSummary = StreamSummaryAggregation[T](name).getOrElse(
      throw new IllegalStateException())

    val arrayTopK = streamSummary.getTopKBetweenTime(startTime, endTime,
      streamSummary.capacity)

    arrayTopK.map(_.toIterator).getOrElse(Iterator.empty)
  }
}
