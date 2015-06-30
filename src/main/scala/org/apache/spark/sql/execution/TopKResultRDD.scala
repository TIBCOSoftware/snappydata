package org.apache.spark.sql.execution

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.collection.ExecutorLocalRDD
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.{Partition, TaskContext}

/**
 * RDD to compute topK results from all executors where the full
 * set of keys across cluster has been pre-computed.
 */
class TopKResultRDD(name: String, startTime: Long, endTime: Long,
    combinedKeys: Array[Any], sqlContext: SQLContext)
    extends ExecutorLocalRDD[(Any, Long)](sqlContext.sparkContext) {

  override def compute(split: Partition,
      context: TaskContext): Iterator[(Any, Long)] = {

    val topKHokusai = TopKHokusai(name).getOrElse(
      throw new IllegalStateException())

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
class TopKKeysRDD(name: String, startTime: Long,
    endTime: Long, sqlContext: SQLContext)
    extends ExecutorLocalRDD[OpenHashSet[Any]](sqlContext.sparkContext) {

  override def compute(split: Partition,
      context: TaskContext): Iterator[OpenHashSet[Any]] = {

    val topKHokusai = TopKHokusai(name).getOrElse(
      throw new IllegalStateException())

    val arrayTopK =
      if (topKHokusai.windowSize == Long.MaxValue)
        Some(topKHokusai.getTopKKeysForCurrentInterval)
      else
        topKHokusai.getTopKKeysBetweenTime(startTime, endTime)

    arrayTopK.map(Iterator(_)).getOrElse(Iterator.empty)
  }
}
