package org.apache.spark.sql.execution

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.collection.ExecutorLocalRDD
import org.apache.spark.util.collection.OpenHashSet
import org.apache.spark.{ Partition, TaskContext }
import scala.reflect.ClassTag
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.sql.collection.ExecutorLocalPartition

/**
 * RDD to compute topK results from all executors where the full
 * set of keys across cluster has been pre-computed.
 */
class TopKResultRDD[T: ClassTag](name: String, val startTime: Long, val endTime: Long,
  val combinedKeys: Array[T], sqlContext: SQLContext)
  extends ExecutorLocalRDD[(T, Approximate)](sqlContext.sparkContext) {

  override def createPartition(index: Int, blockId: BlockManagerId): ExecutorLocalPartition =
    new TopKResultPartition(index, blockId, this.combinedKeys)

  override def compute(split: Partition,
    context: TaskContext): Iterator[(T, Approximate)] = {
    val combinedKeys = split match {
      case p: TopKResultPartition[T] => p.combinedKeys
    }

    val topKHokusai = TopKHokusai[T](name).getOrElse(
      throw new IllegalStateException(s"TopK named '$name' not found"))

    val arrayTopK =
      if (topKHokusai.windowSize == Long.MaxValue)
        Some(topKHokusai.getForKeysInCurrentInterval(null))
      else
        topKHokusai.getTopKBetweenTime(startTime, endTime, combinedKeys)

    arrayTopK.map(_.toIterator).getOrElse(Iterator.empty)
  }
}

class TopKResultPartition[T](index: Int,
  blockId: BlockManagerId, val combinedKeys: Array[T]) extends ExecutorLocalPartition(index, blockId) {

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
