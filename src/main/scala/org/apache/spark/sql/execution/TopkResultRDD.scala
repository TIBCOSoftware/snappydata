package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{CachedBlockPartition, Row, SQLContext}
import org.apache.spark.{Partition, SparkEnv, TaskContext}

/**
 * Created by hemantb on 6/18/15.
 */
class TopkResultRDD(name: String, startTime: Long,
                    endTime: Long)(sqlContext: SQLContext)
  extends RDD[(Any,Long)](sqlContext.sparkContext, Nil) {

  override def getPartitions: Array[Partition] = {
    val master = SparkEnv.get.blockManager.master
    val numberedPeers = master.getMemoryStatus.zipWithIndex

    if (numberedPeers.nonEmpty) {
      numberedPeers.map {
        case (bid, idx) => new CachedBlockPartition(idx, bid._1.host)
      }.toArray[Partition]
    }
    else {
      Array.empty[Partition]
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[(Any,Long)] = {

    val topkHokusai = TopKHokusai(name).getOrElse(throw new IllegalStateException())

    val arrayTopk = if (topkHokusai.windowSize == Long.MaxValue)
      topkHokusai.getTopKForCurrentInterval.getOrElse( return Iterator.empty )
    else
      topkHokusai.getTopKBetweenTime(startTime, endTime).getOrElse( return Iterator.empty )


    arrayTopk.toIterator
  }

}