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
    val blockManager = SparkEnv.get.blockManager
    val part = split.asInstanceOf[CachedBlockPartition]
    val thisHost = blockManager.blockManagerId.host
    if (part.host != thisHost) {
      throw new IllegalStateException(
        s"Expected to execute on ${part.host} but is on $thisHost")
    }
    val topkHokusai = TopKHokusai(name).getOrElse(throw new IllegalStateException())

    val arrayTopk = topkHokusai.getTopKBetweenTime(startTime, endTime).getOrElse( return Iterator.empty )

    arrayTopk.toIterator
  }

}