package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.StratifiedSampler
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Partition, SparkEnv, TaskContext}

/**
 * Encapsulates an RDD over all the cached samples for a sampled table.
 *
 * Created by Soubhik on 5/13/15.
 */
class CachedRDD(name: String, schema: StructType)(sqlContext: SQLContext)
  extends RDD[Row](sqlContext.sparkContext, Nil) {

  override def getPartitions: Array[Partition] = {
    val master = SparkEnv.get.blockManager.master
    val numberedPeers = master.getMemoryStatus.zipWithIndex

    if (numberedPeers.nonEmpty) {
      numberedPeers.map {
        case (bid, idx) => new CachedBlockPartition(null, idx, bid._1.host)
      }.toArray[Partition]
    }
    else {
      Array.empty[Partition]
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val blockManager = SparkEnv.get.blockManager
    val part = split.asInstanceOf[CachedBlockPartition]
    assert(blockManager.blockManagerId.host equals part.host)
    StratifiedSampler(name).map(_.iterator).getOrElse(Iterator[Row]())
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(split.asInstanceOf[CachedBlockPartition].host)
  }
}

class CachedBlockPartition(val parent: Partition, val idx: Int,
                           val host: String) extends Partition {
  val index = idx

  override def toString = s"CachedBlockPartition($idx, $host)"
}

class DummyRDD(sqlContext: SQLContext)
  extends RDD[Row](sqlContext.sparkContext, Nil) {

  /**
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[Row] =
    Iterator.empty

  /**
   * Implemented by subclasses to return the set of partitions in this RDD.
   * This method will only be called once, so it is safe to implement
   * a time-consuming computation in it.
   */
  override protected def getPartitions: Array[Partition] = Array.empty
}
