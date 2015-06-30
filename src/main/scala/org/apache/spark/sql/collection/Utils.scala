package org.apache.spark.sql.collection

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.sql.AnalysisException
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{Partition, SparkContext}

object Utils {

  def fillArray[T](a: Array[_ >: T], v: T, start: Int, endP1: Int) = {
    var index = start
    while (index < endP1) {
      a(index) = v
      index += 1
    }
  }

  def columnIndex(col: String, cols: Array[String]) = {
    val colT = col.trim
    cols.indices.collectFirst {
      case index if colT.equalsIgnoreCase(cols(index)) => index
    }.getOrElse {
      throw new AnalysisException(
        s"""StratifiedSampler: Cannot resolve column name "$col" among
            (${cols.mkString(", ")})""")
    }
  }

  def getAllExecutorsMemoryStatus(sc: SparkContext): Map[BlockManagerId,
      (Long, Long)] = {
    val memoryStatus = sc.env.blockManager.master.getMemoryStatus
    // no filtering for local backend
    sc.schedulerBackend match {
      case lb: LocalBackend => memoryStatus
      case _ => memoryStatus.filter(!_._1.isDriver)
    }
  }

  def getHostExecutorId(blockId: BlockManagerId) =
    blockId.host + '_' + blockId.executorId
}

abstract class ExecutorLocalRDD[T: ClassTag](@transient _sc: SparkContext)
    extends RDD[T](_sc, Nil) {

  override def getPartitions: Array[Partition] = {
    val numberedPeers = Utils.getAllExecutorsMemoryStatus(sparkContext).
        keySet.zipWithIndex

    if (numberedPeers.nonEmpty) {
      numberedPeers.map {
        case (bid, idx) => new ExecutorLocalPartition(idx, bid)
      }.toArray[Partition]
    }
    else {
      Array.empty[Partition]
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    Seq(split.asInstanceOf[ExecutorLocalPartition].hostExecutorId)
}

class ExecutorLocalPartition(override val index: Int,
    val blockId: BlockManagerId) extends Partition {

  def hostExecutorId = Utils.getHostExecutorId(blockId)

  override def toString = s"ExecutorLocalPartition($index, $blockId)"
}
