package org.apache.spark.sql.collection

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.sql.AnalysisException
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.{Partition, SparkContext}

object Utils {

  implicit class StringExtensions(val s: String) extends AnyVal {
    def ci = new {
      def unapply(other: String) = s.equalsIgnoreCase(other)
    }
  }

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

  final val timeIntervalSpec = "([0-9]+)(ms|s|m|h)".r

  def parseTimeInterval(optV: Any, module: String): Long = {
    optV match {
      case tii: Int => tii.toLong
      case til: Long => til
      case tis: String => tis match {
        case timeIntervalSpec(interval, unit) =>
          unit match {
            case "ms" => interval.toLong
            case "s" => interval.toLong * 1000L
            case "m" => interval.toLong * 60000L
            case "h" => interval.toLong * 3600000L
            case _ => throw new AssertionError(
              s"unexpected regex match 'unit'=$unit")
          }
        case _ => throw new AnalysisException(
          s"$module: Cannot parse 'timeInterval'=$tis")
      }
      case _ => throw new AnalysisException(
        s"$module: Cannot parse 'timeInterval'=$optV")
    }
  }
}

abstract class ExecutorLocalRDD[T: ClassTag](@transient _sc: SparkContext)
    extends RDD[T](_sc, Nil) {

  override def getPartitions: Array[Partition] = {
    val numberedPeers = Utils.getAllExecutorsMemoryStatus(sparkContext).
        keySet.zipWithIndex

    if (numberedPeers.nonEmpty) {
      numberedPeers.map {
        case (bid, idx) => createPartition(idx, bid)
      }.toArray[Partition]
    }
    else {
      Array.empty[Partition]
    }
  }
  
 def createPartition(index: Int , blockId: BlockManagerId) : ExecutorLocalPartition = 
    new ExecutorLocalPartition(index, blockId)

  override def getPreferredLocations(split: Partition): Seq[String] =
    Seq(split.asInstanceOf[ExecutorLocalPartition].hostExecutorId)
}

class ExecutorLocalPartition(override val index: Int,
    val blockId: BlockManagerId) extends Partition {

  def hostExecutorId = Utils.getHostExecutorId(blockId)

  override def toString = s"ExecutorLocalPartition($index, $blockId)"
}
