package org.apache.spark.sql.collection

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.local.LocalBackend
import org.apache.spark.sql.AnalysisException
import org.apache.spark.storage.BlockManagerId

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
