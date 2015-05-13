package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.{Partition, SparkEnv, TaskContext}

/**
 * Created by soubhikc on 5/13/15.
 */
class CachedRDD()(sqlContext: SQLContext)
  extends RDD[Row](sqlContext.sparkContext, Nil) {

  override def getPartitions: Array[Partition] = {
    val numbrd = SparkEnv.get.blockManager.getPeers(false).zipWithIndex

    if (numbrd.length == 0) {
      return Array.empty[Partition]
    }

    val partitions = numbrd.map {
      case (bid, idx) => new CachedBlockPartition(idx, bid.host)
    }

    partitions.toArray[Partition]
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = {
    val blockManager = SparkEnv.get.blockManager
    val part = split.asInstanceOf[CachedBlockPartition]
    assert(blockManager.blockManagerId.host equals part.host)
    new Iterator[Row] {
      override def hasNext: Boolean = false

      override def next(): Row = throw new NoSuchElementException("next on empty iterator")
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    Seq(split.asInstanceOf[CachedBlockPartition].host)
  }
}

object CachedRDD {
  def apply()(sqlContext: SQLContext): CachedRDD = {
    new CachedRDD()(sqlContext)
  }
}

private[spark] class CachedBlockPartition(val idx: Int, val host: String)
  extends Partition {
  val index = idx
}

class DummyRDD(output: Seq[Attribute])(sqlContext: SQLContext) extends LogicalRDD(output, null)(sqlContext) {
  private val id: Int = sqlContext.sparkContext.newRddId();

  override def sameResult(plan: LogicalPlan): Boolean = plan match {
    case DummyRDD(otherID) => id == otherID
    case _ => false
  }
}

object DummyRDD {
  def apply(output: Seq[Attribute])(sqlContext: SQLContext): DummyRDD = {
    new DummyRDD(output)(sqlContext)
  }

  def unapply(dumb: DummyRDD): Option[Int] = Some(dumb.id)
}
