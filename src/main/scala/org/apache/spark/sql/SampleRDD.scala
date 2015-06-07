package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{StratifiedSampler, LogicalRDD}
import org.apache.spark.{Partition, SparkEnv, TaskContext}

/**
 * Encapsulates an RDD over all the cached samples for a sampled table.
 *
 * Created by Soubhik on 5/13/15.
 */
class CachedRDD(name: String)(sqlContext: SQLContext)
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

object CachedRDD {
  def apply(name: String)(sqlContext: SQLContext): CachedRDD = {
    new CachedRDD(name)(sqlContext)
  }
}

class CachedBlockPartition(val parent: Partition, val idx: Int,
                           val host: String) extends Partition {
  val index = idx
  override def toString = s"CachedBlockPartition($idx, $host)"
}

class DummyRDD(output: Seq[Attribute])(sqlContext: SQLContext)
  extends LogicalRDD(output, null)(sqlContext) {
  private val id: Int = sqlContext.sparkContext.newRddId()

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
