package org.apache.spark.sql.execution.joins

import java.io.{ObjectOutputStream, IOException}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.{MapPartitionsWithPreparationRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.physical.{UnspecifiedDistribution, AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.{Partition, SparkContext, TaskContext}


/**
 * :: DeveloperApi ::
 * Performs an local hash join of two child relations.  If a relation (out of a datasource) is already replicated
 * accross all nodes then rather than doing a Broadcast join which can be expensive, this join just
 * scans through the single partition of the replicated relation while streaming through the other relation
 */
@DeveloperApi
case class LocalJoin(leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    left: SparkPlan,
    right: SparkPlan)
    extends BinaryNode with HashJoin {


  override private[sql] lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createLongMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createLongMetric(sparkContext, "number of right rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning


  override def requiredChildDistribution: Seq[Distribution] =
    UnspecifiedDistribution :: UnspecifiedDistribution :: Nil


  /**
   * Overridden by concrete implementations of SparkPlan.
   * Produces the result of the query as an RDD[InternalRow]
   */
  override protected def doExecute(): RDD[InternalRow] = {
    val (numBuildRows, numStreamedRows) = buildSide match {
      case BuildLeft => (longMetric("numLeftRows"), longMetric("numRightRows"))
      case BuildRight => (longMetric("numRightRows"), longMetric("numLeftRows"))
    }
    val numOutputRows = longMetric("numOutputRows")

    narrowPartitions(buildPlan.execute(), streamedPlan.execute(), true) {
      (buildIter, streamIter) => {
        val hashed = HashedRelation(buildIter, numBuildRows, buildSideKeyGenerator)
        hashJoin(streamIter, numStreamedRows, hashed, numOutputRows)
      }
    }
  }


  def narrowPartitions(buildRDD: RDD[InternalRow], streamRDD: RDD[InternalRow], preservesPartitioning: Boolean)
      (f: (Iterator[InternalRow], Iterator[InternalRow]) => Iterator[InternalRow]): NarrowPartitionsRDD = {
    val sc = buildRDD.sparkContext
    new NarrowPartitionsRDD(sc, sc.clean(f), buildRDD, streamRDD, preservesPartitioning)
  }


}


private[spark] class NarrowPartitionsRDD(
    @transient sc: SparkContext,
    var f: (Iterator[InternalRow], Iterator[InternalRow]) => Iterator[InternalRow],
    var buildRDD: RDD[InternalRow],
    var streamRDD: RDD[InternalRow],
    preservesPartitioning: Boolean = false)
    extends RDD[InternalRow](sc, Nil) {

  override def compute(s: Partition, context: TaskContext): Iterator[InternalRow] = {
    val partitions = s.asInstanceOf[NarrowPartitionsPartition]
    f(buildRDD.iterator(partitions.buildPartition, context), streamRDD.iterator(partitions.streamPartition, context))
  }

  override def getPartitions: Array[Partition] = {
    val numParts = streamRDD.partitions.length
    val part = buildRDD.partitions.head
    Array.tabulate[Partition](numParts) { i =>
      val prefs = streamRDD.preferredLocations(streamRDD.partitions(i))
      new NarrowPartitionsPartition(part, streamRDD.partitions(i) ,prefs)
    }
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    s.asInstanceOf[NarrowPartitionsPartition].preferredLocations
  }

}

private[spark] class NarrowPartitionsPartition(
    var buildRDDPartition: Partition,
    var streamRDDPartition: Partition,
    @transient val preferredLocations: Seq[String])
    extends Partition {
  override val index: Int = streamRDDPartition.index
  val buildPartition = buildRDDPartition
  val streamPartition = streamRDDPartition
}
