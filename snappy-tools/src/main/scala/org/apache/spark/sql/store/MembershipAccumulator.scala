package org.apache.spark.sql.store

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember

import org.apache.spark.AccumulatorParam
import org.apache.spark.storage.BlockManagerId


object MembershipAccumulator extends AccumulatorParam[Map[InternalDistributedMember, BlockManagerId]]{
  /**
   * Merge two accumulated values together. Is allowed to modify and return the first value
   * for efficiency (to avoid allocating objects).
   *
   * @param r1 one set of accumulated data
   * @param r2 another set of accumulated data
   * @return both data sets merged together
   */
  override def addInPlace(r1: Map[InternalDistributedMember, BlockManagerId], r2: Map[InternalDistributedMember, BlockManagerId]): Map[InternalDistributedMember, BlockManagerId] = r1 ++ r2

  /**
   * Return the "zero" (identity) value for an accumulator type, given its initial value. For
   * example, if R was a vector of N dimensions, this would return a vector of N zeroes.
   */
  override def zero(initialValue: Map[InternalDistributedMember, BlockManagerId]): Map[InternalDistributedMember, BlockManagerId] = Map.empty[InternalDistributedMember, BlockManagerId]
}
