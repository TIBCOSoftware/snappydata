package org.apache.spark.sql.columnar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute

/**
 * Created by ashahid on 12/14/15.
 */
private[sql] trait InMemoryAppendableRelationTrait {

  private[sql] val reservoirRDD: Option[RDD[InternalRow]]
  def withOutput(newOutput: Seq[Attribute]): InMemoryAppendableRelation

  def newInstance(): this.type
}
