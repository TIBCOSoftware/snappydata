package org.apache.spark.sql.streaming

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.streaming.dstream.DStream


/**
 * Created by ymahajan on 25/09/15.
 */
//case class LogicalDStreamPlan(output: Seq[Attribute], stream: DStream[InternalRow])
//                             (val snStrCtx: SnappyStreamingContext)
//  extends LogicalPlan with MultiInstanceRelation {
case class LogicalDStreamPlan(output: Seq[Attribute], stream: DStream[Any])
                             (val snStrCtx: StreamingSnappyContext)
  extends LogicalPlan with MultiInstanceRelation {
  def newInstance() =
    LogicalDStreamPlan(output.map(_.newInstance()), stream)(snStrCtx).asInstanceOf[this.type]

  @transient override lazy val statistics = Statistics(
    sizeInBytes = BigInt(snStrCtx.conf.defaultSizeInBytes)
  )

  def children = Nil
}
