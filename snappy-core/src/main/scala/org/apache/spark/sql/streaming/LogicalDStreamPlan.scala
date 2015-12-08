package org.apache.spark.sql.streaming

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.streaming.dstream.DStream


/**
 *
 * A LogicalPlan wrapper of row based DStream.
 *
 * @param output
 * @param stream
 * @param streaminSnappy
 *
 * Created by ymahajan on 25/09/15.
 */
case class LogicalDStreamPlan(output: Seq[Attribute],
                              stream: DStream[InternalRow])
                             (val streaminSnappy: StreamingSnappyContext)
  extends LogicalPlan with MultiInstanceRelation {

  def newInstance() =
    LogicalDStreamPlan(output.map(_.newInstance()),
      stream)(streaminSnappy).asInstanceOf[this.type]

  @transient override lazy val statistics = Statistics(
    sizeInBytes = BigInt(streaminSnappy.conf.defaultSizeInBytes)
  )

  def children = Nil
}
