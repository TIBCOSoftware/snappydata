package org.apache.spark.sql.streaming

import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream

/**
 * A PhysicalPlan wrapper of SchemaDStream, inject the validTime and
 * generate an effective RDD of current batchDuration.
 *
 * @param output
 * @param stream
 *
 *
 */
case class PhysicalDStreamPlan(output: Seq[Attribute],
                               @transient stream: DStream[InternalRow])
  extends SparkPlan with StreamPlan {

  def children = Nil

  override def doExecute(): RDD[InternalRow] = {
    import StreamHelper._
    assert(validTime != null)
    //For dynamic CQ
    if(!stream.isInitialized) stream
     .initializeAfterContextStart(validTime)
    stream.getOrCompute(validTime)
      .getOrElse(new EmptyRDD[InternalRow](sparkContext))
  }
}