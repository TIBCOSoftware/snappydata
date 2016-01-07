package org.apache.spark.sql.streaming

import scala.collection.immutable

import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.streaming.dstream.DStream

/**
  * A PhysicalPlan wrapper of SchemaDStream, inject the validTime and
  * generate an effective RDD of current batchDuration.
  *
  * @param output
  * @param rowStream
  */
case class PhysicalDStreamPlan(output: Seq[Attribute],
    @transient rowStream: DStream[InternalRow])
    extends SparkPlan with StreamPlan {

  def children: immutable.Nil.type = Nil

  override def doExecute(): RDD[InternalRow] = {
    import StreamHelper._
    assert(validTime != null)
    rowStream.getOrCompute(validTime)
        .getOrElse(new EmptyRDD[InternalRow](sparkContext))
  }
}
