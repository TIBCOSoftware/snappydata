package org.apache.spark.sql.streaming

import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by ymahajan on 25/09/15.
 */
case class PhysicalDStreamPlan(output: Seq[Attribute], @transient stream: DStream[InternalRow])
  extends SparkPlan with StreamPlan {

  import DStreamHelper._

  def children = Nil

  override def doExecute() : RDD[InternalRow]= {
    assert(validTime != null)
    StreamUtils.invoke(classOf[DStream[Row]], stream, "getOrCompute", (classOf[Time], validTime))
      .asInstanceOf[Option[RDD[InternalRow]]]
      .getOrElse(new EmptyRDD[InternalRow](sparkContext))
  }
}