package org.apache.spark.sql.streaming

import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.{Row, execution}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, Time}

/**
 * Created by ymahajan on 25/09/15.
 */
case class WindowPhysicalPlan(
                                 windowDuration: Duration,
                                 slideDuration: Option[Duration],
                                 child: SparkPlan)
  extends execution.UnaryNode with StreamPlan {

  override def doExecute(): RDD[InternalRow] = {
    import DStreamHelper._
    assert(validTime != null)
    StreamUtils.invoke(classOf[DStream[InternalRow]], stream, "getOrCompute", (classOf[Time], validTime))
      .asInstanceOf[Option[RDD[InternalRow]]]
      .getOrElse(new EmptyRDD[InternalRow](sparkContext))
  }

  @transient private val wrappedStream =
    new DStream[InternalRow](streamingSnappyCtx.streamingContext) {
      override def dependencies = parentStreams.toList

      override def slideDuration: Duration = parentStreams.head.slideDuration

      override def compute(validTime: Time): Option[RDD[InternalRow]] = Some(child.execute().asInstanceOf[RDD[InternalRow]])

      private lazy val parentStreams = {
        def traverse(plan: SparkPlan): Seq[DStream[InternalRow]] = plan match {
          case x: StreamPlan => x.stream :: Nil
          case _ => plan.children.flatMap(traverse(_))
        }
        val streams = traverse(child)
        //assert(!streams.isEmpty, s"Input query and related plan $child is not a stream plan")
        streams
      }
    }

  @transient val stream = slideDuration.map(wrappedStream.window(windowDuration, _))
    .getOrElse(wrappedStream.window(windowDuration))

  override def output = child.output
}