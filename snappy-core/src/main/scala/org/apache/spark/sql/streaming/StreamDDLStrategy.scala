package org.apache.spark.sql.streaming

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.planning.QueryPlanner
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{ExecutedCommand, SparkPlan}
import org.apache.spark.streaming.Time
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by ymahajan on 25/09/15.
 */

object StreamDDLStrategy extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case CreateStreamTable(streamName, userColumns, options) =>
      ExecutedCommand(
        CreateStreamTableCmd(streamName, userColumns, options)) :: Nil
    case CreateSampledTable(streamName, options) =>
      ExecutedCommand(
        CreateSampledTableCmd(streamName, options)) :: Nil
    case StreamOperationsLogicalPlan(action, batchInterval) =>
      ExecutedCommand(
        StreamingCtxtActionsCmd(action, batchInterval)) :: Nil
    case _ => Nil
  }
}

class StreamStrategies extends QueryPlanner[SparkPlan]{
  def strategies : Seq[Strategy] = StreamStrategy :: Nil
  object StreamStrategy extends Strategy{
    def apply(plan : LogicalPlan): Seq[SparkPlan] = plan match {
      case LogicalDStreamPlan(output, stream) =>
        PhysicalDStreamPlan(output, stream.asInstanceOf[DStream[Row]]) :: Nil
      case x @ WindowLogicalPlan(w,s,child) =>
        WindowPhysicalPlan(w,s,planLater(child)) :: Nil
      case _ => Nil

    }
  }
}

private object DStreamHelper {
  var validTime: Time = null
  def setValidTime(time: Time): Unit = {
    if (validTime == null) {
      validTime = time
    } else if (validTime != time) {
      validTime = time
    } else {
    }
  }
}