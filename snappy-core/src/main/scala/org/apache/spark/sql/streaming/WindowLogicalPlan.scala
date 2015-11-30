package org.apache.spark.sql.streaming

import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.streaming.Duration

/**
 * Created by ymahajan on 25/09/15.
 */
case class WindowLogicalPlan(
                              windowDuration: Duration,
                              slideDuration: Option[Duration],
                              child: LogicalPlan)
  extends logical.UnaryNode {
  override def output = child.output
}
