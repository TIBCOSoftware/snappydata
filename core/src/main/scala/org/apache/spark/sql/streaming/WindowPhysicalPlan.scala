/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package org.apache.spark.sql.streaming

import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.streaming.StreamBaseRelation._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming._

case class WindowPhysicalPlan(
    windowDuration: Duration,
    slide: Option[Duration],
    child: SparkPlan)
    extends execution.UnaryExecNode with StreamPlan {

  override def doExecute(): RDD[InternalRow] = {
    assert(validTime != null)
    StreamUtils.getOrCompute(rowStream, validTime)
        .getOrElse(new EmptyRDD[InternalRow](sparkContext))
  }

  @transient private val wrappedStream =
    new DStream[InternalRow](SnappyStreamingContext.getInstance().get) {
      override def dependencies = parentStreams.toList

      override def slideDuration: Duration = {
        parentStreams.head.slideDuration
      }

      override def compute(validTime: Time): Option[RDD[InternalRow]] =
        Some(child.execute())

      private lazy val parentStreams = {
        def traverse(plan: SparkPlan): Seq[DStream[InternalRow]] = plan match {
          case x: StreamPlan => x.rowStream :: Nil
          case _ => plan.children.flatMap(traverse)
        }
        val streams = traverse(child)
        streams
      }
    }

  @transient val rowStream = slide.map(
    wrappedStream.window(windowDuration, _))
      .getOrElse(wrappedStream.window(windowDuration))

  override def output: Seq[Attribute] = child.output
}
