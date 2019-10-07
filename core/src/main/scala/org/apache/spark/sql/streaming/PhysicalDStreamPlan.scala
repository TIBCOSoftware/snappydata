/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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

import scala.collection.immutable

import org.apache.spark.rdd.{EmptyRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.streaming.StreamBaseRelation._
import org.apache.spark.streaming.StreamUtils
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
    assert(validTime != null)
    StreamUtils.getOrCompute(rowStream, validTime)
        .getOrElse(new EmptyRDD[InternalRow](sparkContext))
  }
}
