/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan

import scala.collection.immutable

case class PhysicalStructStreamPlan(output: Seq[Attribute])
  extends SparkPlan {

  def children: immutable.Nil.type = Nil

  override def doExecute(): RDD[InternalRow] = {
    println("YOGS doExec")
//    sqlContext.emptyDataFrame
//      .writeStream
//      .format("memory")
//      .queryName("simple")
//      .outputMode("append")
//      .trigger(ProcessingTime("1 seconds"))
//      .start
    sparkContext.emptyRDD[InternalRow]
  }
}