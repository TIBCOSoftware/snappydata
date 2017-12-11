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
package org.apache.spark.sql.execution.columnar

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}


case class ColumnPutIntoExec(insertPlan: SparkPlan,
    updatePlan: SparkPlan) extends BinaryExecNode {

  override def left: SparkPlan = insertPlan

  override def right: SparkPlan = updatePlan

  override def output: Seq[Attribute] = insertPlan.output

  override protected def doExecute(): RDD[InternalRow] = {
    // First update the rows which are present in the table
    updatePlan.execute()
    // Then insert the rows which are not there in the table
    insertPlan.execute()
  }
}
