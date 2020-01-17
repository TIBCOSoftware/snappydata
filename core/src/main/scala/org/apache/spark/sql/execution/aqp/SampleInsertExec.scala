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
package org.apache.spark.sql.execution.aqp

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.{PlanLater, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.hive.SnappySessionState
import org.apache.spark.sql.sources.SamplingRelation
import org.apache.spark.sql.types.StructType

case class SampleInsertExec (baseTableInsert: SparkPlan,
  sourceDataPlan: SparkPlan, baseTableIdentifier: TableIdentifier,
  aqpRelations: Seq[(LogicalPlan, String)],
  baseTableSchema: StructType) extends SparkPlan {

  override protected def doExecute(): RDD[InternalRow] = {
    val retVal = baseTableInsert.execute()
    val data = sourceDataPlan.execute()
    val ds = sqlContext.sparkSession.asInstanceOf[SnappySession].
      internalCreateDataFrame(data, baseTableSchema)
    val catalog = this.sqlContext.sessionState.asInstanceOf[SnappySessionState].catalog

    aqpRelations.foreach {
      case (LogicalRelation(sr: SamplingRelation, _, _), _) => sr.insert(ds, false)
    }
    retVal
  }

  override def output: Seq[Attribute] = baseTableInsert.output

  override def children: Seq[SparkPlan] = Seq(baseTableInsert, sourceDataPlan)
}
