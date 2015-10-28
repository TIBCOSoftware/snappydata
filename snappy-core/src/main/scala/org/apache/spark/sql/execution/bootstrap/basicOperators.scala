/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.bootstrap

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.rdd.{PartitionPruningRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.AllTuples
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}



case class OpId(id: Int) {
  override def toString = s"<$id>"
}

object OpId {
  private[this] val curId = new AtomicInteger()
  def newOpId = OpId(curId.getAndIncrement)
}

case class OnlineProject(


    fixed: Boolean,
    projectList: Seq[NamedExpression],
    child: SparkPlan)(
    @transient val trace: List[Int] = -1 :: Nil,
    opId: OpId = OpId.newOpId)
    extends UnaryNode {

  override def output = projectList.map(_.toAttribute)

  @transient lazy val buildProjection = newMutableProjection(projectList, child.output)




  override def doExecute() = child.execute()

  override protected final def otherCopyArgs =  trace :: opId :: Nil

  override def simpleString = s"${super.simpleString} $opId"


}

case class Collect(
    cacheFilter: Option[Attribute],
    projectList: Seq[NamedExpression],
    child: SparkPlan)(
    @transient val trace: List[Int] = -1 :: Nil,
    opId: OpId = OpId.newOpId)
    extends UnaryNode {
  override def output: Seq[Attribute] = projectList.map(_.toAttribute)

  override def requiredChildDistribution = AllTuples :: Nil






  @transient lazy val buildProjection = newMutableProjection(projectList, child.output)

  override def doExecute(): RDD[InternalRow] = {
      val rdd = child.execute()

      rdd.mapPartitions { iterator =>
        val reusableProjection = buildProjection()
        iterator.map(reusableProjection)
      }
  }

  override protected final def otherCopyArgs = trace :: opId :: Nil

  override def simpleString = s"${super.simpleString} $opId"


}

