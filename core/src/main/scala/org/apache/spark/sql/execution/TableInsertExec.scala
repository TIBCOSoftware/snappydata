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
package org.apache.spark.sql.execution

import com.gemstone.gemfire.internal.cache.PartitionedRegion

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, HashPartitioning, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.sources.DestroyRelation
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.{DelegateRDD, SnappySession}

/**
 * Common methods for bulk inserts into column and row tables.
 */
abstract class TableInsertExec(partitionColumns: Seq[String],
    relationSchema: StructType, relation: Option[DestroyRelation],
    onExecutor: Boolean) extends UnaryExecNode with CodegenSupportOnExecutor {

  @transient protected lazy val (metricAdd, _) = Utils.metricMethods

  def partitionExpressions: Seq[Expression]

  def numBuckets: Int

  override lazy val output: Seq[Attribute] =
    AttributeReference("count", LongType, nullable = false)() :: Nil

  val partitioned: Boolean = numBuckets > 1 && partitionExpressions.nonEmpty

  // Enforce default shuffle partitions to match table buckets.
  // Only one insert plan possible in the plan tree, so no clashes.
  if (partitioned) {
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    session.sessionState.conf.setExecutionShufflePartitions(numBuckets)
  }

  /** Specifies how data is partitioned for the table. */
  override lazy val outputPartitioning: Partitioning = {
    if (partitioned) HashPartitioning(partitionExpressions, numBuckets)
    else super.outputPartitioning
  }

  /** Specifies the partition requirements on the child. */
  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitioned) {
      // For partitionColumns find the matching child columns
      val schema = relationSchema
      val childPartitioningAttributes = partitionColumns.map(partColumn =>
        child.output(schema.indexWhere(_.name.equalsIgnoreCase(partColumn))))
      ClusteredDistribution(childPartitioningAttributes) :: Nil
    } else UnspecifiedDistribution :: Nil
  }

  override lazy val metrics: Map[String, SQLMetric] = {
    if (onExecutor) Map.empty
    else Map("numInsertedRows" -> SQLMetrics.createMetric(sparkContext,
      "number of inserted rows"))
  }

  override protected def doExecute(): RDD[InternalRow] = {
    // don't expect code generation to fail
    WholeStageCodegenExec(this).execute()
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    val inputRDDs = child.asInstanceOf[CodegenSupport].inputRDDs()
    // wrap shuffle RDDs to set preferred locations as per target table
    if (partitioned) {
      inputRDDs.map { rdd =>
        val region = relation.get.asInstanceOf[PartitionedDataSourceScan]
            .region.asInstanceOf[PartitionedRegion]
        assert(numBuckets == rdd.getNumPartitions)
        new DelegateRDD(sparkContext, rdd,
          Array.tabulate(numBuckets)(
            StoreUtils.getBucketPreferredLocations(region, _)))
      }
    }
    else inputRDDs
  }

  protected def doChildProduce(ctx: CodegenContext): String = {
    val childProduce = child match {
      case c: CodegenSupportOnExecutor if onExecutor =>
        c.produceOnExecutor(ctx, this)
      case c: CodegenSupport => c.produce(ctx, this)
      case _ => throw new UnsupportedOperationException(
        s"Expected a child supporting code generation. Got: $child")
    }
    if (!ctx.addedFunctions.contains("shouldStop")) {
      // no need to stop in iteration at any point
      ctx.addNewFunction("shouldStop",
        s"""
           |@Override
           |protected final boolean shouldStop() {
           |  return false;
           |}
        """.stripMargin)
    }
    childProduce
  }
}
