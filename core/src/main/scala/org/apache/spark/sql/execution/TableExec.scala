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
package org.apache.spark.sql.execution

import com.gemstone.gemfire.internal.cache.PartitionedRegion

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.collection.{SmartExecutorBucketPartition, Utils}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.sources.{DestroyRelation, JdbcExtendedUtils}
import org.apache.spark.sql.store.StoreUtils
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.{DelegateRDD, SnappyContext, SnappySession, SparkSupport, ThinClientConnectorMode}

/**
 * Base class for bulk insert/mutation operations for column and row tables.
 */
trait TableExec extends UnaryExecNode with CodegenSupportOnExecutor with SparkSupport {

  def partitionColumns: Seq[String]

  def tableSchema: StructType

  def relation: Option[DestroyRelation]

  def onExecutor: Boolean

  @transient protected lazy val (metricAdd, metricValue) = Utils.metricMethods

  def partitionExpressions: Seq[Expression]

  def numBuckets: Int

  def isPartitioned: Boolean

  protected def opType: String

  protected def isInsert: Boolean = false

  override lazy val output: Seq[Attribute] =
    AttributeReference("count", LongType, nullable = false)() :: Nil

  val partitioned: Boolean = isPartitioned && partitionExpressions.nonEmpty

  // Enforce default shuffle partitions to match table buckets.
  // Only one insert plan possible in the plan tree, so no clashes.
  if (partitioned) {
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    session.sessionState.snappyConf.setExecutionShufflePartitions(numBuckets)
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
      val schema = tableSchema
      val childOutput = child.output
      // for inserts the column names can be different and need to match
      // by index else search in child output by name
      val childPartitioningAttributes = partitionColumns.map(partColumn =>
        if (isInsert) childOutput(schema.indexWhere(_.name.equalsIgnoreCase(partColumn)))
        else childOutput.find(_.name.equalsIgnoreCase(partColumn)).getOrElse(
          throw new IllegalStateException("Cannot find partitioning column " +
              s"$partColumn in child output for $toString")))
      ClusteredDistribution(childPartitioningAttributes) :: Nil
    } else UnspecifiedDistribution :: Nil
  }

  override lazy val metrics: Map[String, SQLMetric] = {
    if (onExecutor) Map.empty
    else Map(s"num${opType}Rows" -> SQLMetrics.createMetric(sparkContext,
      s"number of ${opType.toLowerCase} rows"))
  }

  override protected def doExecute(): RDD[InternalRow] = {
    // don't expect code generation to fail
    internals.newWholeStagePlan(this).execute()
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    val inputRDDs = child.asInstanceOf[CodegenSupport].inputRDDs()
    if (partitioned) {
      SnappyContext.getClusterMode(sqlContext.sparkContext) match {
        case ThinClientConnectorMode(_, _) =>
          getInputRDDsForConnector(inputRDDs)
        case _ =>
          // wrap shuffle RDDs to set preferred locations as per target table
          inputRDDs.map { rdd =>
            val region = relation.get.asInstanceOf[PartitionedDataSourceScan]
                .region.asInstanceOf[PartitionedRegion]
            // if the two are different then its partition pruning case
            if (numBuckets == rdd.getNumPartitions) {
              new DelegateRDD(sparkContext, rdd, Nil,
                Array.tabulate(numBuckets)(
                  StoreUtils.getBucketPreferredLocations(region, _, forWrite = true)))
            } else rdd
          }
      }
    } else {
      inputRDDs
    }
  }

  private def getInputRDDsForConnector(
      inputRDDs: Seq[RDD[InternalRow]]): Seq[RDD[InternalRow]] = {
    def preferredLocations(table: String): Array[Seq[String]] = {
      val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
      val (schemaName, tableName) = JdbcExtendedUtils.getTableWithSchema(table,
        conn = null, Some(session))
      val relInfo = session.externalCatalog.getRelationInfo(schemaName, tableName,
        isRowTable = false)._1
      val locations = new Array[Seq[String]](numBuckets)
      var i = 0
      relInfo.partitions.foreach(x => {
        locations(i) = x.asInstanceOf[SmartExecutorBucketPartition].
            hostList.map(_._1.asInstanceOf[String])
        i = i + 1
      })
      locations
    }
    inputRDDs.map { rdd =>
      // if the two are different then its partition pruning case
      if (numBuckets == rdd.getNumPartitions) {
        val table = relation.get.asInstanceOf[PartitionedDataSourceScan].table
        new DelegateRDD(sparkContext, rdd, Nil, preferredLocations(table))
      } else rdd
    }
  }

  protected def doChildProduce(ctx: CodegenContext): String = {
    val childProduce = child match {
      case c: CodegenSupportOnExecutor if onExecutor =>
        c.produceOnExecutor(ctx, this)
      case c: CodegenSupport => c.produce(ctx, this)
      case _ => throw new UnsupportedOperationException(
        s"Expected a child supporting code generation. Got: $child")
    }
    if (!internals.isFunctionAddedToOuterClass(ctx, "shouldStop")) {
      // no need to stop in iteration at any point
      internals.addFunction(ctx, "shouldStop",
        s"""
           |@Override
           |protected final boolean shouldStop() {
           |  return false;
           |}
        """.stripMargin, inlineToOuterClass = true)
    }
    childProduce
  }
}
