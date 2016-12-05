/*
 * Copyright (c) 2016 SnappyData, Inc. All rights reserved.
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
package org.apache.spark.sql.execution.aggregate

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.CachedDataFrame
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{BinaryExecNode, BufferedRowIterator, InputAdapter, SparkPlan, WholeStageCodegenExec}

/**
 * Special plan to collect top-level aggregation on driver itself and avoid
 * an exchange for simple aggregates.
 */
case class CollectAggregateExec(
    left: SparkPlan,
    right: SparkPlan,
    @transient basePlan: SnappyHashAggregateExec) extends BinaryExecNode {

  override def nodeName: String = "CollectAggregate"

  override def output: Seq[Attribute] = right.output

  override def requiredChildDistribution: List[Distribution] =
    UnspecifiedDistribution :: UnspecifiedDistribution :: Nil

  @transient private[sql] lazy val childRDD = left.execute()

  @transient private[sql] lazy val (generatedSource, generatedReferences,
  generatedClass) = {
    // temporarily switch producer to an InputAdapter for rows as normal
    // Iterator[UnsafeRow] which will be set explicitly in executeCollect()
    basePlan.childProducer = InputAdapter(left)
    val (ctx, cleanedSource) = WholeStageCodegenExec(basePlan).doCodeGen()
    basePlan.childProducer = left
    val clazz = CodeGenerator.compile(cleanedSource)
    (cleanedSource, ctx.references.toArray, clazz)
  }

  /**
   * Return collected data as partition-wise array of raw compressed bytes
   * either as a byte array or RDDBlockId stored in BlockManager (latter if
   * large), without any aggregation. Callers need to get hold of generatedClass
   * or generatedSource + generatedReferences separately.
   */
  private[sql] def executeCollectData(): Array[Any] = {
    val childRDD = this.childRDD
    val sc = sqlContext.sparkContext
    val bm = sc.env.blockManager

    val numPartitions = childRDD.getNumPartitions
    val partitionBlocks = new Array[Any](numPartitions)
    val rddId = childRDD.id

    sc.runJob(childRDD, CachedDataFrame, 0 until numPartitions,
      (index: Int, r: (Array[Byte], Int)) =>
        // store the partition results in BlockManager for large results
        partitionBlocks(index) = CachedDataFrame.localBlockStoreResultHandler(
          rddId, bm)(index, r._1))

    partitionBlocks
  }

  override def executeCollect(): Array[InternalRow] = {
    val sc = sqlContext.sparkContext
    val bm = sc.env.blockManager

    val partitionBlocks = executeCollectData()
    // create an iterator over the blocks and pass to generated iterator
    val numFields = left.schema.length
    val results = partitionBlocks.iterator.flatMap(
      CachedDataFrame.localBlockStoreDecoder(numFields, bm))
    val buffer = generatedClass.generate(generatedReferences)
        .asInstanceOf[BufferedRowIterator]
    buffer.init(0, Array(results))
    val processedResults = new ArrayBuffer[InternalRow]
    while (buffer.hasNext) {
      processedResults += buffer.next().copy()
    }
    processedResults.toArray
  }

  override protected def doExecute(): RDD[InternalRow] = {
    right.execute()
  }
}
