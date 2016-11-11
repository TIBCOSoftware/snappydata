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
package org.apache.spark.sql.execution

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, SinglePartition}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, _}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, SinglePartition}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.collection.ToolsCallbackInit
import org.apache.spark.sql.execution.columnar.impl.{BaseColumnFormatRelation, IndexColumnFormatRelation}
import org.apache.spark.sql.execution.columnar.{ColumnTableScan, ConnectionType}
import org.apache.spark.sql.execution.exchange.ShuffleExchange
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.row.RowFormatRelation
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedUnsafeFilteredScan, SamplingRelation}
import org.apache.spark.sql.types._


/** Physical plan node for scanning data from an DataSource scan RDD.
  * If user knows that the data is partitioned or replicated across
  * all nodes this SparkPla can be used to avoid expensive shuffle
  * and Broadcast joins. This plan overrides outputPartitioning and
  * make it inline with the partitioning of the underlying DataSource */
private[sql] abstract class PartitionedPhysicalScan(
    output: Seq[Attribute],
    dataRDD: RDD[Any],
    numBuckets: Int,
    partitionColumns: Seq[Expression],
    @transient override val relation: BaseRelation,
    // not used currently (if need to use then get from relation.table)
    override val metastoreTableIdentifier: Option[TableIdentifier] = None)
    extends DataSourceScanExec with CodegenSupport {

  def getMetrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override lazy val metrics: Map[String, SQLMetric] = getMetrics

  private val extraInformation = relation.toString

  protected lazy val numPartitions: Int = dataRDD.getNumPartitions

  override lazy val schema: StructType = StructType.fromAttributes(output)

  // RDD cast as RDD[InternalRow] below just to satisfy interfaces like
  // inputRDDs though its actually of CachedBatches, CompactExecRows, etc
  override val rdd: RDD[InternalRow] = dataRDD.asInstanceOf[RDD[InternalRow]]

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    rdd :: Nil
  }

  protected override def doExecute(): RDD[InternalRow] = {
    WholeStageCodegenExec(this).execute()
  }

  /** Specifies how data is partitioned across different nodes in the cluster. */
  override lazy val outputPartitioning: Partitioning = {
    if (numPartitions == 1) {
      SinglePartition
    } else if (partitionColumns.nonEmpty) {
      val callbacks = ToolsCallbackInit.toolsCallback
      if (callbacks != null) {
        callbacks.getOrderlessHashPartitioning(partitionColumns,
          numPartitions, numBuckets)
      } else {
        HashPartitioning(partitionColumns, numPartitions)
      }
    } else super.outputPartitioning
  }

  override def simpleString: String = "Partitioned Scan " + extraInformation +
      " , Requested Columns = " + output.mkString("[", ",", "]") +
      " partitionColumns = " + partitionColumns.mkString("[", ",", "]" +
      " numBuckets= " + numBuckets +
      " numPartitions= " + numPartitions)
}

private[sql] object PartitionedPhysicalScan {

  private[sql] val CT_NUMROWS_POSITION = 3
  private[sql] val CT_STATROW_POSITION = 4
  private[sql] val CT_COLUMN_START = 5

  def createFromDataSource(
      output: Seq[Attribute],
      numBuckets: Int,
      partitionColumns: Seq[Expression],
      rdd: RDD[Any],
      otherRDDs: Seq[RDD[InternalRow]],
      relation: PartitionedDataSourceScan,
      allFilters: Seq[Expression],
      schemaAttributes: Seq[AttributeReference],
      scanBuilderArgs: => (Seq[AttributeReference], Seq[Filter]) ): SparkPlan =
    relation match {
      case i: IndexColumnFormatRelation =>
        val columnScan = ColumnTableScan(output, rdd, otherRDDs, numBuckets,
          partitionColumns, relation, allFilters, schemaAttributes)
        val table = i.getBaseTableRelation
        val (a, f) = scanBuilderArgs
        val baseTableRDD = table.buildRowBufferRDD(Array.empty,
          a.map(_.name).toArray, f.toArray, false)
        val rowBufferScan = RowTableScan(output, baseTableRDD, numBuckets,
          Seq.empty, table)
        val bufferExchange = ShuffleExchange(columnScan.outputPartitioning,
          rowBufferScan)
        ZipPartitionScan(columnScan, bufferExchange)
      case r: BaseColumnFormatRelation =>
        ColumnTableScan(output, rdd, otherRDDs, numBuckets,
          partitionColumns, relation, allFilters, schemaAttributes)
      case r: SamplingRelation =>
        if (r.isPartitioned) {
          val columnScan = ColumnTableScan(output, rdd, Nil, numBuckets,
            partitionColumns, relation, allFilters, schemaAttributes)
          ZipPartitionScan(columnScan, null, otherRDDs.head)
        } else {
          ColumnTableScan(output, rdd, otherRDDs, numBuckets,
            partitionColumns, relation, allFilters, schemaAttributes)
        }
      case _: RowFormatRelation =>
        RowTableScan(output, rdd, numBuckets,
          partitionColumns, relation)
    }
}

trait PartitionedDataSourceScan extends PrunedUnsafeFilteredScan {

  def table: String

  def schema: StructType

  def numBuckets: Int

  def partitionColumns: Seq[String]

  def connectionType: ConnectionType.Value
}

private[sql] final case class ZipPartitionScan(basePlan: SparkPlan with CodegenSupport,
    otherPlan: SparkPlan, otherRDD: RDD[InternalRow] = null) extends LeafExecNode with
    CodegenSupport {

  private var consumedCode: String = _
  private val consumedVars: ArrayBuffer[ExprCode] = ArrayBuffer.empty
  private val inputCode = basePlan.asInstanceOf[CodegenSupport]

  override def inputRDDs(): Seq[RDD[InternalRow]] =
    inputCode.inputRDDs ++ Seq(Option(otherPlan).fold(otherRDD)(_.execute()))

  override protected def doProduce(ctx: CodegenContext): String = {
    val child1Produce = inputCode.produce(ctx, this)
    val input = ctx.freshName("input")
    ctx.addMutableState("scala.collection.Iterator", input, s" $input = inputs[1]; ")

    val row = ctx.freshName("row")
    val columnsInputEval = Option(otherPlan).getOrElse(basePlan).output.zipWithIndex.map {
      case (ref, ordinal) =>
        val baseIndex = basePlan.asInstanceOf[ColumnTableScan].baseRelation.schema.fieldIndex(
          ref.name)
      val ev = consumedVars(ordinal)
      val dataType = ref.dataType
      val javaType = ctx.javaType(dataType)
      val value = ctx.getValue(row, dataType, baseIndex.toString)
      if (ref.nullable) {
        s"""
            boolean ${ev.isNull} = $row.isNullAt($ordinal);
            $javaType ${ev.value} = ${ev.isNull} ? ${ctx.defaultValue(dataType)} : ($value);
            """
      } else {
        s"""$javaType ${ev.value} = $value;"""
      }
    }.mkString("\n")

    s"""
       |while ($input.hasNext()) {
       |  InternalRow $row = (InternalRow) $input.next();
       |  // numOutputRows.add(1);
       |  $columnsInputEval
       |  $consumedCode
       |  if (shouldStop()) return;
       |}
       |$child1Produce
     """.stripMargin
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val consumeInput = evaluateVariables(input)
    consumedCode = consume(ctx, input)
    consumedVars.clear()
    input.map(_.copy()).foreach(consumedVars += _)
    consumeInput + "\n" + consumedCode
  }

  override protected def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    WholeStageCodegenExec(this).execute()
  }

  override def output: Seq[Attribute] = basePlan.output
}

trait BatchConsumer extends CodegenSupport {

  /**
    * Generate Java source code to do any processing before a batch is consumed
    * by a [[DataSourceScanExec]] that does batch processing (e.g. per-batch
    * optimizations, initializations etc).
    * <p>
    * Implementations should use this for additional optimizations that can be
    * done at batch level when a batched scan is being done. They should not
    * depend on this being invoked since many scans will not be batched.
    */
  def batchConsume(ctx: CodegenContext, input: Seq[ExprCode]): String = ""
}

/**
  * Extended information for ExprCode to also hold the hashCode variable,
  * variable having dictionary reference and its index when dictionary
  * encoding is being used.
  */
final case class ExprCodeEx(var hash: Option[String], dictionaryCode: String,
    dictionary: String, dictionaryIndex: String)

