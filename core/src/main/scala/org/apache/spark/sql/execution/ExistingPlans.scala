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

import com.gemstone.gemfire.internal.cache.LocalRegion

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.errors.attachTree
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, _}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, HashPartitioning, Partitioning, SinglePartition}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.collection.{ToolsCallbackInit, Utils}
import org.apache.spark.sql.execution.columnar.impl.{BaseColumnFormatRelation, IndexColumnFormatRelation}
import org.apache.spark.sql.execution.columnar.{ColumnTableScan, ConnectionType}
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchange}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetricInfo, SQLMetrics}
import org.apache.spark.sql.execution.row.{RowFormatRelation, RowTableScan}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedUnsafeFilteredScan, SamplingRelation}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, CachedDataFrame, SnappySession}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}


/**
 * Physical plan node for scanning data from an DataSource scan RDD.
 * If user knows that the data is partitioned or replicated across
 * all nodes this SparkPla can be used to avoid expensive shuffle
 * and Broadcast joins. This plan overrides outputPartitioning and
 * make it inline with the partitioning of the underlying DataSource
 */
private[sql] abstract class PartitionedPhysicalScan(
    output: Seq[Attribute],
    dataRDD: RDD[Any],
    numBuckets: Int,
    partitionColumns: Seq[Expression],
    partitionColumnAliases: Seq[Seq[Attribute]],
    @transient override val relation: BaseRelation,
    // not used currently (if need to use then get from relation.table)
    override val metastoreTableIdentifier: Option[TableIdentifier] = None)
    extends DataSourceScanExec with CodegenSupportOnExecutor {

  def getMetrics: Map[String, SQLMetric] = {
    if (sqlContext eq null) Map.empty
    else Map("numOutputRows" -> SQLMetrics.createMetric(sparkContext,
      "number of output rows"))
  }

  override lazy val metrics: Map[String, SQLMetric] = getMetrics

  private lazy val extraInformation = if (relation != null) {
    relation.toString
  } else {
    "<extraInformation:NULL>"
  }

  protected lazy val numPartitions: Int = if (dataRDD != null) {
    dataRDD.getNumPartitions
  } else {
    -1
  }

  @transient val (metricAdd, metricValue): (String => String, String => String) =
    Utils.metricMethods

  // RDD cast as RDD[InternalRow] below just to satisfy interfaces like
  // inputRDDs though its actually of ColumnBatches, CompactExecRows, etc
  override val rdd: RDD[InternalRow] = dataRDD.asInstanceOf[RDD[InternalRow]]

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    rdd :: Nil
  }

  protected override def doExecute(): RDD[InternalRow] = {
    WholeStageCodegenExec(CachedPlanHelperExec(this)).execute()
  }

  /** Specifies how data is partitioned across different nodes in the cluster. */
  override lazy val outputPartitioning: Partitioning = {
    if (numPartitions == 1) {
      SinglePartition
    } else if (partitionColumns.nonEmpty) {
      val callbacks = ToolsCallbackInit.toolsCallback
      if (callbacks != null) {
        // when buckets are linked to partitions then numBuckets have
        // to be sent as zero to skip considering buckets in partitioning
        val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
        callbacks.getOrderlessHashPartitioning(partitionColumns,
          partitionColumnAliases, numPartitions,
          if (session.hasLinkPartitionsToBuckets) 0 else numBuckets)
      } else {
        HashPartitioning(partitionColumns, numPartitions)
      }
    } else super.outputPartitioning
  }

  override lazy val simpleString: String = "Partitioned Scan " + extraInformation +
      " , Requested Columns = " + output.mkString("[", ",", "]") +
      " partitionColumns = " + partitionColumns.mkString("[", ",", "]" +
      " numBuckets= " + numBuckets +
      " numPartitions= " + numPartitions)
}

private[sql] object PartitionedPhysicalScan {

  private[sql] val CT_BLOB_POSITION = 4

  def createFromDataSource(
      output: Seq[Attribute],
      numBuckets: Int,
      partitionColumns: Seq[Expression],
      partitionColumnAliases: Seq[Seq[Attribute]],
      rdd: RDD[Any],
      otherRDDs: Seq[RDD[InternalRow]],
      relation: PartitionedDataSourceScan,
      allFilters: Seq[Expression],
      schemaAttributes: Seq[AttributeReference],
      scanBuilderArgs: => (Seq[AttributeReference], Seq[Filter])): SparkPlan =
    relation match {
      case i: IndexColumnFormatRelation =>
        val columnScan = ColumnTableScan(output, rdd, otherRDDs, numBuckets,
          partitionColumns, partitionColumnAliases, relation, relation.schema,
          allFilters, schemaAttributes)
        val table = i.getBaseTableRelation
        val (a, f) = scanBuilderArgs
        val baseTableRDD = table.buildRowBufferRDD(() => Array.empty,
          a.map(_.name).toArray, f.toArray, useResultSet = false)

        def resolveCol(left: Attribute, right: AttributeReference) =
          columnScan.sqlContext.sessionState.analyzer.resolver(left.name, right.name)

        val rowBufferScan = RowTableScan(output, StructType.fromAttributes(
          output), baseTableRDD, numBuckets, Seq.empty, Seq.empty, table)
        val otherPartKeys = partitionColumns.map(_.transform {
          case a: AttributeReference => rowBufferScan.output.find(resolveCol(_, a)).getOrElse {
            throw new AnalysisException(s"RowBuffer output column $a not found in " +
                s"${rowBufferScan.output.mkString(",")}")
          }
        })
        assert(columnScan.outputPartitioning.satisfies(
          ClusteredDistribution(columnScan.partitionColumns)))
        ZipPartitionScan(columnScan, columnScan.partitionColumns,
          rowBufferScan, otherPartKeys)
      case _: BaseColumnFormatRelation =>
        ColumnTableScan(output, rdd, otherRDDs, numBuckets,
          partitionColumns, partitionColumnAliases, relation, relation.schema,
          allFilters, schemaAttributes)
      case r: SamplingRelation =>
        if (r.isReservoirAsRegion) {
          ColumnTableScan(output, rdd, Nil, numBuckets, partitionColumns,
            partitionColumnAliases, relation, relation.schema, allFilters,
            schemaAttributes, isForSampleReservoirAsRegion = true)
        } else {
          ColumnTableScan(output, rdd, otherRDDs, numBuckets,
            partitionColumns, partitionColumnAliases, relation, relation.schema,
            allFilters, schemaAttributes)
        }
      case _: RowFormatRelation =>
        RowTableScan(output, StructType.fromAttributes(output), rdd, numBuckets,
          partitionColumns, partitionColumnAliases, relation)
    }

  def getSparkPlanInfo(fullPlan: SparkPlan): SparkPlanInfo = {
    val plan = fullPlan match {
      case CodegenSparkFallback(CachedPlanHelperExec(child)) => child
      case CodegenSparkFallback(child) => child
      case CachedPlanHelperExec(child) => child
      case _ => fullPlan
    }
    val children = plan match {
      case ReusedExchangeExec(_, child) => child :: Nil
      case _ => plan.children ++ plan.subqueries
    }
    val metrics = plan.metrics.toSeq.map { case (key, metric) =>
      new SQLMetricInfo(metric.name.getOrElse(key), metric.id, metric.metricType)
    }

    new SparkPlanInfo(plan.nodeName, plan.simpleString,
      children.map(getSparkPlanInfo), plan.metadata, metrics)
  }
}

/**
 * A wrapper plan to immediately execute the child plan without having to do
 * an explicit collect. Only use for plans returning small results.
 */
case class ExecutePlan(child: SparkPlan, preAction: () => Unit = () => ())
    extends UnaryExecNode {

  override def output: Seq[Attribute] = child.output

  protected[sql] lazy val sideEffectResult: Array[InternalRow] = {
    preAction()
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    val (queryStringShortForm, queryString, planInfo) = session.currentKey match {
      case null =>
        val callSite = sqlContext.sparkContext.getCallSite()
        (callSite.shortForm, callSite.longForm,
            PartitionedPhysicalScan.getSparkPlanInfo(child))
      case key => (CachedDataFrame.queryStringShortForm(key.sqlText), key.sqlText,
          CachedDataFrame.queryPlanInfo(child, session.getAllLiterals(key)))
    }
    CachedDataFrame.withNewExecutionId(session, queryStringShortForm,
      queryString, child.treeString(verbose = true), planInfo) {
      child.executeCollect()
    }
  }

  override def executeCollect(): Array[InternalRow] = sideEffectResult

  override def executeTake(limit: Int): Array[InternalRow] =
    sideEffectResult.take(limit)

  override protected def doExecute(): RDD[InternalRow] = {
    sqlContext.sparkContext.parallelize(sideEffectResult, 1)
  }
}

trait PartitionedDataSourceScan extends PrunedUnsafeFilteredScan {

  def table: String

  def region: LocalRegion

  def schema: StructType

  def numBuckets: Int

  def partitionColumns: Seq[String]

  def connectionType: ConnectionType.Value
}

/** Combines two SparkPlan or one SparkPlan and another RDD and acts as a LeafExecNode for the
 * higher operators.  Typical usage is like combining additional plan or rdd with
 * ColumnTableScan without breaking WholeStageCodegen.
 *
 * @param basePlan      left plan that must be code generated.
 * @param basePartKeys  left partitioner expression
 * @param otherPlan     optional. otherRDD can be used instead of this.
 * @param otherPartKeys right partitioner expression
 */
private[sql] final case class ZipPartitionScan(basePlan: CodegenSupport,
    basePartKeys: Seq[Expression],
    otherPlan: SparkPlan,
    otherPartKeys: Seq[Expression]) extends LeafExecNode with CodegenSupport {

  private var consumedCode: String = _
  private val consumedVars: ArrayBuffer[ExprCode] = ArrayBuffer.empty
  private val inputCode = basePlan.asInstanceOf[CodegenSupport]

  private val withShuffle = ShuffleExchange(HashPartitioning(
    ClusteredDistribution(otherPartKeys)
        .clustering, inputCode.inputRDDs().head.getNumPartitions), otherPlan)

  override def children: Seq[SparkPlan] = basePlan :: withShuffle :: Nil

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(basePartKeys) :: ClusteredDistribution(otherPartKeys) :: Nil

  override def inputRDDs(): Seq[RDD[InternalRow]] =
    inputCode.inputRDDs ++ Some(withShuffle.execute())

  override protected def doProduce(ctx: CodegenContext): String = {
    val child1Produce = inputCode.produce(ctx, this)
    val input = ctx.freshName("input")
    ctx.addMutableState("scala.collection.Iterator", input, s" $input = inputs[1]; ")

    val row = ctx.freshName("row")
    val columnsInputEval = otherPlan.output.zipWithIndex.map { case (ref, ordinal) =>
      val baseIndex = ordinal
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
    WholeStageCodegenExec(CachedPlanHelperExec(this)).execute()
  }

  override def output: Seq[Attribute] = basePlan.output
}

class StratumInternalRow(val weight: Long) extends InternalRow {

  var actualRow: InternalRow = _

  def numFields: Int = throw new UnsupportedOperationException("not implemented")

  def getUTF8String(ordinal: Int): UTF8String = throw new UnsupportedOperationException("not " +
      "implemented")

  def copy(): InternalRow = throw new UnsupportedOperationException("not implemented")

  def anyNull: Boolean = throw new UnsupportedOperationException("not implemented")

  def isNullAt(ordinal: Int): Boolean = throw new UnsupportedOperationException("not implemented")

  def getBoolean(ordinal: Int): Boolean = throw new UnsupportedOperationException("not implemented")

  def getByte(ordinal: Int): Byte = throw new UnsupportedOperationException("not implemented")

  def getShort(ordinal: Int): Short = throw new UnsupportedOperationException("not implemented")

  def getInt(ordinal: Int): Int = throw new UnsupportedOperationException("not implemented")

  def getLong(ordinal: Int): Long = throw new UnsupportedOperationException("not implemented")

  def getFloat(ordinal: Int): Float = throw new UnsupportedOperationException("not implemented")

  def getDouble(ordinal: Int): Double = throw new UnsupportedOperationException("not implemented")

  def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal =
    throw new UnsupportedOperationException("not implemented")


  def getBinary(ordinal: Int): Array[Byte] =
    throw new UnsupportedOperationException("not implemented")

  def getInterval(ordinal: Int): CalendarInterval =
    throw new UnsupportedOperationException("not implemented")

  def getStruct(ordinal: Int, numFields: Int): InternalRow =
    throw new UnsupportedOperationException("not implemented")

  def getArray(ordinal: Int): ArrayData = throw new UnsupportedOperationException("not implemented")

  def getMap(ordinal: Int): MapData = throw new UnsupportedOperationException("not implemented")

  def get(ordinal: Int, dataType: DataType): Object =
    throw new UnsupportedOperationException("not implemented")
}

trait BatchConsumer extends CodegenSupport {

  /**
   * Returns true if the given plan returning batches of data can be consumed
   * by this plan.
   */
  def canConsume(plan: SparkPlan): Boolean

  /**
   * Generate Java source code to do any processing before a batch is consumed
   * by a [[DataSourceScanExec]] that does batch processing (e.g. per-batch
   * optimizations, initializations etc).
   * <p>
   * Implementations should use this for additional optimizations that can be
   * done at batch level when a batched scan is being done. They should not
   * depend on this being invoked since many scans will not be batched.
   */
  def batchConsume(ctx: CodegenContext, plan: SparkPlan,
      input: Seq[ExprCode]): String
}

/**
 * Extended information for ExprCode variable to also hold the variable having
 * dictionary reference and its index when dictionary encoding is being used.
 */
case class DictionaryCode(private var dictionaryCode: String,
    valueAssignCode: String, dictionary: String, dictionaryIndex: String,
    dictionaryLen: String) {

  def evaluateDictionaryCode(ev: ExprCode): String = {
    if (ev.code.isEmpty) ""
    else {
      val code = dictionaryCode
      dictionaryCode = ""
      code
    }
  }
}
