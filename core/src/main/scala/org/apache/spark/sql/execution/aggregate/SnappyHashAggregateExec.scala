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
/*
 * Some code adapted from Spark's HashAggregateExec having the license below.
 */
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

package org.apache.spark.sql.execution.aggregate


import java.nio.ByteBuffer

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl
import com.gemstone.gemfire.internal.shared.BufferAllocator
import io.snappydata.Property
import io.snappydata.collection.{ByteBufferData, ObjectHashSet, SHAMap}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.columnar.encoding.ColumnEncoding
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SnappySession, collection}
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * Hash-based aggregate operator that can also fallback to sorting when
 * data exceeds memory size.
 *
 * Parts of this class have been adapted from Spark's HashAggregateExec.
 * That class is not extended because it forces that the limitations
 * <code>HashAggregateExec.supportsAggregate</code> of that implementation in
 * the constructor itself while this implementation has no such restriction.
 */
case class SnappyHashAggregateExec(
    requiredChildDistributionExpressions: Option[Seq[Expression]],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    aggregateAttributes: Seq[Attribute],
    __resultExpressions: Seq[NamedExpression],
    child: SparkPlan,
    hasDistinct: Boolean)
    extends NonRecursivePlans with UnaryExecNode with BatchConsumer {


  val useByteBufferMapBasedAggregation: Boolean = {
    val useDictionaryOptimizationForSingleKey = (groupingExpressions.length == 1 &&
      groupingExpressions.exists(_.dataType match {
      case StringType => true
      case _ => false
    }) && !Property.TestUseBBMapInSHAFor1StringGrouBy.get(
      sqlContext.sparkSession.asInstanceOf[SnappySession].sessionState.conf))

    aggregateBufferAttributes.forall(attr =>
      TypeUtilities.isFixedWidth(attr.dataType)) &&
      !Property.TestDisableByteBufferMapInSHA.get(
        sqlContext.sparkSession.asInstanceOf[SnappySession].sessionState.conf) &&
      !groupingExpressions.isEmpty &&
      groupingExpressions.forall(_.dataType.
        existsRecursively(SHAMapAccessor.supportedDataTypes)) &&
      !useDictionaryOptimizationForSingleKey
  }



  override def nodeName: String = "SnappyHashAggregate"

  @transient def resultExpressions: Seq[NamedExpression] = __resultExpressions

  @transient lazy private[this] val aggregateBufferAttributes = {
    aggregateExpressions.flatMap(_.aggregateFunction.aggBufferAttributes)
  }

  @transient lazy private[this] val aggregateBufferAttributesForGroup = {
    aggregateExpressions.flatMap(a => bufferAttributesForGroup(
      a.aggregateFunction))
  }

  override lazy val allAttributes: AttributeSeq =
    child.output ++ aggregateBufferAttributes ++ aggregateAttributes ++
        aggregateExpressions.flatMap(_.aggregateFunction
            .inputAggBufferAttributes)

  @transient val (metricAdd, _): (String => String, String => String) =
    collection.Utils.metricMethods

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext,
      "number of output rows"),
    "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"),
    "aggTime" -> SQLMetrics.createTimingMetric(sparkContext, "aggregate time"))

  // this is a var to allow CollectAggregateExec to switch temporarily
  @transient private[execution] var childProducer = child

  private def getChildProducer: SparkPlan = {
    if (childProducer eq null) {
      childProducer = child
    }
    childProducer
  }

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def producedAttributes: AttributeSet =
    AttributeSet(aggregateAttributes) ++
        AttributeSet(resultExpressions.diff(groupingExpressions)
            .map(_.toAttribute)) ++
        AttributeSet(aggregateBufferAttributes)

  override def outputPartitioning: Partitioning = {
    child.outputPartitioning
  }

  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.isEmpty => AllTuples :: Nil
      case Some(exprs) if exprs.nonEmpty => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }

  /**
   * Optimized attributes for grouping that are non-nullable when the
   * children are. The case of no results for a group are handled by the fact
   * that if that happens then no row for the group will be present at all.
   */
  private def bufferAttributesForGroup(
      aggregate: AggregateFunction): Seq[AttributeReference] = aggregate match {
    case g: GroupAggregate => g.aggBufferAttributesForGroup
    case sum: Sum if !sum.child.nullable =>
      val sumAttr = sum.aggBufferAttributes.head
      sumAttr.copy(nullable = false)(sumAttr.exprId, sumAttr.qualifier,
        sumAttr.isGenerated) :: Nil
    case avg: Average if !avg.child.nullable =>
      val sumAttr = avg.aggBufferAttributes.head
      sumAttr.copy(nullable = false)(sumAttr.exprId, sumAttr.qualifier,
        sumAttr.isGenerated) :: avg.aggBufferAttributes(1) :: Nil
    case max: Max if !max.child.nullable =>
      val maxAttr = max.aggBufferAttributes.head
      maxAttr.copy(nullable = false)(maxAttr.exprId, maxAttr.qualifier,
        maxAttr.isGenerated) :: Nil
    case min: Min if !min.child.nullable =>
      val minAttr = min.aggBufferAttributes.head
      minAttr.copy(nullable = false)(minAttr.exprId, minAttr.qualifier,
        minAttr.isGenerated) :: Nil
    case last: Last if !last.child.nullable =>
      val lastAttr = last.aggBufferAttributes.head
      val tail = if (last.aggBufferAttributes.length == 2) {
        val valueSetAttr = last.aggBufferAttributes(1)
        valueSetAttr.copy(nullable = false)(valueSetAttr.exprId,
          valueSetAttr.qualifier, valueSetAttr.isGenerated) :: Nil
      } else Nil
      lastAttr.copy(nullable = false)(lastAttr.exprId, lastAttr.qualifier,
        lastAttr.isGenerated) :: tail
    case _ => aggregate.aggBufferAttributes
  }

  private def bufferInitialValuesForGroup(
      aggregate: AggregateFunction): Seq[Expression] = aggregate match {
    case g: GroupAggregate => g.initialValuesForGroup
    case sum: Sum if !sum.child.nullable => Seq(Cast(Literal(0), sum.dataType))
    case _ => aggregate.asInstanceOf[DeclarativeAggregate].initialValues
  }

  // This is for testing. We force TungstenAggregationIterator to fall back
  // to the unsafe row hash map and/or the sort-based aggregation once it has
  // processed a given number of input rows.
  private val testFallbackStartsAt: Option[(Int, Int)] = {
    sqlContext.getConf("spark.sql.TungstenAggregate.testFallbackStartsAt",
      null) match {
      case null | "" => None
      case fallbackStartsAt =>
        val splits = fallbackStartsAt.split(",").map(_.trim)
        Some((splits.head.toInt, splits.last.toInt))
    }
  }

  // all the mode of aggregate expressions
  private val modes = aggregateExpressions.map(_.mode).distinct

  // return empty for grouping case as code of required variables
  // is explicitly instantiated for that case
  override def usedInputs: AttributeSet = {
    if (groupingExpressions.isEmpty) inputSet else AttributeSet.empty
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }



  override protected def doProduce(ctx: CodegenContext): String = {
    if (groupingExpressions.isEmpty) {
      doProduceWithoutKeys(ctx)
    } else {
      if (useByteBufferMapBasedAggregation) {
        doProduceWithKeysForSHAMap(ctx)
      } else {
        doProduceWithKeys(ctx)
      }
    }
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode],
      row: ExprCode): String = {
    if (groupingExpressions.isEmpty) {
      doConsumeWithoutKeys(ctx, input)
    } else {
      if (useByteBufferMapBasedAggregation) {
        doConsumeWithKeysForSHAMap(ctx, input)
      } else {
        doConsumeWithKeys(ctx, input)
      }
    }
  }

  override def canConsume(plan: SparkPlan): Boolean = {
    if (groupingExpressions.isEmpty) return true // for beforeStop()
    // check for possible optimized dictionary code path;
    // below is a loose search while actual decision will be taken as per
    // availability of ExprCodeEx with DictionaryCode in doConsume
   if (useByteBufferMapBasedAggregation) {
     false
   } else {
     DictionaryOptimizedMapAccessor.canHaveSingleKeyCase(
       keyBufferAccessor.keyExpressions)
   }
  }

  override def batchConsume(ctx: CodegenContext, plan: SparkPlan,
      input: Seq[ExprCode]): String = {
    if (groupingExpressions.isEmpty || !canConsume(plan) ) ""
    else {
      // create an empty method to populate the dictionary array
      // which will be actually filled with code in consume if the dictionary
      // optimization is possible using the incoming DictionaryCode
      val className = keyBufferAccessor.getClassName
      // this array will be used at batch level for grouping if possible
      dictionaryArrayTerm = ctx.freshName("dictionaryArray")
      dictionaryArrayInit = ctx.freshName("dictionaryArrayInit")
      ctx.addNewFunction(dictionaryArrayInit,
        s"""
           |private $className[] $dictionaryArrayInit() {
           |  return null;
           |}
         """.stripMargin)
      s"final $className[] $dictionaryArrayTerm = $dictionaryArrayInit();"
    }
  }

  override def beforeStop(ctx: CodegenContext, plan: SparkPlan,
      input: Seq[ExprCode]): String = {
    if (bufVars eq null) ""
    else {
      bufVarUpdates = bufVars.indices.map { i =>
        val ev = bufVars(i)
        s"""
           |// update the member result variables from local variables
           |this.${ev.isNull} = ${ev.isNull};
           |this.${ev.value} = ${ev.value};
        """.stripMargin
      }.mkString("\n").trim
      bufVarUpdates
    }
  }

  // The variables used as aggregation buffer
  @transient protected var bufVars: Seq[ExprCode] = _
  // code to update buffer variables with current values
  @transient protected var bufVarUpdates: String = _

  private def doProduceWithoutKeys(ctx: CodegenContext): String = {
    val initAgg = ctx.freshName("initAgg")
    ctx.addMutableState("boolean", initAgg, s"$initAgg = false;")

    // generate variables for aggregation buffer
    val functions = aggregateExpressions.map(_.aggregateFunction
        .asInstanceOf[DeclarativeAggregate])
    val initExpr = functions.flatMap(f => f.initialValues)
    ctx.INPUT_ROW = null
    ctx.currentVars = null
    bufVars = initExpr.map { e =>
      val isNull = ctx.freshName("bufIsNull")
      val value = ctx.freshName("bufValue")
      ctx.addMutableState("boolean", isNull, "")
      ctx.addMutableState(ctx.javaType(e.dataType), value, "")
      // The initial expression should not access any column
      val ev = e.genCode(ctx)
      val initVars =
        s"""
           | $isNull = ${ev.isNull};
           | $value = ${ev.value};
        """.stripMargin
      ExprCode(ev.code + initVars, isNull, value)
    }
    var initBufVar = evaluateVariables(bufVars)

    // generate variables for output
    val (resultVars, genResult) = if (modes.contains(Final) ||
        modes.contains(Complete)) {
      // evaluate aggregate results
      ctx.currentVars = bufVars
      val aggResults = functions.map(_.evaluateExpression).map { e =>
        BindReferences.bindReference(e, aggregateBufferAttributes).genCode(ctx)
      }
      val evaluateAggResults = evaluateVariables(aggResults)
      // evaluate result expressions
      ctx.currentVars = aggResults
      val resultVars = resultExpressions.map { e =>
        BindReferences.bindReference(e, aggregateAttributes).genCode(ctx)
      }
      (resultVars,
          s"""
             |$evaluateAggResults
             |${evaluateVariables(resultVars)}
          """.stripMargin)
    } else if (modes.contains(Partial) || modes.contains(PartialMerge)) {
      // output the aggregate buffer directly
      (bufVars, "")
    } else {
      // no aggregate function, the result should be literals
      val resultVars = resultExpressions.map(_.genCode(ctx))
      (resultVars, evaluateVariables(resultVars))
    }

    val doAgg = ctx.freshName("doAggregateWithoutKey")
    var produceOutput = getChildProducer.asInstanceOf[CodegenSupport].produce(
      ctx, this)
    if (bufVarUpdates ne null) {
      // use local variables while member variables are updated at the end
      initBufVar = bufVars.indices.map { i =>
        val ev = bufVars(i)
        s"""
           |boolean ${ev.isNull} = this.${ev.isNull};
           |${ctx.javaType(initExpr(i).dataType)} ${ev.value} = this.${ev.value};
        """.stripMargin
      }.mkString("", "\n", initBufVar).trim
      produceOutput = s"$produceOutput\n$bufVarUpdates"
    }
    ctx.addNewFunction(doAgg,
      s"""
         |private void $doAgg() throws java.io.IOException {
         |  // initialize aggregation buffer
         |  $initBufVar
         |
         |  $produceOutput
         |}
       """.stripMargin)

    val numOutput = metricTerm(ctx, "numOutputRows")
    val aggTime = metricTerm(ctx, "aggTime")
    val beforeAgg = ctx.freshName("beforeAgg")
    s"""
       |while (!$initAgg) {
       |  $initAgg = true;
       |  long $beforeAgg = System.nanoTime();
       |  $doAgg();
       |  $aggTime.${metricAdd(s"(System.nanoTime() - $beforeAgg) / 1000000")};
       |
       |  // output the result
       |  ${genResult.trim}
       |
       |  $numOutput.${metricAdd("1")};
       |  ${consume(ctx, resultVars).trim}
       |}
     """.stripMargin
  }

  protected def genAssignCodeForWithoutKeys(ctx: CodegenContext, ev: ExprCode, i: Int,
      doCopy: Boolean, inputAttrs: Seq[Attribute]): String = {
    if (doCopy) {
      inputAttrs(i).dataType match {
        case StringType =>
          ObjectHashMapAccessor.cloneStringIfRequired(ev.value, bufVars(i).value, doCopy = true)
        case d@(_: ArrayType | _: MapType | _: StructType) =>
          val javaType = ctx.javaType(d)
          s"${bufVars(i).value} = ($javaType)(${ev.value} != null ? ${ev.value}.copy() : null);"
        case _: BinaryType =>
          s"${bufVars(i).value} = (byte[])(${ev.value} != null ? ${ev.value}.clone() : null);"
        case _ => s"${bufVars(i).value} = ${ev.value};"
      }
    } else s"${bufVars(i).value} = ${ev.value};"
  }

  private def doConsumeWithoutKeys(ctx: CodegenContext,
      input: Seq[ExprCode]): String = {
    // only have DeclarativeAggregate
    val functions = aggregateExpressions.map(_.aggregateFunction
        .asInstanceOf[DeclarativeAggregate])
    val inputAttrs = functions.flatMap(_.aggBufferAttributes) ++ child.output
    val updateExpr = aggregateExpressions.flatMap { e =>
      val aggregate = e.aggregateFunction.asInstanceOf[DeclarativeAggregate]
      e.mode match {
        case Partial | Complete => aggregate.updateExpressions
        case PartialMerge | Final => aggregate.mergeExpressions
      }
    }
    ctx.INPUT_ROW = null
    ctx.currentVars = bufVars ++ input
    val boundUpdateExpr = updateExpr.map(BindReferences.bindReference(_,
      inputAttrs))
    val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(
      boundUpdateExpr)
    val effectiveCodes = subExprs.codes.mkString("\n")
    val aggVals = ctx.withSubExprEliminationExprs(subExprs.states) {
      boundUpdateExpr.map(_.genCode(ctx))
    }
    // aggregate buffer should be updated atomic
    // make copy of results for types that can be wrappers and thus mutable
    val doCopy = !ObjectHashMapAccessor.providesImmutableObjects(child)
    val updates = aggVals.zipWithIndex.map { case (ev, i) =>
      s"""
         | ${bufVars(i).isNull} = ${ev.isNull};
         | ${genAssignCodeForWithoutKeys(ctx, ev, i, doCopy, inputAttrs)}
      """.stripMargin
    }
    s"""
       | // do aggregate
       | // common sub-expressions
       | $effectiveCodes
       | // evaluate aggregate functions
       | ${evaluateVariables(aggVals)}
       | // update aggregation buffer
       | ${updates.mkString("\n").trim}
    """.stripMargin
  }

  private val groupingAttributes = groupingExpressions.map(_.toAttribute)
  private val declFunctions = aggregateExpressions.map(_.aggregateFunction)
      .collect { case d: DeclarativeAggregate => d }

  // The name for ObjectHashSet of generated class.
  private var hashMapTerm: String = _

  // utility to generate class for optimized map, and hash map access methods
  @transient private var keyBufferAccessor: ObjectHashMapAccessor = _
  @transient private var byteBufferAccessor: SHAMapAccessor = _
  @transient private var mapDataTerm: String = _
  @transient private var maskTerm: String = _
  @transient private var dictionaryArrayTerm: String = _
  @transient private var dictionaryArrayInit: String = _

  /**
   * Generate the code for output.
   */
  private def generateResultCode(
      ctx: CodegenContext, keyBufferVars: Seq[ExprCode],
      bufferIndex: Int): String = {
    if (modes.contains(Final) || modes.contains(Complete)) {
      // generate output extracting from ExprCodes
      ctx.INPUT_ROW = null
      ctx.currentVars = keyBufferVars.take(bufferIndex)
      val keyVars = groupingExpressions.zipWithIndex.map {
        case (e, i) => BoundReference(i, e.dataType, e.nullable).genCode(ctx)
      }
      val evaluateKeyVars = evaluateVariables(keyVars)
      ctx.currentVars = keyBufferVars.drop(bufferIndex)
      val bufferVars = aggregateBufferAttributesForGroup.zipWithIndex.map {
        case (e, i) => BoundReference(i, e.dataType, e.nullable).genCode(ctx)
      }
      val evaluateBufferVars = evaluateVariables(bufferVars)
      // evaluate the aggregation result
      ctx.currentVars = bufferVars
      val aggResults = declFunctions.map(_.evaluateExpression).map { e =>
        BindReferences.bindReference(e, aggregateBufferAttributesForGroup)
            .genCode(ctx)
      }
      val evaluateAggResults = evaluateVariables(aggResults)
      // generate the final result
      ctx.currentVars = keyVars ++ aggResults
      val inputAttrs = groupingAttributes ++ aggregateAttributes
      val resultVars = resultExpressions.map { e =>
        BindReferences.bindReference(e, inputAttrs).genCode(ctx)
      }
      s"""
       $evaluateKeyVars
       $evaluateBufferVars
       $evaluateAggResults
       ${consume(ctx, resultVars)}
       """

    } else if (modes.contains(Partial) || modes.contains(PartialMerge)) {
      // Combined grouping keys and aggregate values in buffer
      consume(ctx, keyBufferVars)

    } else {
      // generate result based on grouping key
      ctx.INPUT_ROW = null
      ctx.currentVars = keyBufferVars.take(bufferIndex)
      val eval = resultExpressions.map { e =>
        BindReferences.bindReference(e, groupingAttributes).genCode(ctx)
      }
      consume(ctx, eval)
    }
  }

  private def generateResultCodeForSHAMap(
    ctx: CodegenContext, keyBufferVars: Seq[ExprCode],
    aggBufferVars: Seq[ExprCode], iterValueOffsetTerm: String): String = {
    /* Asif: It appears that we have to put the code of materilization of each grouping column
    & aggreagte before we can send it to parent. The reason is following:
    1) In the byte buffer hashmap data is written consecitively i.e key1, key2 agg1 etc.
    Now the pointer cannot jump arbitrarily to just read key2 without reading key1
     So suppose we have a nested query such that inner query produces code for outputting key1 , key2,
     while outer query is going to use only key2. If we do not write the code of materialzing key1,
     the pointer will not move forward, as the outer query is going to try to materialzie only key2,
     but the pointer will not move to key2 unleass key1 has been consumed.
     We need to resolve this issue. I suppose we can declare  local variable pointers pointing to start location
     of each key/aggregate & use those declared pointers in the materialization code for each key
     */
    if (modes.contains(Final) || modes.contains(Complete)) {
      // generate output extracting from ExprCodes
      ctx.INPUT_ROW = null
      ctx.currentVars = keyBufferVars
      val keyVars = groupingExpressions.zipWithIndex.map {
        case (e, i) => BoundReference(i, e.dataType, e.nullable).genCode(ctx)
      }
      val evaluateKeyVars = evaluateVariables(keyVars)
      ctx.currentVars = aggBufferVars
      val bufferVars = aggregateBufferAttributesForGroup.zipWithIndex.map {
        case (e, i) => BoundReference(i, e.dataType, e.nullable).genCode(ctx)
      }
      val evaluateBufferVars = evaluateVariables(bufferVars)
      // evaluate the aggregation result
      ctx.currentVars = bufferVars
      val aggResults = declFunctions.map(_.evaluateExpression).map { e =>
        BindReferences.bindReference(e, aggregateBufferAttributesForGroup)
          .genCode(ctx)
      }
      val evaluateAggResults = evaluateVariables(aggResults)
      // generate the final result
      ctx.currentVars = keyVars ++ aggResults
      val inputAttrs = groupingAttributes ++ aggregateAttributes
      val resultVars = resultExpressions.map { e =>
        BindReferences.bindReference(e, inputAttrs).genCode(ctx)
      }
      s"""
       ${byteBufferAccessor.readNullBitsCode(iterValueOffsetTerm,
        byteBufferAccessor.nullKeysBitsetTerm, byteBufferAccessor.numBytesForNullKeyBits)}
       $evaluateKeyVars
       ${byteBufferAccessor.readNullBitsCode(iterValueOffsetTerm,
        byteBufferAccessor.nullAggsBitsetTerm, byteBufferAccessor.numBytesForNullAggBits)}
       $evaluateBufferVars
       $evaluateAggResults
       ${consume(ctx, resultVars)}
       """

    } else if (modes.contains(Partial) || modes.contains(PartialMerge)) {
      // Combined grouping keys and aggregate values in buffer
      ctx.INPUT_ROW = null
      ctx.currentVars = keyBufferVars
      val keyVars = groupingExpressions.zipWithIndex.map {
        case (e, i) => BoundReference(i, e.dataType, e.nullable).genCode(ctx)
      }
      val evaluateKeyVars = evaluateVariables(keyVars)
      ctx.currentVars = aggBufferVars
      val bufferVars = aggregateBufferAttributesForGroup.zipWithIndex.map {
        case (e, i) => BoundReference(i, e.dataType, e.nullable).genCode(ctx)
      }
      val evaluateBufferVars = evaluateVariables(bufferVars)
      s"""
       ${byteBufferAccessor.readNullBitsCode(iterValueOffsetTerm,
        byteBufferAccessor.nullKeysBitsetTerm, byteBufferAccessor.numBytesForNullKeyBits)}
       $evaluateKeyVars
       ${byteBufferAccessor.readNullBitsCode(iterValueOffsetTerm,
        byteBufferAccessor.nullAggsBitsetTerm, byteBufferAccessor.numBytesForNullAggBits)}
       $evaluateBufferVars
       ${consume(ctx, keyBufferVars ++ aggBufferVars)}
       """


    } else {
      // generate result based on grouping key
      ctx.INPUT_ROW = null
      ctx.currentVars = keyBufferVars
      val keyVars = groupingExpressions.zipWithIndex.map {
        case (e, i) => BoundReference(i, e.dataType, e.nullable).genCode(ctx)
      }
      val evaluateKeyVars = evaluateVariables(keyVars)
      ctx.currentVars = keyVars
      val resultVars = resultExpressions.map { e =>
        BindReferences.bindReference(e, groupingAttributes).genCode(ctx)
      }

      s"""
       ${byteBufferAccessor.readNullBitsCode(iterValueOffsetTerm,
        byteBufferAccessor.nullKeysBitsetTerm, byteBufferAccessor.numBytesForNullKeyBits)}
       $evaluateKeyVars
       ${consume(ctx, resultVars)}
       """
    }
  }

  private def doProduceWithKeysForSHAMap(ctx: CodegenContext): String = {
    val initAgg = ctx.freshName("initAgg")
    ctx.addMutableState("boolean", initAgg, s"$initAgg = false;")
    // Create a name for iterator from HashMap

    val endIterValueOffset = ctx.freshName("endIterValueOffset")
    val localIterValueOffsetTerm = ctx.freshName("localIterValueOffsetTerm")
    val localIterValueStartOffsetTerm = ctx.freshName("localIterValueStartOffsetTerm")
    val iterValueOffsetTerm = ctx.freshName("iterValueOffsetTerm")
    ctx.addMutableState("long", iterValueOffsetTerm, s"$iterValueOffsetTerm = 0;")

    val nullKeysBitsetTerm = ctx.freshName("nullKeysBitset")
    val nullAggsBitsetTerm = ctx.freshName("nullAggsBitset")

    val numBytesForNullKeyBits = if (this.groupingAttributes.forall(!_.nullable)) {
      0
    } else {
      SHAMapAccessor.calculateNumberOfBytesForNullBits(this.groupingAttributes.length)
    }

    val numBytesForNullAggsBits = SHAMapAccessor.calculateNumberOfBytesForNullBits(
      this.aggregateBufferAttributesForGroup.length)

    if (SHAMapAccessor.isByteArrayNeededForNullBits(numBytesForNullKeyBits)) {
      ctx.addMutableState("byte[]", nullKeysBitsetTerm,
        s"$nullKeysBitsetTerm = new byte[$numBytesForNullKeyBits];")
    }

    if (SHAMapAccessor.isByteArrayNeededForNullBits(numBytesForNullAggsBits)) {
      ctx.addMutableState("byte[]", nullAggsBitsetTerm,
        s"$nullKeysBitsetTerm = new byte[$numBytesForNullAggsBits];")
    }
    val probableSkipLen = this.groupingAttributes.
      lastIndexWhere(attr => !TypeUtilities.isFixedWidth(attr.dataType))

    val skipLenForAttrib = if (probableSkipLen != - 1 &&
      this.groupingAttributes(probableSkipLen).dataType == StringType) {
      probableSkipLen
    } else -1

    val keyLengthTerm = ctx.freshName("keyLength")

    val utf8Class = classOf[UTF8String].getName
    val bbDataClass = classOf[ByteBufferData].getName
    val arrayDataClass = classOf[ArrayData].getName
    val platformClass = classOf[Platform].getName

    val sizeAndNumNotNullFuncForStringArr = ctx.freshName("calculateStringArrSizeAndNumNotNulls")

    if (groupingAttributes.exists(attrib => attrib.dataType.existsRecursively(_ match {
      case ArrayType(StringType, _) | ArrayType(_, true) => true
      case _ => false
    }))) {
      ctx.addNewFunction(sizeAndNumNotNullFuncForStringArr,
        s"""
        private long $sizeAndNumNotNullFuncForStringArr($arrayDataClass arrayData,
         boolean isStringData)  {
           long size = 0L;
           int numNulls = 0;
           for(int i = 0; i < arrayData.numElements(); ++i) {
             if (!arrayData.isNullAt(i)) {
               if (isStringData) {
                 $utf8Class o = arrayData.getUTF8String(i);
                 size += o.numBytes() + 4;
               }
             } else {
               ++numNulls;
             }
           }
           return  (size << 32L) | ((arrayData.numElements() - numNulls) & 0xffffffffL);
        }
       """)
    }



    val valueOffsetTerm = ctx.freshName("valueOffset")
    val currentValueOffSetTerm = ctx.freshName("currentValueOffSet")
    val valueDataTerm = ctx.freshName("valueData")
    val vdBaseObjectTerm = ctx.freshName("vdBaseObjectTerm")
    val vdBaseOffsetTerm = ctx.freshName("vdBaseOffsetTerm")
    val valueDataCapacityTerm = ctx.freshName("valueDataCapacity")
    val doAgg = ctx.freshName("doAggregateWithKeys")
    val setBBMap = ctx.freshName("setBBMap")

    // generate variable name for hash map for use here and in consume
    hashMapTerm = ctx.freshName("hashMap")
    val hashSetClassName = classOf[SHAMap].getName

    val  overflowHashMapsTerm = ctx.freshName("overflowHashMaps")
    val listClassName = classOf[java.util.List[SHAMap]].getName
    val overflowMapIter = ctx.freshName("overflowMapIter")
    val iterClassName = classOf[java.util.Iterator[SHAMap]].getName
    // generate variable names for holding data from the Map buffer
    val aggregateBufferVars = for (i <- this.aggregateBufferAttributesForGroup.indices) yield {
      ctx.freshName(s"buffer_$i")
    }

    val KeyBufferVars = for (i <- groupingExpressions.indices) yield {
      ctx.freshName(s"key_$i")
    }

    val keysDataType = this.groupingAttributes.map(_.dataType)
    // declare nullbitset terms for nested structs if required
    val nestedStructNullBitsTermCreator: ((String, StructType, Int) => Any) => (String, StructType, Int) => Any =
      (f: (String, StructType, Int) => Any) =>
        (structVarName: String, structType: StructType, nestingLevel: Int) => {
          val numBytesForNullBits = SHAMapAccessor.
            calculateNumberOfBytesForNullBits(structType.length)
          if (SHAMapAccessor.isByteArrayNeededForNullBits(numBytesForNullBits)) {
            val nullBitTerm = SHAMapAccessor.
              generateNullKeysBitTermForStruct(structVarName)
            ctx.addMutableState("byte[]", nullBitTerm,
              s"$nullBitTerm = new byte[$numBytesForNullBits];")
          }
          structType.zipWithIndex.foreach { case (sf, index) => sf.dataType match {
            case stt: StructType => val structtVarName = SHAMapAccessor.
              generateExplodedStructFieldVars(structVarName, nestingLevel + 1, index)._1
              f(structtVarName, stt, nestingLevel + 1)
              null
            case _ => null
          }

          }
        }
    val nestedStructNullBitsTermInitializer: ((String, StructType, Int) => Any) =>
      (String, StructType, Int) => Any =
      (f: (String, StructType, Int) => Any) =>
        (structVarName: String, structType: StructType, nestingLevel: Int) => {
          val numBytesForNullBits = SHAMapAccessor.
            calculateNumberOfBytesForNullBits(structType.length)
          val nullBitTerm = SHAMapAccessor.
            generateNullKeysBitTermForStruct(structVarName)
          val snippet1 = SHAMapAccessor.initNullBitsetCode(nullBitTerm, numBytesForNullBits)

          val snippet2 = structType.zipWithIndex.map { case (sf, index) => sf.dataType match {
            case stt: StructType => val structtVarName = SHAMapAccessor.
              generateVarNameForStructField(structVarName, nestingLevel , index)
              f(structtVarName, stt, nestingLevel + 1).toString
            case _ => ""
          }
          }.mkString("\n")
          s"""
             ${snippet1}
             $snippet2
           """.stripMargin
        }

    def recursiveApply(f:
    ((String, StructType, Int) => Any) => (String, StructType, Int) => Any):
    (String, StructType, Int) => Any = f(recursiveApply(f))(_, _, _)

    // Now create nullBitTerms
    KeyBufferVars.zip(keysDataType).foreach {
      case (varName, dataType) => dataType match {
        case st: StructType => recursiveApply(
          nestedStructNullBitsTermCreator)(varName, st, 0)
        case _ =>
      }
    }

    val aggBuffDataTypes = this.aggregateBufferAttributesForGroup.map(_.dataType)
    val allocatorTerm = ctx.freshName("bufferAllocator")
    val allocatorClass = classOf[BufferAllocator].getName
    val gfeCacheImplClass = classOf[GemFireCacheImpl].getName
    val byteBufferClass = classOf[ByteBuffer].getName

    val keyBytesHolderVar = ctx.freshName("keyBytesHolder")
    val baseKeyHolderOffset = ctx.freshName("baseKeyHolderOffset")
    val baseKeyObject = ctx.freshName("baseKeyHolderObject")
    val keyHolderCapacityTerm = ctx.freshName("keyholderCapacity")
    val keyExistedTerm = ctx.freshName("keyExisted")

    val codeForLenOfSkippedTerm = if (skipLenForAttrib != -1) {
      val numToDrop = skipLenForAttrib + 1
      val keysToProcessSize = this.groupingAttributes.drop(numToDrop)
      val suffixSize = if (numBytesForNullKeyBits == 0) {
        keysToProcessSize.foldLeft(0) {
          case (size, attrib) => size + attrib.dataType.defaultSize + (attrib.dataType match {
            case dec: DecimalType if (dec.precision > Decimal.MAX_LONG_DIGITS) => 1
            case _ => 0
          })
        }.toString
      } else {
        keysToProcessSize.zipWithIndex.map {
          case(attrib, i) => {
            val sizeTerm = attrib.dataType.defaultSize + (attrib.dataType match {
              case dec: DecimalType if (dec.precision > Decimal.MAX_LONG_DIGITS) => 1
              case _ => 0
            })
            s"""(int)(${SHAMapAccessor.getExpressionForNullEvalFromMask(i + numToDrop,
              numBytesForNullKeyBits, nullKeysBitsetTerm)} ? 0 : $sizeTerm)
            """
          }
        }.mkString("+")
      }

      s"""$keyLengthTerm -
         |(int)($localIterValueOffsetTerm - $localIterValueStartOffsetTerm)
         |${ if (keysToProcessSize.length > 0) s" - ($suffixSize)" else ""};""".stripMargin
    } else ""


    ctx.addMutableState(hashSetClassName, hashMapTerm, s"$hashMapTerm = null;")
    ctx.addMutableState(listClassName + s"<$hashSetClassName>", overflowHashMapsTerm,
      s"$overflowHashMapsTerm = null;")
    ctx.addMutableState(iterClassName + s"<$hashSetClassName>", overflowMapIter,
      s"$overflowMapIter = null;")

    val storedAggNullBitsTerm = ctx.freshName("storedAggNullBit")
    val cacheStoredAggNullBits = !SHAMapAccessor.isByteArrayNeededForNullBits(
      numBytesForNullAggsBits) && numBytesForNullAggsBits > 0

    val storedKeyNullBitsTerm = ctx.freshName("storedKeyNullBit")
    val cacheStoredKeyNullBits = !SHAMapAccessor.isByteArrayNeededForNullBits(
      numBytesForNullKeyBits) && numBytesForNullKeyBits > 0

    // generate the map accessor to generate key/value class
    // and get map access methods
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    val numKeyBytesTerm = ctx.freshName("numKeyBytes")
    val numValueBytes = SHAMapAccessor.getSizeOfValueBytes(aggBuffDataTypes,
      numBytesForNullAggsBits)

    val keyValSize = groupingAttributes.foldLeft(0)((len, attrib) =>
      len + attrib.dataType.defaultSize +
        (if (TypeUtilities.isFixedWidth(attrib.dataType)) 0 else 4))  +
      + SHAMapAccessor.sizeForNullBits(numBytesForNullKeyBits)  + numValueBytes
    -  (if (skipLenForAttrib != -1) 4 else 0)

    byteBufferAccessor = SHAMapAccessor(session, ctx, groupingExpressions,
      aggregateBufferAttributesForGroup, "ByteBuffer", hashMapTerm, overflowHashMapsTerm,
      keyValSize, valueOffsetTerm, numKeyBytesTerm, numValueBytes,
      currentValueOffSetTerm, valueDataTerm, vdBaseObjectTerm, vdBaseOffsetTerm,
      nullKeysBitsetTerm, numBytesForNullKeyBits, allocatorTerm,
      numBytesForNullAggsBits, nullAggsBitsetTerm, sizeAndNumNotNullFuncForStringArr,
      keyBytesHolderVar, baseKeyObject, baseKeyHolderOffset, keyExistedTerm,
      skipLenForAttrib, codeForLenOfSkippedTerm, valueDataCapacityTerm,
      if (cacheStoredAggNullBits) Some(storedAggNullBitsTerm) else None,
      if (cacheStoredKeyNullBits) Some(storedKeyNullBitsTerm) else None,
      aggregateBufferVars, keyHolderCapacityTerm)





    val maxMemory = ctx.freshName("maxMemory")
    val peakMemory = metricTerm(ctx, "peakMemory")

    val childProduce =
      childProducer.asInstanceOf[CodegenSupport].produce(ctx, this)
    ctx.addNewFunction(doAgg,
      s"""private void $doAgg() throws java.io.IOException {
           |$hashMapTerm = new $hashSetClassName(${Property.initialCapacityOfSHABBMap.get(
        sqlContext.sparkSession.asInstanceOf[SnappySession].sessionState.conf)},
        $keyValSize, ${Property.ApproxMaxCapacityOfBBMap.get(sqlContext.sparkSession.
        asInstanceOf[SnappySession].sessionState.conf)});
           |$allocatorClass $allocatorTerm = $gfeCacheImplClass.
           |getCurrentBufferAllocator();
           |$byteBufferClass $keyBytesHolderVar = null;
           |int $keyHolderCapacityTerm = 0;
           |Object $baseKeyObject = null;
           |long $baseKeyHolderOffset = -1L;
           |$bbDataClass $valueDataTerm = $hashMapTerm.getValueData();
           |Object $vdBaseObjectTerm = $valueDataTerm.baseObject();
           |long $vdBaseOffsetTerm = $valueDataTerm.baseOffset();
           |int $valueDataCapacityTerm = $valueDataTerm.capacity();
           |${SHAMapAccessor.initNullBitsetCode(nullKeysBitsetTerm, numBytesForNullKeyBits)}
           |${SHAMapAccessor.initNullBitsetCode(nullAggsBitsetTerm, numBytesForNullAggsBits)}
           |${byteBufferAccessor.initKeyOrBufferVal(aggBuffDataTypes, aggregateBufferVars)}
           |${byteBufferAccessor.declareNullVarsForAggBuffer(aggregateBufferVars)}
           |${ if (cacheStoredAggNullBits) {
                 SHAMapAccessor.initNullBitsetCode(storedAggNullBitsTerm, numBytesForNullAggsBits)
               } else ""
            }
           |${ if (cacheStoredKeyNullBits) {
                 SHAMapAccessor.initNullBitsetCode(storedKeyNullBitsTerm, numBytesForNullKeyBits)
               } else ""
            }
           |int $numKeyBytesTerm = 0;
           |$childProduce
           |long $maxMemory = $hashMapTerm.maxMemory();
           |$peakMemory.add($maxMemory);
           |if ($hashMapTerm.taskContext() != null) {
           |  $hashMapTerm.taskContext().taskMetrics().incPeakExecutionMemory($maxMemory);
           |}
         |}""".stripMargin)

    ctx.addNewFunction(setBBMap,
      s"""private boolean $setBBMap()  {
         |  if ($overflowHashMapsTerm == null) {
         |    return $hashMapTerm != null;
            } else {
              if ($hashMapTerm != null) {
                 return true;
              } else {
                 if ($overflowMapIter.hasNext()) {
                   $hashMapTerm = ($hashSetClassName)$overflowMapIter.next();
                   $bbDataClass  $valueDataTerm = $hashMapTerm.getValueData();
                   Object $vdBaseObjectTerm = $valueDataTerm.baseObject();
                   $iterValueOffsetTerm = $valueDataTerm.baseOffset();
                   return true;
                 } else {
                   return false;
                 }
              }
            }
         |}""".stripMargin)

    // generate code for output
    /*  val keyBufferTerm = ctx.freshName("keyBuffer")
      val (initCode, keyBufferVars, _) = keyBufferAccessor.getColumnVars(
        keyBufferTerm, keyBufferTerm, onlyKeyVars = false, onlyValueVars = false) */

    val keysExpr = byteBufferAccessor.getBufferVars(keysDataType, KeyBufferVars,
      localIterValueOffsetTerm, true, byteBufferAccessor.nullKeysBitsetTerm,
      byteBufferAccessor.numBytesForNullKeyBits, byteBufferAccessor.numBytesForNullKeyBits == 0)
    val aggsExpr = byteBufferAccessor.getBufferVars(aggBuffDataTypes,
      aggregateBufferVars, localIterValueOffsetTerm, false, byteBufferAccessor.nullAggsBitsetTerm,
      byteBufferAccessor.numBytesForNullAggBits, false)
    val outputCode = generateResultCodeForSHAMap(ctx, keysExpr, aggsExpr, localIterValueOffsetTerm)
    val numOutput = metricTerm(ctx, "numOutputRows")

    // The child could change `copyResult` to true, but we had already
    // consumed all the rows, so `copyResult` should be reset to `false`.
    ctx.copyResult = false

    val aggTime = metricTerm(ctx, "aggTime")
    val beforeAgg = ctx.freshName("beforeAgg")

    val readKeyLengthCode = if (skipLenForAttrib != -1) {
      if (ColumnEncoding.littleEndian) {
        s"int $keyLengthTerm = $platformClass.getInt($vdBaseObjectTerm, $localIterValueOffsetTerm);"
      } else {
        s"""int $keyLengthTerm = java.lang.Integer.reverseBytes($platformClass.getInt(
            $vdBaseObjectTerm, $localIterValueOffsetTerm));
          """
      }
    } else ""
    s"""
      if (!$initAgg) {
        $initAgg = true;
        long $beforeAgg = System.nanoTime();
        $doAgg();
        $aggTime.${metricAdd(s"(System.nanoTime() - $beforeAgg) / 1000000")};
        if ($overflowHashMapsTerm != null) {
          $overflowMapIter = $overflowHashMapsTerm.iterator();
          $hashMapTerm = ($hashSetClassName)$overflowMapIter.next();
        }
        $bbDataClass  $valueDataTerm = $hashMapTerm.getValueData();
        Object $vdBaseObjectTerm = $valueDataTerm.baseObject();
        $iterValueOffsetTerm += $valueDataTerm.baseOffset();
      }
      if ($hashMapTerm == null) {
         return;
      }
      $allocatorClass $allocatorTerm = $gfeCacheImplClass.
                      getCurrentBufferAllocator();
      ${byteBufferAccessor.initKeyOrBufferVal(aggBuffDataTypes, aggregateBufferVars)}
      ${byteBufferAccessor.initKeyOrBufferVal(keysDataType, KeyBufferVars)}
      ${SHAMapAccessor.initNullBitsetCode(nullKeysBitsetTerm, numBytesForNullKeyBits)}
      ${SHAMapAccessor.initNullBitsetCode(nullAggsBitsetTerm, numBytesForNullAggsBits)}
      ${KeyBufferVars.zip(keysDataType).map {
          case (varName, dataType) => dataType match {
          case st: StructType =>
            recursiveApply(nestedStructNullBitsTermInitializer)(varName, st, 0).toString
          case _ => ""
        }
      }.mkString("\n")}

      // output the result
      while($setBBMap()) {
        $bbDataClass  $valueDataTerm = $hashMapTerm.getValueData();
        Object $vdBaseObjectTerm = $valueDataTerm.baseObject();
        long $endIterValueOffset = $hashMapTerm.valueDataSize() + $valueDataTerm.baseOffset();
        long $localIterValueOffsetTerm = $iterValueOffsetTerm;
        ${byteBufferAccessor.declareNullVarsForAggBuffer(aggregateBufferVars)}
        while ($localIterValueOffsetTerm != $endIterValueOffset) {
          $numOutput.${metricAdd("1")};
          $readKeyLengthCode
          // skip the key length
          $localIterValueOffsetTerm += 4;
          long $localIterValueStartOffsetTerm = $localIterValueOffsetTerm;
          $outputCode
          if (shouldStop()) {
            $iterValueOffsetTerm = $localIterValueOffsetTerm;
            return;
          }
        }


        if ($localIterValueOffsetTerm == $endIterValueOffset) {
          // Not releasing memory here as it will be freed by the task completion listener
          //$hashMapTerm.release();
          $hashMapTerm = null;
        }

      }
    """
  }

  private def doProduceWithKeys(ctx: CodegenContext): String = {
    val initAgg = ctx.freshName("initAgg")
    ctx.addMutableState("boolean", initAgg, s"$initAgg = false;")

    // Create a name for iterator from HashMap
    val iterTerm = ctx.freshName("mapIter")
    val iter = ctx.freshName("mapIter")
    val iterObj = ctx.freshName("iterObj")
    val iterClass = "java.util.Iterator"
    ctx.addMutableState(iterClass, iterTerm, "")

    val doAgg = ctx.freshName("doAggregateWithKeys")

    // generate variable name for hash map for use here and in consume
    hashMapTerm = ctx.freshName("hashMap")
    val hashSetClassName = classOf[ObjectHashSet[_]].getName

    ctx.addMutableState(hashSetClassName, hashMapTerm, "")

    // generate variables for HashMap data array and mask
    mapDataTerm = ctx.freshName("mapData")
    maskTerm = ctx.freshName("hashMapMask")
    val maxMemory = ctx.freshName("maxMemory")
    val peakMemory = metricTerm(ctx, "peakMemory")

    // generate the map accessor to generate key/value class
    // and get map access methods
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]

    keyBufferAccessor = ObjectHashMapAccessor(session, ctx, groupingExpressions,
        aggregateBufferAttributesForGroup, "KeyBuffer", hashMapTerm,
        mapDataTerm, maskTerm, multiMap = false, this, this.parent, child)


    val entryClass = keyBufferAccessor.getClassName
    val numKeyColumns = groupingExpressions.length

    val childProduce =
      childProducer.asInstanceOf[CodegenSupport].produce(ctx, this)
    ctx.addNewFunction(doAgg,
      s"""
        private void $doAgg() throws java.io.IOException {
          $hashMapTerm = new $hashSetClassName(128, 0.6, $numKeyColumns, false,
            scala.reflect.ClassTag$$.MODULE$$.apply($entryClass.class));
          $entryClass[] $mapDataTerm = ($entryClass[])$hashMapTerm.data();
          int $maskTerm = $hashMapTerm.mask();

          $childProduce

          $iterTerm = $hashMapTerm.iterator();
          long $maxMemory = $hashMapTerm.maxMemory();
          $peakMemory.add($maxMemory);
          if ($hashMapTerm.taskContext() != null) {
            $hashMapTerm.taskContext().taskMetrics().incPeakExecutionMemory($maxMemory);
          }
        }
       """)

    // generate code for output
    val keyBufferTerm = ctx.freshName("keyBuffer")
    val (initCode, keyBufferVars, _) = keyBufferAccessor.getColumnVars(
      keyBufferTerm, keyBufferTerm, onlyKeyVars = false, onlyValueVars = false)
    val outputCode = generateResultCode(ctx, keyBufferVars,
      groupingExpressions.length)
    val numOutput = metricTerm(ctx, "numOutputRows")

    // The child could change `copyResult` to true, but we had already
    // consumed all the rows, so `copyResult` should be reset to `false`.
    ctx.copyResult = false

    val aggTime = metricTerm(ctx, "aggTime")
    val beforeAgg = ctx.freshName("beforeAgg")

    s"""
      if (!$initAgg) {
        $initAgg = true;
        long $beforeAgg = System.nanoTime();
        $doAgg();
        $aggTime.${metricAdd(s"(System.nanoTime() - $beforeAgg) / 1000000")};
      }

      // output the result
      Object $iterObj;
      final $iterClass $iter = $iterTerm;
      while (($iterObj = $iter.next()) != null) {
        $numOutput.${metricAdd("1")};
        final $entryClass $keyBufferTerm = ($entryClass)$iterObj;
        $initCode

        $outputCode

        if (shouldStop()) return;
      }
    """
  }

  private def doConsumeWithKeysForSHAMap(ctx: CodegenContext,
    input: Seq[ExprCode]): String = {


    // only have DeclarativeAggregate
    val updateExpr = aggregateExpressions.flatMap { e =>
      e.mode match {
        case Partial | Complete =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate]
            .updateExpressions
        case PartialMerge | Final =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate]
            .mergeExpressions
      }
    }

    ctx.currentVars = input
    val keysExpr = ctx.generateExpressions(
      groupingExpressions.map(e => BindReferences.bindReference[Expression](e, child.output)))
    // generate class for key, buffer and hash code evaluation of key columns
    val inputAttr = aggregateBufferAttributesForGroup ++ child.output
    val initVars = ctx.generateExpressions(declFunctions.flatMap(
      bufferInitialValuesForGroup(_).map(BindReferences.bindReference(_,
        inputAttr))))
    val initCode = evaluateVariables(initVars)
    val keysDataType = this.groupingAttributes.map(_.dataType)
    val aggBuffDataTypes = this.aggregateBufferAttributesForGroup.map(_.dataType)


    // if aggregate expressions uses some of the key variables then signal those
    // to be materialized explicitly for the dictionary optimization case (AQP-292)
    val updateAttrs = AttributeSet(updateExpr)
    // evaluate map lookup code before updateEvals possibly modifies the keyVars
    val mapCode = byteBufferAccessor.generateMapGetOrInsert(initVars, initCode, input,
      keysExpr, keysDataType, aggBuffDataTypes)

    val bufferVars = byteBufferAccessor.getBufferVars(aggBuffDataTypes,
      byteBufferAccessor.aggregateBufferVars,
      byteBufferAccessor.currentOffSetForMapLookupUpdt,
      false, byteBufferAccessor.nullAggsBitsetTerm, byteBufferAccessor.numBytesForNullAggBits,
      false)
    val bufferEval = evaluateVariables(bufferVars)
    val bufferVarsFromInitVars = byteBufferAccessor.aggregateBufferVars.zip(initVars).map {
      case (bufferVarName, initExpr) => ExprCode(
        s"""
          |$bufferVarName${SHAMapAccessor.nullVarSuffix} = ${initExpr.isNull};
          |$bufferVarName = ${initExpr.value};""".stripMargin,
        s"$bufferVarName${SHAMapAccessor.nullVarSuffix}", bufferVarName)
    }
    val bufferEvalFromInitVars = evaluateVariables(bufferVarsFromInitVars)
    ctx.currentVars = bufferVars ++ input
    // pre-evaluate input variables used by child expressions and updateExpr
    val inputCodes = evaluateRequiredVariables(child.output,
      ctx.currentVars.takeRight(child.output.length),
      child.references ++ updateAttrs)
    val boundUpdateExpr = updateExpr.map(BindReferences.bindReference(_,
      inputAttr))
    val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(
      boundUpdateExpr)
    val effectiveCodes = subExprs.codes.mkString("\n")
    val updateEvals = ctx.withSubExprEliminationExprs(subExprs.states) {
      boundUpdateExpr.map(_.genCode(ctx))
    }

    // We first generate code to probe and update the hash map. If the probe is
    // successful, the corresponding buffer will hold buffer class object.
    // We try to do hash map based in-memory aggregation first. If there is not
    // enough memory (the hash map will return null for new key), we spill the
    // hash map to disk to free memory, then continue to do in-memory
    // aggregation and spilling until all the rows had been processed.
    // Finally, sort the spilled aggregate buffers by key, and merge
    // them together for same key.
    s"""
       |$mapCode
       |// initialization for buffer fields from the hashmap
       |if (${byteBufferAccessor.keyExistedTerm}) {
       |  ${
            byteBufferAccessor.readNullBitsCode(byteBufferAccessor.
            currentOffSetForMapLookupUpdt, byteBufferAccessor.nullAggsBitsetTerm,
            byteBufferAccessor.numBytesForNullAggBits)
          }
          |$bufferEval
       |} else {
       |  $bufferEvalFromInitVars
       |}
       | // reset the  offset position to start of values for writing update
       |${byteBufferAccessor.currentOffSetForMapLookupUpdt} = ${byteBufferAccessor.valueOffsetTerm};
       | // common sub-expressions
       |$inputCodes
       |$effectiveCodes
       |
       |// evaluate aggregate functions
       |${evaluateVariables(updateEvals)}
       |${byteBufferAccessor.generateUpdate(updateEvals, aggBuffDataTypes)}
      """.stripMargin
  }

  private def doConsumeWithKeys(ctx: CodegenContext,
      input: Seq[ExprCode]): String = {
    val keyBufferTerm = ctx.freshName("keyBuffer")

    // only have DeclarativeAggregate
    val updateExpr = aggregateExpressions.flatMap { e =>
      e.mode match {
        case Partial | Complete =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate]
              .updateExpressions
        case PartialMerge | Final =>
          e.aggregateFunction.asInstanceOf[DeclarativeAggregate]
              .mergeExpressions
      }
    }

    // generate class for key, buffer and hash code evaluation of key columns
    ctx.INPUT_ROW = null
    ctx.currentVars = null
    val inputAttr = aggregateBufferAttributesForGroup ++ child.output
    val initVars = ctx.generateExpressions(declFunctions.flatMap(
      bufferInitialValuesForGroup(_).map(BindReferences.bindReference(_,
        inputAttr))))
    val initCode = evaluateVariables(initVars)
    val (bufferInit, bufferVars, _) = keyBufferAccessor.getColumnVars(
      keyBufferTerm, keyBufferTerm, onlyKeyVars = false, onlyValueVars = true)
    val bufferEval = evaluateVariables(bufferVars)

    // if aggregate expressions uses some of the key variables then signal those
    // to be materialized explicitly for the dictionary optimization case (AQP-292)
    val updateAttrs = AttributeSet(updateExpr)
    val evalKeys = if (DictionaryOptimizedMapAccessor.canHaveSingleKeyCase(groupingExpressions)) {
      var hasEvalKeys = false
      val keys = groupingAttributes.map { a =>
        if (updateAttrs.contains(a)) {
          hasEvalKeys = true
          true
        } else false
      }
      if (hasEvalKeys) keys else Nil
    } else Nil

    // evaluate map lookup code before updateEvals possibly modifies the keyVars
    val mapCode = keyBufferAccessor.generateMapGetOrInsert(keyBufferTerm,
      initVars, initCode, input, evalKeys, dictionaryArrayTerm, dictionaryArrayInit)

    ctx.currentVars = bufferVars ++ input
    // pre-evaluate input variables used by child expressions and updateExpr
    val inputCodes = evaluateRequiredVariables(child.output,
      ctx.currentVars.takeRight(child.output.length),
      child.references ++ updateAttrs)
    val boundUpdateExpr = updateExpr.map(BindReferences.bindReference(_,
      inputAttr))
    val subExprs = ctx.subexpressionEliminationForWholeStageCodegen(
      boundUpdateExpr)
    val effectiveCodes = subExprs.codes.mkString("\n")
    val updateEvals = ctx.withSubExprEliminationExprs(subExprs.states) {
      boundUpdateExpr.map(_.genCode(ctx))
    }

    // We first generate code to probe and update the hash map. If the probe is
    // successful, the corresponding buffer will hold buffer class object.
    // We try to do hash map based in-memory aggregation first. If there is not
    // enough memory (the hash map will return null for new key), we spill the
    // hash map to disk to free memory, then continue to do in-memory
    // aggregation and spilling until all the rows had been processed.
    // Finally, sort the spilled aggregate buffers by key, and merge
    // them together for same key.
    s"""
       |$mapCode
       |
       |// -- Update the buffer with new aggregate results --
       |
       |// initialization for buffer fields
       |$bufferInit
       |$bufferEval
       |
       |// common sub-expressions
       |$inputCodes
       |$effectiveCodes
       |
       |// evaluate aggregate functions
       |${evaluateVariables(updateEvals)}
       |
       |// update generated class object fields
       |${keyBufferAccessor.generateUpdate(keyBufferTerm, bufferVars,
          updateEvals, forKey = false, doCopy = false, forInit = false)}
    """.stripMargin
  }

  override def verboseString: String = toString(verbose = true)

  override lazy val simpleString: String = toString(verbose = false)

  private def toString(verbose: Boolean): String = {
    val name = nodeName
    val allAggregateExpressions = aggregateExpressions

    testFallbackStartsAt match {
      case None =>
        val keyString = Utils.truncatedString(groupingExpressions,
          "[", ", ", "]")
        val functionString = Utils.truncatedString(allAggregateExpressions,
          "[", ", ", "]")
        val outputString = Utils.truncatedString(output, "[", ", ", "]")
        val modesStr = modes.mkString(",")
        if (verbose) {
          s"$name(ByteBufferHashMap used = $useByteBufferMapBasedAggregation;" +
            s" keys=$keyString, modes=$modesStr, " +
            s"functions=$functionString, output=$outputString)"
        } else {
          s"$name(ByteBufferHashMap used = $useByteBufferMapBasedAggregation; " +
            s"keys=$keyString, modes=$modesStr, functions=$functionString)"
        }
      case Some(fallbackStartsAt) =>
        s"${name}WithControlledFallback;" +
          s"ByteBufferHashMap used = $useByteBufferMapBasedAggregation; $groupingExpressions " +
          s"$allAggregateExpressions $resultExpressions " +
          s"fallbackStartsAt=$fallbackStartsAt"
    }
  }
}

trait GroupAggregate {

  /**
   * Expressions for initializing empty aggregation buffers for group by.
   */
  def initialValuesForGroup: Seq[Expression]

  /** Attributes of fields in aggBufferSchema used for group by. */
  def aggBufferAttributesForGroup: Seq[AttributeReference]
}
