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
/*
 * Adapted from Spark's HashAggregateExec having the license below.
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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SnappySession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetrics
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
    initialInputBufferOffset: Int,
    __resultExpressions: Seq[NamedExpression],
    child: SparkPlan)
    extends UnaryExecNode with BatchConsumer {

  @transient lazy val resultExpressions = __resultExpressions

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

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext,
      "number of output rows"),
    "peakMemory" -> SQLMetrics.createSizeMetric(sparkContext, "peak memory"),
    "spillSize" -> SQLMetrics.createSizeMetric(sparkContext, "spill size"),
    "aggTime" -> SQLMetrics.createTimingMetric(sparkContext, "aggregate time"))

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)

  override def producedAttributes: AttributeSet =
    AttributeSet(aggregateAttributes) ++
        AttributeSet(resultExpressions.diff(groupingExpressions)
            .map(_.toAttribute)) ++
        AttributeSet(aggregateBufferAttributes)

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
      lastAttr.copy(nullable = false)(lastAttr.exprId, lastAttr.qualifier,
        lastAttr.isGenerated) :: Nil
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

  protected override def doExecute(): RDD[InternalRow] = {
    // code generation should never fail
    WholeStageCodegenExec(this).execute()
  }

  // all the mode of aggregate expressions
  private val modes = aggregateExpressions.map(_.mode).distinct

  override def usedInputs: AttributeSet = inputSet

  override def supportCodegen: Boolean = {
    // ImperativeAggregate is not supported right now
    !aggregateExpressions.exists(_.aggregateFunction
        .isInstanceOf[ImperativeAggregate])
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }

  protected override def doProduce(ctx: CodegenContext): String = {
    batchConsumeDone = false
    if (groupingExpressions.isEmpty) {
      doProduceWithoutKeys(ctx)
    } else {
      doProduceWithKeys(ctx)
    }
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode],
      row: ExprCode): String = {
    if (groupingExpressions.isEmpty) {
      doConsumeWithoutKeys(ctx, input)
    } else {
      doConsumeWithKeys(ctx, input)
    }
  }

  override def batchConsume(ctx: CodegenContext,
      input: Seq[ExprCode]): String = {
    if (groupingExpressions.isEmpty) ""
    else {
      batchConsumeDone = true
      val entryClass = keyBufferAccessor.getClassName
      // add declarations for mask and data to enable using register variables
      s"""
        int $maskTerm = $hashMapTerm.mask();
        $entryClass[] $mapDataTerm = ($entryClass[])$hashMapTerm.data();
      """
    }
  }

  // The variables used as aggregation buffer
  private var bufVars: Seq[ExprCode] = _

  private def doProduceWithoutKeys(ctx: CodegenContext): String = {
    val initAgg = ctx.freshName("initAgg")
    ctx.addMutableState("boolean", initAgg, s"$initAgg = false;")

    // generate variables for aggregation buffer
    val functions = aggregateExpressions.map(_.aggregateFunction
        .asInstanceOf[DeclarativeAggregate])
    val initExpr = functions.flatMap(f => f.initialValues)
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
    val initBufVar = evaluateVariables(bufVars)

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
    ctx.addNewFunction(doAgg,
      s"""
         |private void $doAgg() throws java.io.IOException {
         |  // initialize aggregation buffer
         |  $initBufVar
         |
         |  ${child.asInstanceOf[CodegenSupport].produce(ctx, this)}
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
       |  $aggTime.add((System.nanoTime() - $beforeAgg) / 1000000);
       |
       |  // output the result
       |  ${genResult.trim}
       |
       |  $numOutput.add(1);
       |  ${consume(ctx, resultVars).trim}
       |}
     """.stripMargin
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
    val updates = aggVals.zipWithIndex.map { case (ev, i) =>
      s"""
         | ${bufVars(i).isNull} = ${ev.isNull};
         | ${bufVars(i).value} = ${ev.value};
      """.stripMargin
    }
    s"""
       | // do aggregate
       | // common sub-expressions
       | $effectiveCodes
       | // evaluate aggregate function
       | ${evaluateVariables(aggVals)}
       | // update aggregation buffer
       | ${updates.mkString("\n").trim}
    """.stripMargin
  }

  private val groupingAttributes = groupingExpressions.map(_.toAttribute)
  private val declFunctions = aggregateExpressions.map(_.aggregateFunction)
      .collect { case d: DeclarativeAggregate => d }

  // The name for UnsafeRow HashMap
  private var hashMapTerm: String = _

  // utility to generate class for optimized map, and hash map access methods
  @transient private var keyBufferAccessor: ObjectHashMapAccessor = _
  @transient private var mapDataTerm: String = _
  @transient private var maskTerm: String = _
  @transient private var batchConsumeDone = false

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

    // generate local variables for HashMap data array and mask
    mapDataTerm = ctx.freshName("mapData")
    maskTerm = ctx.freshName("hashMapMask")

    // generate the map accessor to generate key/value class
    // and get map access methods
    val session = sqlContext.sparkSession.asInstanceOf[SnappySession]
    keyBufferAccessor = ObjectHashMapAccessor(session, ctx, "KeyBuffer",
      groupingExpressions, aggregateBufferAttributesForGroup, hashMapTerm,
      mapDataTerm, maskTerm, multiMap = false, this, this.parent, child)

    val entryClass = keyBufferAccessor.getClassName
    val numKeyColumns = groupingExpressions.length

    val childProduce = child.asInstanceOf[CodegenSupport].produce(ctx, this)
    // if batchConsume has not been done then declare mask/data variables
    val declareVars = if (batchConsumeDone) ""
    else {
      s"""
        int $maskTerm = $hashMapTerm.mask();
        $entryClass[] $mapDataTerm = ($entryClass[])$hashMapTerm.data();
      """
    }
    ctx.addNewFunction(doAgg,
      s"""
        private void $doAgg() throws java.io.IOException {
          $hashMapTerm = new $hashSetClassName(128, 0.6, $numKeyColumns,
            scala.reflect.ClassTag$$.MODULE$$.apply($entryClass.class));
          $declareVars
          $childProduce

          $iterTerm = $hashMapTerm.iterator();
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
        $aggTime.add((System.nanoTime() - $beforeAgg) / 1000000);
      }

      // output the result
      Object $iterObj;
      final $iterClass $iter = $iterTerm;
      while (($iterObj = $iter.next()) != null) {
        $numOutput.add(1);
        final $entryClass $keyBufferTerm = ($entryClass)$iterObj;
        $initCode

        $outputCode

        if (shouldStop()) return;
      }
    """
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
    val inputAttr = aggregateBufferAttributesForGroup ++ child.output
    val initVars = ctx.generateExpressions(declFunctions.flatMap(
      bufferInitialValuesForGroup(_).map(BindReferences.bindReference(_,
        inputAttr))))
    val initCode = evaluateVariables(initVars)
    val (bufferInit, bufferVars, _) = keyBufferAccessor.getColumnVars(
      keyBufferTerm, keyBufferTerm, onlyKeyVars = false, onlyValueVars = true)
    val bufferEval = evaluateVariables(bufferVars)

    ctx.currentVars = bufferVars ++ input
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
       |${keyBufferAccessor.generateMapGetOrInsert(keyBufferTerm,
            initVars, initCode, input)}
       |
       |// -- Update the buffer with new aggregate results --
       |
       |// initialization for buffer fields
       |$bufferInit
       |$bufferEval
       |
       |// common sub-expressions
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
    val allAggregateExpressions = aggregateExpressions

    testFallbackStartsAt match {
      case None =>
        val keyString = Utils.truncatedString(groupingExpressions,
          "[", ", ", "]")
        val functionString = Utils.truncatedString(allAggregateExpressions,
          "[", ", ", "]")
        val outputString = Utils.truncatedString(output, "[", ", ", "]")
        if (verbose) {
          s"SnappyHashAggregate(keys=$keyString, functions=$functionString, " +
              s"output=$outputString)"
        } else {
          s"SnappyHashAggregate(keys=$keyString, functions=$functionString)"
        }
      case Some(fallbackStartsAt) =>
        s"SnappyHashAggregateWithControlledFallback $groupingExpressions " +
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
