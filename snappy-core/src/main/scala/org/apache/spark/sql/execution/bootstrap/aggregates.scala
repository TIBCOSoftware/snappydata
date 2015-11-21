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

import java.util.{HashMap => JHashMap, HashSet => JHashSet, LinkedHashMap => JLinkedHashMap}

import org.apache.spark.SparkEnv
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateMode, AggregateExpression2, AggregateFunction2, Partial, Complete, Final}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution.aggregate.SortBasedAggregationIterator
import org.apache.spark.sql.execution.metric.{LongSQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.{SparkPlan, UnaryNode}
//import org.apache.spark.sql.hive.online.ComposeRDDFunctions._
import org.apache.spark.storage.{BlockId,  StorageLevel}
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.execution.aggregate.SnappySortBasedAggregationIterator

trait DelegatedAggregate {
  self: UnaryNode =>

  val partial: Boolean
  val groupingExpressions: Seq[NamedExpression]
  val aggregateExpressions: Seq[NamedExpression]

  override def requiredChildDistribution: Seq[Distribution] =
    if (partial) {
      UnspecifiedDistribution :: Nil
    } else {
      if (groupingExpressions == Nil) {
        AllTuples :: Nil
      } else {
        ClusteredDistribution(groupingExpressions) :: Nil
      }
    }

  override def output: Seq[Attribute] = resultExpressions.map(_.toAttribute)//aggregateExpressions.map(_.toAttribute)

  /**
   * An aggregate that needs to be computed for each row in a group.
   *
   * @param unbound Unbound version of this delegated aggregate, used for result substitution.
   * @param resultAttribute An attribute used to refer to the result of this aggregate in the final
   *                        output.
   */
  case class ComputedAggregate(unbound: DelegateCommon, resultAttribute: AttributeReference)

  private[this] val groupedAggregates = {
    val seen = new JHashSet[DelegateCommon]()
    val grouped = new JLinkedHashMap[Expression, ArrayBuffer[ComputedAggregate]]()
    aggregateExpressions.foreach { agg =>
      agg.collect {
        case d@Delegate(r, a) if !seen.contains(d) =>
          seen.add(d)
          var buffer = grouped.get(a)
          if (buffer == null) {
            buffer = new ArrayBuffer[ComputedAggregate]()
            grouped.put(a, buffer)
          }
          buffer += ComputedAggregate(
            d, AttributeReference(s"aggResult:$a", a.dataType, a.nullable)())

        case d@DelegateFunction(r, a) if !seen.contains(d) =>
          seen.add(d)
          var buffer = grouped.get(a)
          if (buffer == null) {
            buffer = new ArrayBuffer[ComputedAggregate]()
            grouped.put(a, buffer)
          }
          buffer += ComputedAggregate(
            d, AttributeReference(s"aggResult:$a", a.dataType, a.nullable)())
      }
    }

    // Each buffer should be sorted by _.unbound.repetition
    grouped
  }

  /** A list of aggregates that need to be computed for each group. */
  protected[this] val computedAggregates = groupedAggregates.values().flatten.toArray

  private[this] val delegateeMap: JHashMap[ArrayBuffer[Attribute], Delegatee] = {
    def bind(repetitions: Seq[Expression]): Array[Int] = repetitions.map { repetition =>
      BindReferences.bindReference(repetition, child.output) match {
        case BoundReference(ordinal, _, _) => ordinal
        case _ => -1
      }
    }.toArray

    val toDelegatee = new JHashMap[ArrayBuffer[Attribute], Delegatee]()
    groupedAggregates.values().foreach { (as: ArrayBuffer[ComputedAggregate]) =>
      val repetitions = as.map(_.unbound.multiplicity)
      if (!toDelegatee.containsKey(repetitions)) {
        toDelegatee.put(repetitions, Delegatee(bind(repetitions)))
      }
    }
    toDelegatee
  }

  protected[this] val delegatees = delegateeMap.values().toSeq.toArray

  /** A list of aggregates that are actually computed */
  protected[this] val updatedAggregates: Array[DelegateAggregateExpression] = {
    def bind(as: ArrayBuffer[ComputedAggregate]): Delegatee =
      delegateeMap(as.map(_.unbound.multiplicity))

    var offset = 0
    try {
      groupedAggregates.map {
        case (Sum(e), as) =>
          val delegate = BindReferences.bindReference(DelegateSum(e, bind(as), offset), child.output)
          offset += as.length
          delegate
        case (Count(e), as) =>
          val delegate = BindReferences.bindReference(DelegateCount(e, bind(as), offset), child.output)
          offset += as.length
          delegate
        case (Count01(e), as) =>
          val delegate = BindReferences.bindReference(DelegateCount01(e, bind(as), offset), child.output)
          offset += as.length
          delegate

        case (func@org.apache.spark.sql.catalyst.expressions.aggregate.Sum(e), as) =>
          //if(true) {
            val del = BindReferences.bindReference(DelegateSum(e, bind(as), offset), child.output)
            offset += as.length
            del
          /*}else {
            val modifiedE = e.transform {
              case a: AttributeReference => {
                 System.out.print("a")
                 func.cloneBufferAttributes (0)
              }
            }
            val del = BindReferences.bindReference(DelegateSum(modifiedE, bind(as), offset), child.output)
            offset += as.length
            del
          }*/

        case (org.apache.spark.sql.catalyst.expressions.aggregate.Count(e), as) =>
          val delegate = BindReferences.bindReference(DelegateCount(e, bind(as), offset), child.output)
          offset += as.length
          delegate
        case (Count01(e), as) =>
          val delegate = BindReferences.bindReference(DelegateCount01(e, bind(as), offset), child.output)
          offset += as.length
          delegate
      }.toArray
    }catch {
      case th: Throwable => {
        th.printStackTrace()
        throw th
      }
    }
  }

  /** The schema of the result of all aggregate evaluations */
  protected[this] val computedSchema = computedAggregates.map(_.resultAttribute)

  /** Creates a new aggregate buffer for a group. */
  protected[this] def newAggregateBuffer() = {
    val buffer = new Array[DelegateAggregateFunction](delegatees.length + updatedAggregates.length)
    var i = 0
    while (i < delegatees.length) {
      buffer(i) = delegatees(i)
      i += 1
    }
    i = 0
    while (i < updatedAggregates.length) {
      buffer(i + delegatees.length) = updatedAggregates(i).newInstance()
      i += 1
    }
    buffer
  }

  /** Named attributes used to substitute grouping attributes into the final result. */
  protected[this] val namedGroups = groupingExpressions.map {
    case ne: NamedExpression => ne -> ne.toAttribute
    case e => e -> Alias(e, s"groupingExpr:$e")().toAttribute
  }

  /**
   * A map of substitutions that are used to insert the aggregate expressions and grouping
   * expression into the final result expression.
   */
  protected[this] val resultMap =
    (computedAggregates.map { agg => agg.unbound -> agg.resultAttribute } ++ namedGroups).toMap

  /**
   * Substituted version of aggregateExpressions expressions which are used to compute final
   * output rows given a group and the result of all aggregate computations.
   */
  protected[this] val resultExpressions: Seq[NamedExpression] = groupingExpressions ++ aggregateExpressions.map { agg =>
    agg.transform {

      case exp@TaggedAggregateExpression2(tag:Tag, aggFunc:AggregateFunction2,_,_,_) if resultMap.contains(aggFunc) => TaggedAlias(tag, resultMap(aggFunc), exp.name)(exp.exprId)

      case exp@UnTaggedAggregateExpression2( aggFunc: AggregateFunction2,_,_,_) if resultMap.contains(aggFunc) => Alias(resultMap(aggFunc), exp.name)(exp.exprId)
      case exp@TaggedAlias( tag:Tag,aggFunc:AggregateFunction2,_) if resultMap.contains(aggFunc) => TaggedAlias(tag,resultMap(aggFunc), exp.name)(exp.exprId)
      case exp@TaggedAlias( tag:Tag,aggExp:AggregateExpression1,_) if resultMap.contains(aggExp) => TaggedAlias(tag,resultMap(aggExp), exp.name)(exp.exprId)
      case exp@TaggedAlias( tag:Tag,TaggedAggregateExpression2(_, aggFunc:AggregateFunction2,_,_,_) ,_) if resultMap.contains(aggFunc) => TaggedAlias(tag,resultMap(aggFunc), exp.name)(exp.exprId)
      case e: Expression  if(!(e .isInstanceOf[DelegateFunction]) && resultMap.contains(e))   => resultMap(e)
    }.asInstanceOf[NamedExpression]
  }
}

case class BootstrapAggregate(
    partial: Boolean,
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan)
    extends UnaryNode with DelegatedAggregate {

  override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    if (groupingExpressions.isEmpty) {
      child.execute().mapPartitions { iter =>
        val buffer = newAggregateBuffer()
        var currentRow: InternalRow = null
        while (iter.hasNext) {
          currentRow = iter.next()
          var i = 0
          while (i < buffer.length) {
            buffer(i).update(currentRow)
            i += 1
          }
        }
        val resultProjection = new InterpretedProjection(resultExpressions, computedSchema)
        val aggregateResults = new GenericMutableRow(computedAggregates.length)

        var i = delegatees.length
        while (i < buffer.length) {
          buffer(i).evaluate(aggregateResults)
          i += 1
        }

        Iterator(resultProjection(aggregateResults))
      }
    } else {
      child.execute().mapPartitions { iter =>
        val hashTable = new JHashMap[InternalRow, Array[DelegateAggregateFunction]]
        val groupingProjection = new InterpretedMutableProjection(groupingExpressions, child.output)

        var currentRow: InternalRow = null
        while (iter.hasNext) {
          currentRow = iter.next()
          val currentGroup = groupingProjection(currentRow)
          var currentBuffer = hashTable.get(currentGroup)
          if (currentBuffer == null) {
            currentBuffer = newAggregateBuffer()
            hashTable.put(currentGroup.copy(), currentBuffer)
          }

          var i = 0
          while (i < currentBuffer.length) {
            currentBuffer(i).update(currentRow)
            i += 1
          }
        }

        new Iterator[InternalRow] {
          private[this] val hashTableIterator = hashTable.entrySet().iterator()
          private[this] val aggregateResults = new GenericMutableRow(computedAggregates.length)
          private[this] val resultProjection =
            new InterpretedMutableProjection(
              resultExpressions, computedSchema ++ namedGroups.map(_._2))
          private[this] val joinedRow = new JoinedRow

          override final def hasNext: Boolean = hashTableIterator.hasNext

          override final def next(): InternalRow = {
            val currentEntry = hashTableIterator.next()
            val currentGroup = currentEntry.getKey
            val currentBuffer = currentEntry.getValue

            var i = delegatees.length
            while (i < currentBuffer.length) {
              currentBuffer(i).evaluate(aggregateResults)
              i += 1
            }
            resultProjection(joinedRow(aggregateResults, currentGroup))
          }
        }
      }
    }
  }
}


case class BootstrapSortedAggregate(
    partial: Boolean,
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan)
    extends UnaryNode with DelegatedAggregate {

  override private[sql] lazy val metrics = Map(
    "numInputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  override def outputsUnsafeRows: Boolean = false

  override def canProcessUnsafeRows: Boolean = false

  override def canProcessSafeRows: Boolean = true

 /*
  override def requiredChildDistribution: List[Distribution] = {
    requiredChildDistributionExpressions match {
      case Some(exprs) if exprs.length == 0 => AllTuples :: Nil
      case Some(exprs) if exprs.length > 0 => ClusteredDistribution(exprs) :: Nil
      case None => UnspecifiedDistribution :: Nil
    }
  }*/

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    groupingExpressions.map(SortOrder(_, Ascending)) :: Nil
  }

  override def outputOrdering: Seq[SortOrder] = {
    groupingExpressions.map(SortOrder(_, Ascending))
  }

  protected override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {val numInputRows = longMetric("numInputRows")
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitions { iter =>
      // Because the constructor of an aggregation iterator will read at least the first row,
      // we need to get the value of iter.hasNext first.
      val hasInput = iter.hasNext
      if (!hasInput && groupingExpressions.nonEmpty) {
        // This is a grouped aggregate and the input iterator is empty,
        // so return an empty iterator.
        Iterator[InternalRow]()
      } else {
        val outputIter = SnappySortBasedAggregationIterator.createFromInputIterator(
          groupingExpressions,

          initialInputBufferOffset=0,

          newProjection _,
          child.output,
          iter,
          outputsUnsafeRows,
          numInputRows,
          numOutputRows,
          newAggregateBuffer _,
          resultExpressions,
          computedSchema,
        computedAggregates.length,
        delegatees,
        if(this.partial) Partial else Final


        )
        if (!hasInput && groupingExpressions.isEmpty) {
          // There is no input and there is no grouping expressions.
          // We need to output a single row as the output.
          numOutputRows += 1
          Iterator[InternalRow](outputIter.outputForEmptyGroupingKeyWithoutInput())
        } else {
          outputIter
        }
      }
    }
  }

  override def simpleString: String = {
    val allAggregateExpressions = this.aggregateExpressions //++ completeAggregateExpressions

    val keyString = groupingExpressions.mkString("[", ",", "]")
    val functionString = allAggregateExpressions.mkString("[", ",", "]")
    val outputString = output.mkString("[", ",", "]")
    s"SortBasedAggregate(key=$keyString, functions=$functionString, output=$outputString)"
  }
}

case class SortedAggregateWith2Inputs2Outputs(
    cacheFilter: Option[Attribute],

    lineageRelayInfo: LineageRelay,
    integrityInfo: Option[IntegrityInfo],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan

    )(

    val trace: List[Int] = -1 :: Nil,
    val opId: OpId = OpId.newOpId)
    extends UnaryNode with DelegatedAggregate   {
  override private[sql] lazy val metrics = Map(
    "numInputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of input rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))
  val partial = false

  override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val numInputRows = longMetric("numInputRows")
    val numOutputRows = longMetric("numOutputRows")
    child.execute().mapPartitions { iter =>
      // Because the constructor of an aggregation iterator will read at least the first row,
      // we need to get the value of iter.hasNext first.
      val hasInput = iter.hasNext
      if (!hasInput && groupingExpressions.nonEmpty) {
        // This is a grouped aggregate and the input iterator is empty,
        // so return an empty iterator.
        Iterator[InternalRow]()
      } else {
        val outputIter = SnappySortBasedAggregationIterator.createFromInputIterator(
          groupingExpressions,

          initialInputBufferOffset=0,

          newProjection _,
          child.output,
          iter,
          outputsUnsafeRows,
          numInputRows,
          numOutputRows,
          newAggregateBuffer _,
          resultExpressions,
          computedSchema,
          computedAggregates.length,
          delegatees,

          if(this.partial) Partial else Final)

          if (!hasInput && groupingExpressions.isEmpty) {
            // There is no input and there is no grouping expressions.
            // We need to output a single row as the output.
            numOutputRows += 1
            Iterator[InternalRow](outputIter.outputForEmptyGroupingKeyWithoutInput())
          } else {
            outputIter
          }
      }
    }
  }

  override protected final def otherCopyArgs =  trace :: opId :: Nil

  override def simpleString: String = s"${super.simpleString} $opId"


}


case class AggregateWith2Inputs2Outputs(
    cacheFilter: Option[Attribute],

    lineageRelayInfo: LineageRelay,
    integrityInfo: Option[IntegrityInfo],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan)(

    val trace: List[Int] = -1 :: Nil,
    val opId: OpId = OpId.newOpId)
    extends UnaryNode with DelegatedAggregate   {

  val partial = false



  override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val rdd = child.execute()

    if (groupingExpressions.isEmpty) {
      rdd.mapPartitions { iter =>
        val buffer = newAggregateBuffer()
        var currentRow: InternalRow = null

        while (iter.hasNext) {
          currentRow = iter.next()
          var i = 0
          while (i < buffer.length) {
            buffer(i).update(currentRow)
            i += 1
          }
        }

        val aggregateResults = new GenericMutableRow(computedAggregates.length)
        val resultProjection = new InterpretedMutableProjection(resultExpressions, computedSchema)
        val result = new ArrayBuffer[InternalRow]( 16)


        var i = delegatees.length
        while (i < buffer.length) {
          buffer(i).evaluate(aggregateResults)
          i += 1
        }
        result += aggregateResults
        /*
        val filter = buildRelayFilter()
        if (filter(result)) {
          // Broadcast the old results
          val broadcastProjection = buildRelayProjection()
          val toBroadcast = broadcastProjection(result)
          SparkEnv.get.blockManager.putSingle(
            LazyBlockId(opId.id, currentBatch, index), toBroadcast, StorageLevel.MEMORY_AND_DISK)
        }

        // Pass on the new results
        if (prevRow != null) Iterator() else*/
        result.iterator
      }
    } else {
      rdd.mapPartitions { iter =>
        val hashTable = new JHashMap[InternalRow, Array[DelegateAggregateFunction]]
        val groupingProjection = new InterpretedMutableProjection(groupingExpressions, child.output)
        var currentRow: InternalRow = null

        while (iter.hasNext) {
          currentRow = iter.next()
          val currentGroup = groupingProjection(currentRow)
          var currentBuffer = hashTable.get(currentGroup)
          if (currentBuffer == null) {
            currentBuffer = newAggregateBuffer()
            hashTable.put(currentGroup.copy(), currentBuffer)
          }
          var i = 0
          while (i < currentBuffer.length) {
            currentBuffer(i).update(currentRow)
            i += 1
          }
        }

        val aggregateResults = new GenericMutableRow(computedAggregates.length)
        val joinedRow = new JoinedRow
        val resultProjection = new InterpretedMutableProjection(resultExpressions,
          computedSchema ++ namedGroups.map(_._2))

        val results = new ArrayBuffer[InternalRow]( 16)


        hashTable.foreach { case (currentGroup, currentBuffer) =>
          var i = delegatees.length
          while (i < currentBuffer.length) {
            currentBuffer(i).evaluate(aggregateResults)
            i += 1
          }
          var row = resultProjection(joinedRow(aggregateResults, currentGroup))
          results += row.copy()

        }


        // Pass on the new results
        results.iterator
      }
    }
  }

  override protected final def otherCopyArgs =  trace :: opId :: Nil

  override def simpleString: String = s"${super.simpleString} $opId"


}


case class AggregateWith2Inputs(
    cacheFilter: Option[Attribute],

    partial: Boolean,
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[NamedExpression],
    child: SparkPlan)(

    @transient val trace: List[Int] = -1 :: Nil,
    val opId: OpId = OpId.newOpId)
    extends UnaryNode with DelegatedAggregate  {

  /*def retrieveState(rdd: RDD[InternalRow]): RDD[InternalRow] = {
    val batchSizes = prevBatches.map(bId => (controller.olaBlocks(opId, bId), bId)).toArray
    val cached = OLABlockRDD.create[Row](sparkContext, opId.id, batchSizes, rdd)
    refresh(cached)
  }*/

  override def doExecute(): RDD[InternalRow] = attachTree(this, "execute") {
    val rdd = child.execute()
   // controller.olaBlocks((opId, currentBatch)) = rdd.partitions.length
    //val state = retrieveState(rdd)

    if (groupingExpressions.isEmpty) {
      rdd.mapPartitions { iter =>
        val buffer = newAggregateBuffer()
        var currentRow: InternalRow = null


        while (iter.hasNext) {
          currentRow = iter.next()
          var i = 0
          while (i < buffer.length) {
            buffer(i).update(currentRow)
            i += 1
          }
        }
        val resultProjection = new InterpretedProjection(resultExpressions, computedSchema)
        val aggregateResults = new GenericMutableRow(computedAggregates.length)

        var i = delegatees.length
        while (i < buffer.length) {
          buffer(i).evaluate(aggregateResults)
          i += 1
        }

        Iterator(resultProjection(aggregateResults))
      }
    } else {
      rdd.mapPartitions{ iter =>
        val hashTable = new JHashMap[InternalRow, Array[DelegateAggregateFunction]]
        val groupingProjection = new InterpretedMutableProjection(groupingExpressions, child.output)
        var currentRow: InternalRow = null

        while (iter.hasNext) {
          currentRow = iter.next()
          val currentGroup = groupingProjection(currentRow)
          var currentBuffer = hashTable.get(currentGroup)
          if (currentBuffer == null) {
            currentBuffer = newAggregateBuffer()
            hashTable.put(currentGroup.copy(), currentBuffer)
          }
          var i = 0
          while (i < currentBuffer.length) {
            currentBuffer(i).update(currentRow)
            i += 1
          }
        }
         new Iterator[InternalRow] {
          private[this] val hashTableIterator = hashTable.entrySet().iterator()
          private[this] val aggregateResults = new GenericMutableRow(computedAggregates.length)
          private[this] val resultProjection =
            new InterpretedMutableProjection(
              resultExpressions, computedSchema ++ namedGroups.map(_._2))
          private[this] val joinedRow = new JoinedRow()

          override final def hasNext: Boolean = hashTableIterator.hasNext

          override final def next(): InternalRow = {
            val currentEntry = hashTableIterator.next()
            val currentGroup: InternalRow = currentEntry.getKey
            val currentBuffer = currentEntry.getValue

            var i = delegatees.length
            while (i < currentBuffer.length) {
              currentBuffer(i).evaluate(aggregateResults)
              i += 1
            }
            resultProjection(joinedRow(aggregateResults, currentGroup))
          }
        }
      }
    }
  }

  override protected final def otherCopyArgs =   trace :: opId :: Nil

  override def simpleString: String = s"${super.simpleString} $opId"


}
