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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateMode, AggregateExpression2, AggregateFunction2}
import org.apache.spark.sql.execution.bootstrap.{Delegatee, DelegateAggregateFunction}
import org.apache.spark.sql.execution.metric.LongSQLMetric
import org.apache.spark.unsafe.KVIterator

/**
 * An iterator used to evaluate [[AggregateFunction2]]. It assumes the input rows have been
 * sorted by values of [[groupingKeyAttributes]].
 */
class SnappySortBasedAggregationIterator(
    groupingExpressions: Seq[NamedExpression],
    valueAttributes: Seq[Attribute],
    inputKVIterator: KVIterator[InternalRow, InternalRow],

    initialInputBufferOffset: Int,


    outputsUnsafeRows: Boolean,
    numInputRows: LongSQLMetric,
    numOutputRows: LongSQLMetric,
        newAggregateBuffer : ()=> Array[DelegateAggregateFunction] ,
resultExpressions: Seq[Expression],
computedSchema: Seq[Attribute],
lenComptedAggregates: Int,
delegatees: Array[Delegatee],
    mode: AggregateMode,
    outputSchema: Seq[Attribute]
    )
    extends SnappyAggregationIterator(
      groupingExpressions,
      valueAttributes,
      mode,
      initialInputBufferOffset,


      outputsUnsafeRows,

      newAggregateBuffer  ,
      resultExpressions,
      computedSchema,
      lenComptedAggregates,
      delegatees,
      outputSchema
    ) {



  ///////////////////////////////////////////////////////////////////////////
  // Mutable states for sort based aggregation.
  ///////////////////////////////////////////////////////////////////////////

  // The partition key of the current partition.
  private[this] var currentGroupingKey: InternalRow = _

  // The partition key of next partition.
  private[this] var nextGroupingKey: InternalRow = _

  // The first row of next partition.
  private[this] var firstRowInNextGroup: InternalRow = _

  // Indicates if we has new group of rows from the sorted input iterator
  private[this] var sortedInputHasNewGroup: Boolean = false

  // The aggregation buffer used by the sort-based aggregation.
 // private[this] val sortBasedAggregationBuffer: MutableRow = newBuffer

  private[this] var sortBasedAggregationBuffer = newAggregateBuffer()
  /** Processes rows in the current group. It will stop when it find a new group. */
  protected def processCurrentSortedGroup(): Unit = {
    currentGroupingKey = nextGroupingKey
    // Now, we will start to find all rows belonging to this group.
    // We create a variable to track if we see the next group.
    var findNextPartition = false
    // firstRowInNextGroup is the first row of this group. We first process it.
    processRow(sortBasedAggregationBuffer, firstRowInNextGroup)

    // The search will stop when we see the next group or there is no
    // input row left in the iter.
    var hasNext = inputKVIterator.next()
    while (!findNextPartition && hasNext) {
      // Get the grouping key.
      val groupingKey = inputKVIterator.getKey
      val currentRow = inputKVIterator.getValue
      numInputRows += 1

      // Check if the current row belongs the current input row.
      if (currentGroupingKey == groupingKey) {
        processRow(sortBasedAggregationBuffer, currentRow)

        hasNext = inputKVIterator.next()
      } else {
        // We find a new group.
        findNextPartition = true
        nextGroupingKey = groupingKey.copy()
        firstRowInNextGroup = currentRow.copy()
      }
    }
    // We have not seen a new group. It means that there is no new row in the input
    // iter. The current group is the last group of the iter.
    if (!findNextPartition) {
      sortedInputHasNewGroup = false
    }
  }

  ///////////////////////////////////////////////////////////////////////////
  // Iterator's public methods
  ///////////////////////////////////////////////////////////////////////////

  override final def hasNext: Boolean = sortedInputHasNewGroup

  override final def next(): InternalRow = {
    if (hasNext) {
      // Process the current group.
      processCurrentSortedGroup()
      // Generate output row for the current group.
      val outputRow = generateOutput(currentGroupingKey, sortBasedAggregationBuffer)
      // Initialize buffer values for the next group.
      sortBasedAggregationBuffer =initializeBuffer()
      numOutputRows += 1
      outputRow
    } else {
      // no more result
      throw new NoSuchElementException
    }
  }

  protected def initialize(): Unit = {
    if (inputKVIterator.next()) {
      sortBasedAggregationBuffer = initializeBuffer()

      nextGroupingKey = inputKVIterator.getKey().copy()
      firstRowInNextGroup = inputKVIterator.getValue().copy()
      numInputRows += 1
      sortedInputHasNewGroup = true
    } else {
      // This inputIter is empty.
      sortedInputHasNewGroup = false
    }
  }

  initialize()

  def outputForEmptyGroupingKeyWithoutInput(): InternalRow = {
    sortBasedAggregationBuffer = initializeBuffer()
    generateOutput(new GenericInternalRow(0), sortBasedAggregationBuffer)
  }
}

object SnappySortBasedAggregationIterator {
  // scalastyle:off
  def createFromInputIterator(
      groupingExprs: Seq[NamedExpression],

      initialInputBufferOffset: Int,

      newProjection: (Seq[Expression], Seq[Attribute]) => Projection,
      inputAttributes: Seq[Attribute],
      inputIter: Iterator[InternalRow],
      outputsUnsafeRows: Boolean,
      numInputRows: LongSQLMetric,
      numOutputRows: LongSQLMetric,
      newAggregateBuffer : ()=> Array[DelegateAggregateFunction] ,
  resultExpressions: Seq[Expression],
  computedSchema: Seq[Attribute],
  lenComptedAggregates: Int,
  delegatees: Array[Delegatee],
      mode: AggregateMode,
      outputSchema: Seq[Attribute]


      ): SnappySortBasedAggregationIterator = {
    val kvIterator = if (UnsafeProjection.canSupport(groupingExprs)) {
      SnappyAggregationIterator.unsafeKVIterator(
        groupingExprs,
        inputAttributes,
        inputIter).asInstanceOf[KVIterator[InternalRow, InternalRow]]
    } else {
      SnappyAggregationIterator.kvIterator(groupingExprs, newProjection, inputAttributes, inputIter)
    }

    new SnappySortBasedAggregationIterator(
      groupingExprs,
      inputAttributes,
      kvIterator,

      initialInputBufferOffset,


      outputsUnsafeRows,
      numInputRows,
      numOutputRows,
      newAggregateBuffer  ,
      resultExpressions,
      computedSchema,
      lenComptedAggregates,
      delegatees,
      mode,
    outputSchema
    )
  }
  // scalastyle:on
}
