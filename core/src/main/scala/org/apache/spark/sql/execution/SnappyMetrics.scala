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

import java.text.NumberFormat
import java.util.Locale
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList

import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}

/** Additional metric types supported by SnappyData. */
object SnappyMetrics {

  /**
   * Combine multiple "sum" metrics into a list of values for compact display.
   * Metrics having the same "splitSumX" prefix will be added to the first
   * accumulator in the series for display. For example, for metric types
   * "splitSum0_0", "splitSum0_1", ... all will be collapsed into a list
   * of values into "splitSum0_0" for display.
   */
  val SPLIT_SUM_METRIC = "splitSum"

  // in-built metric names for ColumnTableScan
  val NUM_ROWS_DISK = "numRowsBufferDisk"
  val NUM_BATCHES_DISK_FULL = "columnBatchesDiskFull"
  val NUM_BATCHES_DISK_PARTIAL = "columnBatchesDiskPartial"
  val NUM_BATCHES_REMOTE = "columnBatchesRemote"

  private val splitMetricId = new AtomicLong(0L)

  /**
   * Get a new ID that can be used for [[createSplitSumMetric]].
   */
  def newSplitMetricId(): Long = math.abs(splitMetricId.getAndIncrement())

  /**
   * Create a metric to report multiple sums as a single metric. All metrics are combined
   * and displayed as comma-separated values against the name for "splitIndex" = 0.
   *
   * The ID should be a positive long common for all metrics that need to be displayed
   * together and should be unique across all instances of the plan.
   * The [[newSplitMetricId]] provides a convenient way to generate a new unique ID.
   */
  def createSplitSumMetric(sc: SparkContext, name: String,
      id: Long, splitIndex: Int): SQLMetric = {
    // The final result of this metric in physical operator UI may looks like:
    // data size total (min, med, max):
    // 100GB (100MB, 1GB, 10GB)
    val acc = new SQLMetric(SPLIT_SUM_METRIC + id + '_' + splitIndex)
    acc.register(sc, name = Some(name), countFailedValues = false)
    acc
  }

  /**
   * Aggregate the final accumulator results across tasks and display as a string
   * for a single physical operator.
   */
  def stringValue(metricType: String, values: Any): String = {
    if (metricType.startsWith(SPLIT_SUM_METRIC)) {
      val valueList = values.asInstanceOf[mutable.ArrayBuffer[LongArrayList]]
      val numberFormat = NumberFormat.getIntegerInstance(Locale.US)
      valueList.collect {
        case l if l ne null => numberFormat.format(l.toArray.sum)
      }.mkString("|")
    } else SQLMetrics.stringValue(metricType, values.asInstanceOf[LongArrayList].toArray)
  }
}
