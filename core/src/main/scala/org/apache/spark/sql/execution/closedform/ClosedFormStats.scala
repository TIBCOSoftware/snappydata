/*
 * Copyright (c) 2017-2020 TIBCO Software Inc. All rights reserved.
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
package org.apache.spark.sql.execution.closedform

import org.apache.spark.sql.catalyst.expressions.BaseGenericInternalRow
import org.apache.spark.sql.sources.StatVarianceCounter

trait ClosedFormStats extends StatVarianceCounter {

  self: BaseGenericInternalRow =>

  // New variance as per closed form formula
  var weightedCount: Double
  var trueSum: Double

  override def numFields: Int = 5

  def mergeTrueSum(other: ClosedFormStats): Unit = {
    (trueSum.isNaN, other.trueSum.isNaN) match {
      case (false, false) => trueSum += other.trueSum
      case (true, false) => trueSum = other.trueSum
      case (false, true) => if (other.count > 0) trueSum = other.trueSum
      case _ =>
    }
  }

  protected override def genericGet(ordinal: Int): Any = {
    triggerSerialization()
    ordinal match {
      case 0 => count
      case 1 => mean
      case 2 => nvariance
      case 3 => weightedCount
      case 4 => trueSum
    }
  }

  override def getLong(ordinal: Int): Long = {
    triggerSerialization()
    if (ordinal == 0) {
      count
    } else {
      throw new ClassCastException("cannot cast double to long")
    }
  }

  override def getDouble(ordinal: Int): Double = {
    triggerSerialization()
    ordinal match {
      case 1 => mean
      case 2 => nvariance
      case 3 => weightedCount
      case 0 => count
      case 4 => trueSum
    }
  }

  def triggerSerialization(): Unit

  def copy(other: ClosedFormStats): Unit = {
    other.count = count
    other.mean = mean
    other.nvariance = nvariance
    other.weightedCount = weightedCount
    other.trueSum = trueSum
  }


  def merge(other: ClosedFormStats) {
    if (other ne this) {
      this.mergeDistinctCounter(other)
      weightedCount += other.weightedCount
      mergeTrueSum(other)
    } else {
      merge(other.copy()) // Avoid overwriting fields in a weird order
    }
  }

  ///////////////

  protected def mergeDistinctCounter(other: ClosedFormStats) {
    if (count == 0) {
      mean = other.mean
      count = other.count
    } else if (other.count != 0) {
      val delta = other.mean - mean
      if (other.count * 10 < count) {
        mean = mean + (delta * other.count) / (count + other.count)
      } else if (count * 10 < other.count) {
        mean = other.mean - (delta * count) / (count + other.count)
      } else {
        mean = (mean * count + other.mean * other.count) /
            (count + other.count)
      }
      count += other.count
    }
    nvariance += other.nvariance
  }
}
