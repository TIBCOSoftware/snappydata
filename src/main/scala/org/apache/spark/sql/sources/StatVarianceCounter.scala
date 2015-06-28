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

package org.apache.spark.sql.sources

/**
 * A class for tracking the statistics of a set of numbers (count, mean and
 * variance) in a numerically robust way. Includes support for merging two
 * StatVarianceCounters.
 *
 * Taken from Spark's StatCounter implementation removing max and min.
 */
trait StatVarianceCounter extends Serializable {

  // Running count of our values
  final var count: Long = 0
  // Running mean of our values
  final var mean: Double = 0
  // Running variance times count of our values
  final private[spark] var nvariance: Double = 0

  private[spark] def init(count: Long, mean: Double, nvariance: Double) = {
    this.count = count
    this.mean = mean
    this.nvariance = nvariance
  }

  /**
   * Add a value into this StatVarianceCounter,
   * updating the internal statistics.
   */
  final def merge(value: Double) {
    val delta = value - mean
    count += 1
    mean += delta / count
    nvariance += delta * (value - mean)
  }

  /**
   * Add multiple values into this StatVarianceCounter,
   * updating the internal statistics.
   */
  final def merge(values: TraversableOnce[Double]): Unit = values.foreach(merge)

  /**
   * Merge another StatVarianceCounter into this one,
   * adding up the internal statistics.
   */
  final def merge(other: StatVarianceCounter) {
    if (other != this) {
      mergeDistinctCounter(other)
    }
    else {
      merge(other.copy()) // Avoid overwriting fields in a weird order
    }
  }

  /**
   * Merge another StatVarianceCounter into this one,
   * adding up the internal statistics when other != this.
   */
  protected def mergeDistinctCounter(other: StatVarianceCounter) {
    if (count == 0) {
      mean = other.mean
      nvariance = other.nvariance
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
      nvariance += other.nvariance + (delta * delta * count * other.count) /
          (count + other.count)
      count += other.count
    }
  }

  /** Clone this StatVarianceCounter */
  def copy(): StatVarianceCounter

  final def sum: Double = count * mean

  /** Return the variance of the values. */
  final def variance: Double = {
    if (count == 0) {
      Double.NaN
    } else {
      nvariance / count
    }
  }

  /**
   * Return the sample variance, which corrects for bias in estimating the variance by dividing
   * by N-1 instead of N.
   */
  final def sampleVariance: Double = {
    if (count <= 1) {
      Double.NaN
    } else {
      nvariance / (count - 1)
    }
  }

  /** Return the standard deviation of the values. */
  final def stdev: Double = math.sqrt(variance)

  /**
   * Return the sample standard deviation of the values, which corrects for bias in estimating the
   * variance by dividing by N-1 instead of N.
   */
  final def sampleStdev: Double = math.sqrt(sampleVariance)

  override def toString: String = {
    "(count: %d, mean: %f, stdev: %f)".format(count, mean, stdev)
  }
}

class StatCounter extends StatVarianceCounter with Serializable {
  /** Clone this StatCounter */
  override def copy(): StatCounter = {
    val other = new StatCounter
    other.count = count
    other.mean = mean
    other.nvariance = nvariance
    other
  }
}
