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
package org.apache.spark.sql

/**
 * Manages a time epoch and how to index into it.
 */
class TimeEpoch(val windowSize: Long, val epoch0: Long, var t: Long) {
  val MAX_J = 16 // Using Int.MaxValue is waayyyyyyyyyyyy to computationally expensive
  // we are talking exponents here, so 2^64 ought to be big enough for anyone?
  //var t: Long = 0 // t: The current time epoch

  // TODO: Right now, we treat time as going up one tick per batch, but
  // windowSize and epoch0 are put in place so we can have a time-tick be
  // any number of seconds.  Not really used yet.
  // val epoch0 = System.currentTimeMillis() // The oldest timestamp known to this TimeAggregation
  //val windowSize = 1L // How many time units we increment in an increment

  def increment() = t = t + 1

  // For a given timestamp, what is the smallest index into m which
  // contains data for the timestamp?
  // TODO: This essentially searches O(log n) time periods for the correct one
  // Perhaps there is an O(1) way to calculate?
  def timestampToInterval(ts: Long): Option[Int] = {

    if (ts <= epoch0 && t < 1)
      return None

    if (ts <= epoch0 && t >= 1) {
      Some(1)
    } else {
      /*val interval = if((ts - epoch0) % windowSize == 0) {
      (ts - epoch0)/windowSize
    }else {
      (ts - epoch0)/windowSize + 1
    }*/
      val interval = (ts - epoch0) / windowSize + 1l

      if(interval > Int.MaxValue) {
        Some(Int.MaxValue)
      }else {
        Some(interval.toInt)
      }


    }

    // This is bad: Searches...but this will generate correct test cases!
    /*
    (0 until MAX_J) foreach ({ j =>
      val tp = timePeriodForJ(t, j)
      tp map { x =>
        if (x._1 <= ts1 && ts1 < x._2)
          return Some(j)
      }
    })
    None*/
  }

  def convertEpochToIntervals(epochFrom: Long, epochTo: Long): Option[(Int, Int)] = {
    val fromInterval = this.timestampToInterval(epochFrom) match {
      case Some(x) => x
      case None => return None
    }

    val toInterval = this.timestampToInterval(epochTo) match {
      case Some(x) => x
      case None => return None
    }

    if (fromInterval > toInterval) {
      if (toInterval > this.t) {
        None
      } else {
        Some((fromInterval, toInterval))
      }
    } else {
      if (fromInterval > this.t) {
        None
      } else {
        Some((toInterval, fromInterval))
      }

    }
  }

  /*
  def timePeriodForJ(startTime: Long, j: Int): Option[(Long, Long)] = {
    if (startTime < 0 || j < 0)
      return None

    val twoToJ = 1L << j
    val delta = startTime % twoToJ
    val p1 = startTime - delta
    val p2 = startTime - delta - twoToJ
    if (p2 >= 0L) Some(p2, p1) else None
  }*/

}