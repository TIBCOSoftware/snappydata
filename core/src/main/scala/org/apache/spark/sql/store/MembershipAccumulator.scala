/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
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
package org.apache.spark.sql.store

import com.gemstone.gemfire.distributed.internal.membership.InternalDistributedMember

import org.apache.spark.AccumulatorParam
import org.apache.spark.storage.BlockManagerId


object MembershipAccumulator extends AccumulatorParam[Map[InternalDistributedMember, BlockManagerId]]{
  /**
   * Merge two accumulated values together. Is allowed to modify and return the first value
   * for efficiency (to avoid allocating objects).
   *
   * @param r1 one set of accumulated data
   * @param r2 another set of accumulated data
   * @return both data sets merged together
   */
  override def addInPlace(r1: Map[InternalDistributedMember, BlockManagerId], r2: Map[InternalDistributedMember, BlockManagerId]): Map[InternalDistributedMember, BlockManagerId] = r1 ++ r2

  /**
   * Return the "zero" (identity) value for an accumulator type, given its initial value. For
   * example, if R was a vector of N dimensions, this would return a vector of N zeroes.
   */
  override def zero(initialValue: Map[InternalDistributedMember, BlockManagerId]): Map[InternalDistributedMember, BlockManagerId] = Map.empty[InternalDistributedMember, BlockManagerId]
}
