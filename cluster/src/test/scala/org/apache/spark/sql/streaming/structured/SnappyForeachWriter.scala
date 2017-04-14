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

package org.apache.spark.sql.streaming.structured

import org.apache.spark.sql.ForeachWriter

class SnappyForeachWriter extends ForeachWriter[Int] {

  /**
    * Called when starting to process one partition of new data in the executor. The `version` is
    * for data deduplication when there are failures. When recovering from a failure, some data may
    * be generated multiple times but they will always have the same version.
    *
    * If this method finds using the `partitionId` and `version` that this partition has already
    * been processed, it can return `false` to skip the further data processing.
    * However, `close` still will be called for cleaning up resources.
    *
    * @param partitionId the partition id.
    * @param version     a unique id for data deduplication.
    * @return `true` if the corresponding partition and version id should be processed. `false`
    *         indicates the partition should be skipped.
    */
  override def open(partitionId: Long, version: Long): Boolean = {
    // println("YOGS partitionId: "+ partitionId + " version: " + version)
    return true
  }

  /**
    * Called to process the data in the executor side. This method will be called only when `open`
    * returns `true`.
    */
  override def process(value: Int): Unit = {
    // println("YOGS value: "+ value)
  }

  /**
    * Called when stopping to process one partition of new data in the executor side. This is
    * guaranteed to be called either `open` returns `true` or `false`. However,
    * `close` won't be called in the following cases:
    *  - JVM crashes without throwing a `Throwable`
    *  - `open` throws a `Throwable`.
    *
    * @param errorOrNull the error thrown during processing data or null if there was no error.
    */
  override def close(errorOrNull: Throwable): Unit = {

  }
}
