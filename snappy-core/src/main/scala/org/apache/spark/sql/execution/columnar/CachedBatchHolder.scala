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
package org.apache.spark.sql.execution.columnar

import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.types.StructType

private[sql] final class CachedBatchHolder[T](getColumnBuilders: => Array[ColumnBuilder],
    var rowCount: Int, val batchSize: Int, schema: StructType,
    val init: T, val batchAggregate: (T, CachedBatch) => T) extends Serializable {

  var columnBuilders = getColumnBuilders
  var result = init

  /**
   * Append a single row to the current CachedBatch (creating a new one
   * if not present or has exceeded its capacity)
   * later it can be shifted to REPLICATED Table in gemfireXD
   */
  private def appendRow_(newBuilders: Boolean, row: InternalRow,
    flush: Boolean): Unit = {
    val rowLength = if (row == expressions.EmptyRow) 0 else row.numFields
    if (rowLength > 0) {
      // Added for SPARK-6082. This assertion can be useful for scenarios when
      // something like Hive TRANSFORM is used. The external data generation
      // script used in TRANSFORM may result malformed rows, causing
      // ArrayIndexOutOfBoundsException, which is somewhat hard to decipher.
      assert(rowLength == columnBuilders.length, s"Row column number " +
          s"mismatch, expected ${columnBuilders.length} columns, " +
          s"but got $rowLength. Row content: $row")

      var i = 0
      while (i < rowLength) {
        columnBuilders(i).appendFrom(row, i)
        i += 1
      }
      rowCount += 1
    }
    if (rowCount >= batchSize || flush) {
      // create a new CachedBatch and push into the array of
      // CachedBatches so far in this iteration
      val stats = InternalRow.fromSeq(columnBuilders.map(
        _.columnStats.collectedStatistics).flatMap(_.values))
      // TODO: somehow push into global batchStats
      result = batchAggregate(result,
        CachedBatch(rowCount, columnBuilders.map(_.build().array()), stats))
      // batches += CachedBatch(columnBuilders.map(_.build().array()), stats)
      if (newBuilders) columnBuilders = getColumnBuilders
      rowCount = 0
    }
  }

  def appendRow(u: Unit, row: InternalRow): Unit =
    appendRow_(newBuilders = true, row, flush = false)

  // empty for now
  def endRows(u: Unit): Unit = {}

  def forceEndOfBatch(): T = {
    if (rowCount > 0) {
      appendRow_(newBuilders = false, expressions.EmptyRow, flush = true)
    }
    result
  }
}
