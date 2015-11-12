package org.apache.spark.sql.columnar

import org.apache.spark.sql.catalyst.{InternalRow, expressions}
import org.apache.spark.sql.types.StructType

/**
 * Created by skumar on 9/10/15.
 */
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
  private def appendRow_(newBuilders: Boolean, row: InternalRow): Unit = {
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
    if (rowCount >= batchSize) {
      // create a new CachedBatch and push into the array of
      // CachedBatches so far in this iteration
      val stats = InternalRow.fromSeq(columnBuilders.map(
        _.columnStats.collectedStatistics).flatMap(_.values))
      // TODO: somehow push into global batchStats
      result = batchAggregate(result,
        CachedBatch(columnBuilders.map(_.build().array()), stats))
      // batches += CachedBatch(columnBuilders.map(_.build().array()), stats)
      if (newBuilders) columnBuilders = getColumnBuilders
      rowCount = 0
    }
  }

  def appendRow(u: Unit, row: InternalRow): Unit =
    appendRow_(newBuilders = true, row)

  // empty for now
  def endRows(u: Unit): Unit = {}

  def forceEndOfBatch(): T = {
    if (rowCount > 0) {
      // setting rowCount to batchSize temporarily will automatically
      // force creation of a new batch in appendRow
      rowCount = batchSize
      appendRow_(newBuilders = false, expressions.EmptyRow)
    }
    result
  }
}
