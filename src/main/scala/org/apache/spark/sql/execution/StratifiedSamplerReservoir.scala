package org.apache.spark.sql.execution

import scala.language.reflectiveCalls

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.collection.{WrappedInternalRow, ChangeValue, Utils}
import org.apache.spark.sql.execution.StratifiedSampler._
import org.apache.spark.sql.types.StructType

/**
 * A simple reservoir based stratified sampler that will use the provided
 * reservoir size for every stratum present in the incoming rows.
 */
final class StratifiedSamplerReservoir(_qcs: Array[Int],
    _name: String, _schema: StructType,
    private val reservoirSize: Int)
    extends StratifiedSampler(_qcs, _name, _schema) {

  private final class ProcessRows extends ChangeValue[Row, StratumReservoir] {

    override def keyCopy(row: Row) = row.copy()

    override def defaultValue(row: Row) = {
      // create new stratum if required
      val reservoir = new Array[GenericMutableRow](reservoirSize)
      reservoir(0) = newMutableRow(row)
      Utils.fillArray(reservoir, EMPTY_ROW, 1, reservoirSize)
      new StratumReservoir(reservoir, 1, 1)
    }

    override def mergeValue(row: Row,
        sr: StratumReservoir): StratumReservoir = {
      // else update meta information in current stratum
      sr.batchTotalSize += 1
      val stratumSize = sr.reservoirSize
      if (stratumSize >= reservoirSize) {
        // copy into the reservoir as per probability (stratumSize/totalSize)
        val rnd = rng.nextInt(sr.batchTotalSize)
        if (rnd < stratumSize) {
          // pick up this row and replace a random one from reservoir
          sr.reservoir(rng.nextInt(stratumSize)) = newMutableRow(row)
        }
      } else {
        // always copy into the reservoir for this case
        sr.reservoir(stratumSize) = newMutableRow(row)
        sr.reservoirSize += 1
      }
      sr
    }
  }

  override protected def strataReservoirSize: Int = reservoirSize

  override def append[U](rows: Iterator[Row], init: U,
      processFlush: (U, InternalRow) => U, endBatch: U => U): U = {
    if (rows.hasNext) {
      strata.bulkChangeValues(rows, new ProcessRows)
    }
    init
  }

  override def sample(items: Iterator[InternalRow],
      flush: Boolean): Iterator[InternalRow] = {
    val processRow = new ProcessRows
    val wrappedRow = new WrappedInternalRow(schema,
      WrappedInternalRow.createConverters(schema))
    for (row <- items) {
      wrappedRow.internalRow = row
      // TODO: also allow invoking the optimized methods in
      // MultiColumnOpenHash* classes for WrappedInternalRow
      strata.changeValue(wrappedRow, processRow)
    }

    if (flush) {
      // iterate over all the reservoirs for marked partition
      waitForSamplers(1, 5000)
      setFlushStatus(true)
      // remove sampler used only for DataFrame => DataFrame transformation
      removeSampler(name, markFlushed = true)
      // at this point we don't have a problem with concurrency
      strata.flatMap(_.valuesIterator.flatMap(_.iterator(reservoirSize,
        reservoirSize, schema.length, doReset = true, fullReset = false)))
    } else {
      if (numSamplers.decrementAndGet() == 1) numSamplers.synchronized {
        numSamplers.notifyAll()
      }
      Iterator.empty
    }
  }

  override def clone: StratifiedSamplerReservoir =
    new StratifiedSamplerReservoir(qcs, name, schema, reservoirSize)
}
