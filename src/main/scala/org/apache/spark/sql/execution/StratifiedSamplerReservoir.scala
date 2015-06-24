package org.apache.spark.sql.execution

import scala.language.reflectiveCalls

import org.apache.spark.sql.catalyst.expressions.{MutableRow, Row}
import org.apache.spark.sql.collection.{ChangeValue, Utils}
import org.apache.spark.sql.execution.StratifiedSampler._
import org.apache.spark.sql.types.StructType

/**
 * A simple reservoir based stratified sampler that will use the provided
 * reservoir size for every strata present in the incoming rows.
 */
final class StratifiedSamplerReservoir(override val qcs: Array[Int],
    override val name: String,
    override val schema: StructType,
    private val reservoirSize: Int)
    extends StratifiedSampler(qcs, name, schema) {

  private final class ProcessRows(val processSelected: Any => Any)
      extends ChangeValue[Row, StrataReservoir] {

    override def keyCopy(row: Row) = row.copy()

    override def defaultValue(row: Row) = {
      // create new strata if required
      val reservoir = new Array[MutableRow](reservoirSize)
      reservoir(0) = newMutableRow(row, processSelected)
      Utils.fillArray(reservoir, EMPTY_ROW, 1, reservoirSize)
      new StrataReservoir(1, 1, reservoir, 1, 0)
    }

    override def mergeValue(row: Row, sr: StrataReservoir): StrataReservoir = {
      // else update meta information in current strata
      sr.batchTotalSize += 1
      val strataSize = sr.reservoirSize
      if (strataSize >= reservoirSize) {
        // copy into the reservoir as per probability (strataSize/totalSize)
        val rnd = rng.nextInt(sr.batchTotalSize)
        if (rnd < strataSize) {
          // pick up this row and replace a random one from reservoir
          sr.reservoir(rng.nextInt(strataSize)) = newMutableRow(row,
            processSelected)
        }
      } else {
        // always copy into the reservoir for this case
        sr.reservoir(strataSize) = newMutableRow(row, processSelected)
        sr.reservoirSize += 1
      }
      sr
    }
  }

  override protected def strataReservoirSize: Int = reservoirSize

  override def append[U](rows: Iterator[Row], processSelected: Any => Any,
      init: U, processFlush: (U, Row) => U,
      endBatch: U => U): U = {
    if (rows.hasNext) {
      stratas.bulkChangeValues(rows, new ProcessRows(processSelected))
    }
    init
  }

  override def sample(items: Iterator[Row], flush: Boolean): Iterator[Row] = {
    val processRow = new ProcessRows(null)
    for (row <- items) {
      stratas.changeValue(row, processRow)
    }

    if (flush) {
      // iterate over all the strata reservoirs for marked partition
      waitForSamplers(1, 5000)
      setFlushStatus(true)
      // remove sampler used only for DataFrame => DataFrame transformation
      removeSampler(name, markFlushed = true)
      // at this point we don't have a problem with concurrency
      stratas.flatMap(_.valuesIterator.flatMap(_.iterator(reservoirSize,
        reservoirSize, schema.length, doReset = true, fullReset = false)))
    }
    else {
      if (numSamplers.decrementAndGet() == 1) numSamplers.synchronized {
        numSamplers.notifyAll()
      }
      Iterator.empty
    }
  }

  override def clone: StratifiedSamplerReservoir =
    new StratifiedSamplerReservoir(qcs, name, schema, reservoirSize)
}
