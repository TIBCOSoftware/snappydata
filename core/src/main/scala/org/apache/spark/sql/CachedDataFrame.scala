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
package org.apache.spark.sql

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}

import org.apache.spark.io.CompressionCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.backwardcomp.ExecutedCommand
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodeAndComment
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.aggregate.CollectAggregateExec
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.sql.execution.{CollectLimitExec, LocalTableScanExec, PartitionedPhysicalScan, SQLExecution, SparkPlanInfo, WholeStageCodegenExec}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.{BlockManager, RDDBlockId, StorageLevel}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.CallSite
import org.apache.spark.{Logging, SparkEnv, TaskContext}

class CachedDataFrame(df: Dataset[Row],
    cachedRDD: RDD[InternalRow], shuffleDependencies: Array[Int],
    val rddId: Int, val hasLocalCollectProcessing: Boolean)
    extends Dataset[Row](df.sparkSession, df.queryExecution, df.exprEnc) {

  /**
   * Return true if [[collectWithHandler]] supports partition-wise separate
   * result handling by default, else result handler is invoked for a
   * single partition result.
   */
  def hasPartitionWiseHandling: Boolean = cachedRDD ne null

  private lazy val boundEnc = exprEnc.resolveAndBind(logicalPlan.output,
    sparkSession.sessionState.analyzer)

  private lazy val queryExecutionString = queryExecution.toString
  private lazy val queryPlanInfo = PartitionedPhysicalScan.getSparkPlanInfo(
    queryExecution.executedPlan)

  /**
   * Wrap a Dataset action to track the QueryExecution and time cost,
   * then report to the user-registered callback functions.
   */
  private def withCallback[U](name: String)(action: DataFrame => U) = {
    try {
      queryExecution.executedPlan.foreach { plan =>
        plan.resetMetrics()
      }
      val start = System.nanoTime()
      val result = action(this)
      val end = System.nanoTime()
      sparkSession.listenerManager.onSuccess(name, queryExecution,
        end - start)
      result
    } catch {
      case e: Exception =>
        sparkSession.listenerManager.onFailure(name, queryExecution, e)
        throw e
    }
  }

  override def collect(): Array[Row] = {
    collectInternal().map(boundEnc.fromRow).toArray
  }

  def collectInternal(): Iterator[InternalRow] = {
    if (hasPartitionWiseHandling) {
      collectWithHandler[Array[Byte], Iterator[UnsafeRow]](CachedDataFrame,
        (_, data) => CachedDataFrame.decodeUnsafeRows(schema.length, data, 0,
          data.length), identity).flatten
    } else {
      // skip double encoding/decoding for the case when underlying plan
      // execution returns an Iterator[InternalRow] itself
      collectWithHandler[InternalRow, InternalRow](null, null, null,
        skipUnpartitionedDataProcessing = true)
    }
  }

  def collectWithHandler[U: ClassTag, R: ClassTag](
      processPartition: (TaskContext, Iterator[InternalRow]) => (U, Int),
      resultHandler: (Int, U) => R,
      decodeResult: R => Iterator[InternalRow],
      skipUnpartitionedDataProcessing: Boolean = false,
      skipLocalCollectProcessing: Boolean = false): Iterator[R] = {
    val sc = sparkSession.sparkContext
    val numShuffleDeps = shuffleDependencies.length
    if (numShuffleDeps > 0) {
      sc.cleaner match {
        case Some(cleaner) =>
          var i = 0
          while (i < numShuffleDeps) {
            cleaner.doCleanupShuffle(shuffleDependencies(i), blocking = false)
            i += 1
          }
        case None =>
      }
    }

    val hasLocalCallSite = sc.getLocalProperties.containsKey(CallSite.LONG_FORM)
    val callSite = sc.getCallSite()
    if (!hasLocalCallSite) {
      sc.setCallSite(callSite)
    }
    def execute(): Iterator[R] = CachedDataFrame.withNewExecutionId(
      sparkSession, callSite, queryExecutionString, queryPlanInfo) {
      val executedPlan = queryExecution.executedPlan match {
        case WholeStageCodegenExec(plan) => plan
        case plan => plan
      }
      val results = executedPlan match {
        case plan: CollectLimitExec =>
          CachedDataFrame.executeTake(cachedRDD, plan.limit, processPartition,
            resultHandler, decodeResult, schema, sparkSession)
        /* TODO: SW: optimize this case too
        case plan: TakeOrderedAndProjectExec =>
          CachedDataFrame.executeCollect(plan,
            cachedRDD.asInstanceOf[RDD[InternalRow]])
        */

        case plan: CollectAggregateExec =>
          if (skipLocalCollectProcessing) {
            // special case where caller will do processing of the blocks
            // (returns a AggregatePartialDataIterator)
            new AggregatePartialDataIterator(plan.generatedSource,
              plan.generatedReferences, plan.child.schema.length,
              plan.executeCollectData()).asInstanceOf[Iterator[R]]
          } else if (skipUnpartitionedDataProcessing) {
            // no processing required
            plan.executeCollect().iterator.asInstanceOf[Iterator[R]]
          } else {
            // convert to UnsafeRow
            val converter = UnsafeProjection.create(plan.schema)
            Iterator(resultHandler(0, processPartition(TaskContext.get(),
              plan.executeCollect().iterator.map(converter))._1))
          }

        case plan@(_: ExecutedCommandExec | _: LocalTableScanExec | _: ExecutedCommand) =>
          if (skipUnpartitionedDataProcessing) {
            // no processing required
            plan.executeCollect().iterator.asInstanceOf[Iterator[R]]
          } else {
            // convert to UnsafeRow
            val converter = UnsafeProjection.create(plan.schema)
            Iterator(resultHandler(0, processPartition(TaskContext.get(),
              plan.executeCollect().iterator.map(converter))._1))
          }

        case _ =>
          val numPartitions = cachedRDD.getNumPartitions
          val results = new Array[R](numPartitions)
          sc.runJob(cachedRDD, processPartition, 0 until numPartitions,
            (index: Int, r: (U, Int)) =>
              results(index) = resultHandler(index, r._1))
          results.iterator
      }
      results
    }

    try {
      withCallback("collect")(_ => execute())
    } finally {
      if (!hasLocalCallSite) {
        sc.clearCallSite()
      }
    }
  }
}

final class AggregatePartialDataIterator(
    val generatedSource: CodeAndComment,
    val generatedReferences: Array[Any], val numFields: Int,
    val partialAggregateResult: Array[Any]) extends Iterator[Any] {

  private val numResults = partialAggregateResult.length

  private var index = 0

  override def hasNext: Boolean = index < numResults

  override def next(): Any = {
    val data = partialAggregateResult(index)
    index += 1
    data
  }
}

object CachedDataFrame
    extends ((TaskContext, Iterator[InternalRow]) => PartitionResult)
    with Serializable with KryoSerializable with Logging {

  override def write(kryo: Kryo, output: Output): Unit = {}

  override def read(kryo: Kryo, input: Input): Unit = {}

  private def flushBufferOutput(bufferOutput: Output, position: Int,
      output: Output, codec: CompressionCodec): Unit = {
    if (position > 0) {
      val compressedBytes = Utils.codecCompress(codec,
        bufferOutput.getBuffer, position)
      val len = compressedBytes.length
      // write the uncompressed length too
      output.writeVarInt(position, true)
      output.writeVarInt(len, true)
      output.writeBytes(compressedBytes, 0, len)
      bufferOutput.clear()
    }
  }

  override def apply(context: TaskContext,
      iter: Iterator[InternalRow]): PartitionResult = {
    var count = 0
    val buffer = new Array[Byte](4 << 10) // 4K
    // final output is written to this buffer
    val output = new Output(4 << 10, -1)
    // holds intermediate bytes which are compressed and flushed to output
    val maxOutputBufferSize = 64 << 10 // 64K
    // can't enforce maxOutputBufferSize due to a row larger than that limit
    val bufferOutput = new Output(4 << 10, -1)
    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    while (iter.hasNext) {
      val row = iter.next().asInstanceOf[UnsafeRow]
      val numBytes = row.getSizeInBytes
      // if capacity has been exceeded then compress and store
      val bufferPosition = bufferOutput.position()
      if (maxOutputBufferSize - bufferPosition < numBytes + 5) {
        flushBufferOutput(bufferOutput, bufferPosition, output, codec)
      }
      bufferOutput.writeVarInt(numBytes, true)
      row.writeToStream(bufferOutput, buffer)
      count += 1
    }
    flushBufferOutput(bufferOutput, bufferOutput.position(), output, codec)
    if (count > 0) {
      if (output.position() == output.getBuffer.length) {
        new PartitionResult(output.getBuffer, count)
      } else {
        new PartitionResult(output.toBytes, count)
      }
    } else {
      new PartitionResult(Array.empty, 0)
    }
  }

  /**
   * Minimum size of block beyond which data will be stored in BlockManager
   * before being consumed to store data from multiple partitions safely.
   */
  @transient val MIN_LOCAL_BLOCK_SIZE: Int = 32 * 1024 // 32K

  def localBlockStoreResultHandler(rddId: Int, bm: BlockManager)(
      partitionId: Int, data: Array[Byte]): Any = {
    // put in block manager only if result is large
    if (data.length <= MIN_LOCAL_BLOCK_SIZE) data
    else {
      val blockId = RDDBlockId(rddId, partitionId)
      bm.putBytes(blockId, Utils.newChunkedByteBuffer(Array(ByteBuffer.wrap(
        data))), StorageLevel.MEMORY_AND_DISK_SER, tellMaster = false)
      blockId
    }
  }

  def localBlockStoreDecoder(numFields: Int, bm: BlockManager)(
      block: Any): Iterator[UnsafeRow] = block match {
    case null => Iterator.empty
    case data: Array[Byte] => decodeUnsafeRows(numFields, data, 0, data.length)
    case id: RDDBlockId =>
      val data = Utils.getPartitionData(id, bm)
      // remove the block once a local handle to it has been obtained
      bm.removeBlock(id, tellMaster = false)
      decodeUnsafeRows(numFields, data.array(),
        data.arrayOffset() + data.position(), data.remaining())
  }

  @transient private val nextExecutionIdMethod = {
    val m = SQLExecution.getClass.getDeclaredMethod("nextExecutionId")
    m.setAccessible(true)
    m
  }

  /**
   * Wrap an action that will execute "queryExecution" to track all Spark jobs
   * in the body so that we can connect them with an execution.
   *
   * Custom method to allow passing in cached SparkPlanInfo and queryExecution string.
   */
  def withNewExecutionId[T](sparkSession: SparkSession,
      callSite: CallSite, queryExecutionStr: String,
      queryPlanInfo: SparkPlanInfo)(body: => T): T = {
    val sc = sparkSession.sparkContext
    val oldExecutionId = sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    if (oldExecutionId == null) {
      val executionId = nextExecutionIdMethod.invoke(SQLExecution).asInstanceOf[Long]
      sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, executionId.toString)
      val r = try {
        sparkSession.sparkContext.listenerBus.post(SparkListenerSQLExecutionStart(
          executionId, callSite.shortForm, callSite.longForm, queryExecutionStr,
          queryPlanInfo, System.currentTimeMillis()))
        try {
          body
        } finally {
          sparkSession.sparkContext.listenerBus.post(SparkListenerSQLExecutionEnd(
            executionId, System.currentTimeMillis()))
        }
      } finally {
        sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, null)
      }
      r
    } else {
      // Don't support nested `withNewExecutionId`.
      throw new IllegalArgumentException(
        s"${SQLExecution.EXECUTION_ID_KEY} is already set")
    }
  }

  /**
   * Decode the byte arrays back to UnsafeRows and put them into buffer.
   */
  def decodeUnsafeRows(numFields: Int,
      data: Array[Byte], offset: Int, dataLen: Int): Iterator[UnsafeRow] = {
    if (dataLen == 0) return Iterator.empty

    val codec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val input = new Input(data, offset, dataLen)
    val dataLimit = offset + dataLen
    var decompressedLen = input.readVarInt(true)
    var inputLen = input.readVarInt(true)
    val inputPosition = input.position()
    val bufferInput = new Input(Utils.codecDecompress(codec, data,
      inputPosition, inputLen, decompressedLen))
    input.setPosition(inputPosition + inputLen)

    new Iterator[UnsafeRow] {
      private var sizeOfNextRow = bufferInput.readVarInt(true)

      override def hasNext: Boolean = sizeOfNextRow >= 0

      override def next(): UnsafeRow = {
        val row = new UnsafeRow(numFields)
        val position = bufferInput.position()
        row.pointTo(bufferInput.getBuffer,
          position + Platform.BYTE_ARRAY_OFFSET, sizeOfNextRow)
        val newPosition = position + sizeOfNextRow

        sizeOfNextRow = if (newPosition < decompressedLen) {
          bufferInput.setPosition(newPosition)
          bufferInput.readVarInt(true)
        } else if (input.position() < dataLimit) {
          decompressedLen = input.readVarInt(true)
          inputLen = input.readVarInt(true)
          val inputPosition = input.position()
          bufferInput.setBuffer(Utils.codecDecompress(codec, data,
            inputPosition, inputLen, decompressedLen))
          input.setPosition(inputPosition + inputLen)
          bufferInput.readVarInt(true)
        } else -1
        row
      }
    }
  }

  private def takeRows[U, R](n: Int, results: Array[(R, Int)],
      processPartition: (TaskContext, Iterator[InternalRow]) => (U, Int),
      resultHandler: (Int, U) => R,
      decodeResult: R => Iterator[InternalRow]): Iterator[R] = {
    val takeResults = new ArrayBuffer[R](n)
    var numRows = 0
    results.indices.foreach { index =>
      val r = results(index)
      if ((r ne null) && r._1 != null) {
        if (numRows + r._2 <= n) {
          takeResults += r._1
          numRows += r._2
        } else {
          // need to split this partition result to take only remaining rows
          val decoded = decodeResult(r._1).take(n - numRows)
          // encode back and add
          takeResults += resultHandler(index,
            processPartition(TaskContext.get(), decoded)._1)
          return takeResults.iterator
        }
      }
    }
    takeResults.iterator
  }

  /**
   * Runs this query returning the first `n` rows as an array.
   *
   * This is modeled after RDD.take but never runs any job locally on the driver.
   */
  private[sql] def executeTake[U: ClassTag, R](rdd: RDD[InternalRow], n: Int,
      processPartition: (TaskContext, Iterator[InternalRow]) => (U, Int),
      resultHandler: (Int, U) => R, decodeResult: R => Iterator[InternalRow],
      schema: StructType, session: SparkSession): Iterator[R] = {
    if (n == 0) {
      return Iterator.empty
    }

    var numResults = 0
    val totalParts = rdd.partitions.length
    var partsScanned = 0
    val results = new Array[(R, Int)](totalParts)
    while (numResults < n && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this
      // number to be greater than totalParts because we actually cap it at
      // totalParts in runJob.
      var numPartsToTry = 1L
      if (partsScanned > 0) {
        // If we didn't find any rows after the first iteration, just try all
        // partitions next. Otherwise, interpolate the number of partitions
        // we need to try, but overestimate it by 50%.
        if (numResults == 0) {
          numPartsToTry = totalParts - 1
        } else {
          numPartsToTry = (1.5 * n * partsScanned / numResults).toInt
        }
      }
      // guard against negative num of partitions
      numPartsToTry = math.max(0, numPartsToTry)

      val p = partsScanned.until(math.min(partsScanned + numPartsToTry,
        totalParts).toInt)
      val sc = session.sparkContext
      sc.runJob(rdd, processPartition, p, (index: Int, r: (U, Int)) => {
        results(index) = (resultHandler(index, r._1), r._2)
        numResults += r._2
      })

      partsScanned += p.size
    }

    if (numResults > n) {
      takeRows(n, results, processPartition, resultHandler, decodeResult)
    } else {
      results.iterator collect {
        case r if (r ne null) && r._1 != null => r._1
      }
    }
  }

  /*
  def executeCollect(plan: TakeOrderedAndProjectExec,
      rdd: RDD[InternalRow]): Array[InternalRow] = {
    val child = plan.child
    val ord = new LazilyGeneratedOrdering(plan.sortOrder, child.output)
    val data = rdd.map(_.copy()).takeOrdered(plan.limit)(ord)
    if (plan.projectList.isDefined) {
      val proj = UnsafeProjection.create(plan.projectList.get, child.output)
      data.map(r => proj(r).copy())
    } else {
      data
    }
  }
  */

  override def toString(): String =
    s"CachedDataFrame: Iterator[InternalRow] => Array[Byte]"
}

/**
 * Encapsulates result of a partition having data and number of rows.
 *
 * Note: this uses an optimized external serializer for PooledKryoSerializer
 * so any changes to this class need to be reflected in the serializer.
 */
class PartitionResult(_data: Array[Byte], _numRows: Int)
    extends Tuple2[Array[Byte], Int](_data, _numRows) with Serializable
