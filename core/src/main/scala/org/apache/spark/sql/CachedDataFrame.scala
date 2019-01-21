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

import java.nio.ByteBuffer

import scala.annotation.tailrec
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.gemstone.gemfire.cache.LowMemoryException
import com.gemstone.gemfire.internal.shared.ClientSharedUtils
import com.gemstone.gemfire.internal.shared.unsafe.DirectBufferAllocator
import com.gemstone.gemfire.internal.{ByteArrayDataInput, ByteBufferDataOutput}
import io.snappydata.Constant

import org.apache.spark._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.memory.DefaultMemoryConsumer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.CodeAndComment
import org.apache.spark.sql.catalyst.expressions.{ParamLiteral, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.CollectAggregateExec
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.sql.store.CompressionUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.{BlockManager, RDDBlockId, StorageLevel}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.CallSite

class CachedDataFrame(snappySession: SnappySession, queryExecution: QueryExecution,
    private[sql] val queryExecutionString: String,
    private[sql] val queryPlanInfo: SparkPlanInfo,
    private[sql] var currentQueryExecutionString: String,
    private[sql] var currentQueryPlanInfo: SparkPlanInfo,
    cachedRDD: RDD[InternalRow], shuffleDependencies: Array[Int], encoder: Encoder[Row],
    shuffleCleanups: Array[Future[Unit]], val rddId: Int, noSideEffects: Boolean,
    val queryHints: java.util.Map[String, String], private[sql] var currentExecutionId: Long,
    private[sql] var planStartTime: Long, private[sql] var planEndTime: Long,
    val linkPart : Boolean = false)
    extends Dataset[Row](snappySession, queryExecution, encoder) with Logging {

  private[sql] final def isCached: Boolean = cachedRDD ne null

  private def getExecRDD: RDD[InternalRow] =
    if (isCached) cachedRDD else queryExecution.executedPlan.execute()

  @transient
  private var _boundEnc: ExpressionEncoder[Row] = _

  // not using lazy val so that duplicate() can copy over existing value, if any
  private def boundEnc: ExpressionEncoder[Row] = {
    if (_boundEnc ne null) _boundEnc
    else {
      _boundEnc = exprEnc.resolveAndBind(logicalPlan.output,
        snappySession.sessionState.analyzer)
      _boundEnc
    }
  }

  private def isLowLatencyQuery: Boolean = {
    // use a small value for number of RDD partitions
    isCached && shuffleDependencies.length == 0 && cachedRDD.getNumPartitions <= 2
  }

  @transient
  private var _rowConverter: UnsafeProjection = _

  // not using lazy val so that duplicate() can copy over existing value, if any
  private def rowConverter: UnsafeProjection = {
    if (_rowConverter ne null) _rowConverter
    else {
      _rowConverter = UnsafeProjection.create(queryExecution.executedPlan.schema)
      _rowConverter
    }
  }

  private[sql] var paramLiterals: Array[ParamLiteral] = _
  private[sql] var paramsId: Int = _

  @transient
  private[sql] var currentLiterals: Array[ParamLiteral] = _

  @transient
  private[sql] var queryShortString: String = _

  @transient
  private[sql] var queryString: String = _

  @transient
  private var prepared: Boolean = _

  private[sql] def startShuffleCleanups(sc: SparkContext): Unit = {
    val numShuffleDeps = shuffleDependencies.length
    if (numShuffleDeps > 0) {
      sc.cleaner match {
        case Some(cleaner) =>
          var i = 0
          while (i < numShuffleDeps) {
            val shuffleDependency = shuffleDependencies(i)
            // Cleaning the  shuffle artifacts asynchronously
            shuffleCleanups(i) = Future {
              cleaner.doCleanupShuffle(shuffleDependency, blocking = true)
            }
            i += 1
          }
        case None =>
      }
    }
  }

  private[sql] def waitForPendingShuffleCleanups(): Unit = {
    val numShuffles = shuffleCleanups.length
    if (numShuffles > 0) {
      var i = 0
      while (i < numShuffles) {
        val cleanup = shuffleCleanups(i)
        if (cleanup ne null) {
          Await.ready(cleanup, Duration.Inf)
          shuffleCleanups(i) = null
        }
        i += 1
      }
    }
  }

  private[sql] def duplicate(): CachedDataFrame = {
    val cdf = new CachedDataFrame(snappySession, queryExecution, queryExecutionString,
      queryPlanInfo, null, null, cachedRDD, shuffleDependencies, encoder, shuffleCleanups,
      rddId, noSideEffects, queryHints, -1L, -1L, -1L, linkPart)
    cdf.log_ = log_
    cdf.levelFlags = levelFlags
    cdf._boundEnc = boundEnc // force materialize boundEnc which is commonly used
    cdf._rowConverter = _rowConverter
    cdf.paramLiterals = paramLiterals
    cdf.paramsId = paramsId
    cdf
  }

  private def reset(): Unit = clearPartitions(cachedRDD :: Nil)

  private def applyCurrentLiterals(): Unit = {
    if (paramLiterals.length > 0 && (paramLiterals ne currentLiterals) &&
        (currentLiterals ne null)) {
      for (pos <- paramLiterals.indices) {
        paramLiterals(pos).value = currentLiterals(pos).value
      }
    }
  }

  private def getChildren(parent: RDD[_]): Seq[RDD[_]] = {
    parent match {
      case rdd: DelegateRDD[_] =>
        (rdd.baseRdd +: (rdd.dependencies.map(_.rdd) ++ rdd.otherRDDs)).distinct
      case _ => parent.dependencies.map(_.rdd)
    }
  }

  @tailrec
  private def clearPartitions(rdds: Seq[RDD[_]]): Unit = {
    val children = rdds.flatMap {
      case null => Nil
    case r =>
        // f.set(r, null)
        Platform.putObjectVolatile(r, Utils.rddPartitionsOffset, null)
        getChildren(r)
    }
    if (children.nonEmpty) {
      clearPartitions(children)
    }
  }

  // Its a bit costly operation,
  // but we are safe as anyway the partitions will be evaluated during RDD execution time.
  // Once partitions are determined on an RDD it will not do an evaluation again.
  @tailrec
  private def reEvaluatePartitions(rdds: Seq[RDD[_]]): Unit = {
    val children = rdds.flatMap {
      case null => Nil
      case r =>
        r.getNumPartitions
        getChildren(r)

    }
    if (children.nonEmpty) {
      reEvaluatePartitions(children)
    }
  }

  private def setPoolForExecution(): Unit = {
    var pool = snappySession.sessionState.conf.activeSchedulerPool
    // Check if it is pruned query, execute it automatically on the low latency pool
    if (isLowLatencyQuery && pool == "default") {
      if (snappySession.sparkContext.getPoolForName(Constant.LOW_LATENCY_POOL).isDefined) {
        pool = Constant.LOW_LATENCY_POOL
      }
    }
    snappySession.sparkContext.setLocalProperty("spark.scheduler.pool", pool)
  }


  private def prepareForCollect(): Boolean = {
    if (prepared) return false
    if (isCached) {
      reset()
      applyCurrentLiterals()
    }
    // Reset the linkPartitionToBuckets flag before determining RDD partitions.
    snappySession.linkPartitionsToBuckets(flag = linkPart)
    // Forcibly re-evaluate the partitions.
    reEvaluatePartitions(cachedRDD :: Nil)
    setPoolForExecution()
    // update the strings in query execution and planInfo
    if (currentQueryExecutionString eq null) {
      currentQueryExecutionString = SnappySession.replaceParamLiterals(
        queryExecutionString, currentLiterals, paramsId)
      currentQueryPlanInfo = PartitionedPhysicalScan.updatePlanInfo(
        queryPlanInfo, currentLiterals, paramsId)
    }
    // set the query hints as would be set at the end of un-cached sql()
    snappySession.synchronized {
      snappySession.queryHints.clear()
      snappySession.queryHints.putAll(queryHints)
    }
    queryExecution.executedPlan.foreach(_.resetMetrics())
    waitForPendingShuffleCleanups()
    prepared = true
    true
  }

  private def endCollect(didPrepare: Boolean): Unit = {
    if (didPrepare) {
      prepared = false
      // reset the pool
      if (isLowLatencyQuery) {
        val pool = snappySession.sessionState.conf.activeSchedulerPool
        snappySession.sparkContext.setLocalProperty("spark.scheduler.pool", pool)
      }
      // clear the shuffle dependencies asynchronously after the execution.
      startShuffleCleanups(snappySession.sparkContext)
    }
  }

  /**
   * Wrap a Dataset action to track the QueryExecution and time cost,
   * then report to the user-registered callback functions.
   */
  private def withCallback[U](name: String)(action: DataFrame => (U, Long)): U = {
    var didPrepare = false
    try {
      didPrepare = prepareForCollect()
      CachedDataFrame.withCallback(snappySession, this, queryExecution, name)(action)
    } finally {
      endCollect(didPrepare)
    }
  }

  override def collect(): Array[Row] = {
    collectInternal().map(boundEnc.fromRow).toArray
  }

  override def withNewExecutionId[T](body: => T): T = withNewExecutionIdTiming(body)._1

  private def withNewExecutionIdTiming[T](body: => T): (T, Long) = if (noSideEffects) {
    var didPrepare = false
    try {
      didPrepare = prepareForCollect()
      val (result, elapsedMillis) = CachedDataFrame.withNewExecutionId(snappySession,
        queryShortString, queryString, currentQueryExecutionString, currentQueryPlanInfo,
        currentExecutionId, planStartTime, planEndTime)(body)
      (result, elapsedMillis * 1000000L)
    } finally {
      if (isCached) {
        currentExecutionId = -1L
        planStartTime = -1L
        planEndTime = -1L
      }
      endCollect(didPrepare)
    }
  } else {
    // don't create a new executionId for ExecutedCommandExec/ExecutePlan
    // since they have already done so
    val start = System.nanoTime()
    val result = body
    (result, System.nanoTime() - start)
  }

  override def count(): Long = withCallback("count") { df =>
    val start = System.nanoTime()
    val result = df.groupBy().count().collect().head.getLong(0)
    (result, System.nanoTime() - start)
  }

  override def head(n: Int): Array[Row] = withCallback("head") { df =>
    val start = System.nanoTime()
    val result = df.limit(n).collect()
    (result, System.nanoTime() - start)
  }

  override def collectAsList(): java.util.List[Row] = {
    java.util.Arrays.asList(collect(): _*)
  }

  override def toLocalIterator(): java.util.Iterator[Row] = {
    collectInternal().map(boundEnc.fromRow).asJava
  }

  def collectInternal(): Iterator[InternalRow] = {
    if (noSideEffects) {
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
    val sc = snappySession.sparkContext
    val hasLocalCallSite = sc.getLocalProperties.containsKey(CallSite.LONG_FORM)
    if (!hasLocalCallSite) {
      sc.setCallSite(sc.getCallSite())
    }
    val (executedPlan, withFallback) = SnappySession.getExecutedPlan(queryExecution.executedPlan)

    def execute(): (Iterator[R], Long) = withNewExecutionIdTiming {

      def executeCollect(): Array[InternalRow] = {
        if (withFallback ne null) withFallback.executeCollect()
        else executedPlan.executeCollect()
      }

      executedPlan match {
        case plan: CollectLimitExec =>
          CachedDataFrame.executeTake(getExecRDD, plan.limit, processPartition,
            resultHandler, decodeResult, schema, snappySession)

        case plan: CollectAggregateExec =>
          if (skipLocalCollectProcessing) {
            // special case where caller will do processing of the blocks
            // (returns a AggregatePartialDataIterator)
            // TODO: handle fallback for this case
            new AggregatePartialDataIterator(plan.generatedSource,
              plan.generatedReferences, plan.child.schema.length,
              plan.executeCollectData()).asInstanceOf[Iterator[R]]
          } else if (skipUnpartitionedDataProcessing) {
            // no processing required
            executeCollect().iterator.asInstanceOf[Iterator[R]]
          } else {
            // convert to UnsafeRow
            Iterator(resultHandler(0, processPartition(TaskContext.get(),
              executeCollect().iterator.map(rowConverter))._1))
          }

        case _: ExecutedCommandExec | _: LocalTableScanExec | _: ExecutePlan =>
          if (skipUnpartitionedDataProcessing) {
            // no processing required
            executeCollect().iterator.asInstanceOf[Iterator[R]]
          } else {
            // convert to UnsafeRow
            Iterator(resultHandler(0, processPartition(TaskContext.get(),
              executeCollect().iterator.map(rowConverter))._1))
          }

        case _ =>
          if (skipUnpartitionedDataProcessing) {
            // no processing required
            executeCollect().iterator.asInstanceOf[Iterator[R]]
          } else {
            val rdd = getExecRDD
            val numPartitions = rdd.getNumPartitions
            val results = new Array[R](numPartitions)
            sc.runJob(rdd, processPartition, 0 until numPartitions,
              (index: Int, r: (U, Int)) =>
                results(index) = resultHandler(index, r._1))
            results.iterator
          }
      }
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

  @transient @volatile var sparkConf: SparkConf = _
  @transient @volatile var compressionCodec: String = _

  override def write(kryo: Kryo, output: Output): Unit = {}

  override def read(kryo: Kryo, input: Input): Unit = {}

  private def flushBufferOutput(bufferOutput: Output, position: Int,
      output: ByteBufferDataOutput, codec: CompressionCodec): Unit = {
    if (position > 0) {
      val compressedBytes = CompressionUtils.codecCompress(codec,
        bufferOutput.getBuffer, position)
      val len = compressedBytes.length
      // write the uncompressed length too
      output.writeInt(position)
      output.writeInt(len)
      output.write(compressedBytes, 0, len)
      bufferOutput.clear()
    }
  }

  private def getCompressionCodec: CompressionCodec = {
    var conf = sparkConf
    var codecName = compressionCodec
    if ((conf eq null) || (codecName eq null)) synchronized {
      conf = sparkConf
      codecName = compressionCodec
      if ((conf eq null) || (codecName eq null)) {
        SparkEnv.get match {
          case null => conf = new SparkConf()
          case env => conf = env.conf
        }
        codecName = CompressionCodec.getCodecName(conf)
        sparkConf = conf
        compressionCodec = codecName
      }
    }
    CompressionCodec.createCodec(conf, codecName)
  }

  override def apply(context: TaskContext,
      iter: Iterator[InternalRow]): PartitionResult = {
    var count = 0
    val buffer = new Array[Byte](4 << 10)

    // final output is written to this buffer
    var output: ByteBufferDataOutput = null
    // holds intermediate bytes which are compressed and flushed to output
    val maxOutputBufferSize = 64 << 10

    // can't enforce maxOutputBufferSize due to a row larger than that limit
    val bufferOutput = new Output(4 << 10, -1)
    var outputRetained = false
    try {
      output = new ByteBufferDataOutput(4 << 10,
        DirectBufferAllocator.instance(),
        null,
        DirectBufferAllocator.DIRECT_STORE_DATA_FRAME_OUTPUT)

      val codec = getCompressionCodec
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
        val finalBuffer = output.getBufferRetain
        outputRetained = true
        finalBuffer.flip
        val memSize = finalBuffer.limit().toLong
        // Ask UMM before getting the array to heap.
        // Taking execution memory as this memory is cleaned up on task completion.
        // On connector mode also this should account to the overall memory usage.
        // We will ensure that sufficient memory is available by reserving
        // four times as Kryo serialization will expand its buffer accordingly
        // and transport layer can create another copy.
        if (context ne null) {
          val memoryConsumer = new DefaultMemoryConsumer(context.taskMemoryManager())
          // TODO Remove the 4 times check once SNAP-1759 is fixed
          val required = 4L * memSize
          val granted = memoryConsumer.acquireMemory(4L * memSize)
          context.addTaskCompletionListener(_ => {
            memoryConsumer.freeMemory(granted)
          })
          if (granted < required) {
            throw new LowMemoryException(s"Could not obtain ${memoryConsumer.getMode} " +
                s"memory of size $required ",
              java.util.Collections.emptySet())
          }
        }

        val bytes = ClientSharedUtils.toBytes(finalBuffer)
        new PartitionResult(bytes, count)
      } else {
        new PartitionResult(Array.empty, 0)
      }
    } catch {
      case oom: OutOfMemoryError if oom.getMessage.contains("Direct buffer") =>
        throw new LowMemoryException(s"Could not allocate Direct buffer for" +
            s" result data. Please check -XX:MaxDirectMemorySize while starting the server",
          java.util.Collections.emptySet())
    } finally {
      bufferOutput.clear()
      // one additional release for the explicit getBufferRetain
      if (output ne null) {
        if (outputRetained) {
          output.release()
        }
        output.release()
      }
    }
  }

  def localBlockStoreResultHandler(rddId: Int, bm: BlockManager)(
      partitionId: Int, data: Array[Byte]): Any = {
    // put in block manager only if result is large
    if (data.length <= Utils.MIN_LOCAL_BLOCK_SIZE) data
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

  private[sql] def queryStringShortForm(queryString: String): String = {
    val trimSize = 100
    if (queryString.length > trimSize) {
      queryString.substring(0, trimSize).concat("...")
    } else queryString
  }

  /**
   * Wrap an action that will execute "queryExecution" to track all Spark jobs
   * in the body so that we can connect them with an execution.
   *
   * Custom method to allow passing in cached SparkPlanInfo and queryExecution string.
   */
  def withNewExecutionId[T](snappySession: SnappySession, queryShortForm: String,
      queryLongForm: String, queryExecutionStr: String, queryPlanInfo: SparkPlanInfo,
      currentExecutionId: Long = -1L, planStartTime: Long = -1L, planEndTime: Long = -1L,
      postGUIPlans: Boolean = true)(body: => T): (T, Long) = {
    val sc = snappySession.sparkContext
    val localProperties = sc.getLocalProperties
    val oldExecutionId = localProperties.getProperty(SQLExecution.EXECUTION_ID_KEY)
    if (oldExecutionId eq null) {
      // If the execution ID is set in the CDF that means the plan execution has already
      // been done. Use the same execution ID to link this execution to the previous one.
      val executionId = if (currentExecutionId >= 0) currentExecutionId
      else Utils.nextExecutionIdMethod.invoke(SQLExecution).asInstanceOf[Long]
      val executionIdStr = java.lang.Long.toString(executionId)
      localProperties.setProperty(SQLExecution.EXECUTION_ID_KEY, executionIdStr)
      localProperties.setProperty(SparkContext.SPARK_JOB_DESCRIPTION, queryShortForm)
      localProperties.setProperty(SparkContext.SPARK_JOB_GROUP_ID, executionIdStr)

      val startTime = System.currentTimeMillis()
      var endTime = -1L
      try {
        if (postGUIPlans) sc.listenerBus.post(SparkListenerSQLExecutionStart(executionId,
          queryShortForm, queryLongForm, queryExecutionStr, queryPlanInfo, startTime))
        val result = body
        endTime = System.currentTimeMillis()
        (result, endTime - startTime)
      } finally {
        if (endTime == -1L) endTime = System.currentTimeMillis()
        // the total duration displayed will be completion time provided below
        // minus the start time of either above, or else the start time of
        // original planning submission, so adjust the endTime accordingly
        if (planEndTime != -1L) {
          endTime -= (startTime - planEndTime)
        }
        // add the time of plan execution to the end time.
        if (postGUIPlans) sc.listenerBus.post(SparkListenerSQLExecutionEnd(executionId, endTime))

        localProperties.remove(SparkContext.SPARK_JOB_GROUP_ID)
        localProperties.remove(SparkContext.SPARK_JOB_DESCRIPTION)
        localProperties.remove(SQLExecution.EXECUTION_ID_KEY)
      }
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

    val codec = getCompressionCodec
    val input = new ByteArrayDataInput
    input.initialize(data, offset, dataLen, null)
    val dataLimit = offset + dataLen
    var decompressedLen = input.readInt()
    var inputLen = input.readInt()
    val inputPosition = input.position()
    val bufferInput = new Input(CompressionUtils.codecDecompress(codec, data,
      inputPosition, inputLen, decompressedLen))
    input.setPosition(inputPosition + inputLen)

    new Iterator[UnsafeRow] {
      private var sizeOfNextRow = bufferInput.readInt(true)

      override def hasNext: Boolean = sizeOfNextRow >= 0

      override def next(): UnsafeRow = {
        val row = new UnsafeRow(numFields)
        val position = bufferInput.position()
        row.pointTo(bufferInput.getBuffer,
          position + Platform.BYTE_ARRAY_OFFSET, sizeOfNextRow)
        val newPosition = position + sizeOfNextRow

        sizeOfNextRow = if (newPosition < decompressedLen) {
          bufferInput.setPosition(newPosition)
          bufferInput.readInt(true)
        } else if (input.position() < dataLimit) {
          decompressedLen = input.readInt()
          inputLen = input.readInt()
          val inputPosition = input.position()
          bufferInput.setBuffer(CompressionUtils.codecDecompress(codec, data,
            inputPosition, inputLen, decompressedLen))
          input.setPosition(inputPosition + inputLen)
          bufferInput.readInt(true)
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
      schema: StructType, session: SnappySession): Iterator[R] = {
    if (n == 0) {
      return Iterator.empty
    }

    val takeRDD = if (n > 0) rdd.mapPartitionsInternal(_.take(n)) else rdd
    var numResults = 0
    val totalParts = takeRDD.partitions.length
    var partsScanned = 0
    val results = new Array[(R, Int)](totalParts)
    while (numResults < n && partsScanned < totalParts) {
      // The number of partitions to try in this iteration. It is ok for this
      // number to be greater than totalParts because we actually cap it at
      // totalParts in runJob.
      var numPartsToTry = 1L
      if (partsScanned > 0) {
        // If we didn't find any rows after the previous iteration, quadruple and retry.
        // Otherwise, interpolate the number of partitions we need to try, but overestimate
        // it by 50%. We also cap the estimation in the end.
        val limitScaleUpFactor = Math.max(session.sessionState.conf.limitScaleUpFactor, 2)
        if (numResults == 0) {
          numPartsToTry = partsScanned * limitScaleUpFactor
        } else {
          // the left side of max is >=1 whenever partsScanned >= 2
          numPartsToTry = Math.max((1.5 * n * partsScanned / numResults).toInt - partsScanned, 1)
          numPartsToTry = Math.min(numPartsToTry, partsScanned * limitScaleUpFactor)
        }
      }
      // guard against negative num of partitions
      numPartsToTry = math.max(0, numPartsToTry)

      val p = partsScanned.until(math.min(partsScanned + numPartsToTry,
        totalParts).toInt)
      val sc = session.sparkContext
      sc.runJob(takeRDD, processPartition, p, (index: Int, r: (U, Int)) => {
        results(partsScanned + index) = (resultHandler(index, r._1), r._2)
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

  /**
   * Wrap a Dataset action to track the QueryExecution and time cost,
   * then report to the user-registered callback functions.
   */
  def withCallback[U](session: SparkSession, df: DataFrame, queryExecution: QueryExecution,
      name: String)(action: DataFrame => (U, Long)): U = {
    try {
      val (result, elapsed) = action(df)
      session.listenerManager.onSuccess(name, queryExecution, elapsed)
      result
    } catch {
      case e: Exception =>
        session.listenerManager.onFailure(name, queryExecution, e)
        throw e
    }
  }

  private[sql] def clear(): Unit = synchronized {
    sparkConf = null
    compressionCodec = null
  }

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
