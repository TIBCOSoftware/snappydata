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
package org.apache.spark.sql

import java.io.{BufferedOutputStream, DataInputStream, DataOutputStream, FileOutputStream, OutputStream}
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, ByteOrder}
import java.sql.SQLException
import java.util.concurrent.atomic.AtomicLong

import scala.annotation.tailrec
import scala.collection.AbstractIterator
import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.gemstone.gemfire.SystemFailure
import com.gemstone.gemfire.cache.LowMemoryException
import com.gemstone.gemfire.internal.shared.ClientSharedUtils
import com.gemstone.gemfire.internal.{ByteArrayDataInput, ByteBufferOutputStream}
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState
import io.snappydata.{Constant, Property}

import org.apache.spark._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.memory.DefaultMemoryConsumer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.CodeAndComment
import org.apache.spark.sql.catalyst.expressions.{ParamLiteral, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.collection.{ToolsCallbackInit, Utils}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.CollectAggregateExec
import org.apache.spark.sql.execution.columnar.ExternalStoreUtils
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.storage.{BlockId, BlockManager, BroadcastBlockId, RDDBlockId, StorageLevel}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.io.ChunkedByteBuffer
import org.apache.spark.util.{ByteBufferInputStream, CallSite}

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

  @transient @volatile
  private[this] var pendingBlockIdForLargeResults = -1L
  @transient @volatile
  private[this] var activeBlockIdForLargeResults = -1L

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
            }(CommonUtils.waiterExecutionContext)
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
          CommonUtils.awaitResult(cleanup, Duration.Inf)
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

  override def withNewExecutionId[T](body: => T): T = withNewExecutionIdTiming(body)._1

  private def withNewExecutionIdTiming[T](body: => T): (T, Long) = if (noSideEffects) {
    var didPrepare = false
    try {
      didPrepare = prepareForCollect()
      val (result, elapsedMillis) = CachedDataFrame.withNewExecutionId(snappySession,
        queryShortString, queryString, currentQueryExecutionString, currentQueryPlanInfo,
        queryExecution.executedPlan, currentExecutionId, planEndTime)(body)
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

  override def collect(): Array[Row] = {
    collectInternal().map(boundEnc.fromRow).toArray
  }

  override def collectAsList(): java.util.List[Row] = {
    collectInternal().map(boundEnc.fromRow).toList.asJava
  }

  override def toLocalIterator(): java.util.Iterator[Row] = {
    val rows = collectInternal()
    new java.util.Iterator[Row] with AutoCloseable {
      override def hasNext: Boolean = rows.hasNext

      override def next(): Row = boundEnc.fromRow(rows.next())

      override def close(): Unit = rows match {
        case a: AutoCloseable => a.close()
        case _ =>
      }
    }
  }

  def collectInternal(): Iterator[InternalRow] = {
    if (noSideEffects) {
      val iters = collectWithHandler[Array[Byte], Iterator[UnsafeRow]](
        CachedDataFrame(this), (_, result) => {
          val data = result._1
          if (result._2 < 0) registerBlockIdForLargeResults()
          CachedDataFrame.decodeUnsafeRows(schema.length, data, 0, data.length)
        }, identity)
      new AbstractIterator[InternalRow] with AutoCloseable {
        private[this] var iter: Iterator[InternalRow] = Iterator.empty
        private[this] val activeBlockId = activeBlockIdForLargeResults

        @tailrec
        def hasNext: Boolean = {
          if (iter.hasNext) true else if (iters.hasNext) {
            iter = iters.next()
            hasNext
          } else {
            close()
            false
          }
        }

        def next(): InternalRow =
          if (hasNext) iter.next() else throw new NoSuchElementException("end already reached")

        override def close(): Unit = if (activeBlockId >= 0) {
          snappySession.largeResultBlockIdsForCleanup.invalidate(activeBlockId)
        }
      }
    } else {
      // skip double encoding/decoding for the case when underlying plan
      // execution returns an Iterator[InternalRow] itself
      collectWithHandler[InternalRow, InternalRow](null, null, null,
        skipUnpartitionedDataProcessing = true)
    }
  }

  private def markPendingBlockIdForLargeResults(blockId: Long): Unit = {
    pendingBlockIdForLargeResults = blockId
    activeBlockIdForLargeResults = -1L
  }

  // multiple threads can call this concurrently, hence the synchronized
  private def registerBlockIdForLargeResults(): Unit = synchronized {
    val blockId = pendingBlockIdForLargeResults
    assert(blockId >= 0)
    if (activeBlockIdForLargeResults != blockId) {
      activeBlockIdForLargeResults = blockId
      snappySession.largeResultBlockIdsForCleanup.put(blockId, true)
    }
  }

  def collectWithHandler[U: ClassTag, R: ClassTag](
      processPartition: (TaskContext, Iterator[InternalRow]) => (U, Int),
      resultHandler: (Int, (U, Int)) => R,
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
          val takeRDD = if (isCached) cachedRDD else plan.child.execute()
          CachedDataFrame.executeTake(takeRDD, plan.limit, processPartition,
            resultHandler, decodeResult, snappySession)

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
              executeCollect().iterator.map(rowConverter))))
          }

        case _: ExecutedCommandExec | _: LocalTableScanExec | _: ExecutePlan =>
          if (skipUnpartitionedDataProcessing) {
            // no processing required
            executeCollect().iterator.asInstanceOf[Iterator[R]]
          } else {
            // convert to UnsafeRow
            Iterator(resultHandler(0, processPartition(TaskContext.get(),
              executeCollect().iterator.map(rowConverter))))
          }

        case _ =>
          if (skipUnpartitionedDataProcessing) {
            // no processing required
            executeCollect().iterator.asInstanceOf[Iterator[R]]
          } else {
            try {
              val execRDD = getExecRDD
              runAsJob(execRDD, processPartition, resultHandler, sc)
            } catch {
              case t: Throwable
                if CachedDataFrame.isConnectorCatalogStaleException(t, snappySession) =>
                snappySession.sessionCatalog.invalidateAll()
                SnappySession.clearAllCache()
                val execution =
                  snappySession.getContextObject[() => QueryExecution](SnappySession.ExecutionKey)
                execution match {
                  case Some(exec) =>
                    CachedDataFrame.retryOnStaleCatalogException(snappySession = snappySession) {
                      val execRDD = exec().executedPlan.execute()
                      runAsJob(execRDD, processPartition, resultHandler, sc)
                    }
                  case _ => throw t
                }
            }

          }
      }
    }

    var success = false
    try {
      val result = withCallback("collect")(_ => execute())
      success = true
      result
    } finally {
      if (!hasLocalCallSite) {
        sc.clearCallSite()
      }
      if (!success) {
        // clear any persisted result data
        val activeBlockId = activeBlockIdForLargeResults
        if (activeBlockId >= 0) {
          snappySession.largeResultBlockIdsForCleanup.invalidate(activeBlockId)
        } else {
          val pendingBlockId = pendingBlockIdForLargeResults
          if (pendingBlockId >= 0) new BroadcastRemovalListener().removeBroadcast(pendingBlockId)
        }
      }
    }
  }

  private def runAsJob[R: ClassTag, U: ClassTag](
      execRdd: RDD[InternalRow],
      processPartition: (TaskContext, Iterator[InternalRow]) => (U, Int),
      resultHandler: (Int, (U, Int)) => R, sc: SparkContext): Iterator[R] = {
    val numPartitions = execRdd.getNumPartitions
    val results = new Array[R](numPartitions)
    sc.runJob(execRdd, processPartition, 0 until numPartitions,
      (index: Int, r: (U, Int)) =>
        results(index) = resultHandler(index, r))
    results.iterator
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
    extends ((TaskContext, Iterator[InternalRow], Long) => PartitionResult)
    with Serializable with KryoSerializable with Logging {

  @transient @volatile var sparkConf: SparkConf = _
  @transient @volatile var compressionCodecName: String = _
  /** size beyond which result data will be written to disk */
  @transient @volatile var maxMemoryResultSize: Long = Long.MaxValue
  /**
   * Start the local block ID generator at Long.Max / 2 so there is no overlap with those
   * generated for actual broadcast operations.
   */
  @transient private[this] val localBlockId = new AtomicLong(Long.MaxValue >> 1)

  override def write(kryo: Kryo, output: Output): Unit = {}

  override def read(kryo: Kryo, input: Input): Unit = {}

  private def getCompressionCodec: CompressionCodec = {
    var conf = sparkConf
    var codecName = compressionCodecName
    if ((conf eq null) || (codecName eq null)) synchronized {
      conf = sparkConf
      codecName = compressionCodecName
      if ((conf eq null) || (codecName eq null)) {
        SparkEnv.get match {
          case null => conf = new SparkConf()
          case env => conf = env.conf
        }
        codecName = CompressionCodec.getCodecName(conf)
        sparkConf = conf
        compressionCodecName = codecName
        maxMemoryResultSize = ExternalStoreUtils.sizeAsBytes(Property.MaxMemoryResultSize.get(
          conf), Property.MaxMemoryResultSize.name, 1024, Long.MaxValue)
      }
    }
    CompressionCodec.createCodec(conf, codecName)
  }

  private[this] def newLengthBuffer(len: Int): ByteBuffer = {
    val lenBuffer = ByteBuffer.wrap(new Array[Byte](4)).order(ByteOrder.BIG_ENDIAN)
    lenBuffer.putInt(0, len)
    lenBuffer
  }

  private[this] def flushStreamToDisk(baseStream: ByteBufferOutputStream,
      finalStream: DataOutputStream, context: TaskContext, broadcastId: Long,
      fileCount: Long, diskWriter: Option[(OutputStream, FileChannel)],
      flush: Boolean): Option[(OutputStream, FileChannel)] = {
    val outBuffer = baseStream.getContentBuffer
    val len = outBuffer.remaining()
    if (len == 0) return diskWriter
    diskWriter match {
      case None =>
        val env = SparkEnv.get
        // use the passed broadcast ID that cannot overlap with others
        val blockId = BroadcastBlockId(broadcastId, s"${context.partitionId()}_$fileCount")
        // write the size of the block at the start
        val lenBuffer = newLengthBuffer(len)
        // track the blockId using BlockManager and get handle to the disk file for append of
        // further data to the same file till a limit instead of separate files for every chunk
        if (!env.blockManager.putBytes(blockId, Utils.newChunkedByteBuffer(
          Array(lenBuffer, outBuffer)), StorageLevel.DISK_ONLY)) {
          throw new SparkException(s"Failed to store $blockId to BlockManager disk")
        }
        val blockName = blockId.name.getBytes(StandardCharsets.UTF_8)
        finalStream.writeInt(-blockName.length) // negative value < -1 indicates blockId
        finalStream.write(blockName)
        if (flush) None
        else {
          val fileBufferSize = env.conf.getSizeAsKb("spark.shuffle.file.buffer", "32k") * 1024
          val file = env.blockManager.diskBlockManager.getFile(blockId)
          val fileOut = new FileOutputStream(file, true)
          val out = new BufferedOutputStream(fileOut, fileBufferSize.toInt)
          val channel = fileOut.getChannel
          Some(out -> channel)
        }
      case Some((writer, channel)) =>
        // write the size of the block at the start
        val lenBuffer = newLengthBuffer(len)
        writer.write(lenBuffer.array(), 0, 4)
        writer.write(outBuffer.array(), outBuffer.arrayOffset() + outBuffer.position(), len)
        if (flush || channel.size() >= maxMemoryResultSize * 8) {
          // flush and close the file
          writer.flush()
          writer.close()
          None
        } else diskWriter
    }
  }

  def apply(cdf: CachedDataFrame): (TaskContext, Iterator[InternalRow]) => PartitionResult = {
    val broadcastId = localBlockId.getAndIncrement()
    cdf.markPendingBlockIdForLargeResults(broadcastId)
    (context, iter) => apply(context, iter, broadcastId)
  }

  override def apply(context: TaskContext, iter: Iterator[InternalRow],
      broadcastId: Long): PartitionResult = {
    val env = SparkEnv.get
    val persist = (env ne null) && (context ne null) && broadcastId >= 0
    var count = 0
    var fileCount = 1L
    val buffer = new Array[Byte](4 << 10)
    val codec = getCompressionCodec
    var baseStream = new ByteBufferOutputStream(8 << 10)
    var out = new DataOutputStream(codec.compressedOutputStream(baseStream))
    var finalBaseStream = baseStream
    var finalStream = out
    val maxMemorySize = maxMemoryResultSize
    var diskWriter: Option[(OutputStream, FileChannel)] = None
    var success = false
    try {
      while (iter.hasNext) {
        val row = iter.next().asInstanceOf[UnsafeRow]
        out.writeInt(row.getSizeInBytes)
        row.writeToStream(out, buffer)
        // flush to disk if required
        if (persist && baseStream.size() >= maxMemorySize) {
          // there is no clean way to switch the underlying OutputStream in the compressed stream
          // to the file stream, so flush and write the compressed data so far and start a new
          // compressed stream appending to the existing file which means that the file will
          // have multiple compressed streams back-to-back
          out.writeInt(-1)
          out.flush()
          out.close()
          if (finalStream eq out) {
            // switch finalStream to a new compressed stream that will contain the blockIds
            finalBaseStream = new ByteBufferOutputStream(8 << 10)
            finalStream = new DataOutputStream(codec.compressedOutputStream(finalBaseStream))
          }
          diskWriter = flushStreamToDisk(baseStream, finalStream, context, broadcastId, fileCount,
            diskWriter, flush = false)
          baseStream = new ByteBufferOutputStream(8 << 10)
          out = new DataOutputStream(codec.compressedOutputStream(baseStream))
          if (diskWriter.isEmpty) { // indicates that previous diskWriter was full and closed
            fileCount += 1
          }
        }
        count += 1
      }
      out.writeInt(-1)
      out.flush()
      out.close()
      if (finalStream ne out) {
        diskWriter = flushStreamToDisk(baseStream, finalStream, context, broadcastId, fileCount,
          diskWriter, flush = true)
        finalStream.writeInt(-1)
        finalStream.flush()
        finalStream.close()
      }

      if (count > 0) {
        val finalBuffer = finalBaseStream.getContentBuffer
        val memSize = finalBuffer.remaining().toLong
        // Taking execution memory as this memory is cleaned up on task completion.
        // On connector mode also this should account to the overall memory usage.
        // We will ensure that sufficient memory is available by reserving
        // four times as Kryo serialization will expand its buffer accordingly
        // and transport layer can create another copy.
        if (context ne null) {
          val memoryConsumer = new DefaultMemoryConsumer(context.taskMemoryManager())
          val granted = memoryConsumer.acquireMemory(memSize)
          context.addTaskCompletionListener(_ => {
            memoryConsumer.freeMemory(granted)
          })
          if ((finalStream eq out) && granted < memSize) {
            throw new LowMemoryException(s"Could not obtain ${memoryConsumer.getMode} " +
                s"memory of size $memSize ",
              java.util.Collections.emptySet())
          }
        }

        val bytes = ClientSharedUtils.toBytes(finalBuffer)
        success = true
        // negative row count indicates that persistence to file was done so driver should
        // mark the broadcastId for cleanup in case of failures, partial result consumption etc
        new PartitionResult(bytes, if (finalStream eq out) count else -count)
      } else {
        success = true
        new PartitionResult(Array.emptyByteArray, 0)
      }
    } finally {
      try {
        if (diskWriter.isDefined) diskWriter.get._1.close()
      } finally {
        if (!success && persist && (finalStream ne out)) {
          env.blockManager.removeBroadcast(broadcastId, tellMaster = true)
        }
      }
    }
  }

  def localBlockStoreResultHandler(rddId: Int, bm: BlockManager, cdf: CachedDataFrame)(
      partitionId: Int, result: (Array[Byte], Int)): Any = {
    val data = result._1
    if (result._2 < 0) cdf.registerBlockIdForLargeResults()
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

  private[sql] def queryStringShortForm(queryString: String, trimSize: Int = 100): String = {
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
  // scalastyle:off
  def withNewExecutionId[T](snappySession: SnappySession, queryShortForm: String,
      queryLongForm: String, queryExecutionStr: String, queryPlanInfo: SparkPlanInfo,
      plan: SparkPlan, currentExecutionId: Long = -1L,
      planEndTime: Long = -1L, postGUIPlans: Boolean = true,
      removeBroadcastsFromDriver: Boolean = false)(body: => T): (T, Long) = {
    // scalastyle:on
    val sc = snappySession.sparkContext
    val localProperties = sc.getLocalProperties
    val oldExecutionId = localProperties.getProperty(SQLExecution.EXECUTION_ID_KEY)
    if (oldExecutionId eq null) {
      // If the execution ID is set in the CDF that means the plan execution has already
      // been done. Use the same execution ID to link this execution to the previous one.
      val executionId = if (currentExecutionId >= 0) currentExecutionId
      else Utils.nextExecutionIdMethod.invoke(SQLExecution).asInstanceOf[Long]
      val executionIdStr = java.lang.Long.toString(executionId)
      val jobGroupId = snappySession.sessionState.jdbcQueryJobGroupId
          .getOrElse(java.lang.Long.toString(executionId))
      SnappySession.setExecutionProperties(localProperties, executionIdStr,
        jobGroupId, queryLongForm)

      val startTime = System.currentTimeMillis()
      var endTime = -1L
      try {
        if (postGUIPlans) sc.listenerBus.post(SparkListenerSQLExecutionStart(executionId,
          queryShortForm, queryLongForm, queryExecutionStr, queryPlanInfo, startTime))
        val result = body
        endTime = System.currentTimeMillis()
        (result, endTime - startTime)
      } finally {
        try {
          if (endTime == -1L) endTime = System.currentTimeMillis()
          // the total duration displayed will be completion time provided below
          // minus the start time of either above, or else the start time of
          // original planning submission, so adjust the endTime accordingly
          if (planEndTime != -1L) {
            endTime -= (startTime - planEndTime)
          }
          // add the time of plan execution to the end time.
          if (postGUIPlans) {
            sc.listenerBus.post(SparkListenerSQLExecutionEnd(executionId, endTime))
            if ((plan ne null) && (ToolsCallbackInit.toolsCallback ne null)) {
              ToolsCallbackInit.toolsCallback.clearBroadcasts(plan, removeBroadcastsFromDriver)
            }
          }
        } finally {
          SnappySession.clearExecutionProperties(localProperties)
        }
      }
    } else {
      // Don't support nested `withNewExecutionId`.
      throw new IllegalArgumentException(
        s"${SQLExecution.EXECUTION_ID_KEY} is already set to $oldExecutionId")
    }
  }

  /**
   * Decode the byte arrays back to UnsafeRows and return an iterator over the resulting rows.
   */
  def decodeUnsafeRows(numFields: Int, data: Array[Byte],
      offset: Int, dataLen: Int): Iterator[UnsafeRow] = {
    if (dataLen == 0) return Iterator.empty

    new AbstractIterator[UnsafeRow] with AutoCloseable {
      private[this] val codec = getCompressionCodec
      private[this] var in = {
        val input = new ByteArrayDataInput
        input.initialize(data, offset, dataLen, null)
        new DataInputStream(codec.compressedInputStream(input))
      }
      private[this] val finalIn = in
      private[this] var diskBuffer: Option[ChunkedByteBuffer] = None
      private[this] var diskData: Option[ByteBuffer] = None
      private[this] var sizeOfNextRow: Int = _
      private[this] var broadcastId: Long = -1L

      readNextRowLength()

      /** read the length of compressed block and return a buffer around that block */
      private def readCompressedBlock(data: ByteBuffer): ByteBuffer = {
        val len = data.getInt
        val slicedData = data.slice()
        slicedData.limit(len)
        // set the position to the end of current compressed block
        data.position(math.min(data.position() + len, data.limit()))
        slicedData
      }

      @tailrec
      private def readNextRowLength(): Unit = {
        sizeOfNextRow = in.readInt()
        if (sizeOfNextRow == -1) {
          // check for existing disk data
          diskData match {
            case Some(data) =>
              if (data.hasRemaining) {
                // start a new compressed stream since those are stored back-to-back in the file
                val stream = new ByteBufferInputStream(readCompressedBlock(data))
                in = new DataInputStream(codec.compressedInputStream(stream))
              } else {
                in = finalIn
                diskBuffer.get.dispose()
                diskBuffer = None
                diskData = None
              }
              readNextRowLength()
            case _ =>
          }
        } else if (sizeOfNextRow < -1) {
          // indicates disk data
          val size = -sizeOfNextRow
          val bytes = new Array[Byte](size)
          in.readFully(bytes)
          val blockId = BlockId(new String(bytes, StandardCharsets.UTF_8))
          if (broadcastId == -1L && blockId.isInstanceOf[BroadcastBlockId]) {
            broadcastId = blockId.asInstanceOf[BroadcastBlockId].broadcastId
          }
          val env = SparkEnv.get
          env.blockManager.getRemoteBytes(blockId) match {
            case s@Some(buffers) =>
              env.blockManager.master.removeBlock(blockId)
              val data = buffers.toByteBuffer.order(ByteOrder.BIG_ENDIAN)
              val stream = new ByteBufferInputStream(readCompressedBlock(data))
              in = new DataInputStream(codec.compressedInputStream(stream))
              diskBuffer = s
              diskData = Some(data)
              readNextRowLength()
            case _ => throw new SparkException(s"Failed to get $blockId from BlockManager")
          }
        }
      }

      override def hasNext: Boolean = sizeOfNextRow >= 0

      override def next(): UnsafeRow = {
        val bs = new Array[Byte](sizeOfNextRow)
        in.readFully(bs)
        val row = new UnsafeRow(numFields)
        row.pointTo(bs, sizeOfNextRow)
        readNextRowLength()
        row
      }

      override def close(): Unit = {
        diskBuffer match {
          case Some(buffer) => try {
            buffer.dispose()
          } catch {
            case _: Throwable => // ignore
          }
          case _ =>
        }
        if (broadcastId != -1L) {
          new BroadcastRemovalListener().removeBroadcast(broadcastId)
        }
      }
    }
  }

  private def takeRows[U, R](n: Int, results: Array[(R, Int)],
      processPartition: (TaskContext, Iterator[InternalRow]) => (U, Int),
      resultHandler: (Int, (U, Int)) => R,
      decodeResult: R => Iterator[InternalRow]): Iterator[R] = {
    val takeResults = new ArrayBuffer[R](n)
    var numRows = 0
    results.indices.foreach { partitionId =>
      val r = results(partitionId)
      if ((r ne null) && r._1 != null) {
        val resultRows = math.abs(r._2)
        if (numRows + resultRows <= n) {
          takeResults += r._1
          numRows += resultRows
        } else {
          // need to split this partition result to take only remaining rows
          val decoded = decodeResult(r._1).take(n - numRows)
          // encode back and add
          takeResults += resultHandler(partitionId,
            processPartition(TaskContext.get(), decoded))
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
      resultHandler: (Int, (U, Int)) => R, decodeResult: R => Iterator[InternalRow],
      session: SnappySession): Iterator[R] = {
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
        val resultRows = math.abs(r._2)
        results(partsScanned + index) = (resultHandler(partsScanned + index, r), resultRows)
        numResults += resultRows
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
      if (queryExecution ne null) session.listenerManager.onSuccess(name, queryExecution, elapsed)
      result
    } catch {
      case e: Exception =>
        if (queryExecution ne null) session.listenerManager.onFailure(name, queryExecution, e)
        throw e
    }
  }

  def isConnectorCatalogStaleException(t: Throwable, session: SnappySession): Boolean = {

    // error check needed in SmartConnector mode only
    val isSmartConnectorMode = SnappyContext.getClusterMode(session.sparkContext) match {
      case ThinClientConnectorMode(_, _) => true
      case _ => false
    }
    if (!isSmartConnectorMode) return false

    var cause = t
    do {
      cause match {
        case sqle: SQLException
          if SQLState.SNAPPY_CATALOG_SCHEMA_VERSION_MISMATCH.equals(sqle.getSQLState) =>
          return true
        case e: Error =>
          if (SystemFailure.isJVMFailureError(e)) {
            SystemFailure.initiateFailure(e)
            // If this ever returns, rethrow the error. We're poisoned
            // now, so don't let this thread continue.
            throw e
          }
        case _ =>
      }
      cause = cause.getCause
    } while (cause ne null)
    false
  }

  def catalogStaleFailure(cause: Throwable, session: SnappySession): Exception = {
    logWarning(s"SmartConnector catalog is not up to date. " +
        s"Please reconstruct the Dataset and retry the operation")
    new CatalogStaleException("Smart connector catalog is out of date due to " +
        "table schema change (DROP/CREATE/ALTER operation). " +
        "Please reconstruct the Dataset and retry the operation", cause)
  }

  def retryOnStaleCatalogException[T](retryCount: Int = 10,
      snappySession: SnappySession)(codeToExecute: => T) : T = {
    var attempts = 1
    var res: Option[T] = None
    logInfo(s"Query will be retried $retryCount times")
    while (res.isEmpty) {
      try {
        logInfo("Retry attempt#" + attempts)
        res = Option(codeToExecute)
      } catch {
        case t: Throwable
          if CachedDataFrame.isConnectorCatalogStaleException(t, snappySession) =>
          snappySession.sessionCatalog.invalidateAll()
          SnappySession.clearAllCache()
          if (attempts < retryCount) {
            Thread.sleep(attempts*100)
            attempts = attempts + 1
          } else {
            throw CachedDataFrame.catalogStaleFailure(t, snappySession)
          }
      }
    }
    res.get
  }

  private[sql] def clear(): Unit = synchronized {
    sparkConf = null
    compressionCodecName = null
    maxMemoryResultSize = Long.MaxValue
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
