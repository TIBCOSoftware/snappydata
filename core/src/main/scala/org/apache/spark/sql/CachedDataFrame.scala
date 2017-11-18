/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
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
import java.sql.SQLException

import scala.annotation.tailrec
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}
import scala.reflect.ClassTag

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.gemstone.gemfire.cache.LowMemoryException
import com.gemstone.gemfire.internal.shared.ClientSharedUtils
import com.gemstone.gemfire.internal.shared.unsafe.{DirectBufferAllocator, UnsafeHolder}
import com.gemstone.gemfire.internal.{ByteArrayDataInput, ByteBufferDataOutput}
import com.pivotal.gemfirexd.internal.shared.common.reference.SQLState

import org.apache.spark._
import org.apache.spark.io.CompressionCodec
import org.apache.spark.memory.MemoryConsumer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodeAndComment
import org.apache.spark.sql.catalyst.expressions.{Literal, LiteralValue, ParamLiteral, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.CollectAggregateExec
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.ui.{SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.{BlockManager, RDDBlockId, StorageLevel}
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.CallSite

class CachedDataFrame(session: SparkSession, queryExecution: QueryExecution,
    encoder: Encoder[Row], var queryString: String,
    cachedRDD: RDD[InternalRow], shuffleDependencies: Array[Int],
    val rddId: Int, val hasLocalCollectProcessing: Boolean,
    val allLiterals: Array[LiteralValue] = Array.empty,
    val allbcplans: mutable.Map[SparkPlan, ArrayBuffer[Any]] = mutable.Map.empty,
    val queryHints: Map[String, String] = Map.empty,
    var planProcessingTime: Long = 0,
    var currentExecutionId: Option[Long] = None)
    extends Dataset[Row](session, queryExecution, encoder) with Logging {

  // scalastyle:off
  def this(ds: Dataset[Row], queryString: String,
      cachedRDD: RDD[InternalRow], shuffleDependencies: Array[Int],
      rddId: Int, hasLocalCollectProcessing: Boolean,
      allLiterals: Array[LiteralValue],
      queryHints: Map[String, String],
      planProcessingTime: Long, currentExecutionId: Option[Long]) = {
    // scalastyle:on
    this(ds.sparkSession, ds.queryExecution, ds.exprEnc, queryString, cachedRDD,
      shuffleDependencies, rddId, hasLocalCollectProcessing, allLiterals, mutable.Map.empty,
      queryHints, planProcessingTime, currentExecutionId)
  }

  /**
   * Return true if [[collectWithHandler]] supports partition-wise separate
   * result handling by default, else result handler is invoked for a
   * single partition result.
   */
  def hasPartitionWiseHandling: Boolean = cachedRDD ne null

  private lazy val boundEnc = exprEnc.resolveAndBind(logicalPlan.output,
    sparkSession.sessionState.analyzer)

  private lazy val queryExecutionString: String = queryExecution.toString()

  private lazy val isLowLatencyQuery: Boolean =
    (cachedRDD ne null) && cachedRDD.getNumPartitions <= 2 /* some small number */

  private lazy val lastShuffleCleanups = new Array[Future[Unit]](
    shuffleDependencies.length)

  private[sql] def clearCachedShuffleDeps(sc: SparkContext): Unit = {
    val numShuffleDeps = shuffleDependencies.length
    if (numShuffleDeps > 0) {
      sc.cleaner match {
        case Some(cleaner) =>
          var i = 0
          while (i < numShuffleDeps) {
            val shuffleDependency = shuffleDependencies(i)
            // Cleaning the  shuffle artifacts asynchronously
            lastShuffleCleanups(i) = Future {
              cleaner.doCleanupShuffle(shuffleDependency, blocking = true)
            }
            i += 1
          }
        case None =>
      }
    }
  }

  private[sql] def reset(): Unit = clearPartitions(Seq(cachedRDD))
  private lazy val unsafe = UnsafeHolder.getUnsafe
  private lazy val rdd_partitions_ = {
    val _f = classOf[RDD[_]].getDeclaredField("org$apache$spark$rdd$RDD$$partitions_")
    _f.setAccessible(true)
    unsafe.objectFieldOffset(_f)
  }

  @tailrec
  private def clearPartitions(rdd: Seq[RDD[_]]): Unit = {
    val children = rdd.flatMap(r => if (r != null) {
      r.dependencies.map {
        case d: NarrowDependency[_] => d.rdd
        case s: ShuffleDependency[_, _, _] => s.rdd
      }
    } else None)

    rdd.foreach {
      case null =>
      case r: RDD[_] =>
        // f.set(r, null)
        unsafe.putObject(r, rdd_partitions_, null)
    }
    if (children.isEmpty) {
      return
    }
    clearPartitions(children)
  }

  private def setPoolForExecution(): Unit = {
    var pool = sparkSession.asInstanceOf[SnappySession].
      sessionState.conf.activeSchedulerPool

    // Check if it is pruned query, execute it automatically on the low latency pool
    if (isLowLatencyQuery && shuffleDependencies.length == 0 && pool == "default") {
      if (sparkSession.sparkContext.getAllPools.exists(_.name == "lowlatency")) {
        pool = "lowlatency"
      }
    }
    sparkSession.sparkContext.setLocalProperty("spark.scheduler.pool", pool)
  }

  /**
   * Wrap a Dataset action to track the QueryExecution and time cost,
   * then report to the user-registered callback functions.
   */
  private def withCallback[U](name: String)(action: DataFrame => U) = {
    try {
      setPoolForExecution()
      // This is needed for cases when collect is called twice on the same DF.
      // The getPlan won't be called and hence the wait has to be done here.
      waitForLastShuffleCleanup()
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
      case se: SparkException =>
        val (isCatalogStale, sqlexception) = staleCatalogError(se)
        if (isCatalogStale) {
          val snSession = SparkSession.getActiveSession.get.asInstanceOf[SnappySession]
          snSession.sessionCatalog.invalidateAll()
          SnappySession.clearAllCache()
          sparkSession.listenerManager.onFailure(name, queryExecution, se)
          logInfo("Operation needs to be retried ", se)
          throw sqlexception
        } else {
          sparkSession.listenerManager.onFailure(name, queryExecution, se)
          throw se
        }
      case e: Exception =>
        sparkSession.listenerManager.onFailure(name, queryExecution, e)
        throw e
    } finally {
      // clear the shuffle dependencies asynchronously after the execution.
      clearCachedShuffleDeps(sparkSession.sparkContext)
    }
  }

  private def staleCatalogError(se: SparkException): (Boolean, SQLException) = {
    var cause = se.getCause
    while (cause != null) {
      cause match {
        case sqle: SQLException
          if SQLState.SNAPPY_RELATION_DESTROY_VERSION_MISMATCH.equals(sqle.getSQLState) =>
          return (true, sqle)
        case _ =>
          cause = cause.getCause
      }
    }
    (false, null)
  }

  override def collect(): Array[Row] = {
    collectInternal().map(boundEnc.fromRow).toArray
  }

  override def withNewExecutionId[T](body: => T): T = queryExecution.executedPlan match {
    // don't create a new executionId for ExecutePlan since it has already done so
    case _: ExecutePlan => body
    case _ =>
      try {
        CachedDataFrame.withNewExecutionId(
          sparkSession, CachedDataFrame.queryStringShortForm(queryString), queryString,
          queryExecutionString, CachedDataFrame.queryPlanInfo(
            queryExecution.executedPlan, allLiterals),
          currentExecutionId, planProcessingTime)(body)
      } finally {
        currentExecutionId = None
        planProcessingTime = 0
      }
  }

  override def count(): Long = withCallback("count") { df =>
    df.groupBy().count().collect().head.getLong(0)
  }

  override def head(n: Int): Array[Row] = withCallback("head") { df =>
    df.limit(n).collect()
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

  private[sql] def waitForLastShuffleCleanup(): Unit = {
    val numShuffles = lastShuffleCleanups.length
    if (numShuffles > 0) {
      var index = 0
      while (index < numShuffles) {
        val cleanup = lastShuffleCleanups(index)
        if (cleanup ne null) {
          Await.ready(cleanup, Duration.Inf)
          lastShuffleCleanups(index) = null
        }
        index += 1
      }
    }
  }

  def collectWithHandler[U: ClassTag, R: ClassTag](
      processPartition: (TaskContext, Iterator[InternalRow]) => (U, Int),
      resultHandler: (Int, U) => R,
      decodeResult: R => Iterator[InternalRow],
      skipUnpartitionedDataProcessing: Boolean = false,
      skipLocalCollectProcessing: Boolean = false): Iterator[R] = {
    val sc = sparkSession.sparkContext
    val hasLocalCallSite = sc.getLocalProperties.containsKey(CallSite.LONG_FORM)
    val callSite = sc.getCallSite()
    if (!hasLocalCallSite) {
      sc.setCallSite(callSite)
    }
    val session = sparkSession.asInstanceOf[SnappySession]
    var withFallback: CodegenSparkFallback = null
    val sparkPlan = queryExecution.executedPlan
    val executedPlan = sparkPlan match {
      case CodegenSparkFallback(WholeStageCodegenExec(CachedPlanHelperExec(plan))) =>
        withFallback = CodegenSparkFallback(plan); plan
      case cg@CodegenSparkFallback(plan) => withFallback = cg; plan
      case WholeStageCodegenExec(CachedPlanHelperExec(plan)) => plan
      case plan => plan
    }

    def execute(): Iterator[R] = withNewExecutionId {
      session.addContextObject(SnappySession.ExecutionKey, () => queryExecution)
      def executeCollect(): Array[InternalRow] = {
        if (withFallback ne null) withFallback.executeCollect()
        else executedPlan.executeCollect()
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
            // TODO: handle fallback for this case
            new AggregatePartialDataIterator(plan.generatedSource,
              plan.generatedReferences, plan.child.schema.length,
              plan.executeCollectData()).asInstanceOf[Iterator[R]]
          } else if (skipUnpartitionedDataProcessing) {
            // no processing required
            executeCollect().iterator.asInstanceOf[Iterator[R]]
          } else {
            // convert to UnsafeRow
            val converter = UnsafeProjection.create(plan.schema)
            Iterator(resultHandler(0, processPartition(TaskContext.get(),
              executeCollect().iterator.map(converter))._1))
          }

        case plan@(_: ExecutedCommandExec | _: LocalTableScanExec | _: ExecutePlan) =>
          if (skipUnpartitionedDataProcessing) {
            // no processing required
            executeCollect().iterator.asInstanceOf[Iterator[R]]
          } else {
            // convert to UnsafeRow
            val converter = UnsafeProjection.create(plan.schema)
            Iterator(resultHandler(0, processPartition(TaskContext.get(),
              executeCollect().iterator.map(converter))._1))
          }

        case _ =>
          if (skipUnpartitionedDataProcessing) {
            // no processing required
            executeCollect().iterator.asInstanceOf[Iterator[R]]
          } else {
            val rdd = if (cachedRDD ne null) cachedRDD
            else queryExecution.executedPlan.execute()
            val numPartitions = rdd.getNumPartitions
            val results = new Array[R](numPartitions)
            sc.runJob(rdd, processPartition, 0 until numPartitions,
              (index: Int, r: (U, Int)) =>
                results(index) = resultHandler(index, r._1))
            results.iterator
          }
      }
      // submit cleanup job for shuffle output
      // clearCachedShuffleDeps(sc)

      results
    }

    try {
      withCallback("collect")(_ => execute())
    } finally {
      session.removeContextObject(SnappySession.ExecutionKey)
      if (!hasLocalCallSite) {
        sc.clearCallSite()
      }
    }
  }

  var firstAccess = true

  // Plan caching involving broadcast will be revisited.
  /*
  def reprepareBroadcast(lp: LogicalPlan,
      newpls: mutable.ArrayBuffer[ParamLiteral]): Unit = {
    if (allbcplans.nonEmpty && !firstAccess) {
      allbcplans.foreach { case (bchj, refs) =>
        logDebug(s"Repreparing for bcplan = ${bchj} with new pls = ${newpls.toSet}")
        val broadcastIndex = refs.indexWhere(_.isInstanceOf[Broadcast[_]])
        val newbchj = bchj.transformAllExpressions {
          case ParamLiteral(_, _, p) =>
            val np = newpls.find(_.pos == p).getOrElse(pl)
            val x = ParamLiteral(np.value, np.dataType, p)
            x.considerUnequal = true
            x
        }
        val tmpCtx = new CodegenContext
        val parameterType = tmpCtx.getClass()
        val method = newbchj.getClass.getDeclaredMethod("prepareBroadcast", parameterType)
        method.setAccessible(true)
        val bc = method.invoke(newbchj, tmpCtx)
        logDebug(s"replacing bc var = ${refs(broadcastIndex)} with " +
            s"new bc = ${bc.asInstanceOf[(Broadcast[_], String)]._1}")
        refs(broadcastIndex) = bc.asInstanceOf[(Broadcast[_], String)]._1
      }
    }
    firstAccess = false
  }
  */
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
      output: ByteBufferDataOutput, codec: CompressionCodec): Unit = {
    if (position > 0) {
      val compressedBytes = Utils.codecCompress(codec,
        bufferOutput.getBuffer, position)
      val len = compressedBytes.length
      // write the uncompressed length too
      output.writeInt(position)
      output.writeInt(len)
      output.write(compressedBytes, 0, len)
      bufferOutput.clear()
    }
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
        if (context != null) {
          // TODO why driver is calling this code with context null ?
          val memoryConsumer = new MemoryConsumer(context.taskMemoryManager()) {
            override def spill(size: Long, trigger: MemoryConsumer): Long = {
              0L
            }
          }
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

  @transient private[sql] val nextExecutionIdMethod = {
    val m = SQLExecution.getClass.getDeclaredMethod("nextExecutionId")
    m.setAccessible(true)
    m
  }

  private[sql] def queryPlanInfo(plan: SparkPlan,
      allLiterals: Array[LiteralValue]): SparkPlanInfo = PartitionedPhysicalScan.getSparkPlanInfo(
    plan.transformAllExpressions {
      case ParamLiteral(_v, _dt, _p) =>
        val x = allLiterals.find(_.position == _p)
        val v = x match {
          case Some(LiteralValue(_, _, _)) => x.get.value
          case None => _v
        }
        Literal(v, _dt)
    })

  private[sql] def queryStringShortForm(queryString: String): String = {
    val trimSize = 100
    if (queryString.length > trimSize) {
      queryString.substring(0, trimSize) + "..."
    } else queryString
  }

  /**
   * Wrap an action that will execute "queryExecution" to track all Spark jobs
   * in the body so that we can connect them with an execution.
   *
   * Custom method to allow passing in cached SparkPlanInfo and queryExecution string.
   */
  def withNewExecutionId[T](sparkSession: SparkSession,
      queryShortForm: String, queryLongForm: String, queryExecutionStr: String,
      queryPlanInfo: SparkPlanInfo, currentExecutionId: Option[Long] = None,
      planProcessingTime: Long = 0)(body: => T): T = {
    val sc = sparkSession.sparkContext
    val oldExecutionId = sc.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    if (oldExecutionId == null) {
      // If the execution ID is set in the CDF that means the plan execution has already
      // been done. Use the same execution ID to link this execution to the previous one.
      val executionId = currentExecutionId match {
        case Some(exId) => exId
        case None => nextExecutionIdMethod.invoke(SQLExecution).asInstanceOf[Long]
      }
      val r = try {
        sc.setLocalProperty(SQLExecution.EXECUTION_ID_KEY, executionId.toString)
        sparkSession.sparkContext.listenerBus.post(SparkListenerSQLExecutionStart(
          executionId, queryShortForm, queryLongForm, queryExecutionStr,
          queryPlanInfo, System.currentTimeMillis()))
        try {
          body
        } finally {
          // add the time of plan execution to the end time.
          sparkSession.sparkContext.listenerBus.post(SparkListenerSQLExecutionEnd(
            executionId, System.currentTimeMillis() + planProcessingTime))
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
    val input = new ByteArrayDataInput
    input.initialize(data, offset, dataLen, null)
    val dataLimit = offset + dataLen
    var decompressedLen = input.readInt()
    var inputLen = input.readInt()
    val inputPosition = input.position()
    val bufferInput = new Input(Utils.codecDecompress(codec, data,
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
          bufferInput.setBuffer(Utils.codecDecompress(codec, data,
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
