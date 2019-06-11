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
package org.apache.spark.serializer

import java.io.{EOFException, Externalizable, IOException, InputStream, OutputStream}
import java.lang.ref.SoftReference
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import com.esotericsoftware.kryo.io.{ByteBufferOutput, Input}
import com.esotericsoftware.kryo.serializers.DefaultSerializers.KryoSerializableSerializer
import com.esotericsoftware.kryo.serializers.ExternalizableSerializer
import com.esotericsoftware.kryo.{Kryo, KryoException}

import org.apache.spark.broadcast.TorrentBroadcast
import org.apache.spark.executor.{InputMetrics, OutputMetrics, ShuffleReadMetrics, ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.rdd.ZippedPartitionsPartition
import org.apache.spark.scheduler._
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{LaunchTask, StatusUpdate}
import org.apache.spark.sql.catalyst.expressions.codegen.CodeAndComment
import org.apache.spark.sql.catalyst.expressions.{DynamicFoldableExpression, ParamLiteral, TokenLiteral, UnsafeRow}
import org.apache.spark.sql.collection.{MultiBucketExecutorPartition, NarrowExecutorLocalSplitDep, SmartExecutorBucketPartition}
import org.apache.spark.sql.execution.columnar.impl.{ColumnarStorePartitionedRDD, JDBCSourceAsColumnarStore, SmartConnectorColumnRDD, SmartConnectorRowRDD}
import org.apache.spark.sql.execution.joins.CacheKey
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.row.RowFormatScanRDD
import org.apache.spark.sql.sources.ConnectionProperties
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{BlockAndExecutorId, CachedDataFrame, PartitionResult}
import org.apache.spark.storage.BlockManagerMessages.{RemoveBlock, RemoveBroadcast, RemoveRdd, RemoveShuffle, UpdateBlockInfo}
import org.apache.spark.storage._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.collection.BitSet
import org.apache.spark.util.{CollectionAccumulator, DoubleAccumulator, LongAccumulator, SerializableBuffer, Utils}
import org.apache.spark.{Logging, SparkConf, SparkEnv}

/**
 * A pooled, optimized version of Spark's KryoSerializer that also works for
 * closure serialization.
 *
 * Note that this serializer is not guaranteed to be wire-compatible across
 * different versions of Spark. It is intended to be used to
 * serialize/de-serialize data within a single Spark application.
 */
final class PooledKryoSerializer(conf: SparkConf)
    extends KryoSerializer(conf) with Serializable {

  /**
   * Sets a class loader for the serializer to use in deserialization.
   *
   * @return this Serializer object
   */
  override def setDefaultClassLoader(classLoader: ClassLoader): Serializer = {
    // clear the pool and cache
    KryoSerializerPool.clear()
    super.setDefaultClassLoader(classLoader)
  }

  override def newKryo(): Kryo = {
    val oldClassLoader = Thread.currentThread.getContextClassLoader
    val kryo = super.newKryo()

    val classLoader = kryo.getClassLoader
    kryo.setClassLoader(oldClassLoader)

    // specific serialization implementations in Spark and commonly used classes
    kryo.register(classOf[UnsafeRow])
    kryo.register(classOf[UTF8String])
    kryo.register(classOf[UpdateBlockInfo], new ExternalizableOnlySerializer)
    kryo.register(classOf[CompressedMapStatus], new ExternalizableOnlySerializer)
    kryo.register(classOf[HighlyCompressedMapStatus],
      new ExternalizableOnlySerializer)
    kryo.register(classOf[IndirectTaskResult[_]])
    kryo.register(classOf[RDDBlockId])
    kryo.register(classOf[ShuffleBlockId])
    kryo.register(classOf[ShuffleDataBlockId])
    kryo.register(classOf[ShuffleIndexBlockId])
    kryo.register(classOf[BroadcastBlockId])
    kryo.register(classOf[TaskResultBlockId])
    kryo.register(classOf[StreamBlockId])
    kryo.register(classOf[TorrentBroadcast[_]])

    // other classes having specific implementations in snappy-spark but for
    // spark compatibility, the serializer is not mentioned explicitly
    kryo.register(classOf[ResultTask[_, _]])
    kryo.register(classOf[ShuffleMapTask])
    kryo.register(classOf[DirectTaskResult[_]])
    kryo.register(classOf[SerializableBuffer])
    kryo.register(Utils.classForName(
      "org.apache.spark.rpc.netty.NettyRpcEndpointRef"))
    kryo.register(Utils.classForName(
      "org.apache.spark.rpc.netty.RequestMessage"))
    kryo.register(classOf[LaunchTask])
    kryo.register(classOf[TaskDescription])
    kryo.register(classOf[StatusUpdate])
    kryo.register(classOf[TaskMetrics])
    kryo.register(classOf[InputMetrics])
    kryo.register(classOf[OutputMetrics])
    kryo.register(classOf[ShuffleReadMetrics])
    kryo.register(classOf[ShuffleWriteMetrics])
    kryo.register(classOf[LongAccumulator])
    kryo.register(classOf[DoubleAccumulator])
    kryo.register(classOf[CollectionAccumulator[_]])
    kryo.register(classOf[SQLMetric])
    kryo.register(classOf[ZippedPartitionsPartition])
    kryo.register(classOf[RemoveBlock])
    kryo.register(classOf[RemoveBroadcast])
    kryo.register(classOf[RemoveRdd])
    kryo.register(classOf[RemoveShuffle])
    kryo.register(classOf[BitSet])

    kryo.register(classOf[BlockManagerId], new ExternalizableResolverSerializer(
      BlockManagerId.getCachedBlockManagerId))
    kryo.register(classOf[StorageLevel], new ExternalizableResolverSerializer(
      StorageLevel.getCachedStorageLevel))
    kryo.register(classOf[BlockAndExecutorId], new ExternalizableOnlySerializer)
    kryo.register(classOf[StructType], StructTypeSerializer)
    kryo.register(classOf[NarrowExecutorLocalSplitDep],
      new KryoSerializableSerializer)
    kryo.register(CachedDataFrame.getClass, new KryoSerializableSerializer)
    kryo.register(classOf[ConnectionProperties], ConnectionPropertiesSerializer)
    kryo.register(classOf[RowFormatScanRDD], new KryoSerializableSerializer)
    kryo.register(classOf[SmartConnectorRowRDD], new KryoSerializableSerializer)
    kryo.register(classOf[ColumnarStorePartitionedRDD],
      new KryoSerializableSerializer)
    kryo.register(classOf[SmartConnectorColumnRDD],
      new KryoSerializableSerializer)
    kryo.register(classOf[MultiBucketExecutorPartition],
      new KryoSerializableSerializer)
    kryo.register(classOf[SmartExecutorBucketPartition],
      new KryoSerializableSerializer)
    kryo.register(classOf[PartitionResult], PartitionResultSerializer)
    kryo.register(classOf[CacheKey], new KryoSerializableSerializer)
    kryo.register(classOf[JDBCSourceAsColumnarStore], new KryoSerializableSerializer)
    kryo.register(classOf[TokenLiteral], new KryoSerializableSerializer)
    kryo.register(classOf[ParamLiteral], new KryoSerializableSerializer)
    kryo.register(classOf[DynamicFoldableExpression], new KryoSerializableSerializer)

    try {
      val launchTasksClass = Utils.classForName(
        "org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.LaunchTasks")
      kryo.register(launchTasksClass, new KryoSerializableSerializer)
    } catch {
      case _: ClassNotFoundException => // ignore
    }

    // use Externalizable by default as last fallback, if available,
    // rather than going to FieldSerializer
    kryo.addDefaultSerializer(classOf[Externalizable],
      new ExternalizableSerializer)

    // use a custom default serializer factory that will honour
    // readObject/writeObject, readResolve/writeReplace methods to fall-back
    // to java serializer else use Kryo's FieldSerializer
    kryo.setDefaultSerializer(new SnappyKryoSerializerFactory)

    kryo.setClassLoader(classLoader)
    kryo
  }

  override def newInstance(): PooledKryoSerializerInstance = {
    new PooledKryoSerializerInstance(this)
  }

  private[spark] override lazy val supportsRelocationOfSerializedObjects: Boolean = {
    // If auto-reset is disabled, then Kryo may store references to duplicate
    // occurrences of objects in the stream rather than writing those objects'
    // serialized bytes, breaking relocation. See
    // https://groups.google.com/d/msg/kryo-users/6ZUSyfjjtdo/FhGG1KHDXPgJ for more details.
    newInstance().getAutoReset
  }
}

final class PooledObject(serializer: PooledKryoSerializer,
    bufferSize: Int) {
  val kryo: Kryo = serializer.newKryo()
  val input: Input = new KryoInputStringFix(0)

  def newOutput(): ByteBufferOutput = new ByteBufferOutput(bufferSize, -1)
  def newOutput(size: Int): ByteBufferOutput = new ByteBufferOutput(size, -1)
}

object KryoSerializerPool {

  private[serializer] val autoResetField =
    classOf[Kryo].getDeclaredField("autoReset")
  autoResetField.setAccessible(true)

  private[serializer] val zeroBytes = new Array[Byte](0)

  private[serializer] lazy val (serializer, bufferSize): (PooledKryoSerializer, Int) = {
    val conf = SparkEnv.get match {
      case null => new SparkConf()
      case env => env.conf
    }
    val bufferSizeKb = conf.getSizeAsKb("spark.kryoserializer.buffer", "4k")
    val bufferSize = ByteUnit.KiB.toBytes(bufferSizeKb).toInt
    (new PooledKryoSerializer(conf), bufferSize)
  }

  private[this] val pool = new java.util.ArrayDeque[SoftReference[PooledObject]]()

  private def readByteBufferAsInput(bb: ByteBuffer, input: Input): Unit = {
    if (bb.hasArray) {
      input.setBuffer(bb.array(),
        bb.arrayOffset() + bb.position(), bb.remaining())
    } else {
      val numBytes = bb.remaining()
      val position = bb.position()
      val bytes = new Array[Byte](numBytes)
      bb.get(bytes, 0, numBytes)
      bb.position(position)
      input.setBuffer(bytes, 0, numBytes)
    }
  }

  def serialize(f: (Kryo, ByteBufferOutput) => Unit, bufferSize: Int = -1): Array[Byte] = {
    val pooled = borrow()
    val output = if (bufferSize == -1) pooled.newOutput() else pooled.newOutput(bufferSize)
    try {
      f(pooled.kryo, output)
      output.toBytes
    } finally {
      output.release()
      release(pooled)
    }
  }

  def deserialize[T: ClassTag](buffer: ByteBuffer, f: (Kryo, Input) => T): T = {
    val pooled = borrow()
    try {
      readByteBufferAsInput(buffer, pooled.input)
      f(pooled.kryo, pooled.input)
    } finally {
      release(pooled, clearInputBuffer = true)
    }
  }

  def deserialize[T: ClassTag](bytes: Array[Byte], offset: Int, count: Int,
      f: (Kryo, Input) => T): T = {
    val pooled = borrow()
    try {
      pooled.input.setBuffer(bytes, offset, count)
      f(pooled.kryo, pooled.input)
    } finally {
      release(pooled, clearInputBuffer = true)
    }
  }

  def borrow(): PooledObject = {
    var ref: SoftReference[PooledObject] = null
    pool.synchronized {
      ref = pool.pollFirst()
    }
    while (ref ne null) {
      val poolObject = ref.get()
      if (poolObject ne null) return poolObject
      pool.synchronized {
        ref = pool.pollFirst()
      }
    }
    new PooledObject(serializer, bufferSize)
  }

  def release(poolObject: PooledObject,
      clearInputBuffer: Boolean = false): Unit = {
    // Call reset() to clear any Kryo state that might have been modified
    // by the last operation to borrow this instance (SPARK-7766).
    poolObject.kryo.reset()
    if (clearInputBuffer) {
      poolObject.input.setBuffer(zeroBytes)
    }
    val ref = new SoftReference[PooledObject](poolObject)
    pool.synchronized {
      pool.addFirst(ref)
    }
  }

  def clear(): Unit = {
    pool.synchronized {
      pool.clear()
    }
  }
}

private[spark] final class PooledKryoSerializerInstance(
    pooledSerializer: PooledKryoSerializer)
    extends SerializerInstance with Logging {

  override def serialize[T: ClassTag](t: T): ByteBuffer = {

    val bufferSize = t match {
      // Special handling for wholeStageCodeGenRDD
      case (rdd: Product, _) =>
        // If it is a wholestageRDD, we know the serialization buffer needs to be
        // bigger than the code string size. If it is not bigger, the writestring call inside
        // WholeStageCodeGenRDD.write calls writeString_slow. Refer Output.writeString.
        // So create a buffer of size greater than the size of code.
            if (rdd.productArity == 5 &&
              // Hackish way to determine if it is a WholeStageRDD.
              // Any change to WholeStageCodeGenRDD needs to reflect here
              rdd.productElement(1).isInstanceOf[CodeAndComment]) {
              val size = rdd.productElement(1).asInstanceOf[CodeAndComment].body.length
              // round off to a multiple of 1024
              ((size + 4 * 1024) >> 10) << 10
            } else -1
      case _ => -1
    }
    ByteBuffer.wrap(KryoSerializerPool.serialize(
      (kryo, out) => kryo.writeClassAndObject(out, t), bufferSize))
  }

  override def deserialize[T: ClassTag](buffer: ByteBuffer): T = {
    KryoSerializerPool.deserialize(buffer,
      (kryo, in) => kryo.readClassAndObject(in).asInstanceOf[T])
  }

  override def deserialize[T: ClassTag](buffer: ByteBuffer,
      loader: ClassLoader): T = {
    KryoSerializerPool.deserialize(buffer, (kryo, in) => {
      val oldClassLoader = kryo.getClassLoader
      try {
        kryo.setClassLoader(loader)
        kryo.readClassAndObject(in).asInstanceOf[T]
      } finally {
        kryo.setClassLoader(oldClassLoader)
      }
    })
  }

  override def serializeStream(stream: OutputStream): SerializationStream = {
    new KryoStringFixSerializationStream(stream)
  }

  override def deserializeStream(stream: InputStream): DeserializationStream = {
    new KryoStringFixDeserializationStream(stream)
  }

  /**
   * Returns true if auto-reset is on. The only reason this would be false is
   * if the user-supplied registrator explicitly turns auto-reset off.
   */
  def getAutoReset: Boolean = {
    val poolObject = KryoSerializerPool.borrow()
    try {
      val result = KryoSerializerPool.autoResetField.get(
        poolObject.kryo).asInstanceOf[Boolean]
      result
    } finally {
      KryoSerializerPool.release(poolObject)
    }
  }
}

private[serializer] class KryoStringFixSerializationStream(
    stream: OutputStream) extends SerializationStream {

  private[this] val poolObject = KryoSerializerPool.borrow()

  private[this] var output = {
    val out = poolObject.newOutput()
    out.setOutputStream(stream)
    out
  }

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    poolObject.kryo.writeClassAndObject(output, t)
    this
  }

  override def flush() {
    if (output != null) {
      output.flush()
    } else {
      throw new IOException("Stream is closed")
    }
  }

  override def close() {
    if (output != null) {
      try {
        output.close()
      } finally {
        output.release()
        output = null
        KryoSerializerPool.release(poolObject)
      }
    }
  }
}

private[spark] class KryoStringFixDeserializationStream(
    stream: InputStream) extends DeserializationStream {

  private[this] val poolObject = KryoSerializerPool.borrow()

  private[this] var input = new KryoInputStringFix(KryoSerializerPool.bufferSize)
  input.setInputStream(stream)

  override def readObject[T: ClassTag](): T = {
    try {
      poolObject.kryo.readClassAndObject(input).asInstanceOf[T]
    } catch {
      // DeserializationStream uses the EOF exception to indicate
      // stopping condition.
      case e: KryoException
        if e.getMessage.toLowerCase.contains("buffer underflow") =>
        throw new EOFException
    }
  }

  override def close() {
    if (input != null) {
      try {
        // Kryo's Input automatically closes the input stream it is using.
        input.close()
      } finally {
        input.setInputStream(null)
        input = null
        KryoSerializerPool.release(poolObject)
      }
    }
  }
}

/**
 * Fix for https://github.com/EsotericSoftware/kryo/issues/128.
 * Uses an additional 0x0 byte as end marker.
 */
private[spark] final class KryoInputStringFix(size: Int)
    extends Input(size) {

  override def readString: String = {
    require(1)
    val b = buffer(position)
    if ((b & 0x80) == 0) {
      // ASCII.
      position += 1
      readAscii
    } else {
      // fallback to super's version (position not incremented)
      super.readString
    }
  }

  override def readStringBuilder: java.lang.StringBuilder = {
    require(1)
    val b = buffer(position)
    if ((b & 0x80) == 0) {
      // ASCII.
      position += 1
      new java.lang.StringBuilder(readAscii)
    } else {
      // fallback to super's version (position not incremented)
      super.readStringBuilder
    }
  }

  private def readAscii: String = {
    val buffer = this.buffer
    var end = position
    val start = end - 1
    val limit = this.limit
    var b = 0
    do {
      if (end == limit) return readAscii_slow
      b = buffer(end)
      end += 1
    } while ((b & 0x80) == 0)
    val nbytes = end - start
    val bytes = new Array[Byte](nbytes)
    System.arraycopy(buffer, start, bytes, 0, nbytes)
    // Mask end of ascii bit.
    bytes(nbytes - 1) = (bytes(nbytes - 1) & 0x7F).toByte
    position = end
    // noinspection ScalaDeprecation
    new String(bytes, 0, 0, nbytes)
  }

  private def readAscii_slow: String = {
    // Re-read the first byte.
    position -= 1
    // Copy chars currently in buffer.
    var charCount = limit - position
    if (charCount > this.chars.length) {
      this.chars = new Array[Char](charCount * 2)
    }
    var chars = this.chars
    val buffer = this.buffer
    var i = position
    var ii = 0
    val n = limit
    while (i < n) {
      chars(ii) = buffer(i).toChar
      i += 1
      ii += 1
    }
    position = limit
    // Copy additional chars one by one.
    var readNextByte = true
    while (readNextByte) {
      require(1)
      val b = buffer(position)
      position += 1
      if (charCount == chars.length) {
        val newChars = new Array[Char](charCount * 2)
        System.arraycopy(chars, 0, newChars, 0, charCount)
        chars = newChars
        this.chars = newChars
      }
      if ((b & 0x80) != 0x80) {
        chars(charCount) = b.toChar
        charCount += 1
      } else {
        chars(charCount) = (b & 0x7F).toChar
        charCount += 1
        readNextByte = false
      }
    }
    new String(chars, 0, charCount)
  }
}
