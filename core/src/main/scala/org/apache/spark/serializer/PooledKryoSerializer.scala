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
package org.apache.spark.serializer

import java.io.{EOFException, InputStream}
import java.lang.ref.SoftReference
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import com.esotericsoftware.kryo.io.Input
import com.esotericsoftware.kryo.pool.KryoFactory
import com.esotericsoftware.kryo.serializers.DefaultSerializers.KryoSerializableSerializer
import com.esotericsoftware.kryo.serializers.ExternalizableSerializer
import com.esotericsoftware.kryo.{Kryo, KryoException}

import org.apache.spark.broadcast.TorrentBroadcast
import org.apache.spark.executor.{BlockStatusesAccumulator, InputMetrics, OutputMetrics, ShuffleReadMetrics, ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.scheduler.{DirectTaskResult, IndirectTaskResult, ResultTask, ShuffleMapTask, Task, TaskDescription}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.collection.NarrowExecutorLocalSplitDep
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.{BlockManagerId, StorageLevel}
import org.apache.spark.unsafe.types.UTF8String
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
    val kryo = super.newKryo()

    // specific serialization implementations in Spark and commonly used classes
    kryo.register(classOf[UnsafeRow], new KryoSerializableSerializer)
    kryo.register(classOf[UTF8String], new KryoSerializableSerializer)
    kryo.register(classOf[DirectTaskResult[_]], new ExternalizableSerializer)
    kryo.register(classOf[IndirectTaskResult[_]])
    kryo.register(classOf[TorrentBroadcast[_]])
    kryo.register(classOf[TaskDescription])

    // other classes having specific implementations in snappy-spark but for
    // spark compatibility, the serializer is not mentioned explicitly
    kryo.register(classOf[Task[_]])
    kryo.register(classOf[ResultTask[_, _]])
    kryo.register(classOf[ShuffleMapTask])
    kryo.register(classOf[SerializableBuffer])
    kryo.register(Utils.classForName(
      "org.apache.spark.rpc.netty.NettyRpcEndpointRef"))
    kryo.register(classOf[TaskMetrics])
    kryo.register(classOf[InputMetrics])
    kryo.register(classOf[OutputMetrics])
    kryo.register(classOf[ShuffleReadMetrics])
    kryo.register(classOf[ShuffleWriteMetrics])
    kryo.register(classOf[BlockStatusesAccumulator])
    kryo.register(classOf[LongAccumulator])
    kryo.register(classOf[DoubleAccumulator])
    kryo.register(classOf[CollectionAccumulator[_]])
    kryo.register(classOf[SQLMetric])

    kryo.register(classOf[BlockManagerId], new ExternalizableResolverSerializer(
      BlockManagerId.getCachedBlockManagerId))
    kryo.register(classOf[StorageLevel], new ExternalizableResolverSerializer(
      StorageLevel.getCachedStorageLevel))
    kryo.register(classOf[StructType], new StructTypeSerializer)
    kryo.register(classOf[NarrowExecutorLocalSplitDep],
      new KryoSerializableSerializer)

    kryo
  }

  override def newInstance(): SerializerInstance = {
    new PooledKryoSerializerInstance(this)
  }
}

// TODO: SW: pool must be per SparkContext or SnappyConf
private[spark] object KryoSerializerPool {

  private val serializer: PooledKryoSerializer = {
    val sparkConf = Option(SparkEnv.get).map(_.conf).getOrElse(new SparkConf())
    new PooledKryoSerializer(sparkConf)
  }

  private val factory = new KryoFactory() {
    def create(): Kryo = {
      serializer.newKryo()
    }
  }

  // not using KryoPool.Builder to use a deque for inserting at head
  private[this] val pool = new java.util.ArrayDeque[SoftReference[Kryo]]()

  def borrow(): Kryo = {
    var ref: SoftReference[Kryo] = null
    pool.synchronized {
      ref = pool.pollFirst()
    }
    while (ref != null) {
      val kryo = ref.get()
      if (kryo != null) {
        return kryo
      }
      pool.synchronized {
        ref = pool.pollFirst()
      }
    }
    factory.create()
  }

  def release(kryo: Kryo): Unit = {
    // Call reset() to clear any Kryo state that might have been modified
    // by the last operation to borrow this instance (SPARK-7766).
    kryo.reset()
    val ref = new SoftReference[Kryo](kryo)
    pool.synchronized {
      pool.addFirst(ref)
    }
  }

  def clear(): Unit = {
    pool.synchronized {
      pool.clear()
    }
  }

  /**
   * Search for the private readResolve() method which must be present
   * to be passed to [[ExternalizableResolverSerializer]].
   */
  def getPrivateReadResolve[T](c: Class[T]): T => T = { (obj: T) =>
    val m = c.getDeclaredMethod("readResolve")
    m.setAccessible(true)
    m.invoke(obj).asInstanceOf[T]
  }
}

private[spark] final class PooledKryoSerializerInstance(
    pooledSerializer: PooledKryoSerializer)
    extends KryoSerializerInstance(pooledSerializer) with Logging {

  private lazy val stringFixInput = new KryoInputStringFix(0)

  // get rid of the cachedKryo of super class (unfortunate overhead)
  KryoSerializerPool.release(super.borrowKryo())

  override private[serializer] def borrowKryo(): Kryo =
    KryoSerializerPool.borrow()

  override private[serializer] def releaseKryo(kryo: Kryo): Unit =
    KryoSerializerPool.release(kryo)

  private def readByteBufferAsInput(bb: ByteBuffer, input: Input): Unit = {
    if (bb.hasArray) {
      input.setBuffer(bb.array(),
        bb.arrayOffset() + bb.position(), bb.remaining())
    } else {
      val numBytes = bb.remaining()
      val bytes = new Array[Byte](numBytes)
      bb.get(bytes, 0, numBytes)
      input.setBuffer(bytes, 0, numBytes)
    }
  }

  /*
  override def serialize[T: ClassTag](t: T): ByteBuffer = {
    var start = System.nanoTime()
    val buffer = super.serialize(t)
    var end = System.nanoTime()
    if (t != null) {
      logInfo(s"SW: time taken to serialize ${t.getClass.getName} " +
          s"object = ${(end - start) / 1000000.0}ms size = ${buffer.remaining()}")
    }
    t match {
      case (p1, p2) =>
        start = System.nanoTime()
        val r1 = super.serialize(p1)
        end = System.nanoTime()
        val p1Str = p1 match {
          case rdd: RDD[_] => rdd.toDebugString
          case _ => p1
        }
        logInfo(s"SW:T1: time taken to serialize [$p1Str] " +
            s"= ${(end - start) / 1000000.0}ms size = ${r1.remaining()}")

        start = System.nanoTime()
        val r2 = super.serialize(p2)
        end = System.nanoTime()
        logInfo(s"SW:T2: time taken to serialize [$p2] " +
            s"= ${(end - start) / 1000000.0}ms size = ${r2.remaining()}")
      case _ =>
    }
    buffer
  }

  override def deserialize[T: ClassTag](buffer: ByteBuffer): T = {
    val start = System.nanoTime()
    var SW_t: T = null.asInstanceOf[T]
    val kryo = borrowKryo()
    try {
      readByteBufferAsInput(buffer, stringFixInput)
      SW_t = kryo.readClassAndObject(stringFixInput).asInstanceOf[T]
      SW_t
    } finally {
      releaseKryo(kryo)
      val end = System.nanoTime()
      if (SW_t != null) {
        logInfo(s"SW: time taken to deserialize ${SW_t.getClass.getName} " +
            s"object = ${(end - start) / 1000000.0}ms size = ${stringFixInput.limit()}")
      }
    }
  }

  override def deserialize[T: ClassTag](buffer: ByteBuffer,
      loader: ClassLoader): T = {
    val start = System.nanoTime()
    var SW_t: T = null.asInstanceOf[T]
    val kryo = borrowKryo()
    val oldClassLoader = kryo.getClassLoader
    try {
      kryo.setClassLoader(loader)
      readByteBufferAsInput(buffer, stringFixInput)
      SW_t = kryo.readClassAndObject(stringFixInput).asInstanceOf[T]
      SW_t
    } finally {
      kryo.setClassLoader(oldClassLoader)
      releaseKryo(kryo)
      val end = System.nanoTime()
      if (SW_t != null) {
        logInfo(s"SW:2: time taken to deserialize ${SW_t.getClass.getName} " +
            s"object = ${(end - start) / 1000000.0}ms size = ${stringFixInput.limit()}")
      }
    }
  }
  */

  override def deserialize[T: ClassTag](buffer: ByteBuffer): T = {
    val kryo = borrowKryo()
    try {
      readByteBufferAsInput(buffer, stringFixInput)
      kryo.readClassAndObject(stringFixInput).asInstanceOf[T]
    } finally {
      releaseKryo(kryo)
    }
  }

  override def deserialize[T: ClassTag](buffer: ByteBuffer,
      loader: ClassLoader): T = {
    val kryo = borrowKryo()
    val oldClassLoader = kryo.getClassLoader
    try {
      kryo.setClassLoader(loader)
      readByteBufferAsInput(buffer, stringFixInput)
      kryo.readClassAndObject(stringFixInput).asInstanceOf[T]
    } finally {
      kryo.setClassLoader(oldClassLoader)
      releaseKryo(kryo)
    }
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new KryoStringFixDeserializationStream(this, s)
  }
}

private[spark] class KryoStringFixDeserializationStream(
    serInstance: PooledKryoSerializerInstance,
    inStream: InputStream) extends DeserializationStream {

  private[this] var input: Input = new KryoInputStringFix(4096)
  input.setInputStream(inStream)
  private[this] var kryo: Kryo = serInstance.borrowKryo()

  override def readObject[T: ClassTag](): T = {
    try {
      kryo.readClassAndObject(input).asInstanceOf[T]
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
        serInstance.releaseKryo(kryo)
        kryo = null
        input = null
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
    position -= 1 // Re-read the first byte.
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
