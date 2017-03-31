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
package org.apache.spark.sql.execution.columnar.encoding

import java.nio.ByteBuffer

import com.gemstone.gemfire.internal.shared.unsafe.UnsafeHolder

import org.apache.spark.unsafe.Platform

/**
 * Allocate, release and expand ByteBuffers (in-place if possible).
 */
trait ColumnAllocator {

  /** Allocate a new ByteBuffer of given size. */
  def allocate(size: Int): ByteBuffer

  /** Clears the memory to be zeros immediately after allocation. */
  def clearPostAllocate(buffer: ByteBuffer): Unit

  /** Get the base object of the ByteBuffer for raw reads by Unsafe API. */
  def baseObject(buffer: ByteBuffer): AnyRef

  /** Get the base offset of the ByteBuffer for raw reads by Unsafe API. */
  def baseOffset(buffer: ByteBuffer): Long

  /**
   * Expand given ByteBuffer to new capacity.
   *
   * @return the new expanded ByteBuffer
   */
  def expand(columnData: ByteBuffer, cursor: Long, startPosition: Long,
      required: Int): ByteBuffer

  /**
   * For direct ByteBuffers the release method is preferred to eagerly release
   * the memory instead of depending on heap GC which can be delayed.
   */
  def release(b: ByteBuffer): Unit

  /**
   * Indicates if this allocator will produce direct ByteBuffers.
   */
  def isDirect: Boolean

  protected def expandedSize(currentUsed: Int, required: Int): Int = {
    val minRequired = currentUsed.toLong + required
    // double the size
    val newLength = math.min(math.max((currentUsed * 3) >>> 1L, minRequired),
      Int.MaxValue - 1).toInt
    if (newLength < minRequired) {
      throw new IndexOutOfBoundsException(
        s"Cannot allocate more than $newLength bytes but required $minRequired")
    }
    newLength
  }
}

object HeapBufferAllocator extends ColumnAllocator {

  override def allocate(size: Int): ByteBuffer = ByteBuffer.allocate(size)

  override def clearPostAllocate(buffer: ByteBuffer): Unit = {}

  override def baseObject(buffer: ByteBuffer): AnyRef = buffer.array()

  override def baseOffset(buffer: ByteBuffer): Long =
    Platform.BYTE_ARRAY_OFFSET + buffer.arrayOffset()

  override def expand(columnData: ByteBuffer, cursor: Long,
      startPosition: Long, required: Int): ByteBuffer = {
    val columnBytes = columnData.array()
    val currentUsed = ColumnEncoding.checkBufferSize(cursor - startPosition)
    val newLength = expandedSize(currentUsed, required)
    val newBytes = new Array[Byte](newLength)
    System.arraycopy(columnBytes, 0, newBytes, 0, currentUsed)
    ByteBuffer.wrap(newBytes)
  }

  override def release(columnData: ByteBuffer): Unit = {}

  override def isDirect: Boolean = false
}

/**
 * Allocate and free direct ByteBuffers without default limitations imposed by
 * JVM on ByteBuffer.allocateDirect. Also release immediately rather than
 * waiting for it to be cleared via PhantomReference after a GC.
 */
object DirectBufferAllocator extends ColumnAllocator {

  override def allocate(size: Int): ByteBuffer =
    UnsafeHolder.allocateDirectBuffer(size)

  override def clearPostAllocate(buffer: ByteBuffer): Unit = {
    UnsafeHolder.getUnsafe.setMemory(null, baseOffset(buffer),
      // use capacity which will be a factor of 8 where setMemory
      // will be more efficient
      buffer.capacity(), 0)
  }

  override def baseObject(buffer: ByteBuffer): AnyRef = null

  override def baseOffset(buffer: ByteBuffer): Long =
    UnsafeHolder.getDirectBufferAddress(buffer)

  override def expand(columnData: ByteBuffer, cursor: Long,
      startPosition: Long, required: Int): ByteBuffer = {
    val currentUsed = ColumnEncoding.checkBufferSize(cursor - startPosition)
    val newLength = expandedSize(currentUsed, required)
    UnsafeHolder.reallocateDirectBuffer(columnData, newLength)
  }

  override def release(buffer: ByteBuffer): Unit =
    UnsafeHolder.releaseDirectBuffer(buffer)

  override def isDirect: Boolean = true
}
