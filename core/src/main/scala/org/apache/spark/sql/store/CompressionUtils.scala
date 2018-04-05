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

package org.apache.spark.sql.store

import java.nio.{ByteBuffer, ByteOrder}

import com.gemstone.gemfire.internal.shared.unsafe.UnsafeHolder
import com.gemstone.gemfire.internal.shared.{BufferAllocator, HeapBufferAllocator, SystemProperties}
import com.ning.compress.lzf.{LZFDecoder, LZFEncoder}
import io.snappydata.Constant
import net.jpountz.lz4.LZ4Factory
import org.xerial.snappy.Snappy

import org.apache.spark.io.{CompressionCodec, LZ4CompressionCodec, LZFCompressionCodec, SnappyCompressionCodec}
import org.apache.spark.memory.MemoryManagerCallback.allocateExecutionMemory

/**
 * Utility methods for compression/decompression.
 */
object CompressionUtils {

  def codecCompress(codec: CompressionCodec, input: Array[Byte],
      inputLen: Int): Array[Byte] = codec match {
    case _: LZ4CompressionCodec =>
      LZ4Factory.fastestInstance().fastCompressor().compress(input, 0, inputLen)
    case _: LZFCompressionCodec => LZFEncoder.encode(input, 0, inputLen)
    case _: SnappyCompressionCodec =>
      Snappy.rawCompress(input, inputLen)
  }

  private[sql] val COMPRESSION_OWNER = "COMPRESSOR"
  private[sql] val DECOMPRESSION_OWNER = "DECOMPRESSOR"
  private[this] val COMPRESSION_HEADER_SIZE = 8
  private[this] val MIN_COMPRESSION_RATIO = 0.75
  /** minimum size of buffer that will be considered for compression */
  private[sql] val MIN_COMPRESSION_SIZE =
    SystemProperties.getServerInstance.getInteger(Constant.COMPRESSION_MIN_SIZE, 2048)

  private def writeCompressionHeader(codecId: Int,
      uncompressedLen: Int, buffer: ByteBuffer): Unit = {
    // assume little-endian to match ColumnEncoding.writeInt/readInt
    assert(buffer.order() eq ByteOrder.LITTLE_ENDIAN)
    buffer.rewind()
    // write the codec and uncompressed size for fastest decompression
    buffer.putInt(0, -codecId) // negative typeId indicates compressed buffer
    buffer.putInt(4, uncompressedLen)
  }

  def codecCompress(codecId: Int, input: ByteBuffer, len: Int,
      allocator: BufferAllocator): ByteBuffer = {
    if (len < MIN_COMPRESSION_SIZE) return input

    var result: ByteBuffer = null
    val resultLen = codecId match {
      case CompressionCodecId.LZ4_ID =>
        val compressor = LZ4Factory.fastestInstance().fastCompressor()
        val maxLength = compressor.maxCompressedLength(len)
        val maxTotal = maxLength + COMPRESSION_HEADER_SIZE
        result = allocateExecutionMemory(maxTotal, COMPRESSION_OWNER, allocator)
        compressor.compress(input, input.position(), len,
          result, COMPRESSION_HEADER_SIZE, maxLength)
      case CompressionCodecId.SNAPPY_ID =>
        val maxTotal = Snappy.maxCompressedLength(len) + COMPRESSION_HEADER_SIZE
        result = allocateExecutionMemory(maxTotal, COMPRESSION_OWNER, allocator)
        if (input.isDirect) {
          result.position(COMPRESSION_HEADER_SIZE)
          Snappy.compress(input, result)
        } else {
          Snappy.compress(input.array(), input.arrayOffset() + input.position(),
            len, result.array(), COMPRESSION_HEADER_SIZE)
        }
    }
    // check if there was some decent reduction else return uncompressed input itself
    if (resultLen.toDouble <= len * MIN_COMPRESSION_RATIO) {
      // caller should trim the buffer (can skip if written to output stream right away)
      writeCompressionHeader(codecId, len, result)
      result.limit(resultLen + COMPRESSION_HEADER_SIZE)
      result
    } else {
      // release the compressed buffer if required
      UnsafeHolder.releaseIfDirectBuffer(result)
      input
    }
  }

  def codecDecompress(codec: CompressionCodec, input: Array[Byte],
      inputOffset: Int, inputLen: Int,
      outputLen: Int): Array[Byte] = codec match {
    case _: LZ4CompressionCodec =>
      LZ4Factory.fastestInstance().fastDecompressor().decompress(input,
        inputOffset, outputLen)
    case _: LZFCompressionCodec =>
      val output = new Array[Byte](outputLen)
      LZFDecoder.decode(input, inputOffset, inputLen, output)
      output
    case _: SnappyCompressionCodec =>
      val output = new Array[Byte](outputLen)
      Snappy.uncompress(input, inputOffset, inputLen, output, 0)
      output
  }

  /**
   * Decompress the given buffer if compressed else return the original.
   * Input buffer must be little-endian and so will be the result.
   */
  def codecDecompressIfRequired(input: ByteBuffer, allocator: BufferAllocator): ByteBuffer = {
    assert(input.order() eq ByteOrder.LITTLE_ENDIAN)
    val position = input.position()
    val codec = -input.getInt(position)
    if (CompressionCodecId.isCompressed(codec)) {
      // prefer heap for small output buffers
      val outputLen = input.getInt(position + 4)
      val useAllocator = if (outputLen <= MIN_COMPRESSION_SIZE && !allocator.isDirect) {
        HeapBufferAllocator.instance()
      } else allocator
      codecDecompress(input, outputLen, useAllocator, position, codec)
    } else input
  }

  private[sql] def codecDecompress(input: ByteBuffer,
      allocator: BufferAllocator, position: Int, codecId: Int): ByteBuffer = {
    val outputLen = input.getInt(position + 4)
    codecDecompress(input, outputLen, allocator, position, codecId)
  }

  private def codecDecompress(input: ByteBuffer, outputLen: Int,
      allocator: BufferAllocator, position: Int, codecId: Int): ByteBuffer = {
    var result: ByteBuffer = null
    codecId match {
      case CompressionCodecId.LZ4_ID =>
        result = allocateExecutionMemory(outputLen, DECOMPRESSION_OWNER, allocator)
        LZ4Factory.fastestInstance().fastDecompressor().decompress(input,
          position + 8, result, 0, outputLen)
      case CompressionCodecId.SNAPPY_ID =>
        result = allocateExecutionMemory(outputLen, DECOMPRESSION_OWNER, allocator)
        input.position(position + 8)
        if (input.isDirect) {
          Snappy.uncompress(input, result)
        } else {
          Snappy.uncompress(input.array(), input.arrayOffset() +
              input.position(), input.remaining(), result.array(), 0)
        }
    }
    result.rewind()
    result
  }
}
