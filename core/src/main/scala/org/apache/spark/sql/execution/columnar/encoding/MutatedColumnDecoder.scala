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

import org.apache.spark.sql.types.StructField

/**
 * Decodes a column of a batch that has seen some changes (updates or deletes) by
 * combining all delta values, delete bitmask and full column value obtained from
 * [[ColumnDeltaEncoder]]s and column encoders. Callers should
 * provide this with the set of all deltas and delete bitmask for the column
 * apart from the full column value, then use it like a normal decoder.
 *
 * Only the first column being decoded should set the "deleteBuffer", if present,
 * and generated code should check the return value of "next*" methods to continue
 * if it is negative.
 *
 * To create an instance, use the companion class apply method which will create
 * a nullable or non-nullable version as appropriate.
 */
final class MutatedColumnDecoder(decoder: ColumnDecoder,
    delta1Position: Int, delta1: ColumnDeltaDecoder,
    delta2Position: Int, delta2: ColumnDeltaDecoder,
    delta3Position: Int, delta3: ColumnDeltaDecoder, deleteBuffer: ByteBuffer)
    extends MutatedColumnDecoderBase(decoder, delta1Position, delta1,
      delta2Position, delta2, delta3Position, delta3, deleteBuffer) {

  def isNull: Int = 0
}

/**
 * Nullable version of [[MutatedColumnDecoder]].
 */
final class MutatedColumnDecoderNullable(decoder: ColumnDecoder,
    delta1Position: Int, delta1: ColumnDeltaDecoder,
    delta2Position: Int, delta2: ColumnDeltaDecoder,
    delta3Position: Int, delta3: ColumnDeltaDecoder, deleteBuffer: ByteBuffer)
    extends MutatedColumnDecoderBase(decoder, delta1Position, delta1,
      delta2Position, delta2, delta3Position, delta3, deleteBuffer) {

  def isNull: Int = currentDeltaBuffer.isNull
}

object MutatedColumnDecoder {
  def apply(decoder: ColumnDecoder, field: StructField,
      delta1Buffer: ByteBuffer, delta2Buffer: ByteBuffer, delta3Buffer: ByteBuffer,
      deleteBuffer: ByteBuffer): MutatedColumnDecoderBase = {

    // positions are initialized at max so that they always are greater
    // than a valid index

    var delta1Position = Int.MaxValue
    val delta1 = if (delta1Buffer ne null) {
      val d = new ColumnDeltaDecoder(ColumnEncoding.getColumnDecoder(delta1Buffer, field))
      delta1Position = d.initialize(delta1Buffer, field).toInt
      d
    } else null

    var delta2Position = Int.MaxValue
    val delta2 = if (delta2Buffer ne null) {
      val d = new ColumnDeltaDecoder(ColumnEncoding.getColumnDecoder(delta2Buffer, field))
      delta2Position = d.initialize(delta2Buffer, field).toInt
      d
    } else null

    var delta3Position = Int.MaxValue
    val delta3 = if (delta3Buffer ne null) {
      val d = new ColumnDeltaDecoder(ColumnEncoding.getColumnDecoder(delta3Buffer, field))
      delta3Position = d.initialize(delta3Buffer, field).toInt
      d
    } else null

    // check if any of the deltas or full value have nulls
    if (field.nullable && (decoder.hasNulls || ((delta1 ne null) && delta1.hasNulls) ||
        ((delta2 ne null) && delta2.hasNulls) || ((delta3 ne null) && delta3.hasNulls))) {
      new MutatedColumnDecoderNullable(decoder, delta1Position, delta1,
        delta2Position, delta2, delta3Position, delta3, deleteBuffer)
    } else {
      new MutatedColumnDecoder(decoder, delta1Position, delta1,
        delta2Position, delta2, delta3Position, delta3, deleteBuffer)
    }
  }
}

abstract class MutatedColumnDecoderBase(decoder: ColumnDecoder,
    private final var delta1Position: Int, delta1: ColumnDeltaDecoder,
    private final var delta2Position: Int, delta2: ColumnDeltaDecoder,
    private final var delta3Position: Int, delta3: ColumnDeltaDecoder,
    deleteBuffer: ByteBuffer) {

  private final var deletePosition = Int.MaxValue
  private final var deleteCursor: Long = _
  private final var deleteEndCursor: Long = _
  private final val deleteBytes = if (deleteBuffer ne null) {
    val allocator = ColumnEncoding.getAllocator(deleteBuffer)
    deleteCursor = allocator.baseOffset(deleteBuffer) + deleteBuffer.position()
    deleteEndCursor = deleteCursor + deleteBuffer.limit()
    // skip 4 bytes header which is unused as of now
    deleteCursor += 4
    deletePosition = nextDeletedPosition()
    allocator.baseObject(deleteBuffer)
  } else null

  protected final var nextMutatedPosition: Int = -1
  protected final var nextDeltaBuffer: ColumnDeltaDecoder = _
  protected final var currentDeltaBuffer: ColumnDeltaDecoder = _

  final def getCurrentDeltaBuffer: ColumnDeltaDecoder = currentDeltaBuffer

  def initialize(): Unit = {
    updateNextMutatedPosition()
  }

  private def nextDeletedPosition(): Int = {
    val cursor = deleteCursor
    if (cursor < deleteEndCursor) {
      deleteCursor += 4
      ColumnEncoding.readInt(deleteBytes, cursor)
    } else Int.MaxValue
  }

  protected final def updateNextMutatedPosition(): Unit = {
    var next = Int.MaxValue
    var movedIndex = -1

    // check deleted first which overrides everyone else
    if (deletePosition < next) {
      next = deletePosition
      movedIndex = 0
    }
    // first delta is the lowest in hierarchy and overrides others
    if (delta1Position < next) {
      next = delta1Position
      movedIndex = 1
    }
    // next delta in hierarchy
    if (delta2Position < next) {
      next = delta2Position
      movedIndex = 2
    }
    // last delta in hierarchy
    if (delta3Position < next) {
      next = delta3Position
      movedIndex = 3
    }
    movedIndex match {
      case 0 =>
        nextDeletedPosition()
        nextDeltaBuffer = null
      case 1 =>
        delta1Position = delta1.moveToNextPosition()
        nextDeltaBuffer = delta1
      case 2 =>
        delta2Position = delta2.moveToNextPosition()
        nextDeltaBuffer = delta2
      case 3 =>
        delta3Position = delta3.moveToNextPosition()
        nextDeltaBuffer = delta3
      case _ =>
    }
    nextMutatedPosition = next
  }

  final def mutated(ordinal: Int): Int = {
    if (nextMutatedPosition != ordinal) 0
    else {
      currentDeltaBuffer = nextDeltaBuffer
      updateNextMutatedPosition()
      // value of -1 indicates deleted to the caller
      if (currentDeltaBuffer ne null) 1 else -1
    }
  }

  def isNull: Int
}
