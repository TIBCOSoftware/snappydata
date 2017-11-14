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

import org.apache.spark.sql.types._

/**
 * Decodes a column of a batch that has seen some updates by combining all
 * delta values, and full column value obtained from [[ColumnDeltaEncoder]]s
 * and column encoders. Callers should provide this with the set of all deltas
 * for the column apart from the full column value.
 *
 * To create an instance, use the companion class apply method which will create
 * a nullable or non-nullable version as appropriate.
 */
final class UpdatedColumnDecoder(decoder: ColumnDecoder, field: StructField,
    delta1Position: Long, delta1: ColumnDeltaDecoder,
    delta2Position: Long, delta2: ColumnDeltaDecoder,
    delta3Position: Long, delta3: ColumnDeltaDecoder)
    extends UpdatedColumnDecoderBase(decoder, field,
      delta1Position, delta1, delta2Position, delta2, delta3Position, delta3) {

  protected def nullable: Boolean = false

  def readNotNull: Boolean = true
}

/**
 * Nullable version of [[UpdatedColumnDecoder]].
 */
final class UpdatedColumnDecoderNullable(decoder: ColumnDecoder, field: StructField,
    delta1Position: Long, delta1: ColumnDeltaDecoder,
    delta2Position: Long, delta2: ColumnDeltaDecoder,
    delta3Position: Long, delta3: ColumnDeltaDecoder)
    extends UpdatedColumnDecoderBase(decoder, field,
      delta1Position, delta1, delta2Position, delta2, delta3Position, delta3) {

  protected def nullable: Boolean = true

  def readNotNull: Boolean = currentDeltaBuffer.readNotNull
}

object UpdatedColumnDecoder {
  def apply(decoder: ColumnDecoder, field: StructField,
      delta1Buffer: ByteBuffer, delta2Buffer: ByteBuffer,
      delta3Buffer: ByteBuffer): UpdatedColumnDecoderBase = {

    // positions are initialized at max so that they always are greater
    // than a valid index

    var delta1Position = Long.MaxValue
    val delta1 = if (delta1Buffer ne null) {
      val d = new ColumnDeltaDecoder(delta1Buffer, field)
      delta1Position = d.moveToNextPosition()
      d
    } else null

    var delta2Position = Long.MaxValue
    val delta2 = if (delta2Buffer ne null) {
      val d = new ColumnDeltaDecoder(delta2Buffer, field)
      delta2Position = d.moveToNextPosition()
      d
    } else null

    var delta3Position = Long.MaxValue
    val delta3 = if (delta3Buffer ne null) {
      val d = new ColumnDeltaDecoder(delta3Buffer, field)
      delta3Position = d.moveToNextPosition()
      d
    } else null

    // check if any of the deltas or full value have nulls
    if (field.nullable && (decoder.hasNulls || ((delta1 ne null) && delta1.hasNulls) ||
        ((delta2 ne null) && delta2.hasNulls) || ((delta3 ne null) && delta3.hasNulls))) {
      new UpdatedColumnDecoderNullable(decoder, field, delta1Position, delta1,
        delta2Position, delta2, delta3Position, delta3)
    } else {
      new UpdatedColumnDecoder(decoder, field, delta1Position, delta1,
        delta2Position, delta2, delta3Position, delta3)
    }
  }
}

abstract class UpdatedColumnDecoderBase(decoder: ColumnDecoder, field: StructField,
    private final var delta1Position: Long, delta1: ColumnDeltaDecoder,
    private final var delta2Position: Long, delta2: ColumnDeltaDecoder,
    private final var delta3Position: Long, delta3: ColumnDeltaDecoder) {

  protected def nullable: Boolean

  protected final var nextDeltaBuffer: ColumnDeltaDecoder = _
  protected final var currentDeltaBuffer: ColumnDeltaDecoder = _
  protected final var nextUpdatedPosition: Long = moveToNextUpdatedPosition(-1, false)

  final def getCurrentDeltaBuffer: ColumnDeltaDecoder = currentDeltaBuffer

  @inline protected final def skipUpdatedPosition(delta: ColumnDeltaDecoder): Unit = {
    if (!nullable || delta.readNotNull) delta.nextNonNullOrdinal()
  }

  protected final def moveToNextUpdatedPosition(ordinal: Int, isCaseOfUpdate: Boolean): Long = {
    if (isCaseOfUpdate) {
      moveToNextUpdatedPosition(ordinal)
    } else {
      var next0 = Long.MaxValue
      var next1 = Long.MaxValue
      var next2 = Long.MaxValue
      var movedIndex = -1

      if (delta1Position < next0) {
        next0 = delta1Position
        movedIndex = 0
      }
      if (delta2Position < next1 && (nextUpdatedPosition >> 32).toInt == ordinal) {
        next1 = delta2Position
        movedIndex = 1
      }
      if (delta3Position < next2 && (nextUpdatedPosition >> 32).toInt == ordinal) {
        next2 = delta3Position
        movedIndex = 2
      }

      movedIndex match {
        case 0 =>
          delta1Position = delta1.moveToNextPosition()
          nextDeltaBuffer = delta1
          next0
        case 1 =>
          delta2Position = delta2.moveToNextPosition()
          nextDeltaBuffer = delta2
          next1
        case 2 =>
          delta3Position = delta3.moveToNextPosition()
          nextDeltaBuffer = delta3
          next2
        case _ => -1
      }
    }
  }

  private final def moveToNextUpdatedPosition(ordinal: Int): Long = {
    var next = Long.MaxValue
    var movedIndex = -1

    // first delta is the lowest in hierarchy and overrides others
    if (delta1Position < next) {
      next = delta1Position
      movedIndex = 0
    }
    // next delta in hierarchy
    if (delta2Position < next) {
      // skip on equality (result should be returned by one of the previous calls)
      if (delta2Position <= ordinal) {
        skipUpdatedPosition(delta2)
        delta2Position = delta2.moveToNextPosition()
        if (delta2Position < next) {
          next = delta2Position
          movedIndex = 1
        }
      } else {
        next = delta2Position
        movedIndex = 1
      }
    }
    // last delta in hierarchy
    if (delta3Position < next) {
      // skip on equality (result should be returned by one of the previous calls)
      if (delta3Position <= ordinal) {
        skipUpdatedPosition(delta3)
        delta3Position = delta3.moveToNextPosition()
        if (delta3Position < next) {
          next = delta3Position
          movedIndex = 2
        }
      } else {
        next = delta3Position
        movedIndex = 2
      }
    }
    movedIndex match {
      case 0 =>
        delta1Position = delta1.moveToNextPosition()
        nextDeltaBuffer = delta1
      case 1 =>
        delta2Position = delta2.moveToNextPosition()
        nextDeltaBuffer = delta2
      case 2 =>
        delta3Position = delta3.moveToNextPosition()
        nextDeltaBuffer = delta3
      case _ =>
    }
    next
  }

  private def skipUntil(ordinal: Int, isCaseOfUpdate: Boolean): Boolean = {
    var nextUpdated = nextUpdatedPosition
    // check if ordinal has moved ahead of updated cursor
    if (nextUpdated < ordinal) {
      do {
        // skip the position in current delta
        skipUpdatedPosition(nextDeltaBuffer)
        // update the cursor and keep on till ordinal is not reached
        nextUpdated = moveToNextUpdatedPosition(nextUpdated.toInt, isCaseOfUpdate)
      } while (nextUpdated < ordinal)
      nextUpdatedPosition = nextUpdated
      if (nextUpdated > ordinal) return true
    }
    currentDeltaBuffer = nextDeltaBuffer
    nextUpdatedPosition = moveToNextUpdatedPosition(ordinal, isCaseOfUpdate)
    false
  }

  final def unchanged(ordinal: Int, isCaseOfUpdate: Boolean): Boolean = {
    if (isCaseOfUpdate) {
      // Original
      if (nextUpdatedPosition > ordinal) true
      else skipUntil(ordinal, isCaseOfUpdate)
    } else {
      if ((nextUpdatedPosition >> 32).toInt == ordinal) {
        currentDeltaBuffer = nextDeltaBuffer
        nextUpdatedPosition = moveToNextUpdatedPosition(ordinal, isCaseOfUpdate)
        false
      } else true
    }
  }

  def readNotNull: Boolean

  // TODO: SW: need to create a combined delta+full value dictionary for this to work

  final def getStringDictionary: StringDictionary = null

  final def readDictionaryIndex: Int = -1
}
