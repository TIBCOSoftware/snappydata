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
    delta1: ColumnDeltaDecoder, delta2: ColumnDeltaDecoder)
    extends UpdatedColumnDecoderBase(decoder, field, delta1, delta2) {

  protected def nullable: Boolean = false

  def readNotNull: Boolean = true
}

/**
 * Nullable version of [[UpdatedColumnDecoder]].
 */
final class UpdatedColumnDecoderNullable(decoder: ColumnDecoder, field: StructField,
    delta1: ColumnDeltaDecoder, delta2: ColumnDeltaDecoder)
    extends UpdatedColumnDecoderBase(decoder, field, delta1, delta2) {

  protected def nullable: Boolean = true

  def readNotNull: Boolean = currentDeltaBuffer.readNotNull
}

object UpdatedColumnDecoder {
  def apply(decoder: ColumnDecoder, field: StructField,
      delta1Buffer: ByteBuffer, delta2Buffer: ByteBuffer): UpdatedColumnDecoderBase = {

    val delta1 = if (delta1Buffer ne null) new ColumnDeltaDecoder(delta1Buffer, field) else null
    val delta2 = if (delta2Buffer ne null) new ColumnDeltaDecoder(delta2Buffer, field) else null
    // check for nullable column (don't use actual number of nulls to avoid changing
    //   type of decoder that can cause trouble with JVM inlining)
    if (field.nullable) {
      new UpdatedColumnDecoderNullable(decoder, field, delta1, delta2)
    } else {
      new UpdatedColumnDecoder(decoder, field, delta1, delta2)
    }
  }
}

abstract class UpdatedColumnDecoderBase(decoder: ColumnDecoder, field: StructField,
    delta1: ColumnDeltaDecoder, delta2: ColumnDeltaDecoder) {

  protected def nullable: Boolean

  private final var delta1UpdatedPosition: Int =
    if (delta1 ne null) delta1.readUpdatedPosition() else Int.MaxValue
  private final var delta2UpdatedPosition: Int =
    if (delta2 ne null) delta2.readUpdatedPosition() else Int.MaxValue

  protected final var currentDeltaBuffer: ColumnDeltaDecoder = _
  protected final var nextUpdatedPosition: Int = moveToNextUpdatedPosition()

  final def getCurrentDeltaBuffer: ColumnDeltaDecoder = currentDeltaBuffer

  protected final def moveToNextUpdatedPosition(): Int = {
    var next = Int.MaxValue
    var firstDelta = false
    // first delta is the lowest in hierarchy and overrides others
    if (delta1UpdatedPosition != Int.MaxValue) {
      next = delta1UpdatedPosition
      firstDelta = true
    }
    // check next delta in hierarchy
    if (delta2 ne null) {
      if (delta2UpdatedPosition <= next) {
        if (delta2UpdatedPosition < next) {
          next = delta2UpdatedPosition
          currentDeltaBuffer = delta2
          firstDelta = false
        }
        // skip on equality in any case (result should be returned by delta1)
        delta2.moveUpdatePositionCursor()
        delta2UpdatedPosition = delta2.readUpdatedPosition()
      }
    }
    if (firstDelta) {
      currentDeltaBuffer = delta1
      delta1.moveUpdatePositionCursor()
      delta1UpdatedPosition = delta1.readUpdatedPosition()
    }
    next
  }

  private def skipUntil(ordinal: Int): Boolean = {
    while (true) {
      // update the cursor and keep on till ordinal is not reached
      nextUpdatedPosition = moveToNextUpdatedPosition()
      if (nextUpdatedPosition > ordinal) return true
      if (nextUpdatedPosition == ordinal) return false
    }
    false // never reached
  }

  final def unchanged(ordinal: Int): Boolean = {
    if (nextUpdatedPosition > ordinal) true
    else if (nextUpdatedPosition == ordinal) false
    else skipUntil(ordinal)
  }

  def readNotNull: Boolean

  // TODO: SW: need to create a combined delta+full value dictionary for this to work

  final def getStringDictionary: StringDictionary = null

  final def readDictionaryIndex: Int = -1

  def close(): Unit = {
    if (delta1 ne null) delta1.close()
    if (delta2 ne null) delta2.close()
  }
}
