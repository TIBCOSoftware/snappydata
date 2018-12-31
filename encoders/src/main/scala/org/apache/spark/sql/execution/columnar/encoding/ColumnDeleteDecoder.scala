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

/**
 * Decodes the deleted positions of a batch that has seen some deletes.
 */
final class ColumnDeleteDecoder(deleteBuffer: ByteBuffer) {

  private var nextDeletePosition: Int = _
  private var deleteCursor: Long = _
  private var deleteEndCursor: Long = _
  private val deleteBytes = if (deleteBuffer ne null) {
    val allocator = ColumnEncoding.getAllocator(deleteBuffer)
    deleteCursor = allocator.baseOffset(deleteBuffer) + deleteBuffer.position()
    deleteEndCursor = deleteCursor + deleteBuffer.remaining()
    // skip 12 bytes header
    deleteCursor += 12
    val bytes = allocator.baseObject(deleteBuffer)
    nextDeletePosition = moveToNextDeletedPosition(bytes)
    bytes
  } else null

  private def moveToNextDeletedPosition(deleteBytes: AnyRef): Int = {
    val cursor = deleteCursor
    if (cursor < deleteEndCursor) {
      deleteCursor += 4
      ColumnEncoding.readInt(deleteBytes, cursor)
    } else Int.MaxValue
  }

  def deleted(ordinal: Int): Boolean = {
    if (nextDeletePosition != ordinal) false
    else {
      nextDeletePosition = moveToNextDeletedPosition(deleteBytes)
      true
    }
  }
}