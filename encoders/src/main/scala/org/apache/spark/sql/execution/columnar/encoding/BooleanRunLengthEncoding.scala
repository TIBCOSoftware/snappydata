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

/**
 * Run length encoding optimized for booleans. Each short run-length value
 * represents the run-length with the value. Even numbered run-lengths indicate
 * a run of false values having half the length, while odd numbered run-lengths
 * are for true values (having length = run / 2 + 1).
 */
trait BooleanRunLengthDecoder extends RunLengthDecoding {

  /**
   * Reads the boolean value at given position. Calls to this method should
   * be monotonically increasing and never decrease else result may be incorrect.
   */
  @inline final def readBooleanRLE(columnBytes: AnyRef, position: Int,
      nextPosition: Int): Boolean = {
    if (nextPosition < ColumnEncoding.ULIMIT_POSITION) {
      if (nextPosition > position) false
      else updateRunLength(columnBytes, position, nextPosition)
    } else {
      val n = nextPosition & ColumnEncoding.ULIMIT_POSITION
      if (n > position) true
      else updateRunLength(columnBytes, position, n)
    }
  }

  protected final def updateRunLength(columnBytes: AnyRef, position: Int,
      nextPosition: Int): Boolean = {
    var runOfOne = false
    var next = nextPosition
    while (true) {
      val run = readRunLength(columnBytes, next)
      if ((run & 0x1) == 0) {
        // a run of false values
        next += run >>> 1
        if (next > position) {
          // optimize for a run of one and move on to next run (which should be of true)
          if (run == 2 && !runOfOne) {
            runOfOne = true
          } else {
            if (next >= ColumnEncoding.ULIMIT_POSITION) {
              throw new IllegalStateException(
                s"nextPosition exceeded max allowed for position=$position run=$run")
            }
            this.nextPosition = next
            return runOfOne
          }
        }
      } else {
        // a run of true values
        next += (run >>> 1) + 1
        if (next > position) {
          // optimize for a run of one and move on to next run (which should be of false)
          if (run == 1 && !runOfOne) {
            runOfOne = true
          } else {
            if (next >= ColumnEncoding.ULIMIT_POSITION) {
              throw new IllegalStateException(
                s"nextPosition exceeded max allowed for position=$position run=$run")
            }
            this.nextPosition = next | (ColumnEncoding.ULIMIT_POSITION + 1)
            return !runOfOne
          }
        }
      }
    }
    false // should never be reached
  }
}

trait BooleanRunLengthEncoder {

  protected[this] final var startCursorRLE: Long = _
  protected[this] final var lastPosition: Int = -1
  protected[this] final var currentRun: Int = _
  protected[this] final var dataSize: Int = _

  protected final def currentRunSize: Int = if (currentRun > 0x7f) 2 else 1

  private def updateStatsCurrentRunOverflow(maxValue: Int): Unit = {
    if (currentRun >= maxValue) {
      val numRuns = currentRun / maxValue
      currentRun -= (numRuns * maxValue)
      dataSize += (numRuns << 1)
    }
  }

  protected final def updateBooleanStats(position: Int, value: Boolean): Unit = {
    if (position >= 0) {
      val skipped = position - lastPosition - 1
      if (skipped > 0) {
        // writes skipped, assume false values for skipped (e.g. when used for nulls)
        if ((currentRun & 0x1) == 0) {
          currentRun += skipped << 1
        } else {
          dataSize += currentRunSize
          currentRun = skipped << 1
        }
        updateStatsCurrentRunOverflow(Short.MaxValue - 1)
      }
      lastPosition = position
    }
    if (value) {
      if ((currentRun & 0x1) == 0) {
        // run changed from false to true
        dataSize += currentRunSize
        currentRun = 1
      } else {
        currentRun += 2
        updateStatsCurrentRunOverflow(Short.MaxValue)
      }
    } else {
      if ((currentRun & 0x1) == 0) {
        currentRun += 2
        updateStatsCurrentRunOverflow(Short.MaxValue - 1)
      } else {
        // run changed from true to false
        dataSize += currentRunSize
        currentRun = 2
      }
    }
  }

  protected final def encodedSizeRLE: Int = dataSize + currentRunSize

  protected final def initializeRLE(): Unit = {
    startCursorRLE = 0L
    lastPosition = -1
    currentRun = 0
    dataSize = 0
  }

  protected final def initializeRLE(columnBytes: AnyRef, cursor: Long): Unit = {
    startCursorRLE = cursor
    lastPosition = -1
    currentRun = 0
  }

  protected final def writeCurrentRun(columnBytes: AnyRef, cursor: Long): Long =
    ColumnEncoding.writeRunLength(columnBytes, cursor, currentRun)

  private def writeCurrentRunOverflow(columnBytes: AnyRef, cursor: Long, maxValue: Int): Long = {
    if (currentRun >= maxValue) {
      var writeCursor = cursor
      do {
        writeCursor = ColumnEncoding.writeRunLength(columnBytes, writeCursor, maxValue)
        currentRun -= maxValue
      } while (currentRun >= maxValue)
      writeCursor
    } else cursor
  }

  /**
   * Writes the boolean value at given position. If passed position is negative
   * then writes at the next position. Returns the updated cursor.
   */
  final def writeBooleanRLE(columnBytes: AnyRef, cursor: Long, position: Int,
      value: Boolean): Long = {
    var writeCursor = cursor
    if (position >= 0) {
      val skipped = position - lastPosition - 1
      if (skipped > 0) {
        // writes skipped, assume false values for skipped (e.g. when used for nulls)
        if ((currentRun & 0x1) == 0) {
          currentRun += skipped << 1
        } else {
          writeCursor = writeCurrentRun(columnBytes, writeCursor)
          currentRun = skipped << 1
        }
        writeCursor = writeCurrentRunOverflow(columnBytes, writeCursor, Short.MaxValue - 1)
      }
      lastPosition = position
    }
    if (value) {
      if ((currentRun & 0x1) == 0) {
        // run changed from false to true
        writeCursor = writeCurrentRun(columnBytes, writeCursor)
        currentRun = 1
        writeCursor
      } else {
        currentRun += 2
        writeCurrentRunOverflow(columnBytes, writeCursor, Short.MaxValue)
      }
    } else {
      if ((currentRun & 0x1) == 0) {
        currentRun += 2
        writeCurrentRunOverflow(columnBytes, writeCursor, Short.MaxValue - 1)
      } else {
        // run changed from true to false
        writeCursor = writeCurrentRun(columnBytes, writeCursor)
        currentRun = 2
        writeCursor
      }
    }
  }

  protected def writeSizeRLE(columnBytes: AnyRef, size: Int): Unit = {
    ColumnEncoding.writeInt(columnBytes, startCursorRLE, size)
  }

  final def finishRLE(columnBytes: AnyRef, cursor: Long): Long = {
    val endCursor = writeCurrentRun(columnBytes, cursor)
    val size = (endCursor - startCursorRLE - 4).toInt
    assert(size > 0)
    assert(size == encodedSizeRLE)
    writeSizeRLE(columnBytes, size)
    endCursor
  }
}
