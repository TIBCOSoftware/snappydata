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

package org.apache.spark.sql.store

import io.snappydata.Constant

import org.apache.spark.sql.collection.Utils

/**
 * Compression schemes supported by snappy-store.
 */
object CompressionCodecId extends Enumeration {
  type Type = Value

  val LZ4_ID = 1
  val SNAPPY_ID = 2

  // keep below updated with the max ID above
  private val MAX_ID = SNAPPY_ID

  val None: Type = Value(0, "None")
  val LZ4: Type = Value(LZ4_ID, "LZ4")
  val Snappy: Type = Value(SNAPPY_ID, "Snappy")

  /** the [[CompressionCodecId]] of default compression scheme ([[Constant.DEFAULT_CODEC]]) */
  val DEFAULT: CompressionCodecId.Type = CompressionCodecId.fromName(Constant.DEFAULT_CODEC)

  /**
   * The case of codec > MAX_ID should ideally be error but due to backward compatibility
   * the stats row does not have any header to determine compression or not so can fail
   * in rare cases if first integer is a negative value. However it should never be match
   * with the IDs here because negative of codecId which is written are -1, -2, -3 resolve
   * to 0xfffffff... which should never happen since nullCount fields are non-nullable
   * (for not updated columns we keep -1 in null count)
   * in the UnsafeRow created, so bitset cannot have 'ff' kind of patterns.
   */
  def isCompressed(codec: Int): Boolean = codec > 0 && codec <= MAX_ID

  def fromName(name: String): CompressionCodecId.Type =
    if (name eq null) DEFAULT
    else Utils.toLowerCase(name) match {
      case "lz4" => LZ4
      case "snappy" => Snappy
      case "none" | "uncompressed" => None
      case _ => throw new IllegalArgumentException(
        s"Unknown compression scheme '$name'")
    }
}
